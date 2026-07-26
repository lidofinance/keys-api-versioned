package store

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/lidofinance/keys-api-versioned/internal/model"
)

// Pagination is possible here for the reason it is impossible in the canonical
// service: the rows of a published version never move. There, a page-two query
// would run against a table the registry job may have rewritten in between, so the
// second page could skip keys or repeat them — which is why the endpoint returns
// everything or nothing. Pinned to a version, a page is an ordinary range scan in
// key-identity order.

// KeyCursor points at one key, and at the version the walk is pinned to.
//
// The version travels in the cursor on purpose. A consumer that starts a walk and
// then asks for page two gets page two of the same version, even if three versions
// were published in between — without having to know that pinning exists.
type KeyCursor struct {
	Block         int64
	ModuleID      int32
	OperatorIndex int32
	KeyIndex      int32
}

// String renders the cursor for the wire. It is deliberately readable rather than
// opaque: an operator debugging a stuck consumer can tell where it is.
func (c KeyCursor) String() string {
	return fmt.Sprintf("%d.%d.%d.%d", c.Block, c.ModuleID, c.OperatorIndex, c.KeyIndex)
}

// ParseKeyCursor reads a cursor produced by String.
func ParseKeyCursor(s string) (KeyCursor, error) {
	parts := strings.Split(s, ".")
	if len(parts) != 4 {
		return KeyCursor{}, fmt.Errorf("cursor must be block.module.operator.index, got %q", s)
	}
	var (
		c      KeyCursor
		values [4]int64
	)
	for i, p := range parts {
		n, err := strconv.ParseInt(p, 10, 64)
		if err != nil {
			return KeyCursor{}, fmt.Errorf("cursor part %d is not a number: %q", i+1, p)
		}
		values[i] = n
	}
	c.Block = values[0]
	c.ModuleID = int32(values[1])
	c.OperatorIndex = int32(values[2])
	c.KeyIndex = int32(values[3])
	return c, nil
}

// KeyPage is one page request.
type KeyPage struct {
	// Limit is how many keys the page may contain.
	Limit int
	// After is exclusive: the walk resumes at the next key. Nil starts at the top.
	After *KeyCursor
}

// PagedSnapshot is a snapshot that can serve a page of keys in a stable order.
//
// It is an optional capability rather than part of Snapshot: the comparison
// backends in this repo hold only the current state, where paging would be the
// unsafe thing it is in the canonical service.
type PagedSnapshot interface {
	// StreamKeysPage calls fn for each key of the page, in (module, operator, index)
	// order, and returns the cursor to resume from. A nil cursor means the walk is
	// finished.
	StreamKeysPage(ctx context.Context, f model.KeysFilter, page KeyPage, fn func(*model.Key) error) (*KeyCursor, error)
	// PinBlock is the version the page came from, for the cursor to carry.
	PinBlock() int64
}

// keysPageQuery reads one page across every module in identity order.
//
// The row comparison is what makes the cursor exact: `(module_id, operator_index,
// key_index) > (…)` resumes at the next key without an offset, so page N costs the
// same as page 1. An OFFSET-based version of this in an earlier experiment here was
// O(offset) and made writes look slower than they were.
const keysPageQuery = `select module_id, operator_index, key_index, pubkey, signature, used, vetted
	from key_version
	where from_block <= $1 and (to_block is null or to_block > $1)`

func (s *appSnapshot) StreamKeysPage(ctx context.Context, f model.KeysFilter, page KeyPage, fn func(*model.Key) error) (*KeyCursor, error) {
	if err := s.loadModules(ctx); err != nil {
		return nil, err
	}
	// A page that comes back empty because a reorg deleted the version's rows would
	// read as "the walk is finished" — a silent short answer in the one place a
	// consumer is least likely to check.
	if err := s.stillOnChain(ctx); err != nil {
		return nil, err
	}
	if page.Limit <= 0 {
		return nil, fmt.Errorf("page limit must be positive")
	}

	q := keysPageQuery
	args := []any{s.block}

	if page.After != nil {
		args = append(args, page.After.ModuleID, page.After.OperatorIndex, page.After.KeyIndex)
		q += fmt.Sprintf(` and (module_id, operator_index, key_index) > ($%d, $%d, $%d)`,
			len(args)-2, len(args)-1, len(args))
	}
	if f.Used != nil {
		args = append(args, *f.Used)
		q += ` and used = $` + strconv.Itoa(len(args))
	}
	if f.OperatorIndex != nil {
		args = append(args, *f.OperatorIndex)
		q += ` and operator_index = $` + strconv.Itoa(len(args))
	}
	if len(f.ModuleAddress) > 0 {
		ids := make([]int16, 0, len(f.ModuleAddress))
		for _, address := range f.ModuleAddress {
			if id, ok := s.idByHex[strings.ToLower(address)]; ok {
				ids = append(ids, id)
			}
		}
		if len(ids) == 0 {
			return nil, nil
		}
		args = append(args, ids)
		q += ` and module_id = any($` + strconv.Itoa(len(args)) + `)`
	}

	// One row more than asked for: its existence is how the page knows whether to
	// hand back a cursor, without a second count query.
	args = append(args, page.Limit+1)
	q += ` order by module_id, operator_index, key_index limit $` + strconv.Itoa(len(args))

	rows, err := s.conn.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("keys page: %w", err)
	}
	defer rows.Close()

	var (
		sent    int
		last    KeyCursor
		hasMore bool
		k       model.Key
	)
	for rows.Next() {
		var moduleID int16
		if err := rows.Scan(&moduleID, &k.OperatorIndex, &k.Index, &k.PubkeyBin, &k.SignatureBin,
			&k.Used, &k.Vetted); err != nil {
			return nil, fmt.Errorf("scan key: %w", err)
		}
		if sent == page.Limit {
			hasMore = true
			break
		}

		k.ModuleAddress = s.addressOfID(moduleID)
		if err := fn(&k); err != nil {
			return nil, err
		}
		sent++
		last = KeyCursor{
			Block:         s.block,
			ModuleID:      int32(moduleID),
			OperatorIndex: k.OperatorIndex,
			KeyIndex:      k.Index,
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if !hasMore {
		return nil, nil
	}
	return &last, nil
}

// addressOfID returns the checksummed module address for a module id, from the
// module set already read for this snapshot.
func (s *appSnapshot) addressOfID(id int16) string {
	for i := range s.modules {
		if int16(s.modules[i].ID) == id {
			return s.modules[i].StakingModuleAddress
		}
	}
	return ""
}

var _ PagedSnapshot = (*appSnapshot)(nil)
