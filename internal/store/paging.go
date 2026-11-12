package store

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/lidofinance/keys-api-versioned/internal/model"
)

// Paging is safe here, unlike in the canonical service, because a published version's rows
// never move.

// KeyCursor carries the version, so page two is of the same version without the consumer
// knowing pinning exists.
type KeyCursor struct {
	Block         int64
	ModuleID      int32
	OperatorIndex int32
	KeyIndex      int32
}

// String is readable rather than opaque, so a stuck consumer's position is visible.
func (c KeyCursor) String() string {
	return fmt.Sprintf("%d.%d.%d.%d", c.Block, c.ModuleID, c.OperatorIndex, c.KeyIndex)
}

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
		bits := 32
		if i == 0 {
			bits = 64
		}
		n, err := strconv.ParseInt(p, 10, bits)
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

type KeyPage struct {
	Limit int
	// After is exclusive: the walk resumes at the next key. Nil starts at the top.
	After *KeyCursor
}

type PagedSnapshot interface {
	// A nil cursor means the walk is finished.
	StreamKeysPage(ctx context.Context, f model.KeysFilter, page KeyPage, fn func(*model.Key) error) (*KeyCursor, error)
	PinBlock() int64
}

// The row comparison `(module_id, operator_index, key_index) > (…)` resumes without OFFSET, so
// page N costs the same as page 1.
const keysPageQuery = `select module_id, operator_index, key_index, pubkey, signature, used, vetted
	from key_version
	where from_block <= $1 and (to_block is null or to_block > $1)`

func (s *appSnapshot) StreamKeysPage(ctx context.Context, f model.KeysFilter, page KeyPage, fn func(*model.Key) error) (*KeyCursor, error) {
	if err := s.loadModules(ctx); err != nil {
		return nil, err
	}
	// A page emptied by a reorg would read as "walk finished": a silent short answer.
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

	// One row more than asked: whether it exists decides the cursor, with no count query.
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

func (s *appSnapshot) addressOfID(id int16) string {
	for i := range s.modules {
		if int16(s.modules[i].ID) == id {
			return s.modules[i].StakingModuleAddress
		}
	}
	return ""
}

var _ PagedSnapshot = (*appSnapshot)(nil)
