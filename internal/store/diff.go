package store

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/lidofinance/keys-api-versioned/internal/model"
)

// A diff between two published versions is the endpoint that lets a consumer stop
// pulling 211 MiB per poll. It is also nearly free to implement over block ranges:
// a row's range already says which versions it was alive for, so "what changed
// between a and b" is two predicates rather than a comparison of two dumps.
//
//	added   — rows that became visible in (a, b]: from_block > a, still alive at b
//	removed — rows that stopped being visible in (a, b]: alive at a, to_block <= b
//
// A key whose value changed appears in both, because that is what the storage did:
// the old row was closed and a new one opened. A consumer applies removals then
// additions by key identity and lands on the same state as a fresh dump. Rows both
// created and closed inside the interval appear in neither, which is correct: no
// consumer ever saw them.

// DiffKind says which side of the diff a key is on.
type DiffKind string

const (
	DiffRemoved DiffKind = "removed"
	DiffAdded   DiffKind = "added"
)

// DiffStore can answer what changed between two versions.
type DiffStore interface {
	// DiffKeys streams removals and then additions between two pins. It calls fn
	// with DiffRemoved for every key that stopped being visible and DiffAdded for
	// every key that became visible.
	DiffKeys(ctx context.Context, since, to string, moduleRef string, fn func(DiffKind, *model.Key) error) (*DiffMeta, error)
}

// DiffMeta describes the interval a diff covers.
type DiffMeta struct {
	From    *model.ELBlockSnapshot `json:"from"`
	To      *model.ELBlockSnapshot `json:"to"`
	Removed int64                  `json:"removed"`
	Added   int64                  `json:"added"`
}

// DiffKeys resolves both ends, then runs one query per side.
//
// Both ends have to be versions we still hold. If `since` fell out of the retention
// window the honest answer is 410 rather than a diff computed against a version
// whose closed rows have been deleted — that diff would silently miss removals.
func (a *App) DiffKeys(ctx context.Context, since, to string, moduleRef string, fn func(DiffKind, *model.Key) error) (*DiffMeta, error) {
	conn, err := a.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire: %w", err)
	}
	defer conn.Release()

	_, fromBlock, _, err := resolveAppPin(ctx, conn, since)
	if err != nil {
		return nil, err
	}
	if fromBlock == 0 {
		return nil, ErrNotFound{Ref: since}
	}

	toPin := to
	if toPin == "" {
		toPin = "latest"
	}
	head, toBlock, _, err := resolveAppPin(ctx, conn, toPin)
	if err != nil {
		return nil, err
	}
	if head == nil {
		return nil, ErrNotFound{Ref: toPin}
	}

	// The two ends are named by version identity, not by the head each version was
	// last confirmed at: a diff is between two versions, and the head moves.
	fromMeta, err := versionIdentity(ctx, conn, fromBlock)
	if err != nil {
		return nil, err
	}
	toMeta, err := versionIdentity(ctx, conn, toBlock)
	if err != nil {
		return nil, err
	}

	if fromBlock > toBlock {
		return nil, fmt.Errorf("since (%d) is newer than to (%d)", fromBlock, toBlock)
	}

	snap := &appSnapshot{conn: conn, meta: toMeta, block: toBlock}
	if err := snap.loadModules(ctx); err != nil {
		return nil, err
	}

	// A module filter keeps the answer small for the common case: a consumer that
	// noticed one module's nonce move and wants only that module's changes.
	var moduleID *int16
	if moduleRef != "" {
		module, err := snap.ModuleByRef(ctx, moduleRef)
		if err != nil {
			return nil, err
		}
		id := int16(module.ID)
		moduleID = &id
	}

	meta := &DiffMeta{From: fromMeta, To: toMeta}
	if fromBlock == toBlock {
		return meta, nil
	}

	// Removals first, so a consumer applying the stream in order never holds two
	// rows for one key identity.
	removed, err := snap.streamDiffSide(ctx, DiffRemoved, fromBlock, toBlock, moduleID, fn)
	if err != nil {
		return meta, err
	}
	meta.Removed = removed

	added, err := snap.streamDiffSide(ctx, DiffAdded, fromBlock, toBlock, moduleID, fn)
	if err != nil {
		return meta, err
	}
	meta.Added = added

	return meta, nil
}

// versionIdentity reads the block a version's data was read at, which is what names
// it in a diff and in a cursor.
func versionIdentity(ctx context.Context, conn pinReader, block int64) (*model.ELBlockSnapshot, error) {
	var m model.ELBlockSnapshot
	err := conn.QueryRow(ctx, `select block_number, block_hash, timestamp, last_changed_block_hash
		from versions where block_number = $1
		order by (status = '`+StatusPublished+`') desc, id desc limit 1`, block).
		Scan(&m.BlockNumber, &m.BlockHash, &m.Timestamp, &m.LastChangedBlockHash)
	if err != nil {
		return nil, fmt.Errorf("version identity at %d: %w", block, err)
	}
	return &m, nil
}

const diffCols = `select module_id, operator_index, key_index, pubkey, signature, used, vetted
	from key_version where `

func (s *appSnapshot) streamDiffSide(ctx context.Context, kind DiffKind, fromBlock, toBlock int64, moduleID *int16, fn func(DiffKind, *model.Key) error) (int64, error) {
	var (
		q    string
		args []any
	)
	switch kind {
	case DiffAdded:
		// Became visible in the interval and is still visible at the far end.
		q = diffCols + `from_block > $1 and from_block <= $2 and (to_block is null or to_block > $2)`
		args = []any{fromBlock, toBlock}
	case DiffRemoved:
		// Was visible at the near end and is not visible at the far end. The
		// from_block bound excludes rows created and closed inside the interval,
		// which no consumer pinned at either end ever saw.
		q = diffCols + `from_block <= $1 and to_block > $1 and to_block <= $2`
		args = []any{fromBlock, toBlock}
	default:
		return 0, fmt.Errorf("unknown diff side %q", kind)
	}

	if moduleID != nil {
		args = append(args, *moduleID)
		q += ` and module_id = $` + strconv.Itoa(len(args))
	}
	q += ` order by module_id, operator_index, key_index`

	rows, err := s.conn.Query(ctx, q, args...)
	if err != nil {
		return 0, fmt.Errorf("%s side: %w", kind, err)
	}
	defer rows.Close()

	var (
		count int64
		k     model.Key
	)
	for rows.Next() {
		var id int16
		if err := rows.Scan(&id, &k.OperatorIndex, &k.Index, &k.PubkeyBin, &k.SignatureBin,
			&k.Used, &k.Vetted); err != nil {
			return count, fmt.Errorf("scan %s key: %w", kind, err)
		}
		k.ModuleAddress = s.addressOfID(id)
		if err := fn(kind, &k); err != nil {
			return count, err
		}
		count++
	}
	return count, rows.Err()
}

// ModuleDiff reports which modules changed between two versions, by comparing the
// stamps the writer maintains. It is a kilobyte-sized answer and it is what a
// consumer should poll before asking for a key diff at all.
func (a *App) ModuleDiff(ctx context.Context, since, to string) (map[int32]bool, error) {
	conn, err := a.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire: %w", err)
	}
	defer conn.Release()

	_, fromBlock, _, err := resolveAppPin(ctx, conn, since)
	if err != nil {
		return nil, err
	}
	toPin := to
	if toPin == "" {
		toPin = "latest"
	}
	_, toBlock, _, err := resolveAppPin(ctx, conn, toPin)
	if err != nil {
		return nil, err
	}

	rows, err := conn.Query(ctx, `
		select module_id, last_changed_block_hash from module_version
		 where from_block <= $1 and (to_block is null or to_block > $1)`, toBlock)
	if err != nil {
		return nil, err
	}
	atTo := map[int32]string{}
	for rows.Next() {
		var (
			id   int16
			hash string
		)
		if err := rows.Scan(&id, &hash); err != nil {
			rows.Close()
			return nil, err
		}
		atTo[int32(id)] = hash
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rows, err = conn.Query(ctx, `
		select module_id, last_changed_block_hash from module_version
		 where from_block <= $1 and (to_block is null or to_block > $1)`, fromBlock)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	changed := map[int32]bool{}
	for id := range atTo {
		changed[id] = true
	}
	for rows.Next() {
		var (
			id   int16
			hash string
		)
		if err := rows.Scan(&id, &hash); err != nil {
			return nil, err
		}
		// Same stamp on both ends means this module did not move — which is only a
		// trustworthy statement because the writer derives the stamp from content.
		if strings.EqualFold(atTo[int32(id)], hash) {
			delete(changed, int32(id))
		}
	}
	return changed, rows.Err()
}

var _ DiffStore = (*App)(nil)
