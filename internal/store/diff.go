package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/lidofinance/keys-api-versioned/internal/model"
)

// Diff over block ranges: added = from_block in (a, b] and alive at b; removed = alive at a and
// to_block <= b. A changed key is in both; rows born and closed inside (a, b] are in neither.

type DiffKind string

const (
	DiffRemoved DiffKind = "removed"
	DiffAdded   DiffKind = "added"
)

type DiffStore interface {
	// DiffKeys streams removals, then additions.
	DiffKeys(ctx context.Context, since, to string, moduleRef string, fn func(DiffKind, *model.Key) error) (*DiffMeta, error)
}

type DiffMeta struct {
	From    *model.ELBlockSnapshot `json:"from"`
	To      *model.ELBlockSnapshot `json:"to"`
	Removed int64                  `json:"removed"`
	Added   int64                  `json:"added"`
}

// A `since` outside retention answers 410: its closed rows are deleted, so the diff would
// silently miss removals.
func (a *App) DiffKeys(ctx context.Context, since, to string, moduleRef string, fn func(DiffKind, *model.Key) error) (*DiffMeta, error) {
	conn, err := a.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	_, _, fromBlock, _, err := resolveAppPin(ctx, conn, since)
	if err != nil {
		return nil, err
	}
	if fromBlock == 0 {
		return nil, ErrNotFound{Ref: since, What: "snapshot"}
	}
	// Leasing the older end keeps its closed rows (all above it) from retention between the two side
	// queries.
	releaseFrom, err := LeaseVersion(ctx, conn, fromBlock)
	if err != nil {
		return nil, err
	}
	defer releaseFrom()

	toPin := to
	if toPin == "" {
		toPin = "latest"
	}
	head, toVersionID, toBlock, _, err := resolveAppPin(ctx, conn, toPin)
	if err != nil {
		return nil, err
	}
	if head == nil {
		return nil, ErrNotFound{Ref: toPin, What: "snapshot"}
	}

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

	// versionID, not just block: two versions share a height after a reorg.
	snap := &appSnapshot{conn: conn, meta: toMeta, versionID: toVersionID, block: toBlock}
	if err := snap.loadModules(ctx); err != nil {
		return nil, err
	}

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

	// Removals first, so an in-order consumer never holds two rows for one key identity.
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

func versionIdentity(ctx context.Context, conn pinReader, block int64) (*model.ELBlockSnapshot, error) {
	var m model.ELBlockSnapshot
	err := conn.QueryRow(ctx, `select block_number, block_hash, timestamp, last_changed_block_hash
		from versions where block_number = $1
		order by (status = '`+StatusPublished+`') desc, id desc limit 1`, block).
		Scan(&m.BlockNumber, &m.BlockHash, &m.Timestamp, &m.LastChangedBlockHash)
	if errors.Is(err, pgx.ErrNoRows) {
		// Pruned between resolving the pin and leasing it.
		return nil, ErrPruned{Ref: strconv.FormatInt(block, 10)}
	}
	if err != nil {
		return nil, fmt.Errorf("version identity at %d: %w", block, err)
	}
	return &m, nil
}

const diffCols = `select module_id, operator_index, key_index, pubkey, signature, used, vetted
	from key_version where `

func (s *appSnapshot) streamDiffSide(ctx context.Context, kind DiffKind, fromBlock, toBlock int64, moduleID *int16, fn func(DiffKind, *model.Key) error) (int64, error) {
	// A reorg between the two side queries would drop a side; missing removals leave the consumer
	// with keys the chain no longer has.
	if err := s.stillOnChain(ctx); err != nil {
		return 0, err
	}

	var (
		q    string
		args []any
	)
	switch kind {
	case DiffAdded:
		q = diffCols + `from_block > $1 and from_block <= $2 and (to_block is null or to_block > $2)`
		args = []any{fromBlock, toBlock}
	case DiffRemoved:
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

// ModuleDiff compares the writer's content-derived stamps: a kilobyte answer to poll before a
// key diff.
func (a *App) ModuleDiff(ctx context.Context, since, to string) (map[int32]bool, error) {
	conn, err := a.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	_, _, fromBlock, _, err := resolveAppPin(ctx, conn, since)
	if err != nil {
		return nil, err
	}
	if fromBlock > 0 {
		release, err := LeaseVersion(ctx, conn, fromBlock)
		if err != nil {
			return nil, err
		}
		defer release()
	}
	toPin := to
	if toPin == "" {
		toPin = "latest"
	}
	_, _, toBlock, _, err := resolveAppPin(ctx, conn, toPin)
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
		if strings.EqualFold(atTo[int32(id)], hash) {
			delete(changed, int32(id))
		}
	}
	return changed, rows.Err()
}

var _ DiffStore = (*App)(nil)
