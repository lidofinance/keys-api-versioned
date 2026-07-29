package ingest

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/store"
)

//go:embed schema.sql
var schemaSQL string

// PGStore is the versioned Postgres implementation of Store.
//
// Why Postgres and not the embedded store this repo also measured: the embedded
// store is faster on a dump (194 ms against 288 ms) and loses everything that
// matters for running the service — several reader replicas, a writer that is not
// the server process, an ordinary rolling deploy, backups, and adding a field
// with one statement instead of an encoding scheme.
type PGStore struct {
	pool   *pgxpool.Pool
	source string
	// schema is the schema this store lives in, empty when the DSN decides. Migrate
	// creates it; every statement after that is unqualified and lands there through
	// the search_path.
	schema string
	// keyStageBatch bounds how many staged keys are held before a COPY, so memory
	// does not scale with a module's size.
	keyStageBatch int
}

// One schema per chain, and it is a safety property rather than tidiness.
//
// A database filled from one chain and opened with a node on another is silent today:
// the loop probes the stored version's height on the new chain, finds a different hash,
// correctly concludes the branch is gone, finds nothing that survived, and rebuilds from
// scratch. Nothing above `reorg deeper than the retained window` is logged, the data is
// gone, and the service looks healthy. The people most likely to point a mainnet node at
// a Hoodi database are the ones least equipped to work out what happened.
//
// Deriving the schema from the chain id makes that mistake impossible instead of
// detectable. A mainnet run does not find the Hoodi rows because they are not in its
// schema, so it cannot roll them back; it cold-syncs into an empty one and the Hoodi data
// sits untouched. Fix the variable and the data is there again.
//
// It does not replace the chain-id check against the node, which catches the other
// direction, and it cannot catch a devnet reusing chain id 1 — for that, see the
// disjoint-module guard in sync.go.
//
// A DSN that already carries a search_path wins: that is how internal/testpg puts each
// test in its own schema, and it doubles as the escape hatch for a deployment that wants
// the layout it already has.
func chainSchema(chainID int64) string {
	return fmt.Sprintf("chain_%d", chainID)
}

// dsnHasSearchPath reports whether the caller already chose a schema.
func dsnHasSearchPath(dsn string) bool {
	return strings.Contains(dsn, "search_path=")
}

// PGOptions configure the store.
type PGOptions struct {
	MaxConns int32
	// Source is recorded on every version, so a store fed by two providers stays
	// legible.
	Source string
	// ChainID puts the store in a schema of its own, `chain_<id>`, so two chains
	// cannot share one. Zero leaves the schema to the DSN, and so does a DSN that
	// already carries a search_path. See chainSchema for why this exists.
	ChainID int64
	// KeyStageBatch is the COPY batch size; 0 means 20 000.
	KeyStageBatch int
}

// NewPGStore connects to Postgres.
func NewPGStore(ctx context.Context, dsn string, o PGOptions) (*PGStore, error) {
	schema := ""
	if o.ChainID != 0 && !dsnHasSearchPath(dsn) {
		schema = chainSchema(o.ChainID)
		separator := "?"
		if strings.Contains(dsn, "?") {
			separator = "&"
		}
		// pgx passes connection-string keys it does not recognise to the server as
		// runtime parameters, which is exactly what is wanted. Migrate verifies the
		// effect rather than trusting it.
		dsn += separator + "search_path=" + schema
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	if o.MaxConns > 0 {
		cfg.MaxConns = o.MaxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}

	batch := o.KeyStageBatch
	if batch <= 0 {
		batch = 20_000
	}
	return &PGStore{pool: pool, source: o.Source, schema: schema, keyStageBatch: batch}, nil
}

func (s *PGStore) Close() { s.pool.Close() }

// Pool exposes the connection pool so the app can share it with the read side.
func (s *PGStore) Pool() *pgxpool.Pool { return s.pool }

// Migrate creates the schema if it is ours to create, then applies the tables.
//
// The schema.sql statements are unqualified, so they land wherever the search_path points
// — which is the whole mechanism. It is verified rather than trusted: a search_path that
// did not take effect would show up much later as an inexplicably empty store.
func (s *PGStore) Migrate(ctx context.Context) error {
	if s.schema != "" {
		if _, err := s.pool.Exec(ctx, `create schema if not exists `+pgIdentifier(s.schema)); err != nil {
			return fmt.Errorf("create schema %s: %w", s.schema, err)
		}
	}
	if _, err := s.pool.Exec(ctx, schemaSQL); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	if s.schema != "" {
		var landed string
		err := s.pool.QueryRow(ctx, `select n.nspname from pg_class c
			join pg_namespace n on n.oid = c.relnamespace
			where c.oid = to_regclass('versions')`).Scan(&landed)
		if err != nil {
			return fmt.Errorf("verify the schema: %w", err)
		}
		if landed != s.schema {
			return fmt.Errorf("the schema landed in %q, want %q: the search_path in the DSN "+
				"is not taking effect", landed, s.schema)
		}
	}
	return nil
}

// Schema is the schema this store writes to, empty when the DSN decided.
func (s *PGStore) Schema() string { return s.schema }

// pgIdentifier quotes a schema name. The only names reaching it are `chain_<digits>`,
// but an unquoted identifier built by concatenation is a habit worth not having.
func pgIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

const versionColumns = `id, block_number, block_hash, timestamp,
	head_block_number, head_block_hash, head_timestamp,
	last_changed_block_hash, label, status, module_nonces`

func scanVersion(row pgx.Row) (*StoredVersion, error) {
	var (
		v          StoredVersion
		noncesJSON []byte
	)
	if err := row.Scan(&v.ID, &v.BlockNumber, &v.BlockHash, &v.Timestamp,
		&v.HeadBlockNumber, &v.HeadBlockHash, &v.HeadTimestamp,
		&v.LastChangedBlockHash, &v.Label, &v.Status, &noncesJSON); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(noncesJSON, &v.ModuleNonces); err != nil {
		return nil, fmt.Errorf("module nonces of version %d: %w", v.BlockNumber, err)
	}
	if v.ModuleNonces == nil {
		v.ModuleNonces = map[string]int64{}
	}
	return &v, nil
}

// Latest returns the head: the newest version that is published or stale.
//
// Newest wins outright. An older published version is history, not a candidate —
// preferring it because the head is stale would silently serve older data as
// current, and the point of the stale flag is to keep serving the head while saying
// so.
func (s *PGStore) Latest(ctx context.Context) (*StoredVersion, error) {
	v, err := scanVersion(s.pool.QueryRow(ctx, `select `+versionColumns+`
		from versions where status in ('published', 'stale')
		order by id desc limit 1`))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("latest version: %w", err)
	}
	return v, nil
}

// Recent returns versions newest first, whatever their status.
func (s *PGStore) Recent(ctx context.Context, limit int) ([]StoredVersion, error) {
	rows, err := s.pool.Query(ctx, `select `+versionColumns+`
		from versions order by id desc limit $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("recent versions: %w", err)
	}
	defer rows.Close()

	var out []StoredVersion
	for rows.Next() {
		v, err := scanVersion(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *v)
	}
	return out, rows.Err()
}

// Begin opens a version in `building`.
func (s *PGStore) Begin(ctx context.Context, at model.ELBlockSnapshot) (Build, error) {
	var id int64
	err := s.pool.QueryRow(ctx, `
		insert into versions
			(block_number, block_hash, timestamp, head_block_number, head_block_hash, head_timestamp,
			 last_changed_block_hash, label, status, source)
		values ($1, $2, $3, $1, $2, $3, $2, 'latest', 'building', $4)
		on conflict (block_number, block_hash) do nothing
		returning id`,
		at.BlockNumber, strings.ToLower(at.BlockHash), at.Timestamp, s.source).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrVersionExists
	}
	if err != nil {
		return nil, fmt.Errorf("begin version: %w", err)
	}

	return &pgBuild{
		store: s,
		id:    id,
		at:    at,
		hash:  strings.ToLower(at.BlockHash),
	}, nil
}

// Extend advances a published version's observed head. Its identity — the block the
// data was read at — and its lastChangedBlockHash are untouched: the first is what
// pins and cursors name, and the second is what a consumer uses to skip a refetch.
func (s *PGStore) Extend(ctx context.Context, v StoredVersion, at model.ELBlockSnapshot, nonces map[string]int64) error {
	payload, err := json.Marshal(nonces)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		update versions
		   set head_block_number = $2, head_block_hash = $3, head_timestamp = $4, module_nonces = $5
		 where id = $1`,
		v.ID, at.BlockNumber, strings.ToLower(at.BlockHash), at.Timestamp, payload)
	if err != nil {
		return fmt.Errorf("extend version %d: %w", v.BlockNumber, err)
	}
	return nil
}

// MarkStale flags a version as not reflecting the chain.
func (s *PGStore) MarkStale(ctx context.Context, v StoredVersion) error {
	_, err := s.pool.Exec(ctx,
		`update versions set status = 'stale' where id = $1 and status = 'published'`, v.ID)
	if err != nil {
		return fmt.Errorf("mark stale: %w", err)
	}
	return nil
}

// SupersedeFrom rolls the store back to a block after a reorg.
//
// Versions at or above the block come off the chain but their rows are kept where
// they are still needed, because a consumer pinned to one of them has to get a
// definite answer (409 with the head) rather than a 404. Rows that only ever
// existed on the losing branch are dropped, and rows the losing branch closed are
// reopened.
func (s *PGStore) SupersedeFrom(ctx context.Context, block int64) (Superseded, error) {
	var out Superseded

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `update versions set status = 'superseded'
		where block_number >= $1 and status in ('published', 'stale', 'building')`, block)
	if err != nil {
		return out, fmt.Errorf("supersede versions: %w", err)
	}
	out.Versions = tag.RowsAffected()

	for _, table := range versionedTables {
		tag, err := tx.Exec(ctx, `delete from `+table+` where from_block >= $1`, block)
		if err != nil {
			return out, fmt.Errorf("drop rows from %s: %w", table, err)
		}
		out.Dropped += tag.RowsAffected()

		tag, err = tx.Exec(ctx, `update `+table+` set to_block = null where to_block >= $1`, block)
		if err != nil {
			return out, fmt.Errorf("reopen rows in %s: %w", table, err)
		}
		out.Reopened += tag.RowsAffected()
	}

	if err := tx.Commit(ctx); err != nil {
		return out, err
	}
	return out, nil
}

// Prune drops versions outside the retention window and the rows that were
// already closed before the oldest kept version. Rows still current are never
// touched, whatever the window.
func (s *PGStore) Prune(ctx context.Context, keep int) (Pruned, error) {
	var out Pruned
	if keep <= 0 {
		return out, nil
	}

	err := s.pool.QueryRow(ctx, `
		select coalesce(min(block_number), 0) from (
			select block_number from versions order by block_number desc limit $1
		) k`, keep).Scan(&out.OldestKept)
	if err != nil {
		return out, fmt.Errorf("retention window: %w", err)
	}
	if out.OldestKept == 0 {
		return out, nil
	}

	// A reader streaming a version that is about to fall out of the window holds a
	// lease on it, and deleting its rows mid-stream is the one way a concurrent write
	// can corrupt a response here — see internal/store/lease.go for why writes cannot
	// and deletes can. So the boundary drops to the oldest leased version and the rest
	// waits for the next cycle; nothing is skipped permanently.
	boundary := out.OldestKept
	leased, err := store.LeasedVersions(ctx, s.pool)
	if err != nil {
		// Not fatal, but it must not silently fall back to deleting: a failed read of
		// the lease set means we do not know what is in use.
		return out, fmt.Errorf("prune held back, lease set unreadable: %w", err)
	}
	for _, block := range leased {
		if block < boundary {
			boundary = block
			out.HeldAt = block
		}
	}

	// Only rows already closed before the boundary are dropped. A row that is still
	// open belongs to the current version whatever its age, so an open row opened
	// long before the window is normal and stays.
	for _, table := range versionedTables {
		tag, err := s.pool.Exec(ctx, `delete from `+table+`
			where to_block is not null and to_block <= $1`, boundary)
		if err != nil {
			return out, fmt.Errorf("prune %s: %w", table, err)
		}
		out.Rows += tag.RowsAffected()
	}

	tag, err := s.pool.Exec(ctx, `delete from versions where block_number < $1`, boundary)
	if err != nil {
		return out, fmt.Errorf("prune versions: %w", err)
	}
	out.Versions = tag.RowsAffected()
	return out, nil
}

// versionedTables are the three range-versioned tables, in the order a rollback
// and a prune should touch them.
var versionedTables = []string{"key_version", "operator_version", "module_version"}

var _ Store = (*PGStore)(nil)
