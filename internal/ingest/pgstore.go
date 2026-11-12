package ingest

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
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

type PGStore struct {
	pool   *pgxpool.Pool
	source string
	// schema is empty when the DSN decides; statements are unqualified and land via search_path.
	schema        string
	keyStageBatch int
}

// One schema per chain: a mainnet node pointed at a Hoodi database would otherwise "reorg" every
// row away silently. A DSN that already sets search_path wins (tests rely on that).
func chainSchema(chainID int64) string {
	return fmt.Sprintf("chain_%d", chainID)
}

func dsnHasSearchPath(dsn string) bool {
	return strings.Contains(dsn, "search_path=")
}

type PGOptions struct {
	MaxConns int32
	Source   string
	// ChainID selects schema chain_<id>; zero, or a DSN with search_path, leaves it to the DSN.
	ChainID int64
	// KeyStageBatch is the COPY batch size; 0 means 20 000.
	KeyStageBatch int
}

func NewPGStore(ctx context.Context, dsn string, o PGOptions) (*PGStore, error) {
	schema := ""
	if o.ChainID != 0 && !dsnHasSearchPath(dsn) {
		schema = chainSchema(o.ChainID)
		separator := "?"
		if strings.Contains(dsn, "?") {
			separator = "&"
		}
		// pgx passes unknown DSN keys to the server as runtime parameters; Migrate verifies it took.
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

func (s *PGStore) Pool() *pgxpool.Pool { return s.pool }

// Migrate rebuilds the schema when the schema.sql fingerprint changes; there is no migration path.
// A refill costs 14 s on Hoodi, 49 s on mainnet; readers get 425 until the cold sync ends.
//
// One transaction under an advisory lock: two image versions racing no longer drop each other's
// schema mid-apply, and readers see the old layout or the new empty one, never half of either.
func (s *PGStore) Migrate(ctx context.Context) error {
	fingerprint := schemaFingerprint(schemaSQL)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `select pg_advisory_xact_lock($1, `+writerLockKey+`)`, migrateLockClass); err != nil {
		return fmt.Errorf("migrate: lock: %w", err)
	}

	if s.schema != "" {
		stored, err := storedFingerprint(ctx, tx)
		if err != nil {
			return err
		}
		if stored != fingerprint && schemaHasTables(ctx, tx) {
			if _, err := tx.Exec(ctx, `drop schema `+pgIdentifier(s.schema)+` cascade`); err != nil {
				return fmt.Errorf("drop schema %s for rebuild: %w", s.schema, err)
			}
		}
		if _, err := tx.Exec(ctx, `create schema if not exists `+pgIdentifier(s.schema)); err != nil {
			return fmt.Errorf("create schema %s: %w", s.schema, err)
		}
	}
	if _, err := tx.Exec(ctx, schemaSQL); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	if s.schema != "" {
		var landed string
		err := tx.QueryRow(ctx, `select n.nspname from pg_class c
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

	if _, err := tx.Exec(ctx, `
		insert into schema_fingerprint (id, fingerprint) values (1, $1)
		on conflict (id) do update set fingerprint = excluded.fingerprint, applied_at = now()`,
		fingerprint); err != nil {
		return fmt.Errorf("record the schema fingerprint: %w", err)
	}
	return tx.Commit(ctx)
}

// storedFingerprint: "" for a schema with tables means an unknown layout, rebuilt like a mismatch.
func storedFingerprint(ctx context.Context, tx pgx.Tx) (string, error) {
	// to_regclass answers NULL for a missing name, including a missing schema, instead of failing.
	var exists bool
	if err := tx.QueryRow(ctx,
		`select to_regclass('schema_fingerprint') is not null`).Scan(&exists); err != nil {
		return "", fmt.Errorf("look for the schema fingerprint: %w", err)
	}
	if !exists {
		return "", nil
	}

	var fingerprint string
	err := tx.QueryRow(ctx, `select fingerprint from schema_fingerprint where id = 1`).Scan(&fingerprint)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read the schema fingerprint: %w", err)
	}
	return fingerprint, nil
}

func schemaHasTables(ctx context.Context, tx pgx.Tx) bool {
	var exists bool
	if err := tx.QueryRow(ctx,
		`select to_regclass('versions') is not null`).Scan(&exists); err != nil {
		return false
	}
	return exists
}

// schemaFingerprint ignores comments and blank lines, so editing a comment costs no resync.
func schemaFingerprint(sql string) string {
	var meaningful []string
	for _, line := range strings.Split(sql, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}
		if i := strings.Index(trimmed, "--"); i >= 0 {
			trimmed = strings.TrimSpace(trimmed[:i])
		}
		meaningful = append(meaningful, trimmed)
	}
	sum := sha256.Sum256([]byte(strings.Join(meaningful, "\n")))
	return hex.EncodeToString(sum[:])
}

func (s *PGStore) Schema() string { return s.schema }

func pgIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

const versionColumns = `id, block_number, block_hash, timestamp,
	head_block_number, head_block_hash, head_timestamp, finalized_block_number,
	last_changed_block_hash, label, status, module_nonces`

func scanVersion(row pgx.Row) (*StoredVersion, error) {
	var (
		v          StoredVersion
		noncesJSON []byte
	)
	if err := row.Scan(&v.ID, &v.BlockNumber, &v.BlockHash, &v.Timestamp,
		&v.HeadBlockNumber, &v.HeadBlockHash, &v.HeadTimestamp, &v.FinalizedBlockNumber,
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

// Latest: newest wins outright; a stale head is served as stale, never swapped for an older one.
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

// Begin refuses a block below what the store already holds: Commit closes rows at the build's
// block, so a lower build after a higher one rewrites ranges the newer version reads.
func (s *PGStore) Begin(ctx context.Context, at model.ELBlockSnapshot) (Build, error) {
	var (
		newerVersion bool
		newestRow    int64
	)
	if err := s.pool.QueryRow(ctx, `
		select exists (
		         select 1 from versions
		          where status in ('published', 'stale', 'building')
		            and (block_number > $1 or (block_number = $1 and block_hash <> $2))),
		       coalesce(greatest(
		         (select max(from_block) from key_version), (select max(to_block) from key_version),
		         (select max(from_block) from operator_version), (select max(to_block) from operator_version),
		         (select max(from_block) from module_version), (select max(to_block) from module_version)), 0)`,
		at.BlockNumber, strings.ToLower(at.BlockHash)).Scan(&newerVersion, &newestRow); err != nil {
		return nil, fmt.Errorf("begin version: check for newer data: %w", err)
	}
	if newerVersion || newestRow > at.BlockNumber {
		return nil, ErrBuildBehind
	}

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
		var status string
		if err := s.pool.QueryRow(ctx, `select status from versions where block_number = $1 and block_hash = $2`,
			at.BlockNumber, strings.ToLower(at.BlockHash)).Scan(&status); err != nil {
			return nil, ErrVersionExists
		}
		return nil, ExistingVersionError{Status: status}
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

func (s *PGStore) Extend(ctx context.Context, v StoredVersion, at model.ELBlockSnapshot, nonces map[string]int64, finalized int64) error {
	payload, err := json.Marshal(nonces)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		update versions
		   set head_block_number = $2, head_block_hash = $3, head_timestamp = $4, module_nonces = $5,
		       finalized_block_number = greatest(finalized_block_number, $6)
		 where id = $1`,
		v.ID, at.BlockNumber, strings.ToLower(at.BlockHash), at.Timestamp, payload, finalized)
	if err != nil {
		return fmt.Errorf("extend version %d: %w", v.BlockNumber, err)
	}
	return nil
}

func (s *PGStore) MarkStale(ctx context.Context, v StoredVersion) error {
	_, err := s.pool.Exec(ctx,
		`update versions set status = 'stale' where id = $1 and status = 'published'`, v.ID)
	if err != nil {
		return fmt.Errorf("mark stale: %w", err)
	}
	return nil
}

// SupersedeFrom keeps superseded versions' rows so pins get 409 rather than 404. Rows only on the
// losing branch are dropped, and rows it closed are reopened.
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

func (s *PGStore) Leftovers(ctx context.Context, published int64) ([]Leftover, error) {
	rows, err := s.pool.Query(ctx, `
		with touched as (
			select module_id from key_version where from_block > $1 or to_block > $1
			union select module_id from operator_version where from_block > $1 or to_block > $1
			union select module_id from module_version where from_block > $1 or to_block > $1
		)
		select t.module_id, m.from_block, m.nonce, v.block_hash,
		       m.row_id is not null and v.block_hash is not null
		       and not exists (select 1 from key_version k where k.module_id = t.module_id
		                         and (k.from_block > m.from_block or k.to_block > m.from_block))
		       and not exists (select 1 from operator_version o where o.module_id = t.module_id
		                         and (o.from_block > m.from_block or o.to_block > m.from_block))
		  from touched t
		  left join module_version m
		    on m.module_id = t.module_id and m.to_block is null and m.from_block > $1
		  left join lateral (
		        select min(block_hash) as block_hash from versions
		         where block_number = m.from_block and status in ('building', 'superseded')
		        having count(*) = 1) v on true`, published)
	if err != nil {
		return nil, fmt.Errorf("leftovers: %w", err)
	}
	defer rows.Close()

	var out []Leftover
	for rows.Next() {
		var (
			l     Leftover
			id    int16
			block *int64
			nonce *int64
			hash  *string
		)
		if err := rows.Scan(&id, &block, &nonce, &hash, &l.Complete); err != nil {
			return nil, err
		}
		l.ModuleID = int32(id)
		if l.Complete {
			l.Block, l.Nonce, l.BlockHash = *block, *nonce, *hash
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

// finalizedVersionBlock duplicates the reader's predicate in internal/store; a retention test
// keeps the two in step.
func finalizedVersionBlock(ctx context.Context, tx pgx.Tx) (int64, error) {
	var block int64
	err := tx.QueryRow(ctx, `
		select coalesce(max(block_number), 0) from versions
		 where status in ('published', 'stale')
		   and block_number <= (select coalesce(max(finalized_block_number), 0) from versions)`).
		Scan(&block)
	if err != nil {
		return 0, err
	}
	return block, nil
}

// Prune never touches rows still open, whatever the window. One transaction, versions first: a
// reader resolving a pin at the same moment sees the version and its rows, or neither.
func (s *PGStore) Prune(ctx context.Context, keep int) (Pruned, error) {
	var out Pruned
	if keep <= 0 {
		return out, nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback(ctx)

	err = tx.QueryRow(ctx, `
		select coalesce(min(block_number), 0) from (
			select block_number from versions order by block_number desc limit $1
		) k`, keep).Scan(&out.OldestKept)
	if err != nil {
		return out, fmt.Errorf("retention window: %w", err)
	}
	if out.OldestKept == 0 {
		return out, nil
	}
	boundary := out.OldestKept

	// The finalized version is held too: retention counts versions while finality (~13 min) is time,
	// and at one rebuild per minute or two it sits near the window's edge.
	finalizedVersion, err := finalizedVersionBlock(ctx, tx)
	if err != nil {
		return out, fmt.Errorf("prune held back, finalized version unreadable: %w", err)
	}
	if finalizedVersion > 0 && finalizedVersion < boundary {
		boundary = finalizedVersion
		out.HeldAt = finalizedVersion
	}

	// A leased version holds the boundary at itself: deleting rows mid-stream would corrupt a
	// response. Held versions are pruned on a later cycle.
	doomed, err := blocksBelow(ctx, tx, boundary)
	if err != nil {
		return out, err
	}
	for _, block := range doomed {
		claimed, err := store.TryClaimForPrune(ctx, tx, block)
		if err != nil {
			return out, err
		}
		if !claimed {
			boundary = block
			out.HeldAt = block
			break
		}
	}

	tag, err := tx.Exec(ctx, `delete from versions where block_number < $1`, boundary)
	if err != nil {
		return out, fmt.Errorf("prune versions: %w", err)
	}
	out.Versions = tag.RowsAffected()

	for _, table := range versionedTables {
		tag, err := tx.Exec(ctx, `delete from `+table+`
			where to_block is not null and to_block <= $1`, boundary)
		if err != nil {
			return out, fmt.Errorf("prune %s: %w", table, err)
		}
		out.Rows += tag.RowsAffected()
	}

	if pruneBeforeCommit != nil {
		pruneBeforeCommit()
	}
	if err := tx.Commit(ctx); err != nil {
		return out, fmt.Errorf("commit prune: %w", err)
	}
	return out, nil
}

var pruneBeforeCommit func()

func blocksBelow(ctx context.Context, tx pgx.Tx, boundary int64) ([]int64, error) {
	rows, err := tx.Query(ctx, `select distinct block_number from versions
		where block_number < $1 order by block_number`, boundary)
	if err != nil {
		return nil, fmt.Errorf("versions to prune: %w", err)
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

// versionedTables in the order a rollback and a prune touch them.
var versionedTables = []string{"key_version", "operator_version", "module_version"}

var (
	_ Store            = (*PGStore)(nil)
	_ LeftoverReporter = (*PGStore)(nil)
)
