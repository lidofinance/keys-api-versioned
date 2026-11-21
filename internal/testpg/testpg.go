// Package testpg: a schema per test (retention deletes whole tables), reader and writer pools apart
// (a snapshot pins a connection; shared, they deadlock), per-test blocks (advisory locks span the DB).
package testpg

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lidofinance/keys-api-versioned/internal/ingest"
	"github.com/lidofinance/keys-api-versioned/internal/store"
)

func DSN(tb testing.TB) string {
	tb.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		tb.Skip("set TEST_DATABASE_URL to run tests that need Postgres")
	}
	return dsn
}

type DB struct {
	Schema string
	DSN    string
	Writer *ingest.PGStore

	base    int64
	ddl     *pgxpool.Pool
	admin   *pgxpool.Pool
	readers []*pgxpool.Pool
}

type config struct {
	writerConns   int32
	keyStageBatch int
}

type Option func(*config)

func WithWriterConns(n int32) Option { return func(c *config) { c.writerConns = n } }

func WithKeyStageBatch(n int) Option { return func(c *config) { c.keyStageBatch = n } }

var (
	schemaCount atomic.Int64
	exclusive   sync.Mutex
	unsafeChars = regexp.MustCompile(`[^a-z0-9_]+`)
)

func New(tb testing.TB, opts ...Option) *DB {
	tb.Helper()

	cfg := config{writerConns: 4}
	for _, opt := range opts {
		opt(&cfg)
	}

	ctx := context.Background()
	base := DSN(tb)
	n := schemaCount.Add(1)

	ddl, err := pgxpool.New(ctx, base)
	if err != nil {
		tb.Fatalf("connect to %s: %v", redact(base), err)
	}
	schema := schemaName(tb.Name(), n)
	if _, err := ddl.Exec(ctx, `drop schema if exists `+schema+` cascade`); err != nil {
		tb.Fatalf("drop schema %s: %v", schema, err)
	}
	if _, err := ddl.Exec(ctx, `create schema `+schema); err != nil {
		tb.Fatalf("create schema %s: %v", schema, err)
	}

	db := &DB{
		Schema: schema,
		DSN:    withSearchPath(base, schema),
		base:   n * 1_000_000,
		ddl:    ddl,
	}

	db.admin, err = pgxpool.New(ctx, db.DSN)
	if err != nil {
		tb.Fatalf("connect to schema %s: %v", schema, err)
	}

	db.Writer, err = ingest.NewPGStore(ctx, db.DSN, ingest.PGOptions{
		MaxConns: cfg.writerConns, Source: "testpg", KeyStageBatch: cfg.keyStageBatch,
	})
	if err != nil {
		tb.Fatalf("writer: %v", err)
	}
	if err := db.Writer.Migrate(ctx); err != nil {
		tb.Fatalf("migrate %s: %v", schema, err)
	}
	if _, err := db.Writer.TakeEpoch(ctx); err != nil {
		tb.Fatalf("writer epoch: %v", err)
	}
	// A wrong search_path would otherwise surface much later as an empty database.
	db.requireSchema(tb)

	tb.Cleanup(func() {
		for _, pool := range db.readers {
			assertReleased(tb, pool)
			pool.Close()
		}
		db.Writer.Close()
		db.admin.Close()
		// Dropped last, so the drop cannot block on a session that still has it in search_path.
		if _, err := ddl.Exec(context.Background(), `drop schema if exists `+schema+` cascade`); err != nil {
			tb.Logf("drop schema %s: %v", schema, err)
		}
		ddl.Close()
	})
	return db
}

// ReaderPool: a snapshot holds a connection for a whole response, so N concurrent dumps need N+.
func (db *DB) ReaderPool(tb testing.TB, maxConns int32) *pgxpool.Pool {
	tb.Helper()

	cfg, err := pgxpool.ParseConfig(db.DSN)
	if err != nil {
		tb.Fatalf("parse dsn: %v", err)
	}
	cfg.MaxConns = maxConns
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		tb.Fatalf("reader pool: %v", err)
	}
	db.readers = append(db.readers, pool)
	return pool
}

// Reader never calls App.Close: it closes the pool even when it does not own it.
func (db *DB) Reader(tb testing.TB, pin string, maxConns int32) *store.App {
	tb.Helper()
	return store.NewAppWithPool(db.ReaderPool(tb, maxConns), pin)
}

// Admin is a connection pool for a test's own SQL, scoped to this test's schema.
func (db *DB) Admin() *pgxpool.Pool { return db.admin }

func (db *DB) Leases(tb testing.TB) []int64 {
	tb.Helper()
	leased, err := store.LeasedVersions(context.Background(), db.admin)
	if err != nil {
		tb.Fatalf("read leases: %v", err)
	}
	return leased
}

// Block offsets into this test's own range, so concurrent tests cannot touch each other's versions.
func (db *DB) Block(offset int64) int64 { return db.base + offset }

// Exclusive serialises lease/retention tests: advisory locks are per-database. It is in-process
// only, so packages sharing TEST_DATABASE_URL rely on `-p 1`.
func Exclusive(tb testing.TB) {
	tb.Helper()
	exclusive.Lock()
	tb.Cleanup(exclusive.Unlock)
}

func (db *DB) requireSchema(tb testing.TB) {
	tb.Helper()
	// Where unqualified `versions` resolves: an earlier run may have left one in public.
	var got string
	err := db.Writer.Pool().QueryRow(context.Background(),
		`select n.nspname from pg_class c
		   join pg_namespace n on n.oid = c.relnamespace
		  where c.oid = to_regclass('versions')`).Scan(&got)
	if err != nil {
		tb.Fatalf("look for the versions table: %v", err)
	}
	if got != db.Schema {
		tb.Fatalf("migration landed in schema %q, want %q: the search_path in the DSN is not taking effect", got, db.Schema)
	}
}

func assertReleased(tb testing.TB, pool *pgxpool.Pool) {
	tb.Helper()
	if n := pool.Stat().AcquiredConns(); n != 0 {
		tb.Errorf("%d connection(s) still acquired at the end of the test: a snapshot was not released", n)
	}
}

func schemaName(testName string, n int64) string {
	clean := unsafeChars.ReplaceAllString(strings.ToLower(testName), "_")
	if len(clean) > 40 {
		clean = clean[:40]
	}
	return fmt.Sprintf("t%d_%s", n, strings.Trim(clean, "_"))
}

// pgx forwards unknown DSN keys to the server as runtime parameters.
func withSearchPath(dsn, schema string) string {
	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	return dsn + separator + "search_path=" + schema
}

func redact(dsn string) string {
	if at := strings.LastIndex(dsn, "@"); at >= 0 {
		if scheme := strings.Index(dsn, "://"); scheme >= 0 && scheme < at {
			return dsn[:scheme+3] + "…@" + dsn[at+1:]
		}
	}
	return dsn
}
