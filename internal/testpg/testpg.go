// Package testpg boots the pieces of this service against a real Postgres for tests
// that need the whole thing: the writer, the reader and the HTTP surface.
//
// Three decisions in here are not conveniences, they are what makes the tests mean
// something.
//
// A schema per test, rather than truncating tables between them. The schema is
// unqualified SQL and retention and rollback are unqualified deletes over whole
// tables, so two tests sharing one schema would delete each other's rows — and
// `go test ./...` runs packages in parallel, so this also keeps the writer's own
// integration test from dropping tables underneath an e2e run.
//
// Separate pools for the reader and the writer. A snapshot holds one connection for
// the entire response, so a test that blocks N dumps and then publishes would, on a
// shared pool, deadlock rather than fail — the writer could not acquire a connection.
// Separate pools also mirror the deployment the suite is about: KEYS_API_MODE=server
// replicas behind one ingest.
//
// Disjoint block numbers per test. A version's block number is the key of the advisory
// lock a reader takes, and advisory locks are scoped to the database, not the schema —
// so blocks, unlike tables, are shared between tests whether we like it or not.
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

// DSN returns the test database, or skips the test. Same guard as the writer's
// integration test, and the same requirement: a database the tests may create and
// drop schemas in.
func DSN(tb testing.TB) string {
	tb.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		tb.Skip("set TEST_DATABASE_URL to run tests that need Postgres")
	}
	return dsn
}

// DB is one isolated store: its own schema, its own writer, and reader pools on
// demand.
type DB struct {
	Schema string
	DSN    string
	Writer *ingest.PGStore

	base int64
	// ddl is on the plain DSN, because it creates and drops the schema the rest works
	// in; admin is scoped to that schema, because a test writing raw SQL means the
	// test's own tables.
	ddl     *pgxpool.Pool
	admin   *pgxpool.Pool
	readers []*pgxpool.Pool
}

type config struct {
	writerConns   int32
	keyStageBatch int
}

// Option configures a DB.
type Option func(*config)

// WithWriterConns sets the writer's pool size.
func WithWriterConns(n int32) Option { return func(c *config) { c.writerConns = n } }

// WithKeyStageBatch sets the COPY batch size, which matters for the large datasets a
// speed run writes.
func WithKeyStageBatch(n int) Option { return func(c *config) { c.keyStageBatch = n } }

var (
	schemaCount atomic.Int64
	exclusive   sync.Mutex
	unsafeChars = regexp.MustCompile(`[^a-z0-9_]+`)
)

// New creates a schema, migrates it and returns the store, dropping everything when
// the test ends.
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
	// The migration has to have landed in our schema, not in the one the DSN's role
	// happens to default to. A wrong search_path would show up much later as an empty
	// database, so it is worth one query here.
	db.requireSchema(tb)

	tb.Cleanup(func() {
		for _, pool := range db.readers {
			assertReleased(tb, pool)
			pool.Close()
		}
		db.Writer.Close()
		db.admin.Close()
		// Dropped last: every session that had this schema in its search_path is gone by
		// now, so the drop cannot block on one of them.
		if _, err := ddl.Exec(context.Background(), `drop schema if exists `+schema+` cascade`); err != nil {
			tb.Logf("drop schema %s: %v", schema, err)
		}
		ddl.Close()
	})
	return db
}

// ReaderPool is a pool for readers, sized by the caller: a snapshot holds one
// connection for a whole response, so a test with N concurrent dumps needs N plus
// room for its own bookkeeping.
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

// Reader is a store.App at a pin, on its own pool.
//
// Note what is not here: App.Close. It closes the pool it was given whether or not it
// owns it, so a harness calling it would take the writer's pool down with it; the
// pools are closed in New's cleanup instead.
func (db *DB) Reader(tb testing.TB, pin string, maxConns int32) *store.App {
	tb.Helper()
	return store.NewAppWithPool(db.ReaderPool(tb, maxConns), pin)
}

// Admin is a connection pool for a test's own SQL, scoped to this test's schema.
func (db *DB) Admin() *pgxpool.Pool { return db.admin }

// Leases is the version blocks readers are currently holding.
func (db *DB) Leases(tb testing.TB) []int64 {
	tb.Helper()
	leased, err := store.LeasedVersions(context.Background(), db.admin)
	if err != nil {
		tb.Fatalf("read leases: %v", err)
	}
	return leased
}

// Block returns the nth block number in this test's own range, so two tests running
// at once cannot lease or supersede each other's versions.
func (db *DB) Block(offset int64) int64 { return db.base + offset }

// Exclusive serialises tests that assert on leases or on retention. Those read and
// move state that is per-database rather than per-schema: LeasedVersions reports every
// advisory lock in the database, and retention lowers its boundary to any leased block
// below it, so a lease taken by another test would show up as this one's.
//
// It only serialises within one test binary — it is a mutex in this process, while the
// state it guards is in Postgres. Two packages sharing TEST_DATABASE_URL therefore break
// the assumption, and each sees the other's advisory locks; `-p 1` in the Makefile and in
// .github/workflows/test.yaml is what keeps them apart. The alternative, if that flag
// ever becomes inconvenient, is a database per package rather than a shared one.
func Exclusive(tb testing.TB) {
	tb.Helper()
	exclusive.Lock()
	tb.Cleanup(exclusive.Unlock)
}

// requireSchema fails if the tables did not land where we think they did.
func (db *DB) requireSchema(tb testing.TB) {
	tb.Helper()
	// Which schema an unqualified `versions` resolves to on this connection — not
	// merely which schemas contain a table by that name, since a database that has run
	// these tests before may well have one in public too.
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

// assertReleased catches a test that leaked a connection, which in this service means
// a snapshot that never returned — and, since a snapshot holds a lease, retention
// held open with it.
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

// withSearchPath points a DSN at one schema. pgx passes connection-string keys it does
// not recognise to the server as runtime parameters, which is exactly what is wanted
// here — and New verifies the effect rather than trusting it.
func withSearchPath(dsn, schema string) string {
	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	return dsn + separator + "search_path=" + schema
}

// redact keeps credentials out of a failure message.
func redact(dsn string) string {
	if at := strings.LastIndex(dsn, "@"); at >= 0 {
		if scheme := strings.Index(dsn, "://"); scheme >= 0 && scheme < at {
			return dsn[:scheme+3] + "…@" + dsn[at+1:]
		}
	}
	return dsn
}
