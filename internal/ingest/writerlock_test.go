package ingest_test

import (
	"context"
	"errors"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lidofinance/keys-api-versioned/internal/ingest"
	"github.com/lidofinance/keys-api-versioned/internal/store"
	"github.com/lidofinance/keys-api-versioned/internal/testpg"
)

func TestASecondWriterWaitsForTheFirst(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	first, err := db.Writer.LockWriter(ctx, ingest.WriterLockOptions{})
	if err != nil {
		t.Fatalf("first writer: %v", err)
	}

	second, err := ingest.NewPGStore(ctx, db.DSN, ingest.PGOptions{Source: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	waiting := make(chan struct{})
	acquired := make(chan *ingest.WriterLock, 1)
	go func() {
		lock, err := second.LockWriter(ctx, ingest.WriterLockOptions{OnWait: func() { close(waiting) }})
		if err != nil {
			t.Errorf("second writer: %v", err)
		}
		acquired <- lock
	}()

	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("the second writer did not report that it is waiting")
	}
	select {
	case <-acquired:
		t.Fatal("the second writer got the lock while the first held it")
	case <-time.After(300 * time.Millisecond):
	}

	first.Release()
	select {
	case lock := <-acquired:
		if lock != nil {
			lock.Release()
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the second writer did not get the lock after the first released it")
	}
}

func TestWritersOfDifferentSchemasDoNotWaitForEachOther(t *testing.T) {
	a, b := testpg.New(t), testpg.New(t)
	ctx := context.Background()

	lockA, err := a.Writer.LockWriter(ctx, ingest.WriterLockOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer lockA.Release()

	waited := false
	lockB, err := b.Writer.LockWriter(ctx, ingest.WriterLockOptions{OnWait: func() { waited = true }})
	if err != nil {
		t.Fatal(err)
	}
	defer lockB.Release()
	if waited {
		t.Error("a writer of another schema waited for this one")
	}
}

// respell names the test's schema as an operator might: another case, with public after it.
func respell(t *testing.T, db *testpg.DB) string {
	t.Helper()
	dsn := strings.Replace(db.DSN, "search_path="+db.Schema,
		"search_path="+url.QueryEscape(strings.ToUpper(db.Schema)+", public"), 1)
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var resolved string
	if err := pool.QueryRow(context.Background(), `select (current_schemas(false))[1]`).Scan(&resolved); err != nil {
		t.Fatal(err)
	}
	if resolved != db.Schema {
		t.Fatalf("the respelled search_path resolves to %q, want %q", resolved, db.Schema)
	}
	return dsn
}

func TestWritersThatSpellOneSchemaDifferentlyWaitForEachOther(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	first, err := db.Writer.LockWriter(ctx, ingest.WriterLockOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()

	second, err := ingest.NewPGStore(ctx, respell(t, db), ingest.PGOptions{Source: "testpg"})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	waitCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	waited := false
	lock, err := second.LockWriter(waitCtx, ingest.WriterLockOptions{OnWait: func() { waited = true }})
	if err == nil {
		lock.Release()
	}
	if !waited {
		t.Error("a writer of the same schema under another spelling did not wait for the first")
	}
}

func TestALeaseUnderAnotherSpellingHoldsRetention(t *testing.T) {
	testpg.Exclusive(t)
	db := testpg.New(t)
	ctx := context.Background()

	readers, err := pgxpool.New(ctx, respell(t, db))
	if err != nil {
		t.Fatal(err)
	}
	defer readers.Close()
	conn, err := readers.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	block := db.Block(0)
	release, err := store.LeaseVersion(ctx, conn, block)
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	tx, err := db.Writer.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	claimed, err := store.TryClaimForPrune(ctx, tx, block)
	if err != nil {
		t.Fatal(err)
	}
	if claimed {
		t.Error("retention claimed a version a reader of the same schema leases under another spelling")
	}
	if leases := db.Leases(t); !slices.Contains(leases, block) {
		t.Errorf("leases are %v, want %d among them", leases, block)
	}
}

// Leases are read back from pg_locks; a writer lock mistaken for one would pin retention forever.
func TestTheWriterLockIsNotReadAsALease(t *testing.T) {
	testpg.Exclusive(t)
	db := testpg.New(t)

	lock, err := db.Writer.LockWriter(context.Background(), ingest.WriterLockOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()

	if leases := db.Leases(t); len(leases) != 0 {
		t.Errorf("the writer lock reads as leases %v", leases)
	}
}

func TestLosingTheWriterSessionIsReported(t *testing.T) {
	testpg.Exclusive(t)
	db := testpg.New(t)
	ctx := context.Background()

	lock, err := db.Writer.LockWriter(ctx, ingest.WriterLockOptions{CheckInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()

	var terminated bool
	err = db.Admin().QueryRow(ctx, `
		select pg_terminate_backend(pid) from pg_locks
		 where locktype = 'advisory' and objsubid = 2 and classid = x'6b617069'::int::oid and granted
		   and database = (select oid from pg_database where datname = current_database())`).Scan(&terminated)
	if err != nil || !terminated {
		t.Fatalf("terminate the lock's session: %v (terminated %t)", err, terminated)
	}

	select {
	case <-lock.Lost():
		if lock.Err() == nil {
			t.Error("the lock was lost with no error to report")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the writer was not told that its lock went away")
	}
}

// A database restart takes the lock's session with it; the writer used to exit the process, API
// included, and then crash-loop while the database was still down.
func TestALostWriterLockStopsTheWriterAndTakesTheLockAgain(t *testing.T) {
	testpg.Exclusive(t)
	db := testpg.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	starts := make(chan struct{}, 4)
	stops := make(chan error, 4)
	done := make(chan error, 1)
	go func() {
		done <- db.Writer.RunExclusive(ctx, ingest.WriterLockOptions{
			CheckInterval: 50 * time.Millisecond,
			RetryDelay:    50 * time.Millisecond,
			OnStop:        func(err error) { stops <- err },
		}, func(ctx context.Context) error {
			starts <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		})
	}()

	waitFor := func(what string, ch <-chan struct{}) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s did not happen", what)
		}
	}
	waitFor("the first start", starts)

	if _, err := db.Admin().Exec(ctx, `
		select pg_terminate_backend(pid) from pg_locks
		 where locktype = 'advisory' and objsubid = 2 and classid = x'6b617069'::int::oid and granted
		   and database = (select oid from pg_database where datname = current_database())`); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-stops:
		if err == nil {
			t.Error("the writer stopped with no reason")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("losing the lock did not stop the writer")
	}
	waitFor("a start under the lock taken again", starts)

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("RunExclusive returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunExclusive did not return after its context ended")
	}
}
