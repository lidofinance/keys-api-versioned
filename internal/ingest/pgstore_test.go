package ingest_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"strings"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/ingest"
	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/store"
	"github.com/lidofinance/keys-api-versioned/internal/testpg"
	"github.com/lidofinance/keys-api-versioned/internal/testreg"
)

// Skipped unless TEST_DATABASE_URL points at a database the tests may create and drop schemas in.

func TestWriterVersionsAndDiff(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	// The smallest shape with a module boundary and a key that can move on its own.
	spec := testreg.Spec{Modules: 2, OperatorsPerModule: 1, KeysPerOperator: 3}

	v1 := testreg.At(db.Block(0))
	spec.Publish(ctx, t, db.Writer, v1)

	// Driven by hand rather than testreg.Publish: the diff one module's commit reports is under test.
	deposited := spec.WithUsed(func(module, operator, index int) bool {
		return module == 2 && index == 1
	})
	v2 := testreg.At(db.Block(1))

	build, err := db.Writer.Begin(ctx, v2)
	if err != nil {
		t.Fatalf("begin v2: %v", err)
	}
	module2 := deposited.Module(2)
	keys, err := build.Keys(ctx, module2)
	if err != nil {
		t.Fatalf("open keys: %v", err)
	}
	for i := 0; i < spec.KeysPerOperator; i++ {
		if err := keys.Add(ctx, deposited.Key(2, 0, i)); err != nil {
			t.Fatalf("add key: %v", err)
		}
	}
	diff, err := keys.Commit(ctx)
	if err != nil {
		t.Fatalf("commit keys: %v", err)
	}

	// One key changed: one row closed and one opened, module 1 untouched.
	if diff.Opened != 1 || diff.Closed != 1 {
		t.Errorf("second version opened %d and closed %d rows, want 1 and 1", diff.Opened, diff.Closed)
	}
	if diff.Unchanged != 2 {
		t.Errorf("second version left %d rows alone, want 2", diff.Unchanged)
	}

	// A deposit moves the module's nonce on chain, so the module row moves with it.
	module2.Nonce++
	for _, module := range []model.StakingModule{deposited.Module(1), module2} {
		changed := module.ID == 2
		if err := build.Module(ctx, module, changed); err != nil {
			t.Fatalf("module row: %v", err)
		}
	}
	if err := build.Publish(ctx, "latest", 0); err != nil {
		t.Fatalf("publish v2: %v", err)
	}

	var rows int
	if err := db.Writer.Pool().QueryRow(ctx, "select count(*) from key_version").Scan(&rows); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rows != 7 {
		t.Errorf("store holds %d key rows after two versions, want 7 (6 plus the one that changed)", rows)
	}

	pinned := db.Reader(t, fmt.Sprint(v1.BlockNumber), 2)
	if readUsed(ctx, t, pinned, module2, 1) {
		t.Error("the first version reports the key as deposited; it was not")
	}

	latest := db.Reader(t, "latest", 2)
	if !readUsed(ctx, t, latest, module2, 1) {
		t.Error("the latest version does not report the key as deposited")
	}

	var removed, added []model.Key
	meta, err := latest.DiffKeys(ctx, fmt.Sprint(v1.BlockNumber), "latest", "",
		func(kind store.DiffKind, k *model.Key) error {
			copied := *k
			if kind == store.DiffRemoved {
				removed = append(removed, copied)
			} else {
				added = append(added, copied)
			}
			return nil
		})
	if err != nil {
		t.Fatalf("diff: %v", err)
	}
	if meta.Removed != 1 || meta.Added != 1 {
		t.Fatalf("diff reports %d removed and %d added, want 1 and 1", meta.Removed, meta.Added)
	}
	if removed[0].Used || !added[0].Used {
		t.Errorf("diff sides are the wrong way round: removed.used=%t added.used=%t",
			removed[0].Used, added[0].Used)
	}
	if removed[0].Index != 1 || added[0].Index != 1 {
		t.Errorf("diff points at key indexes %d and %d, want 1 and 1", removed[0].Index, added[0].Index)
	}

	pruned, err := db.Writer.Prune(ctx, 1)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if pruned.Versions != 1 {
		t.Errorf("prune dropped %d versions, want 1", pruned.Versions)
	}
	// Two closed rows: the changed key and module 2's own row; Prune counts every versioned table.
	if pruned.Rows != 2 {
		t.Errorf("prune dropped %d rows, want 2 (the closed key row and the closed module row)", pruned.Rows)
	}
	if err := db.Writer.Pool().QueryRow(ctx, "select count(*) from key_version").Scan(&rows); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rows != 6 {
		t.Errorf("store holds %d key rows after pruning, want 6 current ones", rows)
	}
}

// A publish cannot corrupt a pinned read (range predicate), but retention's delete can. The control
// half proves the same prune removes the rows when no lease holds them.
func TestRetentionWaitsForAReaderStreamingAnOldVersion(t *testing.T) {
	testpg.Exclusive(t)

	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 1, OperatorsPerModule: 1, KeysPerOperator: 1}
	module := spec.Module(1)

	old := testreg.At(db.Block(0))
	spec.Publish(ctx, t, db.Writer, old)

	deposited := spec.WithUsed(func(int, int, int) bool { return true })
	deposited.Publish(ctx, t, db.Writer, testreg.At(db.Block(1)))

	// The pin is held open for the callback: what a slow dump looks like.
	reader := db.Reader(t, fmt.Sprint(old.BlockNumber), 2)

	var (
		pruned  ingest.Pruned
		seen    int
		pruneEr error
	)
	err := reader.Snapshot(ctx, func(snap store.Snapshot) error {
		pruned, pruneEr = db.Writer.Prune(ctx, 1)
		if pruneEr != nil {
			return pruneEr
		}
		return snap.StreamKeys(ctx, module.RawAddress(), model.KeysFilter{}, func(k *model.Key) error {
			seen++
			if k.Used {
				t.Errorf("key at the pinned version reads used=true, want the value from block %d", old.BlockNumber)
			}
			return nil
		})
	})
	if err != nil {
		t.Fatalf("read while pruning: %v", err)
	}
	if seen != 1 {
		t.Errorf("streamed %d keys at the pinned version, want 1 — retention took the rows", seen)
	}
	if pruned.HeldAt != old.BlockNumber {
		t.Errorf("Pruned.HeldAt = %d, want %d: retention should report what held it back",
			pruned.HeldAt, old.BlockNumber)
	}

	// Control: without a reader the same prune must remove it, or the check above proves nothing.
	pruned, err = db.Writer.Prune(ctx, 1)
	if err != nil {
		t.Fatalf("prune after the reader left: %v", err)
	}
	if pruned.HeldAt != 0 {
		t.Errorf("Pruned.HeldAt = %d after the reader left, want 0", pruned.HeldAt)
	}
	if pruned.Rows == 0 && pruned.Versions == 0 {
		t.Fatal("nothing was pruned once the lease was gone, so the check above proved nothing")
	}

	var versions int
	if err := db.Writer.Pool().QueryRow(ctx,
		`select count(*) from versions where block_number = $1`, old.BlockNumber).Scan(&versions); err != nil {
		t.Fatalf("count versions: %v", err)
	}
	if versions != 0 {
		t.Errorf("version %d is still stored after an unheld prune", old.BlockNumber)
	}
}

func readUsed(ctx context.Context, t *testing.T, s *store.App, m model.StakingModule, index int32) bool {
	t.Helper()

	var found, used bool
	err := s.Snapshot(ctx, func(snap store.Snapshot) error {
		return snap.StreamKeys(ctx, m.RawAddress(), model.KeysFilter{}, func(k *model.Key) error {
			if k.Index == index {
				found, used = true, k.Used
			}
			return nil
		})
	})
	if err != nil {
		t.Fatalf("read key: %v", err)
	}
	if !found {
		t.Fatalf("key %d of module %d is missing at this pin", index, m.ID)
	}
	return used
}

// Begin reports what holds the identity, so the loop can tell a built version from an abandoned try.
func TestBeginNamesTheStatusOfAnExistingVersion(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()
	at := testreg.At(db.Block(0))

	build, err := db.Writer.Begin(ctx, at)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	build.Abort(ctx)

	_, err = db.Writer.Begin(ctx, at)
	var existing ingest.ExistingVersionError
	if !errors.As(err, &existing) || !errors.Is(err, ingest.ErrVersionExists) {
		t.Fatalf("second begin: got %v, want ExistingVersionError", err)
	}
	if existing.Status != "superseded" || existing.Done() {
		t.Errorf("got status %q (done %t), want an unfinished superseded row", existing.Status, existing.Done())
	}
}

// Before chain-named schemas, a database from another chain looked like a deep reorg and was
// silently rebuilt; now the two stores are simply different places.
func TestTwoChainsCannotShareOneStore(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL to run tests that need Postgres")
	}
	ctx := context.Background()

	// Two chain ids no other test uses, so this can drop its own schemas.
	const (
		hoodiLike   = 990001
		mainnetLike = 990002
	)

	open := func(chainID int64) *ingest.PGStore {
		t.Helper()
		s, err := ingest.NewPGStore(ctx, dsn, ingest.PGOptions{ChainID: chainID, Source: "test"})
		if err != nil {
			t.Fatalf("open the store for chain %d: %v", chainID, err)
		}
		if err := s.Migrate(ctx); err != nil {
			t.Fatalf("migrate chain %d: %v", chainID, err)
		}
		return s
	}

	first := open(hoodiLike)
	defer first.Close()
	if got, want := first.Schema(), "chain_990001"; got != want {
		t.Errorf("schema is %q, want %q", got, want)
	}

	spec := testreg.Spec{Modules: 1, OperatorsPerModule: 1, KeysPerOperator: 2}
	at := testreg.At(4_000_001)
	spec.Publish(ctx, t, first, at)

	second := open(mainnetLike)
	defer second.Close()

	latest, err := second.Latest(ctx)
	if err != nil {
		t.Fatalf("latest in the second store: %v", err)
	}
	if latest != nil {
		t.Fatalf("the store for chain %d sees a version from chain %d at block %d: "+
			"one chain's data is reachable as another's", mainnetLike, hoodiLike, latest.BlockNumber)
	}

	held, err := first.Latest(ctx)
	if err != nil {
		t.Fatalf("latest in the first store: %v", err)
	}
	if held == nil || held.BlockNumber != at.BlockNumber {
		t.Fatalf("the first store no longer holds its version: %+v", held)
	}

	t.Cleanup(func() {
		admin, err := pgxpool.New(context.Background(), dsn)
		if err != nil {
			return
		}
		defer admin.Close()
		for _, schema := range []string{"chain_990001", "chain_990002"} {
			_, _ = admin.Exec(context.Background(), `drop schema if exists `+schema+` cascade`)
		}
	})
}

// A DSN that already names a schema wins; every other test relies on it.
func TestADSNWithASearchPathKeepsIt(t *testing.T) {
	db := testpg.New(t)

	if got := db.Writer.Schema(); got != "" {
		t.Errorf("the store derived schema %q from a DSN that already named one", got)
	}
}

// The store is a cache: a layout change drops and refills it. An earlier `alter ... default 0`
// left older rows with a value the writer never writes.
func TestASchemaChangeRebuildsRatherThanMigrates(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL to run tests that need Postgres")
	}
	ctx := context.Background()

	const chainID = 990003
	schema := "chain_990003"

	t.Cleanup(func() {
		admin, err := pgxpool.New(context.Background(), dsn)
		if err != nil {
			return
		}
		defer admin.Close()
		_, _ = admin.Exec(context.Background(), `drop schema if exists `+schema+` cascade`)
	})

	open := func() *ingest.PGStore {
		t.Helper()
		s, err := ingest.NewPGStore(ctx, dsn, ingest.PGOptions{ChainID: chainID, Source: "test"})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		if err := s.Migrate(ctx); err != nil {
			t.Fatalf("migrate: %v", err)
		}
		return s
	}

	first := open()
	spec := testreg.Spec{Modules: 1, OperatorsPerModule: 1, KeysPerOperator: 3}
	at := testreg.At(7_000_001)
	spec.Publish(ctx, t, first, at)

	held, err := first.Latest(ctx)
	if err != nil || held == nil {
		t.Fatalf("the version was not written: %+v %v", held, err)
	}
	first.Close()

	// Same layout must not rebuild: a rebuild on every boot would make the policy unusable.
	second := open()
	stillThere, err := second.Latest(ctx)
	if err != nil {
		t.Fatalf("latest after an unchanged reopen: %v", err)
	}
	if stillThere == nil || stillThere.BlockNumber != at.BlockNumber {
		t.Fatalf("reopening with an unchanged schema lost the store: %+v", stillThere)
	}
	second.Close()

	// Now say the layout changed, by writing a fingerprint no build will ever produce.
	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	admin, err := pgxpool.New(ctx, dsn+separator+"search_path="+schema)
	if err != nil {
		t.Fatalf("admin pool: %v", err)
	}
	if _, err := admin.Exec(ctx,
		`update schema_fingerprint set fingerprint = 'a-layout-from-another-binary' where id = 1`); err != nil {
		admin.Close()
		t.Fatalf("age the fingerprint: %v", err)
	}
	admin.Close()

	third := open()
	defer third.Close()
	rebuilt, err := third.Latest(ctx)
	if err != nil {
		t.Fatalf("latest after the rebuild: %v", err)
	}
	if rebuilt != nil {
		t.Errorf("the store survived a layout change at version %d; it has to be rebuilt, "+
			"not migrated", rebuilt.BlockNumber)
	}
}

// A comment is not a layout: editing schema.sql comments must not force a resync.
func TestCommentsDoNotCountAsASchemaChange(t *testing.T) {
	layout := "create table t (\n    a int -- trailing\n);\n"
	commented := "-- a new paragraph of reasoning\n--\n-- and more\ncreate table t (\n\n    a int -- a different trailing note\n);\n"

	if ingest.SchemaFingerprintForTest(layout) != ingest.SchemaFingerprintForTest(commented) {
		t.Error("editing comments changed the fingerprint, so every comment costs a full resync")
	}
	if ingest.SchemaFingerprintForTest(layout) == ingest.SchemaFingerprintForTest("create table t (\n    a bigint\n);\n") {
		t.Error("changing a column type did not change the fingerprint")
	}
}
