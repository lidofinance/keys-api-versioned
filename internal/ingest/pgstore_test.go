package ingest_test

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/ingest"
	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/store"
	"github.com/lidofinance/keys-api-versioned/internal/testpg"
	"github.com/lidofinance/keys-api-versioned/internal/testreg"
)

// These tests exercise the writer and the reader against a real Postgres, because what
// they check cannot be checked against a fake: that a second version writes only what
// moved, that a read pinned at the first version still sees the old value, that the
// diff between them reconstructs the second from the first, and that retention cannot
// delete rows from under a reader.
//
// They are skipped unless TEST_DATABASE_URL points at a database the tests may create
// and drop schemas in:
//
//	TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/keys_api_test go test ./internal/ingest/

func TestWriterVersionsAndDiff(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	// Two modules, one operator each, three keys each — the smallest shape that has a
	// module boundary and a key that can move on its own.
	spec := testreg.Spec{Modules: 2, OperatorsPerModule: 1, KeysPerOperator: 3}

	v1 := testreg.At(db.Block(0))
	spec.Publish(ctx, t, db.Writer, v1)

	// Second version: module 2's key 1 is now deposited. Nothing else moved. Driven by
	// hand rather than through testreg.Publish because what is under test is the diff
	// one module's commit reports.
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

	// The point of the layout: one key changed, so one row closed and one opened —
	// not six rewritten, and module 1 untouched.
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
	if err := build.Publish(ctx, "latest"); err != nil {
		t.Fatalf("publish v2: %v", err)
	}

	var rows int
	if err := db.Writer.Pool().QueryRow(ctx, "select count(*) from key_version").Scan(&rows); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rows != 7 {
		t.Errorf("store holds %d key rows after two versions, want 7 (6 plus the one that changed)", rows)
	}

	// A read pinned at the first version must still see the old value.
	pinned := db.Reader(t, fmt.Sprint(v1.BlockNumber), 2)
	if readUsed(ctx, t, pinned, module2, 1) {
		t.Error("the first version reports the key as deposited; it was not")
	}

	latest := db.Reader(t, "latest", 2)
	if !readUsed(ctx, t, latest, module2, 1) {
		t.Error("the latest version does not report the key as deposited")
	}

	// The diff between them has to describe exactly that change, on both sides:
	// a deposit closes the old row and opens a new one.
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

	// Retention keeps the newest version and drops what only the older one needed.
	pruned, err := db.Writer.Prune(ctx, 1)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if pruned.Versions != 1 {
		t.Errorf("prune dropped %d versions, want 1", pruned.Versions)
	}
	// Two closed rows, not one: the key that changed, and module 2's own row, which
	// the writer closed because that module's content moved. Prune counts every
	// versioned table.
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

// TestRetentionWaitsForAReaderStreamingAnOldVersion covers the one way a concurrent
// writer can still corrupt a response in this schema. A publish cannot: the reader
// selects by validity range at a pinned block, so a row closed at a later block is
// still visible and a row opened at a later block is not, whatever the writer is
// doing. A delete can, because a row that is gone matches no predicate — and
// retention deletes exactly the rows of versions falling out of the window, which is
// where a slow consumer streaming 211 MiB ends up.
//
// The control half matters as much as the check: without the lease the same prune
// must take the rows away. Otherwise the test would pass on a retention that simply
// had nothing to delete.
func TestRetentionWaitsForAReaderStreamingAnOldVersion(t *testing.T) {
	testpg.Exclusive(t)

	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 1, OperatorsPerModule: 1, KeysPerOperator: 1}
	module := spec.Module(1)

	// A version, then a second that changes the one key, so the first version's row is
	// closed and becomes retention's candidate.
	old := testreg.At(db.Block(0))
	spec.Publish(ctx, t, db.Writer, old)

	deposited := spec.WithUsed(func(int, int, int) bool { return true })
	deposited.Publish(ctx, t, db.Writer, testreg.At(db.Block(1)))

	// A reader pinned to the older version, mid-stream: the pin is held open for the
	// duration of the callback, which is what a slow dump looks like.
	reader := db.Reader(t, fmt.Sprint(old.BlockNumber), 2)

	var (
		pruned  ingest.Pruned
		seen    int
		pruneEr error
	)
	err := reader.Snapshot(ctx, func(snap store.Snapshot) error {
		// Retention runs while this reader is inside its snapshot, keeping one version.
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

	// Control: with no reader holding the version, the same prune must remove it.
	// Without this the check above would also pass on a retention that had nothing to
	// delete in the first place.
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

// readUsed reads one key's deposited flag at whatever version the reader is pinned to.
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

// TestTwoChainsCannotShareOneStore is the fix for a silent, destructive mistake: a
// database filled from one chain opened with a node on another.
//
// Before the schema was named after the chain, that looked exactly like a reorg deeper
// than the retained window — the loop rolled the store back and cold-rebuilt it, with
// nothing logged above a warning. The data was gone and the service looked healthy.
//
// The property under test is that the two stores are simply different places, so the
// mistake cannot destroy anything: fix the variable and the rows are still there. That is
// a stronger guarantee than detecting the mistake, which is why it is done this way.
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
	// Not fatal: the assertion that matters is further down, and a run that stops here
	// would report the symptom rather than the consequence.
	if got, want := first.Schema(), "chain_990001"; got != want {
		t.Errorf("schema is %q, want %q", got, want)
	}

	// A version in the first store, at a height the second one would also use.
	spec := testreg.Spec{Modules: 1, OperatorsPerModule: 1, KeysPerOperator: 2}
	at := testreg.At(4_000_001)
	spec.Publish(ctx, t, first, at)

	second := open(mainnetLike)
	defer second.Close()

	// The second store does not see the first one's version, so it has nothing to roll
	// back and nothing to delete. This is the whole point.
	latest, err := second.Latest(ctx)
	if err != nil {
		t.Fatalf("latest in the second store: %v", err)
	}
	if latest != nil {
		t.Fatalf("the store for chain %d sees a version from chain %d at block %d: "+
			"one chain's data is reachable as another's", mainnetLike, hoodiLike, latest.BlockNumber)
	}

	// And the first store still holds it, untouched — the guarantee an operator needs
	// when they fix CHAIN_ID and restart.
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

// A DSN that already names a schema wins, which is what puts every other test in this
// repo in a schema of its own — and doubles as the escape hatch for a deployment that
// wants the layout it already has.
func TestADSNWithASearchPathKeepsIt(t *testing.T) {
	db := testpg.New(t)

	if got := db.Writer.Schema(); got != "" {
		t.Errorf("the store derived schema %q from a DSN that already named one", got)
	}
}
