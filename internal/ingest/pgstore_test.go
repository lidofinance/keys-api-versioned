package ingest_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/ingest"
	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/store"
)

// This test exercises the writer and the reader against a real Postgres, because
// what it checks cannot be checked against a fake: that a second version writes only
// what moved, that a read pinned at the first version still sees the old value, and
// that the diff between them reconstructs the second from the first.
//
// It is skipped unless TEST_DATABASE_URL points at a database it may drop tables in:
//
//	TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/keys_api_test go test ./internal/ingest/

func TestWriterVersionsAndDiff(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL to run the writer integration test")
	}

	ctx := context.Background()
	pg, err := ingest.NewPGStore(ctx, dsn, ingest.PGOptions{Source: "test"})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pg.Close)

	for _, table := range []string{"key_version", "operator_version", "module_version", "versions"} {
		if _, err := pg.Pool().Exec(ctx, "drop table if exists "+table); err != nil {
			t.Fatalf("drop %s: %v", table, err)
		}
	}
	if err := pg.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	modules := []model.StakingModule{testModule(1, 100), testModule(2, 200)}

	// First version: two modules, three keys each.
	v1 := model.ELBlockSnapshot{BlockNumber: 1000, BlockHash: hashAt(1000), Timestamp: 12000}
	writeVersion(ctx, t, pg, v1, modules, func(moduleID int32, i int) model.Key {
		return testKey(moduleID, i, false)
	}, 3)

	// Second version: module 2's key 1 is now deposited. Nothing else moved.
	v2 := model.ELBlockSnapshot{BlockNumber: 1001, BlockHash: hashAt(1001), Timestamp: 12012}
	modules[1].Nonce++
	build, err := pg.Begin(ctx, v2)
	if err != nil {
		t.Fatalf("begin v2: %v", err)
	}
	keys, err := build.Keys(ctx, modules[1])
	if err != nil {
		t.Fatalf("open keys: %v", err)
	}
	for i := 0; i < 3; i++ {
		k := testKey(modules[1].ID, i, i == 1)
		if err := keys.Add(ctx, k); err != nil {
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

	for i := range modules {
		if err := build.Module(ctx, modules[i], i == 1); err != nil {
			t.Fatalf("module row: %v", err)
		}
	}
	if err := build.Publish(ctx, "latest"); err != nil {
		t.Fatalf("publish v2: %v", err)
	}

	var rows int
	if err := pg.Pool().QueryRow(ctx, "select count(*) from key_version").Scan(&rows); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rows != 7 {
		t.Errorf("store holds %d key rows after two versions, want 7 (6 plus the one that changed)", rows)
	}

	// A read pinned at the first version must still see the old value.
	reader := store.NewAppWithPool(pg.Pool(), fmt.Sprint(v1.BlockNumber))
	usedAtV1 := readUsed(ctx, t, reader, modules[1], 1)
	if usedAtV1 {
		t.Error("the first version reports the key as deposited; it was not")
	}

	reader = store.NewAppWithPool(pg.Pool(), "latest")
	if !readUsed(ctx, t, reader, modules[1], 1) {
		t.Error("the latest version does not report the key as deposited")
	}

	// The diff between them has to describe exactly that change, on both sides:
	// a deposit closes the old row and opens a new one.
	app := store.NewAppWithPool(pg.Pool(), "latest")
	var removed, added []model.Key
	meta, err := app.DiffKeys(ctx, fmt.Sprint(v1.BlockNumber), "latest", "",
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
	pruned, err := pg.Prune(ctx, 1)
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
	if err := pg.Pool().QueryRow(ctx, "select count(*) from key_version").Scan(&rows); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rows != 6 {
		t.Errorf("store holds %d key rows after pruning, want 6 current ones", rows)
	}
}

// writeVersion writes a full version of every module and publishes it.
func writeVersion(ctx context.Context, t *testing.T, pg *ingest.PGStore, at model.ELBlockSnapshot,
	modules []model.StakingModule, key func(moduleID int32, i int) model.Key, perModule int) {
	t.Helper()

	build, err := pg.Begin(ctx, at)
	if err != nil {
		t.Fatalf("begin %d: %v", at.BlockNumber, err)
	}
	for _, m := range modules {
		keys, err := build.Keys(ctx, m)
		if err != nil {
			t.Fatalf("open keys: %v", err)
		}
		for i := 0; i < perModule; i++ {
			if err := keys.Add(ctx, key(m.ID, i)); err != nil {
				t.Fatalf("add key: %v", err)
			}
		}
		if _, err := keys.Commit(ctx); err != nil {
			t.Fatalf("commit keys: %v", err)
		}
		if _, err := build.Operators(ctx, m, []model.Operator{{
			Name: "operator-0", RewardAddress: fmt.Sprintf("0x%040x", m.ID), Index: 0, Active: true,
		}}); err != nil {
			t.Fatalf("operators: %v", err)
		}
		if err := build.Module(ctx, m, true); err != nil {
			t.Fatalf("module row: %v", err)
		}
	}
	if err := build.Publish(ctx, "latest"); err != nil {
		t.Fatalf("publish %d: %v", at.BlockNumber, err)
	}
}

// readUsed returns the `used` flag of one key at the store's pin.
func readUsed(ctx context.Context, t *testing.T, s *store.App, m model.StakingModule, index int32) bool {
	t.Helper()

	found := false
	used := false
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

func testModule(id int32, nonce int32) model.StakingModule {
	m := model.StakingModule{
		ID: id, Nonce: nonce, Type: "curated-onchain-v1",
		Name: fmt.Sprintf("module-%d", id), Active: true, WithdrawalCredentialsType: 1,
	}
	m.SetRawAddress(fmt.Sprintf("0x%040x", id))
	return m
}

func testKey(moduleID int32, i int, used bool) model.Key {
	pubkey := make([]byte, 48)
	pubkey[0], pubkey[1] = byte(moduleID), byte(i)
	signature := make([]byte, 96)
	signature[0] = byte(i)

	return model.Key{
		PubkeyBin: pubkey, SignatureBin: signature,
		OperatorIndex: 0, Index: int32(i), Used: used, Vetted: true,
		ModuleAddress: fmt.Sprintf("0x%040x", moduleID),
	}
}

func hashAt(block int64) string { return fmt.Sprintf("0x%064x", block) }
