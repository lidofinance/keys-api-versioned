package e2e

import (
	"context"
	"fmt"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/ingest"
	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/store"
	"github.com/lidofinance/keys-api-versioned/internal/testpg"
	"github.com/lidofinance/keys-api-versioned/internal/testreg"
)

// A diff is two queries; retention landing between them drops the removals, which still apply
// cleanly and leave stale keys. The lease on the older end prevents it.
func TestPruneDuringADiffCannotDropRemovals(t *testing.T) {
	testpg.Exclusive(t)

	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 2, OperatorsPerModule: 2, KeysPerOperator: 10}
	from := testreg.At(db.Block(0))
	spec.Publish(ctx, t, db.Writer, from)

	next := spec.WithSalt(spec.Salt + 1)
	to := testreg.At(db.Block(1))
	next.PublishModules(ctx, t, db.Writer, to, 1)

	reader := db.Reader(t, "latest", 3)

	var (
		removed, added int
		pruned         ingest.Pruned
		pruneErr       error
	)
	meta, err := reader.DiffKeys(ctx, fmt.Sprint(from.BlockNumber), "latest", "",
		func(kind store.DiffKind, _ *model.Key) error {
			// Retention runs from inside the diff, on its first key.
			if removed+added == 0 {
				pruned, pruneErr = db.Writer.Prune(ctx, 1)
				if pruneErr != nil {
					return pruneErr
				}
			}
			if kind == store.DiffRemoved {
				removed++
			} else {
				added++
			}
			return nil
		})
	if err != nil {
		t.Fatalf("diff while pruning: %v", err)
	}
	if pruned.HeldAt != from.BlockNumber {
		t.Errorf("Pruned.HeldAt = %d, want %d: the diff's own lease was not seen",
			pruned.HeldAt, from.BlockNumber)
	}

	want := spec.KeysPerModule()
	if removed != want || added != want {
		t.Errorf("diff reported %d removed and %d added, want %d each", removed, added, want)
	}
	if int(meta.Removed) != removed || int(meta.Added) != added {
		t.Errorf("diff meta says %d/%d, the callback saw %d/%d",
			meta.Removed, meta.Added, removed, added)
	}
}

// Control: if this does not come back short, the lease test above proves nothing.
func TestADiffWhoseOlderEndWasDeletedComesBackShort(t *testing.T) {
	testpg.Exclusive(t)

	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 2, OperatorsPerModule: 2, KeysPerOperator: 10}
	from := testreg.At(db.Block(0))
	spec.Publish(ctx, t, db.Writer, from)

	next := spec.WithSalt(spec.Salt + 1)
	to := testreg.At(db.Block(1))
	next.PublishModules(ctx, t, db.Writer, to, 1)

	// What Prune does to rows that left the window, done directly so no lease can stop it.
	tag, err := db.Admin().Exec(ctx,
		`delete from key_version where to_block is not null and to_block <= $1`, to.BlockNumber)
	if err != nil {
		t.Fatalf("delete the closed rows: %v", err)
	}
	if tag.RowsAffected() == 0 {
		t.Fatal("no closed rows to delete, so this control has nothing to prove")
	}

	reader := db.Reader(t, "latest", 2)
	var removed, added int
	if _, err := reader.DiffKeys(ctx, fmt.Sprint(from.BlockNumber), "latest", "",
		func(kind store.DiffKind, _ *model.Key) error {
			if kind == store.DiffRemoved {
				removed++
			} else {
				added++
			}
			return nil
		}); err != nil {
		t.Fatalf("diff after the deletion: %v", err)
	}

	if removed != 0 {
		t.Errorf("the diff still reported %d removals after their rows were deleted; "+
			"then the lease in the test above is not what keeps them", removed)
	}
	if added == 0 {
		t.Error("the diff lost its additions too, so this control deleted more than retention would")
	}
}

func TestApplyingADiffToTheOlderVersionEqualsTheNewerOne(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 3, OperatorsPerModule: 2, KeysPerOperator: 20}
	from := testreg.At(db.Block(0))
	spec.Publish(ctx, t, db.Writer, from)

	next := spec.
		WithSalt(spec.Salt + 1).
		WithUsed(func(module, _, index int) bool { return module == 2 && index == 3 })
	to := testreg.At(db.Block(1))
	next.PublishModules(ctx, t, db.Writer, to, 1, 2)

	reader := db.Reader(t, "latest", 3)

	held := map[string]model.Key{}
	older := db.Reader(t, fmt.Sprint(from.BlockNumber), 2)
	if err := older.Snapshot(ctx, func(snap store.Snapshot) error {
		for _, id := range spec.ModuleIDs() {
			if err := snap.StreamKeys(ctx, testreg.ModuleAddress(id), model.KeysFilter{},
				func(k *model.Key) error {
					held[keyID(k)] = *k
					return nil
				}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("read the older version: %v", err)
	}

	if _, err := reader.DiffKeys(ctx, fmt.Sprint(from.BlockNumber), "latest", "",
		func(kind store.DiffKind, k *model.Key) error {
			if kind == store.DiffRemoved {
				delete(held, keyID(k))
			} else {
				held[keyID(k)] = *k
			}
			return nil
		}); err != nil {
		t.Fatalf("diff: %v", err)
	}

	// Module 3 was not rebuilt, so its rows are inherited at the old salt; a diff must not move them.
	verifier := testreg.NewVerifier(next).Expect(3, spec.Salt)
	for _, k := range held {
		key := k
		if err := verifier.Key(&key); err != nil {
			t.Fatalf("after applying the diff: %v", err)
		}
	}
	if err := verifier.Done(); err != nil {
		t.Fatalf("the diff applied to the older version is not the newer one: %v", err)
	}
}

// Not the pubkey: pubkeys are not unique on a real chain (Hoodi's CSM has duplicates).
func keyID(k *model.Key) string {
	return fmt.Sprintf("%s/%d/%d", k.ModuleAddress, k.OperatorIndex, k.Index)
}
