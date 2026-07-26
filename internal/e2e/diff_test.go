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

// A diff is two queries — what became visible, and what stopped being visible — so
// retention landing between them would drop one side of it whole. That failure is worse
// than an error: a diff missing its removals applies cleanly and leaves the consumer
// holding keys the chain no longer has. The lease on the older end is what prevents it,
// and the control below is what shows the risk was real.
func TestPruneDuringADiffCannotDropRemovals(t *testing.T) {
	testpg.Exclusive(t)

	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 2, OperatorsPerModule: 2, KeysPerOperator: 10}
	from := testreg.At(db.Block(0))
	spec.Publish(ctx, t, db.Writer, from)

	// Every key of module 1 changes, so the diff has a substantial removed side — the
	// side retention would be able to delete.
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
			// Retention runs from inside the diff, on its first key: keeping one version
			// makes the older end of this diff a candidate for deletion.
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

// The control for the test above: with the older end's rows deleted the way retention
// would delete them, the diff has to come back short. If it does not, the lease
// assertion above is not proving anything.
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

	// Exactly what Prune does to rows of a version that left the window, done directly so
	// no lease can stop it.
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

// Applying a diff to the version it starts from has to produce the version it ends at.
// This is the property that lets a consumer stop pulling the whole dump, and it is
// stated as an acceptance criterion in docs/next-steps.md.
func TestApplyingADiffToTheOlderVersionEqualsTheNewerOne(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 3, OperatorsPerModule: 2, KeysPerOperator: 20}
	from := testreg.At(db.Block(0))
	spec.Publish(ctx, t, db.Writer, from)

	// One module rewritten, one key deposited elsewhere: a diff that covers both shapes
	// of change the writer can record.
	next := spec.
		WithSalt(spec.Salt + 1).
		WithUsed(func(module, _, index int) bool { return module == 2 && index == 3 })
	to := testreg.At(db.Block(1))
	next.PublishModules(ctx, t, db.Writer, to, 1, 2)

	reader := db.Reader(t, "latest", 3)

	// Start from the older version, read as a map keyed by identity.
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

	// Apply the diff: removals then additions, keyed by identity.
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

	// What is held now must be the newer version, key for key. Module 3 was not part of
	// that version's rebuild, so it is still the generation it was written at — inherited
	// rows are the cheap half of a scoped rebuild, and a diff must not pretend they moved.
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

// keyID is a key's identity in the store's terms: module, operator, index. Deliberately
// not the pubkey — pubkeys are not unique on a real chain, as Hoodi's CSM shows.
func keyID(k *model.Key) string {
	return fmt.Sprintf("%s/%d/%d", k.ModuleAddress, k.OperatorIndex, k.Index)
}
