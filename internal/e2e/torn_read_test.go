package e2e

import (
	"context"
	"net/http"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/store"
	"github.com/lidofinance/keys-api-versioned/internal/testpg"
	"github.com/lidofinance/keys-api-versioned/internal/testreg"
)

// A dump is not one query: it runs one per module, each in its own implicit
// transaction and therefore its own snapshot. So a version published between two of
// them is visible to the second and not the first, and a reader that decides what is
// current by looking for open-ended rows would answer from two versions at once —
// module 1 from the old one, module 2 from the new one. That happened here: two of six
// dumps came back stitched at a module boundary.
//
// The fix was to read by validity range at a pinned block instead. These tests hold it
// in place, and the control below is what makes them mean something: it runs the same
// interleaving against the naive predicate and requires the stitching to appear. A
// test that passes both with and without the property it claims to check is measuring
// nothing.

func TestAPublishBetweenTwoModuleQueriesCannotStitchAPinnedDump(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 3, OperatorsPerModule: 2, KeysPerOperator: 5}
	first := testreg.At(db.Block(0))
	spec.Publish(ctx, t, db.Writer, first)

	// The next version, staged but not published: everything expensive is already done,
	// so the only thing landing inside the interleaving window is one row update.
	next := spec.WithSalt(spec.Salt + 1)
	staged := next.Fill(ctx, t, db.Writer, testreg.At(db.Block(1)), next.ModuleIDs()...)

	reader := db.Reader(t, "latest", 2)
	verifier := testreg.NewVerifier(spec)

	published := false
	err := reader.Snapshot(ctx, func(snap store.Snapshot) error {
		for _, id := range spec.ModuleIDs() {
			err := snap.StreamKeys(ctx, testreg.ModuleAddress(id), model.KeysFilter{},
				func(k *model.Key) error {
					// Publish synchronously, on the first key of the first module. This is a
					// happens-before, not a race: Publish has committed before the query for
					// the next module is sent.
					if !published {
						if err := staged.Publish(ctx, "latest", 0); err != nil {
							return err
						}
						published = true
					}
					return verifier.Key(k)
				})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read while publishing: %v", err)
	}
	if !published {
		t.Fatal("the new version was never published, so nothing was interleaved")
	}
	if err := verifier.Done(); err != nil {
		t.Fatalf("the pinned dump was not one version: %v", err)
	}

	// And the write really landed — otherwise the test above would pass because there
	// was nothing to be confused by.
	fresh := testreg.NewVerifier(next)
	freshReader := db.Reader(t, "latest", 2)
	if err := freshReader.Snapshot(ctx, func(snap store.Snapshot) error {
		for _, id := range next.ModuleIDs() {
			if err := snap.StreamKeys(ctx, testreg.ModuleAddress(id), model.KeysFilter{}, fresh.Key); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("read the new version: %v", err)
	}
	if err := fresh.Done(); err != nil {
		t.Fatalf("the published version is not readable as itself: %v", err)
	}
}

// TestTheNaivePredicateAnswersFromTwoVersionsAtOnce is the control, and writing it
// turned out to show the rejected predicate is worse than "occasionally stitched".
//
// The first attempt at this control interleaved a *publish* between two naive reads and
// expected them to disagree. They agreed — both saw the new content — because rows do
// not arrive at publication time. A build inserts its rows as it goes and Publish only
// flips a status, which is the whole point of the layout. So "the rows with no to_block"
// exposes a version that is still being written, and the two reads below straddle a
// module the build has already rewritten and one it has not: one response, two versions,
// neither of them published.
//
// If this ever comes back consistent, the mechanism the pinned-dump test relies on has
// changed and that test is passing for the wrong reason.
func TestTheNaivePredicateAnswersFromTwoVersionsAtOnce(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 3, OperatorsPerModule: 2, KeysPerOperator: 5}
	spec.Publish(ctx, t, db.Writer, testreg.At(db.Block(0)))

	// A build in progress: module 1 rewritten, module 2 not touched yet, nothing
	// published.
	next := spec.WithSalt(spec.Salt + 1)
	staged := next.Fill(ctx, t, db.Writer, testreg.At(db.Block(1)), 1)

	firstModule := naiveSalt(ctx, t, db, 1)
	secondModule := naiveSalt(ctx, t, db, 2)

	if firstModule == secondModule {
		t.Fatalf("both naive reads saw salt %d, so an in-flight build is not visible to the "+
			"rejected predicate any more: the pinned-dump test is passing for the wrong reason",
			firstModule)
	}
	if firstModule != next.Salt || secondModule != spec.Salt {
		t.Errorf("naive reads saw salts %d and %d, want %d (the unpublished build) and %d (the published version)",
			firstModule, secondModule, next.Salt, spec.Salt)
	}

	// At that same moment a pinned reader is consistent, and on the published version —
	// which is the difference the predicate makes.
	verifier := testreg.NewVerifier(spec)
	reader := db.Reader(t, "latest", 2)
	if err := reader.Snapshot(ctx, func(snap store.Snapshot) error {
		for _, id := range spec.ModuleIDs() {
			if err := snap.StreamKeys(ctx, testreg.ModuleAddress(id), model.KeysFilter{}, verifier.Key); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("pinned read during a build: %v", err)
	}
	if err := verifier.Done(); err != nil {
		t.Fatalf("a pinned read during an in-flight build was not the published version: %v", err)
	}

	if err := staged.Publish(ctx, "latest", 0); err != nil {
		t.Fatalf("publish the staged version: %v", err)
	}
}

// naiveSalt reads one key of a module the way the rejected implementation did — "open
// rows are the current version" — and reports which generation it came from.
func naiveSalt(ctx context.Context, t *testing.T, db *testpg.DB, moduleID int) uint64 {
	t.Helper()

	// Addresses are stored as bytes, not as the 42-character string: the schema keeps the
	// address once in module_version and puts a two-byte id on every key row.
	address, err := model.DecodeHex(testreg.ModuleAddress(moduleID))
	if err != nil {
		t.Fatalf("decode module address: %v", err)
	}

	var pubkey []byte
	err = db.Admin().QueryRow(ctx, `
		select k.pubkey from key_version k
		  join module_version m on m.module_id = k.module_id and m.to_block is null
		 where m.address = $1 and k.to_block is null
		 limit 1`, address).Scan(&pubkey)
	if err != nil {
		t.Fatalf("naive read of module %d: %v", moduleID, err)
	}
	id, ok := testreg.Decode(pubkey)
	if !ok {
		t.Fatalf("naive read of module %d returned a key this registry did not generate", moduleID)
	}
	return id.Salt
}

// TestAPublishDuringAStreamedDumpDoesNotChangeIt is the same property over HTTP, where
// the interleaving comes from the client rather than from a callback: the body is held
// after the first bytes, a version is published while the server sits blocked inside
// the response, and the dump still has to be one version.
func TestAPublishDuringAStreamedDumpDoesNotChangeIt(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Streaming()
	spec.Publish(ctx, t, db.Writer, testreg.At(db.Block(0)))

	srv := testpg.NewServer(t, db.Reader(t, "latest", 4))

	resp, body := srv.GetHeld(t, "/v1/keys", 64<<10)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/keys: status %d, want 200", resp.StatusCode)
	}
	// The server is inside the response: it has sent bytes and has not finished. Without
	// this the publish below could land after the whole dump was already served.
	if srv.Pending() != 1 {
		t.Fatalf("%d requests in flight, want the dump to be one of them", srv.Pending())
	}

	next := spec.WithSalt(spec.Salt + 1)
	next.Publish(ctx, t, db.Writer, testreg.At(db.Block(1)))

	verifier := testreg.NewVerifier(spec)
	if _, err := verifier.V1Keys(body()); err != nil {
		t.Fatalf("verify the held dump: %v", err)
	}
	if err := verifier.Done(); err != nil {
		t.Fatalf("the held dump was not one version: %v", err)
	}

	srv.WaitForEvents(t, 1, waitLong)
	if event, _ := srv.LastEvent(); event.Status != http.StatusOK {
		t.Errorf("the service reported status %d for the held dump", event.Status)
	}
	if failures := srv.Logs.Messages(slogError); len(failures) > 0 {
		t.Errorf("the service logged failures during a dump that should have been fine: %v", failures)
	}
}
