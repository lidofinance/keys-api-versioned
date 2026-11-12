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

// A dump runs one query per module, each its own snapshot: a reader picking open-ended rows stitched
// two of six dumps at a module boundary. Reads go by validity range at a pinned block instead.

func TestAPublishBetweenTwoModuleQueriesCannotStitchAPinnedDump(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 3, OperatorsPerModule: 2, KeysPerOperator: 5}
	first := testreg.At(db.Block(0))
	spec.Publish(ctx, t, db.Writer, first)

	next := spec.WithSalt(spec.Salt + 1)
	staged := next.Fill(ctx, t, db.Writer, testreg.At(db.Block(1)), next.ModuleIDs()...)

	reader := db.Reader(t, "latest", 2)
	verifier := testreg.NewVerifier(spec)

	published := false
	err := reader.Snapshot(ctx, func(snap store.Snapshot) error {
		for _, id := range spec.ModuleIDs() {
			err := snap.StreamKeys(ctx, testreg.ModuleAddress(id), model.KeysFilter{},
				func(k *model.Key) error {
					// Happens-before, not a race: Publish commits before the next module's query is sent.
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

	// The write really landed, or the test above passes with nothing to be confused by.
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

// Control: open rows expose a build still in progress (Publish only flips a status), so two naive
// reads straddle a rewritten and an untouched module. If they agree, the pinned test proves nothing.
func TestTheNaivePredicateAnswersFromTwoVersionsAtOnce(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 3, OperatorsPerModule: 2, KeysPerOperator: 5}
	spec.Publish(ctx, t, db.Writer, testreg.At(db.Block(0)))

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

// naiveSalt reads the way the rejected implementation did: open rows are current.
func naiveSalt(ctx context.Context, t *testing.T, db *testpg.DB, moduleID int) uint64 {
	t.Helper()

	// Addresses are stored as bytes, once in module_version; key rows carry a two-byte id.
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

// Same property over HTTP: the body is held after the first bytes while a version is published.
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
	// Inside the response; otherwise the publish could land after the whole dump was served.
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
