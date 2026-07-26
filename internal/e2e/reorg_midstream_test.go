package e2e

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/testpg"
	"github.com/lidofinance/keys-api-versioned/internal/testreg"
)

// A reorg is the one case where rows must disappear from under a reader: a version off
// the chain has to stop being current, or the next one would inherit the losing
// branch's keys. So a lease cannot save an in-flight dump, and the only question is
// whether the consumer is told.
//
// It was not. A dump runs one query per module, the rollback landed between two of
// them, and the remaining modules came back empty while the handler finished the JSON —
// a valid, complete-looking answer assembled from a branch that no longer existed. The
// fix re-checks the version at every query that continues a response. These tests are
// what makes that claim checkable, because the failure they describe is invisible from
// the status code: it is 200 either way.

func TestAReorgDuringADumpFailsLoudlyRatherThanCompletingShort(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Streaming()
	version := testreg.At(db.Block(0))
	spec.Publish(ctx, t, db.Writer, version)

	srv := testpg.NewServer(t, db.Reader(t, "latest", 4))

	// A complete dump first, for its byte count: "fewer bytes than a whole dump" is the
	// assertion that survives any change to HTTP framing, and it needs a baseline
	// measured on this machine with this dataset rather than a guess.
	quiet := srv.Get(t, "/v1/keys")
	whole, err := io.Copy(io.Discard, quiet.Body)
	quiet.Body.Close()
	if err != nil {
		t.Fatalf("read the baseline dump: %v", err)
	}
	srv.WaitForEvents(t, 1, waitShort)

	// Now hold a dump open inside the first module and take its version off the chain.
	resp, body := srv.GetHeld(t, "/v1/keys", 64<<10)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/keys: status %d, want 200", resp.StatusCode)
	}
	if srv.Pending() != 1 {
		t.Fatalf("%d requests in flight, want the held dump to be one", srv.Pending())
	}

	if _, err := db.Writer.SupersedeFrom(ctx, version.BlockNumber); err != nil {
		t.Fatalf("roll the version back: %v", err)
	}

	// The client must not be handed a document that parses. Draining the body and
	// decoding it is the check: a clean parse means a complete-looking answer from a
	// branch that is gone, which is the defect.
	drained, readErr := io.ReadAll(body())
	if json.Valid(drained) {
		t.Errorf("the body parsed as a complete document (%d bytes, read error %v): "+
			"the client was handed a complete-looking answer from a superseded version",
			len(drained), readErr)
	}

	srv.WaitForEvents(t, 2, waitLong)
	event, _ := srv.LastEvent()
	if event.Status != http.StatusOK {
		t.Errorf("the aborted dump was reported with status %d, want 200 — the status was "+
			"already sent when it failed", event.Status)
	}
	if event.Bytes >= whole {
		t.Errorf("the aborted dump sent %d bytes, a whole one is %d: it did not stop early",
			event.Bytes, whole)
	}
	if !srv.Logs.Has(slogError, "request failed mid-response") {
		t.Errorf("the service did not record the failure; it logged: %v", srv.Logs.Messages(slogError))
	}
}

// A request that arrives after the rollback is a different case, and an easier one: no
// bytes have gone out, so it gets a status. 409 rather than 404 or 410 because the
// version existed and the consumer needs to know where the chain went instead.
func TestAPinnedReadOfASupersededVersionAnswers409(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 2, OperatorsPerModule: 1, KeysPerOperator: 4}
	first := testreg.At(db.Block(0))
	spec.Publish(ctx, t, db.Writer, first)

	next := spec.WithSalt(spec.Salt + 1)
	second := testreg.At(db.Block(1))
	next.Publish(ctx, t, db.Writer, second)

	if _, err := db.Writer.SupersedeFrom(ctx, second.BlockNumber); err != nil {
		t.Fatalf("roll back: %v", err)
	}

	srv := testpg.NewServer(t, db.Reader(t, "latest", 3))

	resp := srv.Get(t, "/v1/keys?snapshot="+testreg.HashAt(second.BlockNumber))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("pinned read of a superseded version: status %d, want 409", resp.StatusCode)
	}

	var conflict struct {
		StatusCode               int    `json:"statusCode"`
		Message                  string `json:"message"`
		HeadBlockNumber          int64  `json:"headBlockNumber"`
		HeadLastChangedBlockHash string `json:"headLastChangedBlockHash"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&conflict); err != nil {
		t.Fatalf("decode the conflict body: %v", err)
	}
	// The head fields are the point of 409 over 410: they let a consumer jump straight
	// to the version that replaced the one it asked for.
	if conflict.HeadBlockNumber != first.BlockNumber {
		t.Errorf("409 reports head block %d, want %d", conflict.HeadBlockNumber, first.BlockNumber)
	}

	// And the version that survived the rollback still serves.
	current := srv.Get(t, "/v1/keys")
	defer current.Body.Close()
	if current.StatusCode != http.StatusOK {
		t.Fatalf("the surviving version answers %d, want 200", current.StatusCode)
	}
	verifier := testreg.NewVerifier(spec)
	if _, err := verifier.V1Keys(current.Body); err != nil {
		t.Fatalf("verify the surviving version: %v", err)
	}
	if err := verifier.Done(); err != nil {
		t.Fatalf("the surviving version is not the one written before the rollback: %v", err)
	}
}
