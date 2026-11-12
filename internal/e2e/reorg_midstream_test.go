package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/store"
	"github.com/lidofinance/keys-api-versioned/internal/testpg"
	"github.com/lidofinance/keys-api-versioned/internal/testreg"
)

// A reorg must remove rows under a reader, so a lease cannot save the dump. A rollback between module
// queries once left the rest empty under a 200; now every continuing query re-checks the version.

func TestAReorgDuringADumpFailsLoudlyRatherThanCompletingShort(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Streaming()
	version := testreg.At(db.Block(0))
	spec.Publish(ctx, t, db.Writer, version)

	srv := testpg.NewServer(t, db.Reader(t, "latest", 4))

	// Baseline byte count: "fewer bytes than a whole dump" survives any change to HTTP framing.
	quiet := srv.Get(t, "/v1/keys")
	whole, err := io.Copy(io.Discard, quiet.Body)
	quiet.Body.Close()
	if err != nil {
		t.Fatalf("read the baseline dump: %v", err)
	}
	srv.WaitForEvents(t, 1, waitShort)

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

	// A clean parse here would be a complete-looking answer from a branch that is gone.
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

// After the rollback no bytes went out, so it gets 409: the version existed, and the head fields say
// where the chain went.
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

	resp := srv.Get(t, "/ext/v1/keys?snapshot="+testreg.HashAt(second.BlockNumber))
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
	if conflict.HeadBlockNumber != first.BlockNumber {
		t.Errorf("409 reports head block %d, want %d", conflict.HeadBlockNumber, first.BlockNumber)
	}

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

// Operators are the last statement of the grouped stream; a rollback just before them must fail the
// read, not pair the version's keys with reopened pre-reorg operator rows.
func TestOperatorsReadAfterARollbackFail(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 1, OperatorsPerModule: 2, KeysPerOperator: 2}
	spec.Publish(ctx, t, db.Writer, testreg.At(db.Block(0)))
	renamed := testreg.At(db.Block(1))
	spec.WithSalt(1).Publish(ctx, t, db.Writer, renamed)

	err := db.Reader(t, "latest", 2).Snapshot(ctx, func(snap store.Snapshot) error {
		if _, err := db.Writer.SupersedeFrom(ctx, renamed.BlockNumber); err != nil {
			t.Fatalf("roll back: %v", err)
		}
		_, err := snap.Operators(ctx, testreg.ModuleAddress(1), model.OperatorsFilter{})
		return err
	})
	var superseded store.ErrSuperseded
	if !errors.As(err, &superseded) {
		t.Fatalf("operators read after the rollback: %v, want ErrSuperseded", err)
	}
}
