package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/testpg"
	"github.com/lidofinance/keys-api-versioned/internal/testreg"
)

// The states a pinned read can land in are more numerous here than in the canonical
// service, because a version has a life: it is built, published, confirmed, marked
// stale, superseded by a reorg, and finally pruned. Each of those has to be a distinct
// answer, since a consumer's correct reaction differs — retry, follow the head, give up,
// or fix its request.
//
// Two of the cases below record what the service does today rather than what it should
// do; they are marked, and each names the backlog item that would change it. A test that
// quietly encodes the intended behaviour instead would pass only in a future that has
// not arrived.

func TestPinAndErrorMatrix(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 2, OperatorsPerModule: 1, KeysPerOperator: 5}

	// Nothing published yet: the canonical 425, on every read endpoint.
	empty := testpg.NewServer(t, db.Reader(t, "latest", 2))
	for _, path := range []string{"/v1/keys", "/v1/modules", "/v1/operators", "/v1/status"} {
		resp := empty.Get(t, path)
		resp.Body.Close()
		if path == "/v1/status" {
			// Status answers even before there is data — it is how a consumer discovers that.
			if resp.StatusCode != http.StatusOK {
				t.Errorf("GET %s on an empty store: status %d, want 200", path, resp.StatusCode)
			}
			continue
		}
		if resp.StatusCode != http.StatusTooEarly {
			t.Errorf("GET %s on an empty store: status %d, want 425", path, resp.StatusCode)
		}
	}
	ready := empty.Get(t, "/health/ready")
	ready.Body.Close()
	if ready.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("GET /health/ready on an empty store: status %d, want 503", ready.StatusCode)
	}

	// One published version, one superseded, one pruned.
	kept := testreg.At(db.Block(10))
	spec.Publish(ctx, t, db.Writer, kept)

	orphan := testreg.At(db.Block(11))
	spec.WithSalt(1).Publish(ctx, t, db.Writer, orphan)
	if _, err := db.Writer.SupersedeFrom(ctx, orphan.BlockNumber); err != nil {
		t.Fatalf("supersede: %v", err)
	}

	srv := testpg.NewServer(t, db.Reader(t, "latest", 3))

	for _, c := range []struct {
		name string
		path string
		want int
	}{
		{"the current version", "/v1/keys", http.StatusOK},
		{"an explicit block that is held", fmt.Sprintf("/v1/keys?snapshot=%d", kept.BlockNumber), http.StatusOK},
		{"an explicit hash that is held", "/v1/keys?snapshot=" + testreg.HashAt(kept.BlockNumber), http.StatusOK},
		{"a version a reorg took off the chain", "/v1/keys?snapshot=" + testreg.HashAt(orphan.BlockNumber), http.StatusConflict},
		{"an unknown module id", "/v1/modules/99/keys", http.StatusNotFound},
		{"a module id that is neither an id nor an address", "/v1/modules/abc/keys", http.StatusBadRequest},
		{"a malformed pin", "/v1/keys?snapshot=not-a-pin", http.StatusBadRequest},
		{"a limit above the maximum", "/v1/keys?limit=200001", http.StatusBadRequest},
		{"a limit that is not a number", "/v1/keys?limit=ten", http.StatusBadRequest},
		{"a diff without since", "/v1/keys/diff", http.StatusBadRequest},
		{"a cursor that is not a cursor", "/v1/keys?cursor=nonsense", http.StatusBadRequest},
	} {
		resp := srv.Get(t, c.path)
		body := readBody(t, resp)
		if resp.StatusCode != c.want {
			t.Errorf("%s (GET %s): status %d, want %d — body %s", c.name, c.path, resp.StatusCode, c.want, body)
		}
	}

	// A pruned version: retained history is what separates 410 from 409.
	testpg.Exclusive(t)
	spec.WithSalt(2).Publish(ctx, t, db.Writer, testreg.At(db.Block(12)))
	if _, err := db.Writer.Prune(ctx, 1); err != nil {
		t.Fatalf("prune: %v", err)
	}
	gone := srv.Get(t, fmt.Sprintf("/v1/keys?snapshot=%d", kept.BlockNumber))
	gone.Body.Close()
	if gone.StatusCode != http.StatusGone {
		t.Errorf("a pruned version: status %d, want 410", gone.StatusCode)
	}
}

// TestAnUnknownHashRecordsTodaysBehaviour pins down one answer that is not the one the
// design asks for, so that fixing it is a test change rather than a discovery: a hash
// that was never published answers 410 "no longer retained". classifyMissingAppPin works
// out whether the hash was ever known and then ignores the answer, which makes the code
// read as though item 5 of the backlog were done. It is not.
func TestAnUnknownHashRecordsTodaysBehaviour(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 1, OperatorsPerModule: 1, KeysPerOperator: 3}
	spec.Publish(ctx, t, db.Writer, testreg.At(db.Block(0)))

	srv := testpg.NewServer(t, db.Reader(t, "latest", 2))

	unknown := srv.Get(t, "/v1/keys?snapshot="+testreg.HashAt(999999))
	unknown.Body.Close()
	if unknown.StatusCode != http.StatusGone {
		t.Errorf("a hash that was never published: status %d, today's answer is 410 "+
			"(see backlog item 5 — it should become 404)", unknown.StatusCode)
	}

	badOnDiff := srv.Get(t, "/v1/keys/diff?since=not-a-pin")
	badOnDiff.Body.Close()
	if badOnDiff.StatusCode != http.StatusBadRequest {
		t.Errorf("a malformed pin on /v1/keys/diff: status %d, want 400", badOnDiff.StatusCode)
	}
}

// TestADiffFromAPrunedVersionAnswers410 covers the answer a consumer that fell behind
// actually gets, which until now was only covered through `?snapshot=` on /v1/keys.
//
// It matters more on this endpoint than on that one. A pinned dump of a version that is
// gone can only fail; a diff from it could plausibly be computed, and the result would
// apply cleanly and be wrong. Retention deletes closed rows, so the `removed` side —
// and only that side — comes back short: the consumer would keep keys the chain no
// longer has, with nothing in the response saying so. TestADiffWhoseOlderEndWasDeleted-
// ComesBackShort demonstrates that at the store level; this one holds the HTTP contract
// that refuses instead.
//
// The 200 before the prune is what stops this from being vacuous: without it, a diff
// endpoint that answered 410 to everything would pass.
func TestADiffFromAPrunedVersionAnswers410(t *testing.T) {
	testpg.Exclusive(t)

	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 2, OperatorsPerModule: 2, KeysPerOperator: 10}
	from := testreg.At(db.Block(0))
	spec.Publish(ctx, t, db.Writer, from)

	// One module rewritten, so the diff has a substantial removed side — the side
	// retention is able to delete.
	next := spec.WithSalt(spec.Salt + 1)
	next.PublishModules(ctx, t, db.Writer, testreg.At(db.Block(1)), 1)

	srv := testpg.NewServer(t, db.Reader(t, "latest", 3))
	path := fmt.Sprintf("/v1/keys/diff?since=%d", from.BlockNumber)

	before := srv.Get(t, path)
	body := readBody(t, before)
	if before.StatusCode != http.StatusOK {
		t.Fatalf("a diff from a held version: status %d, want 200 — body %s", before.StatusCode, body)
	}
	var answered struct {
		Meta struct {
			Removed int `json:"removed"`
			Added   int `json:"added"`
		} `json:"meta"`
	}
	if err := json.Unmarshal([]byte(body), &answered); err != nil {
		t.Fatalf("decode the diff: %v — body %s", err, body)
	}
	if answered.Meta.Removed == 0 {
		t.Fatalf("the diff reported no removals, so the side retention would delete is empty "+
			"and the 410 below proves nothing — body %s", body)
	}

	// The version the consumer holds leaves the retention window while it is away.
	pruned, err := db.Writer.Prune(ctx, 1)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if pruned.Versions == 0 {
		t.Fatal("nothing was pruned, so the diff below is not resuming from a version that is gone")
	}

	gone := srv.Get(t, path)
	goneBody := readBody(t, gone)
	if gone.StatusCode != http.StatusGone {
		t.Errorf("a diff whose `since` left the retention window: status %d, want 410 — body %s",
			gone.StatusCode, goneBody)
	}
}

// TestANumericPinTellsAPrunedVersionApartFromOneThatNeverExisted pins the distinction
// that decides what a consumer does next: 410 means "you are too far behind, resync",
// 404 means "that is not a version, fix your request".
//
// It is worth holding in place because the two answers come from one branch of
// classifyMissingAppPin whose condition is a comparison against min(block_number) — a
// value that moves every time retention runs. And because the equivalent distinction for
// *hash* pins does not work at all: see TestAnUnknownHashRecordsTodaysBehaviour and
// backlog item 5.
//
// Both endpoints that resolve a pin are checked, since a consumer meets this on either.
func TestANumericPinTellsAPrunedVersionApartFromOneThatNeverExisted(t *testing.T) {
	testpg.Exclusive(t)

	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 1, OperatorsPerModule: 1, KeysPerOperator: 5}
	dropped := testreg.At(db.Block(10))
	spec.Publish(ctx, t, db.Writer, dropped)

	kept := testreg.At(db.Block(12))
	spec.WithSalt(1).Publish(ctx, t, db.Writer, kept)

	if _, err := db.Writer.Prune(ctx, 1); err != nil {
		t.Fatalf("prune: %v", err)
	}

	srv := testpg.NewServer(t, db.Reader(t, "latest", 3))

	// Above the oldest version still held, and never published: a block number the
	// consumer could only have invented — or, more likely, read out of
	// meta.elBlockSnapshot, which carries the head rather than the version's identity.
	invented := db.Block(13)

	for _, c := range []struct {
		name string
		path string
		want int
	}{
		{"a pruned version by block", fmt.Sprintf("/v1/keys?snapshot=%d", dropped.BlockNumber), http.StatusGone},
		{"a block that was never a version", fmt.Sprintf("/v1/keys?snapshot=%d", invented), http.StatusNotFound},
		{"a diff from a pruned version", fmt.Sprintf("/v1/keys/diff?since=%d", dropped.BlockNumber), http.StatusGone},
		{"a diff from a block that was never a version", fmt.Sprintf("/v1/keys/diff?since=%d", invented), http.StatusNotFound},
		// The version that survived retention still serves, which is what makes the two
		// answers above statements about those pins rather than about the store.
		{"the version that was kept", fmt.Sprintf("/v1/keys?snapshot=%d", kept.BlockNumber), http.StatusOK},
	} {
		resp := srv.Get(t, c.path)
		body := readBody(t, resp)
		if resp.StatusCode != c.want {
			t.Errorf("%s (GET %s): status %d, want %d — body %s",
				c.name, c.path, resp.StatusCode, c.want, body)
			continue
		}
		// A 404 about a pin must not talk about modules. It used to: ErrNotFound is
		// shared with the module lookup and hardcoded that wording, so a consumer
		// asking for a version that never existed was told its module was unsupported.
		if c.want == http.StatusNotFound && strings.Contains(body, "module") {
			t.Errorf("%s: the 404 body blames a module for a pin problem — %s", c.name, body)
		}
	}
}

// A stale version keeps being served, with the staleness in a header rather than in the
// body — so a consumer that ignores headers sees the canonical contract unchanged, and
// one that reads them knows what it has. The header must also arrive before the body,
// which for HTTP is automatic and worth asserting anyway: it is the reason the signal
// cannot describe anything that happens later in the stream.
func TestStalenessTravelsInTheHeaderAndTheBodyIsUnchanged(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 2, OperatorsPerModule: 2, KeysPerOperator: 10}
	version := testreg.At(db.Block(0))
	spec.Publish(ctx, t, db.Writer, version)

	srv := testpg.NewServer(t, db.Reader(t, "latest", 3))

	fresh := srv.Get(t, "/v1/keys")
	freshBody := readBody(t, fresh)
	if got := fresh.Header.Get("X-Snapshot-Stale"); got != "" {
		t.Errorf("a fresh version carries X-Snapshot-Stale: %q", got)
	}

	// The writer decides a version no longer reflects the chain. It stays served.
	stored, err := db.Writer.Latest(ctx)
	if err != nil || stored == nil {
		t.Fatalf("read the stored version: %v", err)
	}
	if err := db.Writer.MarkStale(ctx, *stored); err != nil {
		t.Fatalf("mark stale: %v", err)
	}

	stale := srv.Get(t, "/v1/keys")
	staleBody := readBody(t, stale)
	if stale.StatusCode != http.StatusOK {
		t.Fatalf("a stale version answers %d, want 200: a consistent known-stale answer "+
			"beats an error", stale.StatusCode)
	}
	if got := stale.Header.Get("X-Snapshot-Stale"); got != "true" {
		t.Errorf("X-Snapshot-Stale is %q on a stale version, want \"true\"", got)
	}
	if staleBody != freshBody {
		t.Error("the body changed when the version was marked stale; the staleness is " +
			"supposed to live in the header so the JSON stays byte-identical to canonical")
	}
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()

	var buf []byte
	chunk := make([]byte, 32<<10)
	for {
		n, err := resp.Body.Read(chunk)
		buf = append(buf, chunk[:n]...)
		if err != nil {
			break
		}
		if len(buf) > 8<<20 {
			break
		}
	}
	return string(buf)
}
