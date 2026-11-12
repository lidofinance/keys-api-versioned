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

func TestPinAndErrorMatrix(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 2, OperatorsPerModule: 1, KeysPerOperator: 5}

	empty := testpg.NewServer(t, db.Reader(t, "latest", 2))
	for _, path := range []string{"/v1/keys", "/v1/modules", "/v1/operators", "/v1/status"} {
		resp := empty.Get(t, path)
		resp.Body.Close()
		if path == "/v1/status" {
			// Status answers before there is data: that is how a consumer discovers it.
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
		{"an explicit block that is held", fmt.Sprintf("/ext/v1/keys?snapshot=%d", kept.BlockNumber), http.StatusOK},
		{"an explicit hash that is held", "/ext/v1/keys?snapshot=" + testreg.HashAt(kept.BlockNumber), http.StatusOK},
		{"a version a reorg took off the chain", "/ext/v1/keys?snapshot=" + testreg.HashAt(orphan.BlockNumber), http.StatusConflict},
		{"an unknown module id", "/v1/modules/99/keys", http.StatusNotFound},
		{"a module id that is neither an id nor an address", "/v1/modules/abc/keys", http.StatusBadRequest},
		{"a malformed pin", "/ext/v1/keys?snapshot=not-a-pin", http.StatusBadRequest},
		{"a limit above the maximum", "/ext/v1/keys?limit=200001", http.StatusBadRequest},
		{"a limit that is not a number", "/ext/v1/keys?limit=ten", http.StatusBadRequest},
		{"a diff without since", "/ext/v1/keys/diff", http.StatusBadRequest},
		{"a cursor that is not a cursor", "/ext/v1/keys?cursor=nonsense", http.StatusBadRequest},
	} {
		resp := srv.Get(t, c.path)
		body := readBody(t, resp)
		if resp.StatusCode != c.want {
			t.Errorf("%s (GET %s): status %d, want %d — body %s", c.name, c.path, resp.StatusCode, c.want, body)
		}
	}

	// Retained history is what separates 410 from 409.
	testpg.Exclusive(t)
	spec.WithSalt(2).Publish(ctx, t, db.Writer, testreg.At(db.Block(12)))
	if _, err := db.Writer.Prune(ctx, 1); err != nil {
		t.Fatalf("prune: %v", err)
	}
	gone := srv.Get(t, fmt.Sprintf("/ext/v1/keys?snapshot=%d", kept.BlockNumber))
	gone.Body.Close()
	if gone.StatusCode != http.StatusGone {
		t.Errorf("a pruned version: status %d, want 410", gone.StatusCode)
	}
}

// Records today's behaviour, not the design: an unknown hash answers 410, it should be 404.
// classifyMissingAppPin works out whether the hash was ever known, then ignores the answer.
func TestAnUnknownHashRecordsTodaysBehaviour(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 1, OperatorsPerModule: 1, KeysPerOperator: 3}
	spec.Publish(ctx, t, db.Writer, testreg.At(db.Block(0)))

	srv := testpg.NewServer(t, db.Reader(t, "latest", 2))

	unknown := srv.Get(t, "/ext/v1/keys?snapshot="+testreg.HashAt(999999))
	unknown.Body.Close()
	if unknown.StatusCode != http.StatusGone {
		t.Errorf("a hash that was never published: status %d, today's answer is 410 "+
			"(see backlog item 5 — it should become 404)", unknown.StatusCode)
	}

	badOnDiff := srv.Get(t, "/ext/v1/keys/diff?since=not-a-pin")
	badOnDiff.Body.Close()
	if badOnDiff.StatusCode != http.StatusBadRequest {
		t.Errorf("a malformed pin on the diff endpoint: status %d, want 400", badOnDiff.StatusCode)
	}
}

// A diff from a pruned version must refuse: retention deletes closed rows, so `removed` would come
// back short and still apply cleanly. The 200 before the prune keeps the 410 from being vacuous.
func TestADiffFromAPrunedVersionAnswers410(t *testing.T) {
	testpg.Exclusive(t)

	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 2, OperatorsPerModule: 2, KeysPerOperator: 10}
	from := testreg.At(db.Block(0))
	spec.Publish(ctx, t, db.Writer, from)

	next := spec.WithSalt(spec.Salt + 1)
	next.PublishModules(ctx, t, db.Writer, testreg.At(db.Block(1)), 1)

	srv := testpg.NewServer(t, db.Reader(t, "latest", 3))
	path := fmt.Sprintf("/ext/v1/keys/diff?since=%d", from.BlockNumber)

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

// The 410/404 boundary compares against min(block_number), which moves every time retention runs.
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

	// Never published; a consumer most likely read it from meta.elBlockSnapshot, which is the head.
	invented := db.Block(13)

	for _, c := range []struct {
		name string
		path string
		want int
	}{
		{"a pruned version by block", fmt.Sprintf("/ext/v1/keys?snapshot=%d", dropped.BlockNumber), http.StatusGone},
		{"a block that was never a version", fmt.Sprintf("/ext/v1/keys?snapshot=%d", invented), http.StatusNotFound},
		{"a diff from a pruned version", fmt.Sprintf("/ext/v1/keys/diff?since=%d", dropped.BlockNumber), http.StatusGone},
		{"a diff from a block that was never a version", fmt.Sprintf("/ext/v1/keys/diff?since=%d", invented), http.StatusNotFound},
		// The survivor still serves, so the answers above are about those pins, not the store.
		{"the version that was kept", fmt.Sprintf("/ext/v1/keys?snapshot=%d", kept.BlockNumber), http.StatusOK},
	} {
		resp := srv.Get(t, c.path)
		body := readBody(t, resp)
		if resp.StatusCode != c.want {
			t.Errorf("%s (GET %s): status %d, want %d — body %s",
				c.name, c.path, resp.StatusCode, c.want, body)
			continue
		}
		// ErrNotFound is shared with the module lookup, whose wording used to leak into pin 404s.
		if c.want == http.StatusNotFound && strings.Contains(body, "module") {
			t.Errorf("%s: the 404 body blames a module for a pin problem — %s", c.name, body)
		}
	}
}

// Staleness travels in a header so the JSON stays byte-identical to canonical.
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

// The finalized pin is the newest published version at or below the finalized block.
func TestTheFinalizedPinResolvesToTheNewestFinalizedVersion(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 1, OperatorsPerModule: 1, KeysPerOperator: 4}

	final := testreg.At(db.Block(10))
	spec.PublishFinal(ctx, t, db.Writer, final, final.BlockNumber)

	// Different content, so which version answered shows in the body, not only the meta.
	spec.WithSalt(1).PublishFinal(ctx, t, db.Writer, testreg.At(db.Block(11)), final.BlockNumber)
	spec.WithSalt(2).PublishFinal(ctx, t, db.Writer, testreg.At(db.Block(12)), final.BlockNumber)

	srv := testpg.NewServer(t, db.Reader(t, "latest", 3))

	safe := srv.Get(t, "/ext/v1/keys?snapshot=finalized")
	defer safe.Body.Close()
	if safe.StatusCode != http.StatusOK {
		t.Fatalf("GET ?snapshot=finalized: status %d, want 200 — body %s", safe.StatusCode, readBody(t, safe))
	}
	verifier := testreg.NewVerifier(spec)
	if _, err := verifier.V1Keys(safe.Body); err != nil {
		t.Fatalf("the finalized pin did not answer from the finalized version: %v", err)
	}
	if err := verifier.Done(); err != nil {
		t.Fatalf("the finalized pin answered from a version above the finalized block: %v", err)
	}

	// Control: without it, a pin that merely aliased latest would pass.
	head := srv.Get(t, "/v1/keys")
	defer head.Body.Close()
	if _, err := testreg.NewVerifier(spec.WithSalt(2)).V1Keys(head.Body); err != nil {
		t.Errorf("latest is not the newest version, so finalized and latest cannot be told apart: %v", err)
	}
}

// A file-fed store never has finality; 425 would have a consumer retry forever.
func TestTheFinalizedPinWithoutFinalityIsUnsupportedRatherThanTooEarly(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 1, OperatorsPerModule: 1, KeysPerOperator: 3}
	spec.Publish(ctx, t, db.Writer, testreg.At(db.Block(0)))

	srv := testpg.NewServer(t, db.Reader(t, "latest", 2))

	resp := srv.Get(t, "/ext/v1/keys?snapshot=finalized")
	body := readBody(t, resp)
	if resp.StatusCode != http.StatusNotImplemented {
		t.Errorf("?snapshot=finalized with no finality recorded: status %d, want 501 — body %s",
			resp.StatusCode, body)
	}
	if !strings.Contains(body, "finality") {
		t.Errorf("the body does not say what is missing: %s", body)
	}
}

// Retention counts versions while finality is time, so the finalized version drifts to the window
// edge; retention holds for it as for a reader's lease.
func TestRetentionDoesNotPruneTheFinalizedVersion(t *testing.T) {
	testpg.Exclusive(t)

	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 1, OperatorsPerModule: 1, KeysPerOperator: 4}
	final := testreg.At(db.Block(0))
	spec.PublishFinal(ctx, t, db.Writer, final, final.BlockNumber)

	for i := 1; i <= 3; i++ {
		spec.WithSalt(uint64(i)).PublishFinal(ctx, t, db.Writer, testreg.At(db.Block(int64(i))), final.BlockNumber)
	}

	pruned, err := db.Writer.Prune(ctx, 1)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if pruned.HeldAt != final.BlockNumber {
		t.Errorf("Pruned.HeldAt = %d, want %d: retention did not recognise the finalized version",
			pruned.HeldAt, final.BlockNumber)
	}

	srv := testpg.NewServer(t, db.Reader(t, "latest", 2))
	resp := srv.Get(t, "/ext/v1/keys?snapshot=finalized")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("the finalized pin after retention ran: status %d, want 200 — the version it names was pruned",
			resp.StatusCode)
	}
	verifier := testreg.NewVerifier(spec)
	if _, err := verifier.V1Keys(resp.Body); err != nil {
		t.Fatalf("the finalized pin no longer answers its own version: %v", err)
	}
	if err := verifier.Done(); err != nil {
		t.Fatalf("the finalized pin answered something else after retention: %v", err)
	}
}
