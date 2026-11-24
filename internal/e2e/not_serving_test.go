package e2e

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/testpg"
	"github.com/lidofinance/keys-api-versioned/internal/testreg"
)

// Third parties run the service: the cause of a 425 is in the server's log, and in the readiness
// body when the pod is not ready.
func TestTheCauseOfA425IsNamed(t *testing.T) {
	for _, tc := range []struct {
		name    string
		setup   string
		publish bool
		ready   int
		reason  string
	}{
		{name: "first sync", ready: http.StatusServiceUnavailable, reason: "no published version yet"},
		{name: "older writer", publish: true, ready: http.StatusServiceUnavailable, reason: "the writer runs an older release",
			setup: `update schema_fingerprint set version = 0; drop table serving_hold`},
		{name: "held", publish: true, ready: http.StatusOK, reason: "held since",
			setup: `insert into serving_hold (id) values (1)`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := testpg.New(t)
			ctx := context.Background()
			if tc.publish {
				testreg.Spec{Modules: 1, OperatorsPerModule: 1, KeysPerOperator: 1}.Publish(ctx, t, db.Writer, testreg.At(db.Block(0)))
			}
			if tc.setup != "" {
				if _, err := db.Writer.Pool().Exec(ctx, tc.setup); err != nil {
					t.Fatal(err)
				}
			}
			var logs bytes.Buffer
			reader := db.Reader(t, "latest", 2)
			reader.Log = slog.New(slog.NewTextHandler(&logs, nil))
			srv := testpg.NewServer(t, reader)

			if got := statusOf(t, srv, "/v1/keys"); got != http.StatusTooEarly {
				t.Fatalf("/v1/keys: status %d, want 425", got)
			}
			if !strings.Contains(logs.String(), tc.reason) {
				t.Errorf("server log %q does not name %q", logs.String(), tc.reason)
			}
			resp := srv.Get(t, "/health/ready")
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != tc.ready {
				t.Errorf("/health/ready: status %d, want %d", resp.StatusCode, tc.ready)
			}
			if tc.ready != http.StatusOK && !strings.Contains(string(body), tc.reason) {
				t.Errorf("readiness body %q does not name %q", body, tc.reason)
			}
		})
	}
}
