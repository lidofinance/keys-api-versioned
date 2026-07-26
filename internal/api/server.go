// Package api serves the keys-api surface: keys, module keys, modules, operators,
// the combined streams, and status.
//
// The endpoints exist because consumers call them. ethereum-validators-monitoring
// reads /v1/keys?used=true, /v1/operators, /v1/modules and /v1/status;
// lido-mev-monitoring reads /v1/status, /v1/modules and the /v2 combined stream.
// Response framing is byte-identical to the canonical service, whitespace included,
// so pointing a consumer here is a URL change.
//
// Still not implemented: the validator exit endpoints (they need consensus layer
// ingest) and the OpenAPI document.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/lidofinance/keys-api-versioned/internal/store"
)

// Config holds the serving configuration.
type Config struct {
	ChainID    int64
	AppVersion string
	Log        *slog.Logger
	// MetricsHandler is mounted at /metrics when set, and Observe receives one call
	// per served request. The API depends on neither a metrics library nor a
	// registry; the app passes both in.
	MetricsHandler http.Handler
	Observe        ObserveFunc
}

// Server wires the routes to a store.
type Server struct {
	store store.Store
	cfg   Config
}

// New builds the HTTP handler.
func New(s store.Store, cfg Config) *Server {
	if cfg.Log == nil {
		cfg.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Server{store: s, cfg: cfg}
}

// route is one entry in the router. Routes are data rather than a sequence of
// registration calls so that the OpenAPI document can be checked against them: a
// route missing from the spec, or a spec path that no longer exists, fails a test
// instead of being discovered by a consumer.
type route struct {
	pattern string
	handler http.HandlerFunc
	// undocumented marks operational endpoints that are deliberately not in the
	// API document: health probes and the metrics scrape.
	undocumented bool
}

// routes lists everything the service serves.
func (s *Server) routes() []route {
	return []route{
		{pattern: "GET /v1/keys", handler: s.handleKeys},
		{pattern: "GET /v1/keys/diff", handler: s.handleKeysDiff},
		{pattern: "GET /v1/keys/{pubkey}", handler: s.handleKeyByPubkey},
		{pattern: "POST /v1/keys/find", handler: s.handleKeysFind},

		{pattern: "GET /v1/modules", handler: s.handleModules},
		{pattern: "GET /v1/modules/diff", handler: s.handleModulesDiff},
		{pattern: "GET /v1/modules/keys", handler: s.handleGroupedKeys},
		{pattern: "GET /v1/modules/{module_id}", handler: s.handleModule},
		{pattern: "GET /v1/modules/{module_id}/keys", handler: s.handleModuleKeys},
		{pattern: "GET /v1/modules/{module_id}/keys/diff", handler: s.handleKeysDiff},
		{pattern: "POST /v1/modules/{module_id}/keys/find", handler: s.handleModuleKeysFind},

		// Operators, and the two combined streams. These are what the consumers read:
		// ethereum-validators-monitoring takes /v1/operators with /v1/keys?used=true,
		// and lido-mev-monitoring takes /v2/modules/operators/keys.
		{pattern: "GET /v1/operators", handler: s.handleOperators},
		{pattern: "GET /v1/modules/{module_id}/operators", handler: s.handleModuleOperators},
		{pattern: "GET /v1/modules/{module_id}/operators/{operator_id}", handler: s.handleModuleOperator},
		{pattern: "GET /v1/modules/{module_id}/operators/keys", handler: s.handleModuleOperatorsKeys},
		{pattern: "GET /v2/modules/operators/keys", handler: s.handleModulesOperatorsKeysStream},

		{pattern: "GET /v1/status", handler: s.handleStatus},

		// The API document, and a page that renders it.
		{pattern: "GET /openapi.yaml", handler: handleOpenAPISpec},
		{pattern: "GET /docs", handler: handleDocs},

		// Liveness answers as long as the process is up. Readiness answers only once a
		// version is published, so a pod still doing its first sync is not sent traffic
		// that would get a 425.
		{pattern: "GET /health", undocumented: true, handler: func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
		}},
		{pattern: "GET /health/ready", undocumented: true, handler: s.handleReady},
	}
}

// Handler returns the router.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	for _, r := range s.routes() {
		mux.HandleFunc(r.pattern, r.handler)
	}

	if s.cfg.MetricsHandler != nil {
		mux.Handle("GET /metrics", s.cfg.MetricsHandler)
	}

	return withLogging(mux, s.cfg.Log, s.cfg.Observe)
}

// snapshot resolves the version one request is served from: the pin in
// ?snapshot=<latest|safe|block|hash> when given, otherwise the configured default.
//
// An explicit pin is what lets a consumer make several requests against one
// version — a dump and then a follow-up query — and get a definite answer if that
// version has gone away, instead of a quietly different one.
func (s *Server) snapshot(ctx context.Context, r *http.Request, fn func(store.Snapshot) error) error {
	return s.snapshotAt(ctx, r, r.URL.Query().Get("snapshot"), fn)
}

// snapshotAt is snapshot with the pin decided by the caller, which paging needs: a
// page follows the version named by its cursor rather than whatever is current now.
func (s *Server) snapshotAt(ctx context.Context, r *http.Request, pin string, fn func(store.Snapshot) error) error {
	if pin == "" {
		return s.store.Snapshot(ctx, fn)
	}
	pinned, ok := s.store.(store.PinnedStore)
	if !ok {
		return store.ErrPinUnsupported{}
	}
	return pinned.PinnedSnapshot(ctx, pin, fn)
}

// handleReady reports whether the service has something to serve.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ready := false
	err := s.store.Snapshot(ctx, func(snap store.Snapshot) error {
		elMeta, err := snap.ELMeta(ctx)
		if err != nil {
			return err
		}
		ready = elMeta != nil
		return nil
	})
	if err != nil || !ready {
		writeError(w, http.StatusServiceUnavailable, "no published version yet")
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// staleHeader announces that the served snapshot is known not to reflect the
// chain. It is a header, not a body field, so every JSON response stays
// byte-identical to the canonical contract and old consumers see no change.
const staleHeader = "X-Snapshot-Stale"

// markStale sets the header when the store reports a stale snapshot. It must be
// called before the first byte of the body is written.
func markStale(w http.ResponseWriter, snap store.Snapshot) {
	if s, ok := snap.(interface{ Stale() bool }); ok && s.Stale() {
		w.Header().Set(staleHeader, "true")
	}
}

// writeJSON serialises without a trailing newline. json.Encoder.Encode appends
// one, Fastify does not, and that single byte is the difference between
// byte-identical and merely equal on every non-streamed endpoint.
func writeJSON(w http.ResponseWriter, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	_, err = w.Write(body)
	return err
}

// httpError mirrors the canonical error body shape.
type httpError struct {
	StatusCode int    `json:"statusCode"`
	Message    any    `json:"message"`
	Error      string `json:"error,omitempty"`
}

func writeError(w http.ResponseWriter, status int, message any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(httpError{StatusCode: status, Message: message})
}

// tooEarly is returned while the database has no execution layer meta yet,
// matching the canonical 425 response.
func tooEarly(w http.ResponseWriter) {
	writeError(w, http.StatusTooEarly, "Too early response")
}

// writeStoreError maps snapshot resolution failures onto distinct statuses so a
// consumer can tell "not yet" from "gone" from "off-chain".
//
// The refresh signal itself is the one the canonical contract already has:
// lastChangedBlockHash changes whenever the data was re-read, including after a
// reorg. A consumer polling a label therefore needs nothing new. Only an
// explicit pin can land on a version that is no longer on the chain, and that
// is what the 409 below reports — together with the head to jump to.
func writeStoreError(w http.ResponseWriter, err error) bool {
	// Once bytes are on the wire the status is fixed, so the only honest thing left is
	// to make sure the client cannot mistake what it received for a complete answer.
	// Recording the failure covers this side; aborting the connection covers the
	// client's, and it is the part that matters — a handler that simply returns leaves
	// net/http to terminate the chunked body normally, so a truncated dump arrives as
	// a well-formed response and only a consumer that validates the JSON notices.
	// http.ErrAbortHandler is the sentinel that drops the connection without a stack
	// trace; withLogging logs from a defer so the record above survives the unwind.
	if headerSent(w) {
		streamFailed(w, err)
		panic(http.ErrAbortHandler)
	}

	var superseded store.ErrSuperseded
	if errors.As(err, &superseded) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]any{
			"statusCode":               http.StatusConflict,
			"message":                  superseded.Error(),
			"headBlockNumber":          superseded.HeadBlockNumber,
			"headLastChangedBlockHash": superseded.HeadLastChangedBlock,
		})
		return true
	}

	var pruned store.ErrPruned
	if errors.As(err, &pruned) {
		writeError(w, http.StatusGone, pruned.Error())
		return true
	}

	var notFound store.ErrNotFound
	if errors.As(err, &notFound) {
		writeError(w, http.StatusNotFound, notFound.Error())
		return true
	}

	var unsupported store.ErrPinUnsupported
	if errors.As(err, &unsupported) {
		writeError(w, http.StatusBadRequest, unsupported.Error())
		return true
	}

	// A backend that does not hold part of the surface says so, rather than
	// answering with an empty list a consumer would read as "there are none".
	if asNotImplemented(err) {
		writeError(w, http.StatusNotImplemented, err.Error())
		return true
	}

	return false
}
