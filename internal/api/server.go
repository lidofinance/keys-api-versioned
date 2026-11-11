// Package api serves the keys-api surface: keys, module keys, modules, operators,
// the combined streams, and status.
//
// The endpoints exist because consumers call them. ethereum-validators-monitoring
// reads /v1/keys?used=true, /v1/operators, /v1/modules and /v1/status;
// lido-mev-monitoring reads /v1/status, /v1/modules and the /v2 combined stream.
// Response framing is byte-identical to the canonical service, whitespace included,
// so pointing a consumer here is a URL change.
//
// There are two surfaces, and the line between them is the whole compatibility story:
// /v1 and /v2 are that contract and nothing else, /ext/v1 is everything this service
// added. See extsurface.go for the rule and why the prefix is not /v3.
//
// Still not implemented: the validator exit endpoints, which need consensus layer
// ingest.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/lidofinance/keys-api-versioned/internal/store"
)

// Config holds the serving configuration.
type Config struct {
	ChainID    int64
	AppVersion string
	// ExtPin is the pin an /ext/v1 request without `?snapshot=` is served from. Empty
	// means the store's own default, which is what the canonical surface always uses.
	//
	// The default belongs to the surface rather than to the process because the two
	// surfaces promise different things. /v1 and /v2 promise a consumer cannot tell this
	// service from the canonical one, and the canonical one reads the registry at the
	// head — `getBlock('latest')` in its keys-update job — so anything but the head
	// there would be a difference in content, invisible in the framing and impossible to
	// opt out of, since /v1 ignores `?snapshot=` by that same promise. /ext/v1 promises
	// nothing of the sort, so it can default to the version a reorg cannot take away and
	// let a consumer ask for the head explicitly.
	ExtPin string
	Log    *slog.Logger
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
	// mirror marks a route that exists only because its canonical twin does. The
	// document states the mirroring rule once instead of repeating fourteen response
	// bodies, and a test checks the rule — which is stronger than an enumeration,
	// because it also catches a canonical route that was added without a mirror.
	mirror bool
}

// routes lists everything the service serves.
//
// The canonical paths are listed once and mirrored under extPrefix, because the
// extended surface serves the same data from the same handlers — what differs is which
// query parameters are honoured. The diff endpoints exist only under the prefix: this
// service invented them, and a path we invented has no business sitting inside a
// contract someone else owns and may extend. See extsurface.go.
func (s *Server) routes() []route {
	canonical := []route{
		{pattern: "GET /v1/keys", handler: s.handleKeys},
		{pattern: "GET /v1/keys/{pubkey}", handler: s.handleKeyByPubkey},
		{pattern: "POST /v1/keys/find", handler: s.handleKeysFind},

		{pattern: "GET /v1/modules", handler: s.handleModules},
		{pattern: "GET /v1/modules/keys", handler: s.handleGroupedKeys},
		{pattern: "GET /v1/modules/{module_id}", handler: s.handleModule},
		{pattern: "GET /v1/modules/{module_id}/keys", handler: s.handleModuleKeys},
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
	}

	out := make([]route, 0, 2*len(canonical)+8)
	out = append(out, canonical...)
	for _, r := range canonical {
		method, path, _ := strings.Cut(r.pattern, " ")
		out = append(out, route{pattern: method + " " + mirrorPath(path), handler: r.handler, mirror: true})
	}

	// Ours, and only ours.
	out = append(out,
		route{pattern: "GET " + extPrefix + "/keys/diff", handler: s.handleKeysDiff},
		route{pattern: "GET " + extPrefix + "/modules/diff", handler: s.handleModulesDiff},
		route{pattern: "GET " + extPrefix + "/modules/{module_id}/keys/diff", handler: s.handleKeysDiff},
	)

	return append(out,
		// The API document, and a page that renders it.
		route{pattern: "GET /openapi.yaml", handler: handleOpenAPISpec},
		route{pattern: "GET /docs", handler: handleDocs},

		// Liveness answers as long as the process is up. Readiness answers only once a
		// version is published, so a pod still doing its first sync is not sent traffic
		// that would get a 425.
		route{pattern: "GET /health", undocumented: true, handler: func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
		}},
		route{pattern: "GET /health/ready", undocumented: true, handler: s.handleReady},
	)
}

// Handler returns the router.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	for _, r := range s.routes() {
		handler := r.handler
		_, path, _ := strings.Cut(r.pattern, " ")
		switch {
		case r.undocumented:
			handler = withNoStore(handler).ServeHTTP
		case isAPIPath(path):
			handler = withPublicCacheControl(handler)
		}
		// One place, before any handler can write: a canonical-surface request that sent
		// extension parameters is told they were ignored.
		mux.HandleFunc(r.pattern, func(w http.ResponseWriter, req *http.Request) {
			noteIgnoredExtParams(w, req)
			handler(w, req)
		})
	}

	if s.cfg.MetricsHandler != nil {
		mux.Handle("GET /metrics", withNoStore(s.cfg.MetricsHandler))
	}

	return withLogging(withCanonicalMethodMismatch(mux), s.cfg.Log, s.cfg.Observe)
}

// isAPIPath reports whether a path is a data read on either surface, as opposed to
// the API document or a probe.
func isAPIPath(path string) bool {
	return strings.HasPrefix(path, "/v1/") || strings.HasPrefix(path, "/v2/") ||
		strings.HasPrefix(path, extPrefix+"/")
}

// snapshot resolves the version one request is served from: the pin in
// ?snapshot=<latest|finalized|block|hash> when given, otherwise the default for the
// surface the request came in on — Config.ExtPin under /ext/v1, the store's own pin
// under /v1 and /v2.
//
// An explicit pin is what lets a consumer make several requests against one
// version — a dump and then a follow-up query — and get a definite answer if that
// version has gone away, instead of a quietly different one.
func (s *Server) snapshot(ctx context.Context, r *http.Request, fn func(store.Snapshot) error) error {
	// An explicit pin is one of the extensions, so it is honoured only on the extended
	// surface. On the canonical one the parameter is ignored, the way the canonical
	// service ignores a parameter it does not declare — see extsurface.go.
	pin := ""
	if onExtSurface(r) {
		pin = r.URL.Query().Get("snapshot")
	}
	return s.snapshotAt(ctx, r, pin, fn)
}

// snapshotAt is snapshot with the pin decided by the caller, which paging needs: a
// page follows the version named by its cursor rather than whatever is current now.
func (s *Server) snapshotAt(ctx context.Context, r *http.Request, pin string, fn func(store.Snapshot) error) error {
	// The surface default applies only when nothing more specific was decided — an
	// explicit `?snapshot=` or, for a paged walk, the version the cursor names. Both
	// arrive here as a non-empty pin, so both keep winning over it.
	if pin == "" && onExtSurface(r) {
		pin = s.cfg.ExtPin
	}
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
// httpError is the canonical error body, and the field order is its order.
//
// `error` was declared here and never set, so every error response was missing a field
// the canonical service always sends — found by comparing the error surface against a
// running canonical service rather than by reading code, which is how the two defects
// before it were found and is not a method that scales. A consumer that logs or matches
// on `error` saw a difference on every failure, not on an edge case.
type httpError struct {
	Message    any    `json:"message"`
	Error      string `json:"error,omitempty"`
	StatusCode int    `json:"statusCode"`
}

func writeError(w http.ResponseWriter, status int, message any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(httpError{
		Message: message, Error: http.StatusText(status), StatusCode: status,
	})
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

	// Input the store could not interpret at all. Without this case a malformed pin fell
	// through to 500, which tells a consumer to retry something that will never work.
	var badRequest store.ErrBadRequest
	if errors.As(err, &badRequest) {
		writeError(w, http.StatusBadRequest, []string{badRequest.Error()})
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
