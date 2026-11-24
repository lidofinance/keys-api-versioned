// Package api serves the keys-api surface. /v1 and /v2 are byte-identical to the canonical service,
// whitespace included; /ext/v1 is everything this service added.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lidofinance/keys-api-versioned/internal/store"
)

type Config struct {
	ChainID    int64
	AppVersion string
	// ExtPin is the default pin for /ext/v1 (empty = store default). /v1 must read at the head like the
	// canonical service and ignores ?snapshot=, so its default cannot change.
	ExtPin         string
	Log            *slog.Logger
	MetricsHandler http.Handler
	Observe        ObserveFunc
	// StreamWriteTimeout: zero means streamWriteTimeout.
	StreamWriteTimeout time.Duration
	// MaxDiffKeys: zero means maxDiffKeys.
	MaxDiffKeys int
	// MaxStreams caps concurrent streaming responses; keep it below the pool size. Zero means no cap.
	MaxStreams int
	// StreamWait: zero means streamWait.
	StreamWait time.Duration
	// StreamMaxDuration: zero means streamMaxDuration.
	StreamMaxDuration time.Duration
	// MaxBufferedBytes caps the bodies of buffered responses being written at once; zero means
	// maxBufferedBytes.
	MaxBufferedBytes int64
}

type Server struct {
	store   store.Store
	cfg     Config
	streams chan struct{}
	// buffered: bytes of buffered bodies being written now.
	buffered atomic.Int64
}

func New(s store.Store, cfg Config) *Server {
	if cfg.Log == nil {
		cfg.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if cfg.StreamWriteTimeout <= 0 {
		cfg.StreamWriteTimeout = streamWriteTimeout
	}
	if cfg.StreamWait <= 0 {
		cfg.StreamWait = streamWait
	}
	if cfg.StreamMaxDuration <= 0 {
		cfg.StreamMaxDuration = streamMaxDuration
	}
	if cfg.MaxBufferedBytes <= 0 {
		cfg.MaxBufferedBytes = maxBufferedBytes
	}
	srv := &Server{store: s, cfg: cfg}
	if cfg.MaxStreams > 0 {
		srv.streams = make(chan struct{}, cfg.MaxStreams)
	}
	return srv
}

// Routes are data so a test can check them against the OpenAPI document.
type route struct {
	pattern      string
	handler      http.HandlerFunc
	undocumented bool
	// mirror: the document states the mirroring rule once and a test checks it, which also catches a
	// canonical route added without a mirror.
	mirror bool
	// stream: the response can outlive a client's patience, so it takes a stream slot.
	stream bool
}

// Diff endpoints exist only under the prefix: this service invented them, and the canonical
// contract belongs to someone else.
func (s *Server) routes() []route {
	canonical := []route{
		{pattern: "GET /v1/keys", handler: s.handleKeys, stream: true},
		{pattern: "GET /v1/keys/{pubkey}", handler: s.handleKeyByPubkey},
		{pattern: "POST /v1/keys/find", handler: s.handleKeysFind},

		{pattern: "GET /v1/modules", handler: s.handleModules},
		{pattern: "GET /v1/modules/keys", handler: s.handleGroupedKeys, stream: true},
		{pattern: "GET /v1/modules/{module_id}", handler: s.handleModule},
		{pattern: "GET /v1/modules/{module_id}/keys", handler: s.handleModuleKeys, stream: true},
		{pattern: "POST /v1/modules/{module_id}/keys/find", handler: s.handleModuleKeysFind},

		{pattern: "GET /v1/operators", handler: s.handleOperators},
		{pattern: "GET /v1/modules/{module_id}/operators", handler: s.handleModuleOperators},
		{pattern: "GET /v1/modules/{module_id}/operators/{operator_id}", handler: s.handleModuleOperator},
		{pattern: "GET /v1/modules/{module_id}/operators/keys", handler: s.handleModuleOperatorsKeys, stream: true},
		{pattern: "GET /v2/modules/operators/keys", handler: s.handleModulesOperatorsKeysStream, stream: true},

		// Validated as the canonical service does, then answered as it answers without its registry.
		{pattern: "GET /v1/modules/{module_id}/validators/validator-exits-to-prepare/{operator_id}", handler: s.handleValidatorExits},
		{pattern: "GET /v1/modules/{module_id}/validators/generate-unsigned-exit-messages/{operator_id}", handler: s.handleValidatorExits},

		{pattern: "GET /v1/status", handler: s.handleStatus},
	}

	out := make([]route, 0, 2*len(canonical)+8)
	out = append(out, canonical...)
	for _, r := range canonical {
		method, path, _ := strings.Cut(r.pattern, " ")
		out = append(out, route{pattern: method + " " + mirrorPath(path), handler: r.handler, mirror: true, stream: r.stream})
	}

	out = append(out,
		route{pattern: "GET " + extPrefix + "/keys/diff", handler: s.handleKeysDiff, stream: true},
		route{pattern: "GET " + extPrefix + "/modules/diff", handler: s.handleModulesDiff},
		route{pattern: "GET " + extPrefix + "/modules/{module_id}/keys/diff", handler: s.handleKeysDiff, stream: true},
	)

	return append(out,
		route{pattern: "GET /openapi.yaml", handler: handleOpenAPISpec},
		route{pattern: "GET /docs", handler: handleDocs},

		// Readiness waits for a published version, so a pod still in its first sync gets no 425s.
		route{pattern: "GET /health", undocumented: true, handler: func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
		}},
		route{pattern: "GET /health/ready", undocumented: true, handler: s.handleReady},
	)
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	for _, r := range s.routes() {
		handler := r.handler
		if r.stream {
			handler = s.limitStream(handler)
		}
		_, path, _ := strings.Cut(r.pattern, " ")
		switch {
		case r.undocumented:
			handler = withNoStore(handler).ServeHTTP
		case isAPIPath(path):
			handler = withPublicCacheControl(handler)
		}
		// Before any handler writes: a canonical-surface request with extension params is told they were ignored.
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

func isAPIPath(path string) bool {
	return strings.HasPrefix(path, "/v1/") || strings.HasPrefix(path, "/v2/") ||
		strings.HasPrefix(path, extPrefix+"/")
}

// snapshot resolves the request's version: ?snapshot= (extended surface only), else the surface
// default.
func (s *Server) snapshot(ctx context.Context, r *http.Request, fn func(store.Snapshot) error) error {
	pin := ""
	if onExtSurface(r) {
		pin = r.URL.Query().Get("snapshot")
	}
	return s.snapshotAt(ctx, r, pin, fn)
}

// snapshotAt takes the pin from the caller: a page follows its cursor's version, not the current one.
func (s *Server) snapshotAt(ctx context.Context, r *http.Request, pin string, fn func(store.Snapshot) error) error {
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
	if err == nil && !ready {
		// Held for an overdue full read: requests get 425 from here rather than no endpoint at all.
		if held, ok := s.store.(interface {
			Withheld(context.Context) (bool, error)
		}); ok {
			ready, err = held.Withheld(ctx)
		}
	}
	if err != nil || !ready {
		writeError(w, http.StatusServiceUnavailable, "no published version yet")
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// staleHeader is a header, not a body field, so JSON stays byte-identical to the canonical contract.
const staleHeader = "X-Snapshot-Stale"

// markStale must run before the first body byte.
func markStale(w http.ResponseWriter, snap store.Snapshot) {
	if s, ok := snap.(interface{ Stale() bool }); ok && s.Stale() {
		w.Header().Set(staleHeader, "true")
	}
}

// deferJSON marshals inside a snapshot and writeBody sends it after: the pooled connection and the
// lease go back before a slow client reads.
func deferJSON(body *[]byte, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	*body = b
	return nil
}

// maxBufferedBytes: the operator list is ~220 KB on mainnet, so this is ~300 clients that stopped
// reading; their bodies stay in memory until the write deadline.
const maxBufferedBytes = 64 << 20

// writeBody writes nothing for a nil body: the snapshot already answered (425, 404).
func (s *Server) writeBody(w http.ResponseWriter, body []byte) {
	if body == nil {
		return
	}
	n := int64(len(body))
	if !s.takeBuffered(n) {
		w.Header().Set("Retry-After", strconv.Itoa(int(streamRetryAfter/time.Second)))
		writeStatusMessage(w, http.StatusServiceUnavailable, "Too many responses waiting for their clients, retry later")
		return
	}
	defer s.buffered.Add(-n)
	w.Header().Set("Content-Type", "application/json")
	extendWriteDeadline(w, s.cfg.StreamWriteTimeout)
	w.Write(body)
}

// takeBuffered lets one body through on an empty budget, however large.
func (s *Server) takeBuffered(n int64) bool {
	for {
		used := s.buffered.Load()
		if used > 0 && used+n > s.cfg.MaxBufferedBytes {
			return false
		}
		if s.buffered.CompareAndSwap(used, used+n) {
			return true
		}
	}
}

// writeJSON writes no trailing newline (json.Encoder adds one, Fastify does not): byte-identical
// rather than merely equal.
func writeJSON(w http.ResponseWriter, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	extendWriteDeadline(w, streamWriteTimeout)
	_, err = w.Write(body)
	return err
}

// httpError is the canonical error body, in its field order; `error` is always sent there.
type httpError struct {
	Message    any    `json:"message"`
	Error      string `json:"error,omitempty"`
	StatusCode int    `json:"statusCode"`
}

// writeError is NestJS's body for its typed exceptions (BadRequest, NotFound); no trailing newline,
// as Fastify sends none.
func writeError(w http.ResponseWriter, status int, message any) {
	writeErrorBody(w, status, httpError{Message: message, Error: http.StatusText(status), StatusCode: status})
}

// writeStatusMessage is NestJS's body for a bare HttpException and for an unknown error.
func writeStatusMessage(w http.ResponseWriter, status int, message string) {
	writeErrorBody(w, status, struct {
		StatusCode int    `json:"statusCode"`
		Message    string `json:"message"`
	}{status, message})
}

func writeErrorBody(w http.ResponseWriter, status int, v any) {
	body, _ := json.Marshal(v)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(body)
}

// internalError keeps the cause in the log: its text names the database, its user and tables, and
// the canonical service answers a bare "Internal server error".
func (s *Server) internalError(w http.ResponseWriter, r *http.Request, err error) {
	s.cfg.Log.Error("request failed", "method", r.Method, "path", r.URL.Path, "err", err)
	writeStatusMessage(w, http.StatusInternalServerError, "Internal server error")
}

func tooEarly(w http.ResponseWriter) {
	writeStatusMessage(w, http.StatusTooEarly, "Too early response")
}

// writeStoreError maps snapshot failures to distinct statuses: "not yet" vs "gone" vs "off-chain".
func writeStoreError(w http.ResponseWriter, err error) bool {
	// Once bytes are sent the status is fixed. A plain return lets net/http end the chunked body
	// cleanly, so a truncated dump looks complete; ErrAbortHandler drops the connection instead.
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

	// Without this a malformed pin fell through to 500, telling a consumer to retry.
	var badRequest store.ErrBadRequest
	if errors.As(err, &badRequest) {
		writeError(w, http.StatusBadRequest, []string{badRequest.Error()})
		return true
	}

	// Not 200 with an empty list, which a consumer would read as "there are none".
	if asNotImplemented(err) {
		writeError(w, http.StatusNotImplemented, err.Error())
		return true
	}

	return false
}
