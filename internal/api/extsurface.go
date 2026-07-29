package api

import (
	"encoding/json"
	"net/http"
	"strings"
)

// Two surfaces, and the line between them is the whole point.
//
// `/v1` and `/v2` are the canonical lido-keys-api contract and nothing else. A service
// pointed here from there must not be able to tell the difference: same paths, same
// bodies byte for byte, same status codes, and — this is the part that is easy to get
// wrong — the same treatment of parameters the canonical service does not declare.
// Checked rather than assumed: its ValidationPipe is configured `{ transform: true }`
// with no `whitelist` or `forbidNonWhitelisted` (lido-keys-api src/main.ts), so NestJS
// ignores an unknown query parameter. Answering 400 to one here would be a difference
// a consumer could hit, so `/v1` ignores ours too.
//
// `/ext/v1` is where everything this service invented lives: version pins, paging, and
// the diff endpoints. Only there are `snapshot`, `limit` and `cursor` honoured.
//
// # Why not /v2 or /v3
//
// `/v2` is not free: `GET /v2/modules/operators/keys` is a canonical endpoint, and it
// is the one lido-mev-monitoring reads. `/v3` is not free either, in a way that is
// worth writing down — the canonical service versions per endpoint through NestJS URI
// versioning (`app.enableVersioning({ type: VersioningType.URI })`, then `@Version('2')`
// on the one controller that moved). `/v3` is therefore one decorator away in a
// repository this team does not own, on a protocol-critical service. Taking it would put
// two different meanings on one path with nowhere to go.
//
// A separate namespace costs a prefix and buys the canonical service the freedom to
// reach /v3, /v4 and beyond without ever colliding with us. It also says the right
// thing to a consumer: moving to /ext/v1 is a deliberate change of contract, not a
// version bump.
const extPrefix = "/ext/v1"

// extParams are the query parameters that exist only on the extended surface.
var extParams = []string{"snapshot", "limit", "cursor"}

// ignoredParamsHeader names the extension parameters a canonical-surface request sent
// and did not get.
//
// A header rather than an error, for the compatibility reason above, and a header
// rather than silence because the failure it prevents is nasty: `?limit=100` ignored on
// /v1/keys streams the whole 216 MiB dump, and the consumer has no way to tell that
// from a version with 100 keys in it. Same argument as X-Snapshot-Stale — a header
// cannot break a consumer parsing the body.
const ignoredParamsHeader = "X-Ignored-Parameters"

// canonicalMethodMismatch makes a wrong method on a canonical path answer the way the
// canonical service answers it.
//
// Go's ServeMux is right and Fastify is not: a known path with an unsupported method is
// 405 with an Allow header. The canonical service answers 404 `Cannot POST /v1/keys`,
// because that is Fastify's default and nothing there overrides it. On /v1 and /v2 we
// match it — that surface is their contract including its warts, and a consumer or a
// dashboard that counts 404s against 405s would otherwise see a difference. On /ext/v1
// the honest 405 stands, because that surface is ours.
//
// Implemented by intercepting the status rather than by registering a methodless route
// per path, which would be fourteen more entries in the router for one behaviour.
type canonicalMethodMismatch struct {
	http.ResponseWriter
	request *http.Request
	// rewrite is set for the canonical surface only; rewrote and bodyWritten track
	// whether the substitution happened and whether its body has already gone out.
	rewrite     bool
	rewrote     bool
	bodyWritten bool
}

func (w *canonicalMethodMismatch) WriteHeader(status int) {
	if status == http.StatusMethodNotAllowed && w.rewrite {
		w.rewrote = true
		w.Header().Del("Allow")
		w.Header().Set("Content-Type", "application/json")
		w.ResponseWriter.WriteHeader(http.StatusNotFound)
		return
	}
	w.ResponseWriter.WriteHeader(status)
}

// Unwrap exposes the writer underneath, which is the convention http.ResponseController
// uses and what lets headerSent and streamFailed find the recorder they need.
//
// Not optional: those two type-assert for *recorder, so before this existed, inserting
// any wrapper between the logging middleware and the handlers silently turned "abort the
// stream, this version is gone" into "write a 409 into the middle of a dump". The
// mid-response reorg test caught it.
func (w *canonicalMethodMismatch) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// Flush delegates, because embedding an http.ResponseWriter does not promote it — and a
// wrapper that swallows Flush would hold a streamed dump in a buffer.
func (w *canonicalMethodMismatch) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *canonicalMethodMismatch) Write(p []byte) (int, error) {
	if w.rewrote {
		if w.bodyWritten {
			// The mux writes one line; anything after it is not ours to forward.
			return len(p), nil
		}
		w.bodyWritten = true
		body, err := json.Marshal(httpError{
			Message:    "Cannot " + w.request.Method + " " + w.request.URL.Path,
			Error:      http.StatusText(http.StatusNotFound),
			StatusCode: http.StatusNotFound,
		})
		if err != nil {
			return len(p), nil
		}
		if _, err := w.ResponseWriter.Write(body); err != nil {
			return 0, err
		}
		return len(p), nil
	}
	return w.ResponseWriter.Write(p)
}

// withCanonicalMethodMismatch wraps a handler so the rewrite above applies to the
// canonical surface only.
func withCanonicalMethodMismatch(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(&canonicalMethodMismatch{
			ResponseWriter: w, request: r, rewrite: !onExtSurface(r),
		}, r)
	})
}

// mirrorPath places a canonical path on the extended surface.
//
// The canonical version segment is dropped rather than kept: `/v1/keys` becomes
// `/ext/v1/keys`, and `/v2/modules/operators/keys` becomes
// `/ext/v1/modules/operators/keys`. The extended surface has one version axis, its own,
// and the canonical split between /v1 and /v2 — an accident of how that service chose to
// move one endpoint — has no business leaking into it. Keeping it would also produce
// `/ext/v1/v1/keys`, which is nobody's idea of a path.
func mirrorPath(path string) string {
	_, rest, ok := strings.Cut(strings.TrimPrefix(path, "/"), "/")
	if !ok {
		return extPrefix
	}
	return extPrefix + "/" + rest
}

// onExtSurface reports whether a request came in on the extended surface.
//
// Decided from the path rather than carried in the route table, because every handler
// is registered under both prefixes and the alternative is threading a flag through all
// of them.
func onExtSurface(r *http.Request) bool {
	return strings.HasPrefix(r.URL.Path, extPrefix+"/")
}

// noteIgnoredExtParams sets the header on a canonical-surface request that sent
// extension parameters. Called once, in the wrapper, so it lands before any handler can
// write a body.
func noteIgnoredExtParams(w http.ResponseWriter, r *http.Request) {
	if onExtSurface(r) {
		return
	}

	query := r.URL.Query()
	var ignored []string
	for _, name := range extParams {
		if query.Get(name) != "" {
			ignored = append(ignored, name)
		}
	}
	if len(ignored) == 0 {
		return
	}
	w.Header().Set(ignoredParamsHeader, strings.Join(ignored, ", ")+"; use "+extPrefix)
}
