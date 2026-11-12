package api

import (
	"encoding/json"
	"net/http"
	"strings"
)

// /v1 and /v2 are the canonical contract only, down to ignoring undeclared query params (NestJS
// without whitelist). Everything this service invented lives under /ext/v1, never /v3.
const extPrefix = "/ext/v1"

var extParams = []string{"snapshot", "limit", "cursor"}

// ignoredParamsHeader: a header, not a 400, yet not silence: ?limit=100 ignored on /v1/keys streams
// the whole 216 MiB dump, indistinguishable from a 100-key version.
const ignoredParamsHeader = "X-Ignored-Parameters"

// canonicalMethodMismatch turns ServeMux's 405 on a canonical path into Fastify's 404 `Cannot
// POST /v1/keys`; /ext/v1 keeps the honest 405.
type canonicalMethodMismatch struct {
	http.ResponseWriter
	request     *http.Request
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

// Unwrap is required: findRecorder walks it, and without it a mid-dump abort became a 409 written
// into the body.
func (w *canonicalMethodMismatch) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// Embedding does not promote Flush; swallowing it would buffer a streamed dump.
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

func withCanonicalMethodMismatch(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(&canonicalMethodMismatch{
			ResponseWriter: w, request: r, rewrite: !onExtSurface(r),
		}, r)
	})
}

// mirrorPath drops the canonical version segment: /v2/modules/operators/keys -> /ext/v1/modules/operators/keys.
func mirrorPath(path string) string {
	_, rest, ok := strings.Cut(strings.TrimPrefix(path, "/"), "/")
	if !ok {
		return extPrefix
	}
	return extPrefix + "/" + rest
}

func onExtSurface(r *http.Request) bool {
	return strings.HasPrefix(r.URL.Path, extPrefix+"/")
}

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
