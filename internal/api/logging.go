package api

import (
	"log/slog"
	"net/http"
	"time"
)

// recorder captures what actually went out, which for a streamed dump is the
// only way to tell a complete response from a truncated one after the fact.
type recorder struct {
	http.ResponseWriter
	status  int
	written int64
	// failed is set when a handler reports an error after the header was sent.
	// At that point the status cannot be changed, so the log line is the only
	// place the failure is visible.
	failed error
}

func (r *recorder) WriteHeader(status int) {
	if r.status == 0 {
		r.status = status
	}
	r.ResponseWriter.WriteHeader(status)
}

func (r *recorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.written += int64(n)
	return n, err
}

func (r *recorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// fail records an error that happened mid-response.
func (r *recorder) fail(err error) { r.failed = err }

// withLogging logs one line per request: what was asked, what went out, how long
// it took. A dump that dies half way shows up as a completed request with fewer
// bytes than usual, so the byte count is not decoration.
func withLogging(next http.Handler, log *slog.Logger, observe ObserveFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		rec := &recorder{ResponseWriter: w}

		// Deferred, not sequential, because a truncated stream is reported to the
		// client by aborting the connection — a panic with http.ErrAbortHandler — and
		// this line is the only server-side record that it happened. Running during the
		// unwind keeps the record; no recover, so the panic still reaches net/http,
		// which drops the connection without printing a stack for that sentinel.
		defer func() {
			logRequest(r, rec, log, observe, time.Since(started))
		}()
		next.ServeHTTP(rec, r)
	})
}

// logRequest emits the one line per request, and the observation behind it. It is a
// function rather than inline code because it has to run from a defer.
func logRequest(r *http.Request, rec *recorder, log *slog.Logger, observe ObserveFunc, took time.Duration) {
	if observe != nil {
		// Labelled by pattern rather than by path, so a pubkey or a module id
		// cannot turn one metric into thousands of series.
		observe(r.Method, routeLabel(r), rec.status, rec.written, took)
	}
	attrs := []any{
		"method", r.Method,
		"path", r.URL.Path,
		"status", rec.status,
		"bytes", rec.written,
		"took", took.Round(time.Millisecond).String(),
	}
	if q := r.URL.RawQuery; q != "" {
		attrs = append(attrs, "query", q)
	}
	if rec.written > 0 && took > 0 {
		attrs = append(attrs, "MiB/s", int64(float64(rec.written)/(1<<20)/took.Seconds()))
	}

	if rec.failed != nil {
		// The client got a truncated body: the status said 200 and then the stream
		// stopped. This is the only record of it on this side.
		log.Error("request failed mid-response", append(attrs, "err", rec.failed)...)
		return
	}
	if rec.status >= 500 {
		log.Error("request failed", attrs...)
		return
	}
	if rec.status >= 400 {
		log.Warn("request rejected", attrs...)
		return
	}
	log.Info("request served", attrs...)
}

// ObserveFunc receives one served request. The API takes it as a function so this
// package needs no metrics dependency.
type ObserveFunc func(method, route string, status int, bytes int64, d time.Duration)

// routeLabel is the matched pattern, which for Go's mux is available on the
// request. Falling back to the raw path would be a cardinality trap, so an
// unmatched request is labelled as such.
func routeLabel(r *http.Request) string {
	if p := r.Pattern; p != "" {
		return p
	}
	return "unmatched"
}

// streamFailed marks the response as truncated when a handler cannot recover.
func streamFailed(w http.ResponseWriter, err error) {
	if rec := findRecorder(w); rec != nil {
		rec.fail(err)
	}
}

// findRecorder walks the wrapper chain to the recorder.
//
// Both helpers here used to type-assert for *recorder directly, which quietly required
// them to be the outermost writer. Inserting one wrapper — the canonical method-mismatch
// rewrite in extsurface.go — turned "abort the stream, this version left the chain" into
// "write a 409 into the middle of a dump", because headerSent reported that nothing had
// been sent yet. Unwrap is the convention http.ResponseController uses.
func findRecorder(w http.ResponseWriter) *recorder {
	for {
		if rec, ok := w.(*recorder); ok {
			return rec
		}
		unwrapper, ok := w.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			return nil
		}
		w = unwrapper.Unwrap()
	}
}

// headerSent reports whether anything has already gone to the client, which
// decides whether an error can still become a status code.
func headerSent(w http.ResponseWriter) bool {
	rec := findRecorder(w)
	return rec != nil && rec.status != 0
}
