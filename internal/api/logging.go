package api

import (
	"log/slog"
	"net/http"
	"time"
)

// recorder captures what went out: the only way to tell a complete stream from a truncated one.
type recorder struct {
	http.ResponseWriter
	status  int
	written int64
	// failed: set after the header was sent, so the log line is the only place it shows.
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

func (r *recorder) fail(err error) { r.failed = err }

// withLogging logs one line per request; a dump that died half way shows only as fewer bytes.
func withLogging(next http.Handler, log *slog.Logger, observe ObserveFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		rec := &recorder{ResponseWriter: w}

		// Deferred: a truncated stream aborts via panic(http.ErrAbortHandler), and logging during the
		// unwind (no recover) is the only server-side record of it.
		defer func() {
			logRequest(r, rec, log, observe, time.Since(started))
		}()
		next.ServeHTTP(rec, r)
	})
}

func logRequest(r *http.Request, rec *recorder, log *slog.Logger, observe ObserveFunc, took time.Duration) {
	if observe != nil {
		// By pattern, not path: a pubkey or module id would explode series cardinality.
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

type ObserveFunc func(method, route string, status int, bytes int64, d time.Duration)

// routeLabel falls back to "unmatched", never the raw path (cardinality).
func routeLabel(r *http.Request) string {
	if p := r.Pattern; p != "" {
		return p
	}
	return "unmatched"
}

func streamFailed(w http.ResponseWriter, err error) {
	if rec := findRecorder(w); rec != nil {
		rec.fail(err)
	}
}

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

func headerSent(w http.ResponseWriter) bool {
	rec := findRecorder(w)
	return rec != nil && rec.status != 0
}
