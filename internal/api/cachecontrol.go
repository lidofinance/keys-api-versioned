package api

import "net/http"

// Cache-Control as canonical sends it since 5.0.0: public reads cacheable for 30 s, probes and
// metrics never.
const (
	publicCacheControl  = "public, max-age=30, stale-if-error=1200, stale-while-revalidate=30"
	noStoreCacheControl = "no-store"
)

// Errors go out without it, so a shared cache cannot pin a 4xx/5xx for max-age.
func withPublicCacheControl(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			next(w, r)
			return
		}
		next(&cacheControlWriter{ResponseWriter: w}, r)
	}
}

func withNoStore(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", noStoreCacheControl)
		next.ServeHTTP(w, r)
	})
}

// The status is only known at the first WriteHeader or Write.
type cacheControlWriter struct {
	http.ResponseWriter
	decided bool
}

func (c *cacheControlWriter) decide(status int) {
	if c.decided {
		return
	}
	c.decided = true
	if status >= 200 && status < 300 && c.Header().Get("Cache-Control") == "" {
		c.Header().Set("Cache-Control", publicCacheControl)
	}
}

func (c *cacheControlWriter) WriteHeader(status int) {
	c.decide(status)
	c.ResponseWriter.WriteHeader(status)
}

func (c *cacheControlWriter) Write(b []byte) (int, error) {
	c.decide(http.StatusOK)
	return c.ResponseWriter.Write(b)
}

func (c *cacheControlWriter) Flush() {
	c.decide(http.StatusOK)
	if f, ok := c.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap lets http.ResponseController and findRecorder reach the writer underneath.
func (c *cacheControlWriter) Unwrap() http.ResponseWriter { return c.ResponseWriter }
