package api

import (
	"context"
	"net/http"
	"strconv"
	"time"
)

// A stream holds a pooled connection and a version lease for as long as its client reads. With no
// cap, one reader per pooled connection left readiness, /v1/status and, in KEYS_API_MODE=all, the
// writer waiting on an empty pool.
const (
	// streamWait: a burst of dumps queues briefly instead of failing at once.
	streamWait = 10 * time.Second
	// streamMaxDuration: a full dump (~211 MiB) still finishes at ~0.4 MB/s.
	streamMaxDuration = 10 * time.Minute
	streamRetryAfter  = 5 * time.Second
)

type streamSlotKey struct{}

func (s *Server) limitStream(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.streams != nil {
			wait := time.NewTimer(s.cfg.StreamWait)
			select {
			case s.streams <- struct{}{}:
				wait.Stop()
				defer func() { <-s.streams }()
			case <-wait.C:
				w.Header().Set("Retry-After", strconv.Itoa(int(streamRetryAfter/time.Second)))
				writeStatusMessage(w, http.StatusServiceUnavailable, "Too many concurrent streams, retry later")
				return
			case <-r.Context().Done():
				wait.Stop()
				return
			}
		}

		ctx, cancel := context.WithTimeout(context.WithValue(r.Context(), streamSlotKey{}, true), s.cfg.StreamMaxDuration)
		defer cancel()
		next(w, r.WithContext(ctx))
	}
}

// checkStreamSlot catches a streaming handler on a route not marked stream: it would bypass the cap.
func (s *Server) checkStreamSlot(ctx context.Context) {
	if ctx.Value(streamSlotKey{}) == nil {
		s.cfg.Log.Error("a streaming response without a stream slot; mark its route stream")
	}
}
