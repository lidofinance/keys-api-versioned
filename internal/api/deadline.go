package api

import (
	"bufio"
	"context"
	"net/http"
	"time"
)

// streamWriteTimeout bounds each write that reaches the socket, not the response: a consumer that
// keeps reading is never cut off, one that stops is, and its pool connection and lease come back.
const streamWriteTimeout = 30 * time.Second

func extendWriteDeadline(w http.ResponseWriter, timeout time.Duration) {
	setWriteDeadline(w, time.Now().Add(timeout))
}

func setWriteDeadline(w http.ResponseWriter, at time.Time) {
	// Fails only on writers without a connection underneath (test recorders).
	_ = http.NewResponseController(w).SetWriteDeadline(at)
}

type deadlineWriter struct {
	w       http.ResponseWriter
	timeout time.Duration
	// end: a write blocked on the socket must not outlast the stream's own deadline.
	end time.Time
	err error
}

func (d *deadlineWriter) Write(p []byte) (int, error) {
	if d.err != nil {
		return 0, d.err
	}
	at := time.Now().Add(d.timeout)
	if !d.end.IsZero() && d.end.Before(at) {
		at = d.end
	}
	setWriteDeadline(d.w, at)
	n, err := d.w.Write(p)
	if err != nil {
		d.err = err
	}
	return n, err
}

// Err is the first failed socket write; a row callback returns it so the database read stops too.
func (d *deadlineWriter) Err() error { return d.err }

func (s *Server) newStreamWriter(ctx context.Context, w http.ResponseWriter) (*bufio.Writer, *deadlineWriter) {
	s.checkStreamSlot(ctx)
	out := &deadlineWriter{w: w, timeout: s.cfg.StreamWriteTimeout}
	out.end, _ = ctx.Deadline()
	return bufio.NewWriterSize(out, streamBufferSize), out
}
