package api

import (
	"bufio"
	"net/http"
	"time"
)

// streamWriteTimeout bounds each write that reaches the socket, not the response: a consumer that
// keeps reading is never cut off, one that stops is, and its pool connection and lease come back.
const streamWriteTimeout = 30 * time.Second

func extendWriteDeadline(w http.ResponseWriter, timeout time.Duration) {
	// Fails only on writers without a connection underneath (test recorders).
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(timeout))
}

type deadlineWriter struct {
	w       http.ResponseWriter
	timeout time.Duration
	err     error
}

func (d *deadlineWriter) Write(p []byte) (int, error) {
	if d.err != nil {
		return 0, d.err
	}
	extendWriteDeadline(d.w, d.timeout)
	n, err := d.w.Write(p)
	if err != nil {
		d.err = err
	}
	return n, err
}

// Err is the first failed socket write; a row callback returns it so the database read stops too.
func (d *deadlineWriter) Err() error { return d.err }

func (s *Server) newStreamWriter(w http.ResponseWriter) (*bufio.Writer, *deadlineWriter) {
	out := &deadlineWriter{w: w, timeout: s.cfg.StreamWriteTimeout}
	return bufio.NewWriterSize(out, streamBufferSize), out
}
