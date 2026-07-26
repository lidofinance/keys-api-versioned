package testpg

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/lidofinance/keys-api-versioned/internal/api"
	"github.com/lidofinance/keys-api-versioned/internal/store"
)

// Event is one served request, as the service itself reports it.
type Event struct {
	Route  string
	Status int
	Bytes  int64
	Took   time.Duration
}

// Server is the HTTP surface under test.
//
// It captures what the service reports about a request — the observation and the log
// record — because for a response that failed after its headers went out, those are the
// only authoritative signals. The status is already 200 by then and the body is
// whatever reached the client, so asserting on the client alone cannot distinguish a
// truncated dump from a complete one without parsing it.
type Server struct {
	*httptest.Server
	Logs *LogCapture

	mu      sync.Mutex
	events  []Event
	started int
}

// NewServer starts the API over a store, on HTTP/1.1 so that the framing a test
// asserts on — chunked, and an abort in the middle of it — is deterministic.
func NewServer(tb testing.TB, s store.Store) *Server {
	tb.Helper()

	srv := &Server{Logs: NewLogCapture()}
	handler := api.New(s, api.Config{
		ChainID:    1,
		AppVersion: "test",
		Log:        slog.New(srv.Logs),
		Observe: func(route string, status int, bytes int64, took time.Duration) {
			srv.mu.Lock()
			srv.events = append(srv.events, Event{route, status, bytes, took})
			srv.mu.Unlock()
		},
	}).Handler()

	srv.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		srv.mu.Lock()
		srv.started++
		srv.mu.Unlock()
		handler.ServeHTTP(w, r)
	}))
	tb.Cleanup(srv.Close)
	return srv
}

// Client is a client that does not time out — some of these responses are held open on
// purpose — and whose socket read buffer is small.
//
// The small buffer is load-bearing for any test that blocks a reader mid-response: the
// server buffers 256 KiB, net/http adds its own, and loopback socket autotuning can
// add megabytes, so without this the server could finish a whole module while the test
// believes it is still inside one.
func (s *Server) Client() *http.Client {
	dialer := &net.Dialer{}
	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				conn, err := dialer.DialContext(ctx, network, addr)
				if err != nil {
					return nil, err
				}
				if tcp, ok := conn.(*net.TCPConn); ok {
					_ = tcp.SetReadBuffer(4096)
				}
				return conn, nil
			},
			DisableCompression: true,
		},
	}
}

// Get performs a request and returns the response with its body unread.
func (s *Server) Get(tb testing.TB, path string) *http.Response {
	tb.Helper()
	resp, err := s.Client().Get(s.URL + path)
	if err != nil {
		tb.Fatalf("GET %s: %v", path, err)
	}
	return resp
}

// GetHeld starts a request, reads the first gate bytes and leaves the rest in flight,
// which is what a slow consumer looks like from the service's side. The returned reader
// replays the prefix, so a verifier still sees the whole body.
func (s *Server) GetHeld(tb testing.TB, path string, gate int64) (*http.Response, func() io.Reader) {
	tb.Helper()

	resp := s.Get(tb, path)
	prefix := make([]byte, gate)
	read, err := io.ReadFull(resp.Body, prefix)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		resp.Body.Close()
		tb.Fatalf("GET %s: read the first %d bytes: %v", path, gate, err)
	}
	prefix = prefix[:read]

	return resp, func() io.Reader { return io.MultiReader(bytesReader(prefix), resp.Body) }
}

// Events is every request the service has finished.
func (s *Server) Events() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Event(nil), s.events...)
}

// LastEvent is the most recent finished request, or false when none has finished.
func (s *Server) LastEvent() (Event, bool) {
	events := s.Events()
	if len(events) == 0 {
		return Event{}, false
	}
	return events[len(events)-1], true
}

// Pending is how many requests are in flight: started and not yet reported. A pending
// request that has already sent bytes is a server that is inside a response, which is
// how a test proves it created the interleaving it meant to create rather than assuming
// a sleep was long enough.
func (s *Server) Pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.started - len(s.events)
}

// WaitForEvents blocks until the service has reported n requests.
func (s *Server) WaitForEvents(tb testing.TB, n int, within time.Duration) {
	tb.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if len(s.Events()) >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	tb.Fatalf("waited %s for %d served requests, got %d", within, n, len(s.Events()))
}

// LogCapture keeps the service's log records so a test can assert on the one line that
// reports a failure the response itself cannot carry.
type LogCapture struct {
	mu      sync.Mutex
	records []slog.Record
}

func NewLogCapture() *LogCapture { return &LogCapture{} }

func (c *LogCapture) Enabled(context.Context, slog.Level) bool { return true }

func (c *LogCapture) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, r.Clone())
	return nil
}

func (c *LogCapture) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *LogCapture) WithGroup(string) slog.Handler      { return c }

// Has reports whether a record with this level and message was logged.
func (c *LogCapture) Has(level slog.Level, message string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range c.records {
		if r.Level == level && r.Message == message {
			return true
		}
	}
	return false
}

// Messages is every message logged at or above a level, for a failure message that
// says what the service actually reported.
func (c *LogCapture) Messages(level slog.Level) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, r := range c.records {
		if r.Level >= level {
			out = append(out, r.Level.String()+" "+r.Message)
		}
	}
	return out
}

// bytesReader avoids importing bytes for one call site.
func bytesReader(b []byte) io.Reader { return &sliceReader{b: b} }

type sliceReader struct {
	b []byte
	i int
}

func (r *sliceReader) Read(p []byte) (int, error) {
	if r.i >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.i:])
	r.i += n
	return n, nil
}
