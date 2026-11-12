package testpg

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lidofinance/keys-api-versioned/internal/api"
	"github.com/lidofinance/keys-api-versioned/internal/store"
)

type Event struct {
	Route  string
	Status int
	Bytes  int64
	Took   time.Duration
}

// Server records the service's own observation and log per request: after headers went out, a
// truncated dump still answers 200, so the client alone cannot tell.
type Server struct {
	*httptest.Server
	Logs *LogCapture

	mu      sync.Mutex
	events  []Event
	started int
}

// NewServer uses HTTP/1.1 so chunked framing and a mid-stream abort are deterministic.
func NewServer(tb testing.TB, s store.Store) *Server {
	tb.Helper()
	return NewServerWithExtPin(tb, s, "")
}

func NewServerWithExtPin(tb testing.TB, s store.Store, extPin string) *Server {
	tb.Helper()

	srv := &Server{Logs: NewLogCapture()}
	handler := api.New(s, api.Config{
		ChainID:    1,
		AppVersion: "test",
		ExtPin:     extPin,
		Log:        slog.New(srv.Logs),
		Observe: func(method, route string, status int, bytes int64, took time.Duration) {
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

// 64 KiB receive buffer keeps a held reader mid-response (server buffers 256 KiB). At 4 KiB Linux
// disables autotuning: a 20 MB dump took ~10 min instead of 0.4 s; macOS ignores the setting.
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
					_ = tcp.SetReadBuffer(64 << 10)
				}
				return conn, nil
			},
			DisableCompression: true,
		},
	}
}

func (s *Server) Get(tb testing.TB, path string) *http.Response {
	tb.Helper()
	resp, err := s.Client().Get(s.URL + path)
	if err != nil {
		tb.Fatalf("GET %s: %v", path, err)
	}
	return resp
}

func (s *Server) Do(tb testing.TB, method, path, body string) *http.Response {
	tb.Helper()

	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, s.URL+path, reader)
	if err != nil {
		tb.Fatalf("%s %s: %v", method, path, err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := s.Client().Do(req)
	if err != nil {
		tb.Fatalf("%s %s: %v", method, path, err)
	}
	return resp
}

// GetHeld reads the first gate bytes and leaves the rest in flight, like a slow consumer.
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

func (s *Server) Events() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Event(nil), s.events...)
}

func (s *Server) LastEvent() (Event, bool) {
	events := s.Events()
	if len(events) == 0 {
		return Event{}, false
	}
	return events[len(events)-1], true
}

// Pending counts started-but-unreported requests: proof a test created its interleaving, not a sleep.
func (s *Server) Pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.started - len(s.events)
}

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
