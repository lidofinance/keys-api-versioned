package api

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// holdStream opens a dump and reads nothing past the headers, so the server blocks on the socket.
func holdStream(t *testing.T, server *httptest.Server, path string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: x\r\n\r\n", path)
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		conn.Close()
		t.Fatalf("read headers: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		conn.Close()
		t.Fatalf("GET %s: status %d, want 200", path, resp.StatusCode)
	}
	return conn
}

func TestStreamsAboveTheLimitWaitThenAnswer503(t *testing.T) {
	const wait = 300 * time.Millisecond
	st := newMemStore(200_000)
	server := httptest.NewServer(New(st, Config{ChainID: 1, MaxStreams: 1, StreamWait: wait,
		StreamWriteTimeout: time.Minute}).Handler())
	defer server.Close()

	held := holdStream(t, server, "/v1/keys")

	for _, path := range []string{
		"/v1/keys", "/v1/modules/keys", "/v1/modules/1/keys", "/v1/modules/1/operators/keys",
		"/v2/modules/operators/keys", "/ext/v1/keys", "/ext/v1/keys/diff?since=1",
		"/ext/v1/modules/1/keys/diff?since=1",
	} {
		started := time.Now()
		resp, err := http.Get(server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" ||
			!strings.Contains(string(body), `"statusCode":503`) {
			t.Errorf("GET %s: got %d %s (Retry-After %q), want 503 with Retry-After",
				path, resp.StatusCode, body, resp.Header.Get("Retry-After"))
		}
		if took := time.Since(started); took < wait {
			t.Errorf("GET %s: answered after %s, want a wait of %s for a slot first", path, took, wait)
		}
	}

	for _, path := range []string{"/v1/modules", "/v1/status", "/health/ready"} {
		resp, err := http.Get(server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusServiceUnavailable {
			t.Errorf("GET %s: 503 while only the streams are full", path)
		}
	}

	held.Close()
	select {
	case <-st.ended:
	case <-time.After(10 * time.Second):
		t.Fatal("the held stream did not end after its client left")
	}
	resp, err := http.Get(server.URL + "/v1/modules/1/keys")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("after the held stream ended: status %d, want its slot back", resp.StatusCode)
	}
}

// The per-write deadline lets a client that keeps reading go on forever, holding its slot, pooled
// connection and lease.
func TestAStreamIsCutAtItsMaxDuration(t *testing.T) {
	const limit = 500 * time.Millisecond
	newServer := func() (*memStore, *httptest.Server) {
		st := newMemStore(200_000)
		return st, httptest.NewServer(New(st, Config{ChainID: 1, MaxStreams: 1,
			StreamMaxDuration: limit, StreamWriteTimeout: time.Minute}).Handler())
	}

	t.Run("stalled", func(t *testing.T) {
		st, server := newServer()
		defer server.Close()
		conn := holdStream(t, server, "/v1/keys")
		defer conn.Close()

		select {
		case err := <-st.ended:
			if err == nil {
				t.Fatal("the stream ran to the end into a client that reads nothing")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("a stalled stream outlived its max duration by 10 s: the write deadline is not capped by it")
		}
	})

	t.Run("reading slowly", func(t *testing.T) {
		st, server := newServer()
		defer server.Close()
		resp, err := http.Get(server.URL + "/v1/keys")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		started := time.Now()
		buf := make([]byte, 64<<10)
		var total int
		for {
			n, err := resp.Body.Read(buf)
			total += n
			if err != nil {
				if err == io.EOF {
					t.Fatalf("the dump ended cleanly after %d bytes, want it cut", total)
				}
				break
			}
			if time.Since(started) > 20*time.Second {
				t.Fatalf("still reading after %s", time.Since(started))
			}
			time.Sleep(20 * time.Millisecond)
		}
		if err := <-st.ended; err == nil {
			t.Error("the store read ran to the end")
		}

		next, err := http.Get(server.URL + "/v1/modules")
		if err != nil {
			t.Fatal(err)
		}
		next.Body.Close()
		if next.StatusCode != http.StatusOK {
			t.Errorf("after the cut: status %d", next.StatusCode)
		}
	})
}

type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *logBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// A streaming handler on a route without the flag would bypass the cap.
func TestEveryStreamingRouteTakesASlot(t *testing.T) {
	logs := &logBuffer{}
	srv := New(newMemStore(10), Config{ChainID: 1, MaxStreams: 4,
		Log: slog.New(slog.NewTextHandler(logs, nil))})
	handler := srv.Handler()

	params := strings.NewReplacer("{module_id}", "1", "{operator_id}", "0",
		"{pubkey}", "0x"+strings.Repeat("00", 48))
	for _, r := range srv.routes() {
		method, path, _ := strings.Cut(r.pattern, " ")
		if method != http.MethodGet {
			continue
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(method, params.Replace(path)+"?since=1", nil))
		if strings.Contains(logs.String(), "without a stream slot") {
			t.Fatalf("%s streams without a stream slot: mark its route stream", r.pattern)
		}
	}
}
