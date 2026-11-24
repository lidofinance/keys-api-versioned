package main

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestTheHTTPServerBoundsSlowIdleAndOversizedClients(t *testing.T) {
	srv := newHTTPServer(":0", http.NotFoundHandler())
	if srv.ReadTimeout <= 0 || srv.ReadTimeout > time.Minute {
		t.Errorf("ReadTimeout = %s, want a bound on reading a request", srv.ReadTimeout)
	}
	if srv.IdleTimeout <= 0 {
		t.Errorf("IdleTimeout = %s, want idle keep-alive connections reaped", srv.IdleTimeout)
	}
	if srv.WriteTimeout != 0 {
		t.Errorf("WriteTimeout = %s: it would cut every full dump at that age", srv.WriteTimeout)
	}

	ts := httptest.NewUnstartedServer(srv.Handler)
	ts.Config = srv
	ts.Start()
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/status", nil)
	req.Header.Set("X-Padding", strings.Repeat("a", 32<<10))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusRequestHeaderFieldsTooLarge {
		t.Errorf("32 KiB of headers got %d, want 431", resp.StatusCode)
	}
}

// A response still running at the drain deadline is cut, not waited for: it holds a pooled connection,
// and closing the pool waits for it.
func TestShutdownCutsAResponseThatOutlivesTheDrain(t *testing.T) {
	started, ended := make(chan struct{}), make(chan struct{})
	srv := newHTTPServer("127.0.0.1:0", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(ended)
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
	}))
	ts := httptest.NewUnstartedServer(srv.Handler)
	ts.Config = srv
	ts.Start()
	defer ts.Close()

	resp, err := http.Get(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	<-started
	shutdownHTTP(srv, 50*time.Millisecond, slog.New(slog.DiscardHandler))
	select {
	case <-ended:
	case <-time.After(5 * time.Second):
		t.Fatal("the response outlived the shutdown")
	}
}
