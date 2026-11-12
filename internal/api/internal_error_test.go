package api

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/store"
)

type failingStore struct{ err error }

func (f failingStore) Snapshot(context.Context, func(store.Snapshot) error) error { return f.err }
func (failingStore) Close()                                                       {}

func TestA500BodyHidesTheCauseAndTheLogKeepsIt(t *testing.T) {
	cause := errors.New(`failed to connect to user=keys database=keys_api host=db:5432`)
	var logs bytes.Buffer
	handler := New(failingStore{cause}, Config{
		ChainID: 1,
		Log:     slog.New(slog.NewTextHandler(&logs, nil)),
	}).Handler()

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/modules", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	body := rec.Body.String()
	if strings.Contains(body, "keys_api") || strings.Contains(body, "db:5432") {
		t.Fatalf("body leaks the cause: %s", body)
	}
	if !strings.Contains(body, `"message":"Internal server error"`) {
		t.Fatalf("body = %s", body)
	}
	if !strings.Contains(logs.String(), "database=keys_api") {
		t.Fatalf("the cause should be logged:\n%s", logs.String())
	}
}

// Bodies as canonical 5.0.0 sends them, byte for byte: a bare HttpException and an unknown error
// carry only statusCode and message, typed exceptions add error, and none ends in a newline.
func TestErrorBodiesMatchTheCanonicalBytes(t *testing.T) {
	early := newMemStore(1)
	early.early = true
	for _, tc := range []struct {
		name  string
		store store.Store
		path  string
		want  string
	}{
		{"unknown error", failingStore{errors.New("db down")}, "/v1/modules",
			`{"statusCode":500,"message":"Internal server error"}`},
		{"too early", early, "/v1/keys",
			`{"statusCode":425,"message":"Too early response"}`},
		{"not found", newMemStore(1), "/v1/modules/99/keys",
			`{"message":"Module with moduleId 99 is not supported","error":"Not Found","statusCode":404}`},
	} {
		rec := httptest.NewRecorder()
		New(tc.store, Config{ChainID: 1}).Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))
		if got := rec.Body.String(); got != tc.want {
			t.Errorf("%s: body %q, want %q", tc.name, got, tc.want)
		}
	}
}
