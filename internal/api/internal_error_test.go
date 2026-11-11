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
