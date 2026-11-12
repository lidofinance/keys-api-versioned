package api

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/store"
)

type bigDiffStore struct {
	stubStore
	keys int
}

func (d bigDiffStore) DiffKeys(_ context.Context, _, _, _ string, fn func(store.DiffKind, *model.Key) error) (*store.DiffMeta, error) {
	k := model.Key{PubkeyBin: make([]byte, 48), SignatureBin: make([]byte, 96)}
	for i := 0; i < d.keys; i++ {
		if err := fn(store.DiffAdded, &k); err != nil {
			return nil, err
		}
	}
	return &store.DiffMeta{From: &model.ELBlockSnapshot{}, To: &model.ELBlockSnapshot{}}, nil
}

// The whole registry, twice, used to be buffered and marshalled in one piece per request.
func TestADiffAboveTheCapAnswers413(t *testing.T) {
	for keys, want := range map[int]int{10: http.StatusOK, 11: http.StatusRequestEntityTooLarge} {
		handler := New(bigDiffStore{keys: keys}, Config{MaxDiffKeys: 10}).Handler()
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ext/v1/keys/diff?since=1", nil))
		body, _ := io.ReadAll(rec.Body)
		if rec.Code != want {
			t.Errorf("%d keys: status %d (%.120s), want %d", keys, rec.Code, body, want)
		}
	}
}
