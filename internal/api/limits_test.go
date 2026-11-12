package api

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/store"
)

// memStore serves one module of generated keys and records how each key stream ended.
type memStore struct {
	keys int

	mu      sync.Mutex
	streams []error
	ended   chan error
}

func newMemStore(keys int) *memStore { return &memStore{keys: keys, ended: make(chan error, 16)} }

func (m *memStore) Snapshot(_ context.Context, fn func(store.Snapshot) error) error {
	return fn(memSnapshot{m})
}
func (m *memStore) Close() {}

type memSnapshot struct{ m *memStore }

var memModule = func() model.StakingModule {
	mod := model.StakingModule{ID: 1, Type: "curated-onchain-v1", Name: "curated", Active: true}
	mod.SetRawAddress("0x00000000000000000000000000000000000000aa")
	return mod
}()

func (memSnapshot) ELMeta(context.Context) (*model.ELBlockSnapshot, error) {
	return &model.ELBlockSnapshot{BlockNumber: 1, BlockHash: "0x01", Timestamp: 1}, nil
}

func (memSnapshot) Modules(context.Context, []string) ([]model.StakingModule, error) {
	return []model.StakingModule{memModule}, nil
}

func (memSnapshot) ModuleByRef(_ context.Context, ref string) (*model.StakingModule, error) {
	if ref != "1" {
		return nil, store.ErrNotFound{Ref: ref}
	}
	mod := memModule
	return &mod, nil
}

func (s memSnapshot) StreamKeys(_ context.Context, _ string, _ model.KeysFilter, fn func(*model.Key) error) error {
	var err error
	for i := 0; i < s.m.keys && err == nil; i++ {
		err = fn(memKey(i))
	}
	s.m.ended <- err
	return err
}

func (memSnapshot) KeysByPubkeys(_ context.Context, _ string, pubkeys []string) ([]model.Key, error) {
	return []model.Key{}, nil
}

func (memSnapshot) Operators(context.Context, string, model.OperatorsFilter) ([]model.Operator, error) {
	return nil, nil
}

func memKey(i int) *model.Key {
	pub := make([]byte, 48)
	pub[0], pub[1], pub[2] = byte(i>>16), byte(i>>8), byte(i)
	return &model.Key{PubkeyBin: pub, SignatureBin: make([]byte, 96), Index: int32(i),
		ModuleAddress: memModule.RawAddress()}
}

// A consumer that stops reading used to hold its pool connection and lease until its socket died.
func TestAStalledConsumerIsCutOffWhileASlowOneFinishes(t *testing.T) {
	const keys = 200_000 // ~70 MiB, far above any socket buffer
	st := newMemStore(keys)
	server := httptest.NewServer(New(st, Config{ChainID: 1, StreamWriteTimeout: 300 * time.Millisecond}).Handler())
	defer server.Close()

	t.Run("stalled", func(t *testing.T) {
		conn, err := net.Dial("tcp", server.Listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		fmt.Fprintf(conn, "GET /v1/keys HTTP/1.1\r\nHost: x\r\n\r\n")
		if _, err := http.ReadResponse(bufio.NewReader(conn), nil); err != nil {
			t.Fatalf("read headers: %v", err)
		}

		select {
		case err := <-st.ended:
			if err == nil {
				t.Fatal("the stream ran to the end into a client that reads nothing")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("the stream to a stalled client was still running after 10 s")
		}
	})

	t.Run("slow but reading", func(t *testing.T) {
		resp, err := http.Get(server.URL + "/v1/keys")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		// Bursts with pauses under the write timeout: the whole dump takes far longer than it.
		started := time.Now()
		buf := make([]byte, 4<<20)
		var total int
		for {
			n, err := io.ReadFull(resp.Body, buf)
			total += n
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			if err != nil {
				t.Fatalf("after %d bytes: %v", total, err)
			}
			time.Sleep(200 * time.Millisecond)
		}
		if err := <-st.ended; err != nil {
			t.Fatalf("the stream to a reading client ended with %v", err)
		}
		if !strings.HasSuffix(string(buf[:total%len(buf)]), "]}") || total < keys*300 {
			t.Errorf("got %d bytes, want the whole dump", total)
		}
		if took := time.Since(started); took < time.Second {
			t.Errorf("the dump took %s, too fast to show a slow reader survives", took)
		}
	})
}

func TestKeysFindRejectsAnOversizedBodyOnBothRoutes(t *testing.T) {
	handler := New(newMemStore(1), Config{ChainID: 1}).Handler()
	const want = `{"statusCode":413,"message":"Request body is too large"}`
	oversized := `{"pubkeys":["` + strings.Repeat("a", maxFindBodyBytes) + `"]}`

	for _, path := range []string{"/v1/keys/find", "/v1/modules/1/keys/find"} {
		for name, body := range map[string]io.Reader{
			"with a length": strings.NewReader(oversized),
			"chunked":       io.MultiReader(strings.NewReader(oversized)),
		} {
			req := httptest.NewRequest(http.MethodPost, path, body)
			if name == "chunked" {
				req.ContentLength = -1
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != http.StatusRequestEntityTooLarge || rec.Body.String() != want {
				t.Errorf("%s %s: got %d %s, want 413 %s", path, name, rec.Code, rec.Body.String(), want)
			}
		}
	}
}

// The per-module route used to drop malformed keys silently and answer an empty list with 200.
func TestBothFindRoutesValidateTheSameWay(t *testing.T) {
	handler := New(newMemStore(1), Config{ChainID: 1}).Handler()
	valid := "0x" + strings.Repeat("ab", 48)

	for _, tc := range []struct {
		body   string
		status int
		want   string
	}{
		{`{"pubkeys":["` + valid + `"]}`, http.StatusOK, `"data"`},
		{`{"pubkeys":["` + strings.ToUpper(valid[:2]) + valid[2:] + `"]}`, http.StatusBadRequest, badPubkeyMessage},
		{`{"pubkeys":["0x1234"]}`, http.StatusBadRequest, badPubkeyMessage},
		{`{"pubkeys":[]}`, http.StatusBadRequest, "pubkeys must contain at least 1 elements"},
		{`{"keys":["` + valid + `"]}`, http.StatusBadRequest, "pubkeys must be an array"},
		{`not json`, http.StatusBadRequest, "pubkeys must be an array"},
	} {
		for _, path := range []string{"/v1/keys/find", "/v1/modules/1/keys/find"} {
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, strings.NewReader(tc.body)))
			if rec.Code != tc.status || !strings.Contains(rec.Body.String(), tc.want) {
				t.Errorf("%s %.40s: got %d %s, want %d containing %q", path, tc.body, rec.Code, rec.Body.String(), tc.status, tc.want)
			}
		}
	}
}

func TestTheFindCountCapIsNotReachableByValidInputUnderTheBodyLimit(t *testing.T) {
	element := len(`"0x` + strings.Repeat("ab", 48) + `",`)
	if most := maxFindBodyBytes / element; most >= maxFindPubkeys {
		t.Fatalf("%d valid keys fit the body limit, the cap of %d would reject some", most, maxFindPubkeys)
	}
}
