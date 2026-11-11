package e2e

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/testpg"
	"github.com/lidofinance/keys-api-versioned/internal/testreg"
)

// totalWithdrawnKeys is canonical since 5.0.0: on both surfaces, null for curated modules, and right
// after depositableValidatorsCount, since byte order is part of parity.

// Operator i of module 2 holds i withdrawn keys, so operator 0 is a genuine 0.
func withdrawnRegistry(withdrawnBase int32) testreg.Spec {
	spec := testreg.Spec{Modules: 2, OperatorsPerModule: 3, KeysPerOperator: 5}
	spec = spec.WithWithdrawn(func(module, operator int) *int32 {
		if module != 2 {
			return nil
		}
		n := withdrawnBase + int32(operator)
		return &n
	})
	return spec.WithGroups(func(module, operator int) *model.GroupID {
		if module != 2 {
			return nil
		}
		id := model.GroupID(9)
		return &id
	})
}

func TestTotalWithdrawnKeysIsServedOnBothSurfaces(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	withdrawnRegistry(0).Publish(ctx, t, db.Writer, testreg.At(db.Block(0)))
	srv := testpg.NewServer(t, db.Reader(t, "latest", 3))

	for _, tc := range []struct {
		path   string
		want   []string
		absent string
	}{
		{"/v1/modules/1/operators", []string{`"depositableValidatorsCount":5,"totalWithdrawnKeys":null}`}, "groupId"},
		{"/v1/modules/2/operators", []string{
			`"depositableValidatorsCount":5,"totalWithdrawnKeys":0}`,
			`"depositableValidatorsCount":5,"totalWithdrawnKeys":2}`,
		}, "groupId"},
		{"/v1/modules/2/operators/1", []string{`"totalWithdrawnKeys":1}`}, "groupId"},
		{"/v1/operators", []string{`"totalWithdrawnKeys":null}`, `"totalWithdrawnKeys":2}`}, "groupId"},
		{"/v1/modules/2/operators/keys", []string{`"totalWithdrawnKeys":0}`}, "groupId"},
		{"/v2/modules/operators/keys", []string{`"totalWithdrawnKeys":null}`, `"totalWithdrawnKeys":2}`}, "groupId"},
		// Canonical fields in canonical order first, extensions after.
		{"/ext/v1/modules/1/operators", []string{`"totalWithdrawnKeys":null}`}, "groupId"},
		{"/ext/v1/modules/2/operators", []string{`"totalWithdrawnKeys":0,"groupId":9}`}, ""},
		{"/ext/v1/modules/operators/keys", []string{`"totalWithdrawnKeys":2,"groupId":9}`}, ""},
	} {
		resp := srv.Get(t, tc.path)
		body := readBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s: status %d — body %s", tc.path, resp.StatusCode, body)
		}
		for _, want := range tc.want {
			if !strings.Contains(body, want) {
				t.Errorf("GET %s: body lacks %s — body %s", tc.path, want, body)
			}
		}
		if tc.absent != "" && strings.Contains(body, tc.absent) {
			t.Errorf("GET %s: body carries %s", tc.path, tc.absent)
		}
	}
}

// The counter alone changes no key or other field; the writer must still close the old row.
func TestAChangeInTotalWithdrawnKeysAloneIsWritten(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	withdrawnRegistry(0).Publish(ctx, t, db.Writer, testreg.At(db.Block(0)))
	withdrawnRegistry(10).Publish(ctx, t, db.Writer, testreg.At(db.Block(1)))
	srv := testpg.NewServer(t, db.Reader(t, "latest", 3))

	resp := srv.Get(t, "/v1/modules/2/operators/1")
	body := readBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d — body %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, `"totalWithdrawnKeys":11}`) {
		t.Errorf("served %s, want totalWithdrawnKeys 11 from the newer version", body)
	}
}

func TestCacheControlFollowsCanonical(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	withdrawnRegistry(0).Publish(ctx, t, db.Writer, testreg.At(db.Block(0)))
	srv := testpg.NewServer(t, db.Reader(t, "latest", 3))

	const public = "public, max-age=30, stale-if-error=1200, stale-while-revalidate=30"
	for _, tc := range []struct {
		path string
		want string
	}{
		{"/v1/keys", public},
		{"/v1/modules/1/operators", public},
		{"/v2/modules/operators/keys", public},
		{"/ext/v1/modules", public},
		{"/v1/modules/99", ""},
		{"/v1/keys/not-a-pubkey", ""},
		{"/health", "no-store"},
		{"/health/ready", "no-store"},
	} {
		resp := srv.Get(t, tc.path)
		readBody(t, resp)
		if got := resp.Header.Get("Cache-Control"); got != tc.want {
			t.Errorf("GET %s (status %d): Cache-Control %q, want %q", tc.path, resp.StatusCode, got, tc.want)
		}
	}
}
