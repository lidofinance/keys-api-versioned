package api

import (
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/ethaddr"
)

// Bodies of the 400s are what keys-api.lido.fi answered: 5.0.0 on 2026-11-20, 6.0.0 the same on 2026-11-27. Where it answered
// 200 or 404, the request passed validation, and a deployment without its registry answers the 500.
func TestExitRoutesValidateLikeTheCanonicalServiceThenAnswerRegistryDisabled(t *testing.T) {
	const disabled = `{"message":"Validators Registry is disabled. Check environment variables","error":"Internal Server Error","statusCode":500}`
	bad := func(msgs ...string) string {
		return `{"message":["` + strings.Join(msgs, `","`) + `"],"error":"Bad Request","statusCode":400}`
	}
	const exits = "/v1/modules/%s/validators/validator-exits-to-prepare/%s"
	path := func(module, operator string) string {
		return strings.Replace(strings.Replace(exits, "%s", module, 1), "%s", operator, 1)
	}

	cases := []struct {
		path string
		want string
	}{
		{path("abc", "1"), bad("module_id must be a contract address or numeric value")},
		{path("01", "1"), bad("module_id must be a contract address or numeric value")},
		{path("0x0000000000000000000000000000000000000000", "1"), bad("module_id cannot be the zero address")},
		{path("0x55032650b14df07b85bf18a3a3ec8e0af2e028D5", "1"), bad("module_id must be a contract address or numeric value")},
		{path("0X55032650b14df07b85bf18a3a3ec8e0af2e028d5", "1"), bad("module_id must be a contract address or numeric value")},
		{path("1", "-1"), bad("operator_id must not be less than 0")},
		{path("1", "1.5"), bad("operator_id must be an integer number")},
		{path("1", "abc"), bad("operator_id must not be less than 0", "operator_id must be an integer number")},
		{path("1", "1_0"), bad("operator_id must not be less than 0", "operator_id must be an integer number")},
		{path("1", "Infinity"), bad("operator_id must be an integer number")},
		{path("1", "1") + "?percent=-1&max_amount=-1", bad("max_amount must not be less than 0", "percent must not be less than 0")},
		{path("1", "1") + "?max_amount=x&percent=y", bad("max_amount must not be less than 0", "max_amount must be an integer number",
			"percent must not be less than 0", "percent must be an integer number")},
		{path("1", "1") + "?percent=1.5", bad("percent must be an integer number")},
		{path("1", "1") + "?percent=1&percent=2", bad("percent must not be less than 0", "percent must be an integer number")},
		{path("1", "1") + "?percent=-Infinity", bad("percent must not be less than 0", "percent must be an integer number")},
		{path("1", "1") + "?percent=0x", bad("percent must not be less than 0", "percent must be an integer number")},
		{path("1", "1") + "?percent=-0x1", bad("percent must not be less than 0", "percent must be an integer number")},
		{path("1", "1") + "?percent=1e400", bad("percent must be an integer number")},
		// The order is module_id, the query, operator_id; only the first that fails is reported.
		{path("abc", "abc") + "?percent=-1", bad("module_id must be a contract address or numeric value")},
		{path("1", "abc") + "?percent=-1", bad("percent must not be less than 0")},
		{path("1", "-5") + "?max_amount=z", bad("max_amount must not be less than 0", "max_amount must be an integer number")},
		{strings.Replace(path("1", "abc"), "validator-exits-to-prepare", "generate-unsigned-exit-messages", 1),
			bad("operator_id must not be less than 0", "operator_id must be an integer number")},

		{path("1", "1"), disabled},
		{path("999", "1"), disabled},
		{path("%201", "1"), disabled},
		{path("1", "%20"), disabled},
		{path("1", "-0"), disabled},
		{path("1", "0x1"), disabled},
		{path("1", "0b11"), disabled},
		{path("1", "99999999999") + "?percent=0", disabled},
		{path("1", "1") + "?percent=1e2", disabled},
		{path("1", "1") + "?percent=.5e1", disabled},
		{path("1", "1") + "?percent=%2B3", disabled},
		{path("1", "1") + "?percent=%20", disabled},
		{path("1", "1") + "?percent=&max_amount=&unknown=x", disabled},
		{path("55032650b14df07b85bf18a3a3ec8e0af2e028d5", "1"), disabled},
		{path("0000000000000000000000000000000000000000", "1"), disabled},
		{path(ethaddr.Checksum("0x55032650b14df07b85bf18a3a3ec8e0af2e028d5"), "1"), disabled},
		{"/ext/v1/modules/1/validators/generate-unsigned-exit-messages/1", disabled},
	}

	server := httptest.NewServer(New(newMemStore(1), Config{ChainID: 1}).Handler())
	defer server.Close()
	for _, c := range cases {
		resp, err := http.Get(server.URL + c.path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(body) != c.want {
			t.Errorf("GET %s:\n got %d %s\nwant %s", c.path, resp.StatusCode, body, c.want)
		}
		if want := map[bool]int{true: 500, false: 400}[c.want == disabled]; resp.StatusCode != want {
			t.Errorf("GET %s: status %d, want %d", c.path, resp.StatusCode, want)
		}
	}
}

func TestJSNumber(t *testing.T) {
	for in, want := range map[string]float64{
		"": 0, " ": 0, "\ufeff7\u00a0": 7, "+3": 3, "-0": 0, "1.": 1, ".5": 0.5, "1e2": 100, "0x1F": 31, "0O17": 15,
		"0b11": 3, "Infinity": math.Inf(1), "+Infinity": math.Inf(1), "-Infinity": math.Inf(-1), "1e400": math.Inf(1),
	} {
		if got := jsNumber(in); got != want {
			t.Errorf("jsNumber(%q) = %v, want %v", in, got, want)
		}
	}
	for _, in := range []string{"abc", "1_0", "0x", "-0x1", "+0x1", ".", "e5", "inf", "NaN", "1e", "0x1p2", "--1", "1 2"} {
		if got := jsNumber(in); !math.IsNaN(got) {
			t.Errorf("jsNumber(%q) = %v, want NaN", in, got)
		}
	}
}

// Consumers parse appVersion: the council daemon as semver, the oracle as PEP 440. A plain X.Y.Z
// reads the same in both.
func TestContractVersionIsTheCanonicalSnapshotsRelease(t *testing.T) {
	if !regexp.MustCompile(`^\d+\.\d+\.\d+$`).MatchString(ContractVersion) {
		t.Fatalf("ContractVersion %q is not a plain X.Y.Z", ContractVersion)
	}
	if got := specVersion(loadCanonicalSpec(t)); got != "canonical "+ContractVersion {
		t.Errorf("ContractVersion is %s, the canonical snapshot is %s: bump it with the snapshot", ContractVersion, got)
	}
}
