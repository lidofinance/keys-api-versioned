// Command diff compares two keys-api implementations response by response.
//
// This is the parity gate: it is meant to run continuously against both
// services over the same database, not once by hand. Keys arrays are sorted
// before comparison because neither implementation promises an order.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
)

type keysResponse struct {
	Meta json.RawMessage `json:"meta"`
	Data []key           `json:"data"`
}

type key struct {
	Key              string `json:"key"`
	DepositSignature string `json:"depositSignature"`
	OperatorIndex    int32  `json:"operatorIndex"`
	Used             bool   `json:"used"`
	ModuleAddress    string `json:"moduleAddress"`
	Index            int32  `json:"index"`
	Vetted           bool   `json:"vetted"`
}

func main() {
	var (
		a         = flag.String("a", "http://localhost:3000", "base url of implementation A (canonical)")
		b         = flag.String("b", "http://localhost:3001", "base url of implementation B (PoC)")
		endpoints = flag.String("endpoints", strings.Join(defaultEndpoints, ","), "comma separated paths to compare")
		maxReport = flag.Int("max-report", 5, "how many differing items to print per endpoint")
		ignore    = flag.String("ignore", ".appVersion", "comma separated JSON paths expected to differ")
		live      = flag.Bool("live", false, "the services follow a live chain: also ignore the meta block, "+
			"which legitimately advances between two sequential fetches")
	)
	flag.Parse()

	ignored := map[string]bool{}
	for _, p := range strings.Split(*ignore, ",") {
		if p = strings.TrimSpace(p); p != "" {
			ignored[p] = true
		}
	}
	if *live {
		// Against a syncing pair these differ by construction: the two fetches
		// happen seconds apart and the writer advances the block meta in between.
		// The key sets are still compared in full.
		for _, p := range []string{
			".meta.elBlockSnapshot.blockNumber", ".meta.elBlockSnapshot.blockHash",
			".meta.elBlockSnapshot.timestamp", ".meta.elBlockSnapshot.lastChangedBlockHash",
			".elBlockSnapshot.blockNumber", ".elBlockSnapshot.blockHash",
			".elBlockSnapshot.timestamp", ".elBlockSnapshot.lastChangedBlockHash",
		} {
			ignored[p] = true
		}
		fmt.Println("note: -live is set, the meta block is excluded from comparison")
	}

	client := &http.Client{Timeout: 30 * time.Minute}
	failed := false

	for _, ep := range strings.Split(*endpoints, ",") {
		ep = strings.TrimSpace(ep)
		if ep == "" {
			continue
		}

		ra, err := fetch(client, *a+ep)
		if err != nil {
			fmt.Printf("FAIL %s: A: %v\n", ep, err)
			failed = true
			continue
		}
		rb, err := fetch(client, *b+ep)
		if err != nil {
			fmt.Printf("FAIL %s: B: %v\n", ep, err)
			failed = true
			continue
		}

		if ra.status != rb.status {
			fmt.Printf("FAIL %s: status %d vs %d\n", ep, ra.status, rb.status)
			failed = true
			continue
		}

		identical := string(ra.body) == string(rb.body)
		note := fmt.Sprintf("%s: status %d, %.1f MiB vs %.1f MiB",
			ep, ra.status, mib(len(ra.body)), mib(len(rb.body)))

		if identical {
			fmt.Printf("OK   %s, byte-identical\n", note)
			continue
		}

		diffs, err := compare(ra.body, rb.body, *maxReport, ignored)
		if err != nil {
			fmt.Printf("FAIL %s: %v\n", note, err)
			failed = true
			continue
		}
		if len(diffs) == 0 {
			fmt.Printf("OK   %s, equal ignoring key order and ignored fields\n", note)
			continue
		}

		fmt.Printf("FAIL %s\n", note)
		for _, d := range diffs {
			fmt.Printf("       %s\n", d)
		}
		failed = true
	}

	if failed {
		os.Exit(1)
	}
}

var defaultEndpoints = []string{
	"/v1/status",
	"/v1/modules",
	"/v1/modules/1",
	"/v1/keys",
	"/v1/keys?used=true",
	"/v1/keys?operatorIndex=1",
	"/v1/modules/1/keys",
	"/v1/modules/keys",
}

type response struct {
	status int
	body   []byte
}

func fetch(client *http.Client, url string) (response, error) {
	resp, err := client.Get(url)
	if err != nil {
		return response{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return response{}, err
	}
	return response{status: resp.StatusCode, body: body}, nil
}

// compare reports semantic differences, sorting key arrays first.
func compare(a, b []byte, maxReport int, ignored map[string]bool) ([]string, error) {
	var ka, kb keysResponse
	if isKeysResponse(a, &ka) && isKeysResponse(b, &kb) {
		// The ignore list has to reach this path too: against a live pair the meta
		// block advances between the two fetches, and a point-in-time store (the
		// LSM copy) reports the block it was loaded at by design.
		return compareKeys(ka, kb, maxReport, ignored[".meta.elBlockSnapshot.blockNumber"])
	}
	return compareGeneric(a, b, maxReport, ignored)
}

// isKeysResponse reports whether the body is a flat keys list. Other endpoints
// also have a "data" array (modules, grouped keys), so the element shape
// decides, not the envelope.
func isKeysResponse(body []byte, out *keysResponse) bool {
	if err := json.Unmarshal(body, out); err != nil {
		return false
	}
	return len(out.Data) > 0 && out.Data[0].Key != ""
}

func compareKeys(a, b keysResponse, maxReport int, ignoreMeta bool) ([]string, error) {
	var diffs []string

	if !ignoreMeta && !jsonEqual(a.Meta, b.Meta) {
		diffs = append(diffs, fmt.Sprintf("meta differs: %s vs %s", a.Meta, b.Meta))
	}
	if len(a.Data) != len(b.Data) {
		diffs = append(diffs, fmt.Sprintf("key count %d vs %d", len(a.Data), len(b.Data)))
	}

	sortKeys(a.Data)
	sortKeys(b.Data)

	n := min(len(a.Data), len(b.Data))
	mismatches := 0
	for i := 0; i < n; i++ {
		if a.Data[i] == b.Data[i] {
			continue
		}
		mismatches++
		if len(diffs) < maxReport {
			diffs = append(diffs, fmt.Sprintf("key %d: %+v vs %+v", i, a.Data[i], b.Data[i]))
		}
	}
	if mismatches > maxReport {
		diffs = append(diffs, fmt.Sprintf("... %d differing keys total", mismatches))
	}
	return diffs, nil
}

func sortKeys(keys []key) {
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].ModuleAddress != keys[j].ModuleAddress {
			return keys[i].ModuleAddress < keys[j].ModuleAddress
		}
		if keys[i].OperatorIndex != keys[j].OperatorIndex {
			return keys[i].OperatorIndex < keys[j].OperatorIndex
		}
		return keys[i].Index < keys[j].Index
	})
}

func compareGeneric(a, b []byte, maxReport int, ignored map[string]bool) ([]string, error) {
	var va, vb any
	if err := json.Unmarshal(a, &va); err != nil {
		return nil, fmt.Errorf("parse A: %w", err)
	}
	if err := json.Unmarshal(b, &vb); err != nil {
		return nil, fmt.Errorf("parse B: %w", err)
	}

	// Neither service promises an order, and key arrays are nested differently
	// per endpoint (`data.keys`, `data[].keys`, plain `data`). Sort every array
	// of keys wherever it sits, otherwise an order difference reads as a content
	// difference — which is exactly what it did before this pass existed.
	normalizeKeyArrays(va)
	normalizeKeyArrays(vb)

	var diffs []string
	walk("", va, vb, &diffs, maxReport, ignored)
	return diffs, nil
}

func walk(path string, a, b any, diffs *[]string, maxReport int, ignored map[string]bool) {
	if len(*diffs) >= maxReport || ignored[path] {
		return
	}

	switch av := a.(type) {
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok {
			*diffs = append(*diffs, path+": object vs "+fmt.Sprintf("%T", b))
			return
		}
		for k, v := range av {
			other, ok := bv[k]
			if !ok {
				*diffs = append(*diffs, path+"."+k+": missing in B")
				continue
			}
			walk(path+"."+k, v, other, diffs, maxReport, ignored)
		}
		for k := range bv {
			if _, ok := av[k]; !ok {
				*diffs = append(*diffs, path+"."+k+": missing in A")
			}
		}
	case []any:
		bv, ok := b.([]any)
		if !ok {
			*diffs = append(*diffs, path+": array vs "+fmt.Sprintf("%T", b))
			return
		}
		if len(av) != len(bv) {
			*diffs = append(*diffs, fmt.Sprintf("%s: length %d vs %d", path, len(av), len(bv)))
		}
		for i := 0; i < min(len(av), len(bv)); i++ {
			walk(fmt.Sprintf("%s[%d]", path, i), av[i], bv[i], diffs, maxReport, ignored)
		}
	default:
		if fmt.Sprint(a) != fmt.Sprint(b) {
			*diffs = append(*diffs, fmt.Sprintf("%s: %v vs %v", path, a, b))
		}
	}
}

// normalizeKeyArrays walks a decoded response and sorts any array whose
// elements look like registry keys.
func normalizeKeyArrays(v any) {
	switch tv := v.(type) {
	case map[string]any:
		for _, child := range tv {
			normalizeKeyArrays(child)
		}
	case []any:
		for _, child := range tv {
			normalizeKeyArrays(child)
		}
		if looksLikeKeys(tv) {
			sort.SliceStable(tv, func(i, j int) bool {
				return keySortKey(tv[i]) < keySortKey(tv[j])
			})
		}
	}
}

func looksLikeKeys(arr []any) bool {
	if len(arr) == 0 {
		return false
	}
	m, ok := arr[0].(map[string]any)
	if !ok {
		return false
	}
	_, hasKey := m["key"]
	_, hasSig := m["depositSignature"]
	return hasKey && hasSig
}

// keySortKey builds a total order: module, then operator, then index, with the
// pubkey as the final tiebreaker.
func keySortKey(v any) string {
	m, ok := v.(map[string]any)
	if !ok {
		return ""
	}
	num := func(field string) float64 {
		if f, ok := m[field].(float64); ok {
			return f
		}
		return 0
	}
	str := func(field string) string {
		if s, ok := m[field].(string); ok {
			return s
		}
		return ""
	}
	return fmt.Sprintf("%s|%012.0f|%012.0f|%s",
		strings.ToLower(str("moduleAddress")), num("operatorIndex"), num("index"), str("key"))
}

func jsonEqual(a, b json.RawMessage) bool {
	var va, vb any
	if err := json.Unmarshal(a, &va); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &vb); err != nil {
		return false
	}
	return fmt.Sprint(va) == fmt.Sprint(vb)
}

func mib(n int) float64 { return float64(n) / (1 << 20) }
