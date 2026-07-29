package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
)

// The error surface, compared against the canonical service.
//
// It exists because of a gap this repo recorded and then left open: `cmd/diff` compares
// successful responses, so parity has never covered what either service does with a
// request it cannot serve. Two defects hid there — a malformed `?snapshot=` fell through
// to 500, and a module id that is neither a number nor an address answered 404 where the
// canonical service answers 400 — and both were found by reading code rather than by a
// run. Parity verified only on the happy path is parity of half the contract.
//
// What is compared is the status code, and the shape of the error body where both send
// one. Not the message text: the canonical service's wording is its own and matching it
// word for word is neither achievable nor useful. A consumer branches on the status.
//
// Cases live here rather than in a test because they need two running services. The
// hermetic half — that our own answers are what we intend — is internal/e2e's job.
type errorCase struct {
	name string
	// method and path are sent to both services.
	method string
	path   string
	body   string
	// note explains a case whose point is not obvious from the path.
	note string
	// canonicalOnly marks a case the extended surface has no opinion about.
	canonicalOnly bool
}

// errorCases is the matrix. Everything here is on the canonical surface: the extended
// one is ours alone and has nothing to be compared against.
func errorCases(moduleID string) []errorCase {
	return []errorCase{
		{name: "unknown module id", method: "GET", path: "/v1/modules/9999/keys"},
		{name: "module id that is neither an id nor an address", method: "GET", path: "/v1/modules/abc/keys",
			note: "the case that used to answer 404 here and 400 there"},
		{name: "unknown module on the module endpoint", method: "GET", path: "/v1/modules/9999"},
		{name: "unknown module on operators", method: "GET", path: "/v1/modules/9999/operators"},
		{name: "unknown operator", method: "GET", path: "/v1/modules/" + moduleID + "/operators/999999"},
		{name: "operator id that is not a number", method: "GET", path: "/v1/modules/" + moduleID + "/operators/abc"},

		{name: "pubkey that is not a pubkey", method: "GET", path: "/v1/keys/not-a-pubkey"},
		{name: "pubkey that is well formed and unknown", method: "GET",
			path: "/v1/keys/0x" + strings.Repeat("ab", 48)},

		{name: "used that is not a boolean", method: "GET", path: "/v1/keys?used=yesplease"},
		{name: "operatorIndex that is not a number", method: "GET", path: "/v1/keys?operatorIndex=abc"},
		{name: "negative operatorIndex", method: "GET", path: "/v1/keys?operatorIndex=-1"},

		// The claim this whole surface split rests on: the canonical service ignores a
		// query parameter it does not declare, so ignoring ours on /v1 is the compatible
		// behaviour and answering 400 would not be. Read off its ValidationPipe
		// configuration; this is the run that checks it.
		{name: "an undeclared query parameter", method: "GET", path: "/v1/keys?limit=3",
			note: "the compatibility claim: canonical ignores it, so /v1 must too"},
		{name: "another undeclared parameter", method: "GET", path: "/v1/modules?snapshot=12345",
			note: "same claim on a small response"},
		{name: "a parameter neither service knows", method: "GET", path: "/v1/modules?nonsense=1"},

		{name: "find with an empty pubkey list", method: "POST", path: "/v1/keys/find", body: `{"pubkeys":[]}`},
		{name: "find with a malformed body", method: "POST", path: "/v1/keys/find", body: `{`},
		{name: "find with the wrong field", method: "POST", path: "/v1/keys/find", body: `{"keys":["0x00"]}`},
		{name: "find on an unknown module", method: "POST", path: "/v1/modules/9999/keys/find",
			body: `{"pubkeys":["0x` + strings.Repeat("ab", 48) + `"]}`},

		{name: "a path neither service serves", method: "GET", path: "/v1/nothing-here"},
		{name: "the wrong method on a real path", method: "POST", path: "/v1/keys"},
		{name: "a versionless path", method: "GET", path: "/keys"},
	}
}

// runErrorMatrix sends every case to both services and reports where the answers differ.
func runErrorMatrix(client *http.Client, a, b, moduleID string) bool {
	fmt.Printf("error surface: %d cases, A=%s B=%s\n\n", len(errorCases(moduleID)), a, b)

	var (
		failed  bool
		matched int
	)
	for _, c := range errorCases(moduleID) {
		if c.canonicalOnly && strings.Contains(b, "/ext/") {
			continue
		}

		ra, err := send(client, c.method, a+c.path, c.body)
		if err != nil {
			fmt.Printf("SKIP %-46s A unreachable: %v\n", c.name, err)
			continue
		}
		rb, err := send(client, c.method, b+c.path, c.body)
		if err != nil {
			fmt.Printf("SKIP %-46s B unreachable: %v\n", c.name, err)
			continue
		}

		label := c.name
		if c.note != "" {
			label += " (" + c.note + ")"
		}

		switch {
		case ra.status != rb.status:
			fmt.Printf("DIFF %-46s %s %s\n       canonical %d %s\n       ours      %d %s\n",
				c.name, c.method, c.path, ra.status, summarise(ra.body), rb.status, summarise(rb.body))
			failed = true
		default:
			matched++
			shape := ""
			if s := shapeDiff(ra.body, rb.body); s != "" {
				shape = "  body shape: " + s
			}
			fmt.Printf("OK   %-46s %d%s\n", label, ra.status, shape)
		}
	}

	fmt.Printf("\n%d of %d cases agree on the status code\n", matched, len(errorCases(moduleID)))
	return failed
}

// send performs one request, with a body for POST.
func send(client *http.Client, method, url, body string) (response, error) {
	var reader *strings.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}

	var req *http.Request
	var err error
	if reader != nil {
		req, err = http.NewRequest(method, url, reader)
		req.Header.Set("Content-Type", "application/json")
	} else {
		req, err = http.NewRequest(method, url, nil)
	}
	if err != nil {
		return response{}, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return response{}, err
	}
	defer resp.Body.Close()

	out := response{status: resp.StatusCode}
	// An error body is small; a 200 on a dump is not, and none of these cases needs it.
	limited := make([]byte, 4<<10)
	n, _ := resp.Body.Read(limited)
	out.body = limited[:n]
	return out, nil
}

// shapeDiff reports whether the two error bodies carry the same field names, which is
// what a consumer that logs or matches on them sees. Message text is deliberately not
// compared.
func shapeDiff(a, b []byte) string {
	fa, oka := fieldNames(a)
	fb, okb := fieldNames(b)
	if !oka || !okb {
		return ""
	}
	if strings.Join(fa, ",") == strings.Join(fb, ",") {
		return ""
	}
	return fmt.Sprintf("canonical {%s} vs ours {%s}", strings.Join(fa, ","), strings.Join(fb, ","))
}

func fieldNames(body []byte) ([]string, bool) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(body, &obj); err != nil {
		return nil, false
	}
	names := make([]string, 0, len(obj))
	for k := range obj {
		names = append(names, k)
	}
	sort.Strings(names)
	return names, true
}

// summarise keeps a DIFF line readable when a service answers with a page of HTML or a
// stack trace.
func summarise(body []byte) string {
	s := strings.TrimSpace(string(body))
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > 160 {
		s = s[:160] + "…"
	}
	return s
}
