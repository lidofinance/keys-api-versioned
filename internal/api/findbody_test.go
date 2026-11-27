package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Every body below is what keys-api.lido.fi (6.0.0) answered to the same request on 2026-11-27.
func TestFindBodiesAnswerAsCanonicalDoes(t *testing.T) {
	handler := New(newMemStore(1), Config{ChainID: 1}).Handler()
	p := "0x" + strings.Repeat("ab", 48)

	const (
		all      = `{"message":["Each pubkey must be a valid 0x-prefixed 48-byte hex string","pubkeys must contain at least 1 elements","pubkeys must be an array"],"error":"Bad Request","statusCode":400}`
		notArray = `{"message":["pubkeys must contain at least 1 elements","pubkeys must be an array"],"error":"Bad Request","statusCode":400}`
		badKey   = `{"message":["Each pubkey must be a valid 0x-prefixed 48-byte hex string"],"error":"Bad Request","statusCode":400}`
		minSize  = `{"message":["pubkeys must contain at least 1 elements"],"error":"Bad Request","statusCode":400}`
		empty    = `{"statusCode":400,"message":"Body cannot be empty when content-type is set to 'application/json'"}`
		invalid  = `{"statusCode":400,"message":"Body is not valid JSON but content-type is set to 'application/json'"}`
		media    = `{"statusCode":415,"message":"Unsupported Media Type"}`
		ok       = ""
	)
	const (
		jsonType = "application/json"
		textType = "text/plain"
		formType = "application/x-www-form-urlencoded"
	)

	for _, tc := range []struct {
		name, contentType, body string
		chunked                 bool
		want                    string
	}{
		{"json, empty", jsonType, "", false, empty},
		{"json, empty and chunked", jsonType, "", true, empty},
		{"json, whitespace", jsonType, " ", false, invalid},
		{"json, cut short", jsonType, "{", false, invalid},
		{"json, data after the value", jsonType, `{"pubkeys":["` + p + `"]} x`, false, invalid},
		{"json, two values", jsonType, `{"pubkeys":["` + p + `"]}{}`, false, invalid},
		{"json, __proto__ key", jsonType, `{"__proto__":{"a":1},"pubkeys":["` + p + `"]}`, false, invalid},
		{"json, escaped __proto__ key", jsonType, `{"__proto__":{},"pubkeys":["` + p + `"]}`, false, invalid},
		{"json, nested __proto__ key", jsonType, `{"pubkeys":["` + p + `"],"x":{"__proto__":1}}`, false, invalid},
		{"json, __proto__ as a value", jsonType, `{"pubkeys":["` + p + `"],"x":"__proto__"}`, false, ok},
		{"json, constructor with prototype", jsonType, `{"constructor":{"prototype":{}},"pubkeys":["` + p + `"]}`, false, invalid},
		{"json, plain constructor", jsonType, `{"constructor":1,"pubkeys":["` + p + `"]}`, false, ok},
		{"json, null", jsonType, "null", false, all},
		{"json, top-level array", jsonType, "[]", false, all},
		{"json, top-level string", jsonType, `"s"`, false, all},
		{"json, no field", jsonType, "{}", false, all},
		{"json, field in another case", jsonType, `{"Pubkeys":["` + p + `"]}`, false, all},
		{"json, field a string", jsonType, `{"pubkeys":"x"}`, false, all},
		{"json, field one valid pubkey", jsonType, `{"pubkeys":"` + p + `"}`, false, notArray},
		{"json, field null", jsonType, `{"pubkeys":null}`, false, all},
		{"json, field an object", jsonType, `{"pubkeys":{}}`, false, all},
		{"json, number element", jsonType, `{"pubkeys":[1]}`, false, badKey},
		{"json, null element", jsonType, `{"pubkeys":[null]}`, false, badKey},
		{"json, empty string element", jsonType, `{"pubkeys":[""]}`, false, badKey},
		{"json, last duplicate wins", jsonType, `{"pubkeys":"x","pubkeys":["` + p + `"]}`, false, ok},
		{"json, last duplicate empty", jsonType, `{"pubkeys":["` + p + `"],"pubkeys":[]}`, false, minSize},
		{"json, byte order mark", jsonType, "\xef\xbb\xbf" + `{"pubkeys":["` + p + `"]}`, false, ok},
		{"json, escaped digit", jsonType, `{"pubkeys":["0x` + p[2:] + `"]}`, false, ok},
		{"json, type in capitals", "APPLICATION/JSON", `{"pubkeys":["` + p + `"]}`, false, ok},
		{"json, type with parameters", "application/json ; charset=utf-8", `{"pubkeys":["` + p + `"]}`, false, ok},
		{"json, parameter without a value", "application/json; charset", `{"pubkeys":["` + p + `"]}`, false, ok},
		{"text, a valid JSON body", textType, `{"pubkeys":["` + p + `"]}`, false, all},
		{"text, empty", textType, "", false, all},
		{"form, one valid pubkey", formType, "pubkeys=" + p, false, notArray},
		{"form, repeated key", formType, "pubkeys=" + p + "&pubkeys=" + p, false, ok},
		{"form, bracketed key", formType, "pubkeys[]=" + p, false, all},
		{"form, semicolon is not a separator", formType, "pubkeys=" + p + ";pubkeys=" + p, false, all},
		{"form, undecodable part kept as sent", formType, "pubkeys=%ZZ&pubkeys=" + p, false, badKey},
		{"form, key without a value", formType, "pubkeys&pubkeys=" + p, false, badKey},
		{"form, encoded key and value", formType, "pub%6Beys=%30x" + p[2:] + "&pubkeys=" + p, false, ok},
		{"form, empty", formType, "", false, all},
		{"form, type in mixed case", "Application/X-WWW-Form-Urlencoded", "pubkeys=" + p + "&pubkeys=" + p, false, ok},
		{"no type, no body", "", "", false, all},
		{"no type, a body", "", `{"pubkeys":["` + p + `"]}`, false, media},
		{"no type, chunked and empty", "", "", true, all},
		{"no type, chunked with a body", "", "{}", true, media},
		{"another JSON type", "application/vnd.api+json", `{"pubkeys":["` + p + `"]}`, false, media},
		{"text/json", "text/json", `{"pubkeys":["` + p + `"]}`, false, media},
		{"a bare word", "json", `{"pubkeys":["` + p + `"]}`, false, media},
		{"an unknown type, empty", "application/xml", "", false, media},
		{"multipart", "multipart/form-data; boundary=x", "--x--", false, media},
	} {
		for _, path := range []string{"/v1/keys/find", "/v1/modules/1/keys/find"} {
			var body io.Reader = strings.NewReader(tc.body)
			if tc.chunked {
				body = io.MultiReader(body)
			}
			req := httptest.NewRequest(http.MethodPost, path, body)
			if tc.chunked {
				req.ContentLength = -1
				req.TransferEncoding = []string{"chunked"}
			}
			if tc.contentType != "" {
				req.Header.Set("Content-Type", tc.contentType)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if tc.want == ok {
				if rec.Code != http.StatusOK {
					t.Errorf("%s %s: got %d %s, want 200", path, tc.name, rec.Code, rec.Body.String())
				}
				continue
			}
			if got := rec.Body.String(); got != tc.want {
				t.Errorf("%s %s: got %d %s\n want %s", path, tc.name, rec.Code, got, tc.want)
			}
		}
	}
}
