package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"unicode/utf8"
)

// The find routes read their body as the canonical service's Fastify does: the media type picks a
// parser (JSON, plain text, or a URL-encoded form), the parser's own failures have their own
// answers, and whatever it yields is validated as class-validator validates FindKeyValidation.

const (
	pubkeysMinSizeMessage = "pubkeys must contain at least 1 elements"
	pubkeysArrayMessage   = "pubkeys must be an array"

	emptyJSONMessage   = "Body cannot be empty when content-type is set to 'application/json'"
	invalidJSONMessage = "Body is not valid JSON but content-type is set to 'application/json'"
)

func decodeFindBody(w http.ResponseWriter, r *http.Request) ([]string, bool) {
	var body any

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		// Without a type, a body is 415 and no body reaches the validator. An empty chunked body
		// counts as none: measured, keys-api.lido.fi answers it as it answers Content-Length: 0.
		if n, _ := r.Body.Read(make([]byte, 1)); n > 0 {
			unsupportedMediaType(w)
			return nil, false
		}
		return validateFindBody(w, nil)
	}

	// A parameter without a value is accepted there, so only the media type itself must parse.
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil && !errors.Is(err, mime.ErrInvalidMediaParameter) {
		unsupportedMediaType(w)
		return nil, false
	}
	switch mediaType {
	case "application/json", "text/plain", "application/x-www-form-urlencoded":
	default:
		unsupportedMediaType(w)
		return nil, false
	}

	if r.ContentLength > maxFindBodyBytes {
		bodyTooLarge(w)
		return nil, false
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxFindBodyBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			bodyTooLarge(w)
		} else {
			writeError(w, http.StatusBadRequest, []string{badPubkeyMessage, pubkeysMinSizeMessage, pubkeysArrayMessage})
		}
		return nil, false
	}

	switch mediaType {
	case "application/json":
		if len(raw) == 0 {
			writeStatusMessage(w, http.StatusBadRequest, emptyJSONMessage)
			return nil, false
		}
		body, err = parseJSONBody(raw)
		if err != nil {
			writeStatusMessage(w, http.StatusBadRequest, invalidJSONMessage)
			return nil, false
		}
	case "text/plain":
		body = string(raw)
	case "application/x-www-form-urlencoded":
		body = parseFormBody(string(raw))
	}
	return validateFindBody(w, body)
}

// parseJSONBody is JSON.parse behind secure-json-parse: a leading BOM is dropped, the last of
// duplicate keys wins, and a "__proto__" key or a "constructor" object with a "prototype" key
// anywhere makes the body invalid.
func parseJSONBody(raw []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(bytes.TrimPrefix(raw, []byte("\xef\xbb\xbf"))))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, errors.New("data after the JSON value")
	}
	if poisoned(v) {
		return nil, errors.New("prototype poisoning")
	}
	return v, nil
}

func poisoned(v any) bool {
	switch v := v.(type) {
	case map[string]any:
		if _, ok := v["__proto__"]; ok {
			return true
		}
		if c, ok := v["constructor"].(map[string]any); ok {
			if _, ok := c["prototype"]; ok {
				return true
			}
		}
		for _, child := range v {
			if poisoned(child) {
				return true
			}
		}
	case []any:
		for _, child := range v {
			if poisoned(child) {
				return true
			}
		}
	}
	return false
}

// parseFormBody is fast-querystring, @fastify/formbody's parser: pairs split on '&' only, '+' is a
// space, a repeated key becomes an array, and a part that does not decode is kept as sent.
func parseFormBody(s string) map[string]any {
	out := map[string]any{}
	for _, pair := range strings.Split(s, "&") {
		key, value, hasValue := strings.Cut(pair, "=")
		if !hasValue && key == "" {
			continue
		}
		key, value = formUnescape(key), formUnescape(value)
		switch prev := out[key].(type) {
		case nil:
			out[key] = value
		case string:
			out[key] = []any{prev, value}
		case []any:
			out[key] = append(prev, value)
		}
	}
	return out
}

func formUnescape(s string) string {
	s = strings.ReplaceAll(s, "+", " ")
	if !strings.Contains(s, "%") {
		return s
	}
	decoded, err := url.PathUnescape(s)
	if err != nil || !utf8.ValidString(decoded) {
		return s
	}
	return decoded
}

// validateFindBody reports every failed constraint at once, in class-validator's order. A value
// that is not an array fails the size and array checks, and the pubkey check unless it is itself
// one valid pubkey string.
func validateFindBody(w http.ResponseWriter, body any) ([]string, bool) {
	obj, _ := body.(map[string]any)
	value := obj["pubkeys"]

	list, isArray := value.([]any)
	if !isArray {
		messages := []string{pubkeysMinSizeMessage, pubkeysArrayMessage}
		if s, ok := value.(string); !ok || !validPubkey(s) {
			messages = append([]string{badPubkeyMessage}, messages...)
		}
		writeError(w, http.StatusBadRequest, messages)
		return nil, false
	}
	if len(list) == 0 {
		writeError(w, http.StatusBadRequest, []string{pubkeysMinSizeMessage})
		return nil, false
	}
	pubkeys := make([]string, 0, len(list))
	for _, element := range list {
		// Case-sensitive prefix, as canonical's startsWith('0x').
		s, ok := element.(string)
		if !ok || !validPubkey(s) {
			writeError(w, http.StatusBadRequest, []string{badPubkeyMessage})
			return nil, false
		}
		pubkeys = append(pubkeys, s)
	}
	if len(pubkeys) > maxFindPubkeys {
		bodyTooLarge(w)
		return nil, false
	}
	return pubkeys, true
}

func unsupportedMediaType(w http.ResponseWriter) {
	writeStatusMessage(w, http.StatusUnsupportedMediaType, "Unsupported Media Type")
}
