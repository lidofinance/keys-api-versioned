// Package logmask replaces known secret values in every log record, whatever path the value took to
// get there: an error from a library, a wrapped URL, a panic message.
package logmask

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"sort"
	"strings"
	"sync"
	"unicode"
)

// Replacement is what a secret becomes; the same token as the canonical service's logger.
const Replacement = "<removed>"

// Values shorter than this are not masked: "1" or "true" would blank out unrelated text.
const minSecretLen = 6

// Masker holds the secret values. Rotated values are added, never swapped: a request in flight
// can still log the old one.
type Masker struct {
	mu      sync.RWMutex
	secrets []string
}

// Add registers values, longest first so a value containing another is replaced whole.
func (m *Masker) Add(values ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := make(map[string]bool, len(m.secrets))
	for _, s := range m.secrets {
		seen[s] = true
	}
	for _, v := range values {
		if len(v) >= minSecretLen && !seen[v] {
			seen[v] = true
			m.secrets = append(m.secrets, v)
		}
	}
	sort.Slice(m.secrets, func(i, j int) bool { return len(m.secrets[i]) > len(m.secrets[j]) })
}

// Mask returns s with every registered value replaced.
func (m *Masker) Mask(s string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, secret := range m.secrets {
		if strings.Contains(s, secret) {
			s = strings.ReplaceAll(s, secret, Replacement)
		}
	}
	return s
}

// FromURLs masks the password and key-like path segments and query values, so the host stays
// readable; with nothing key-like the whole path and query go, and an unparsable URL goes whole.
func FromURLs(list string) []string {
	var out []string
	for _, raw := range strings.Split(list, ",") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		u, err := url.Parse(raw)
		if err != nil || u.Host == "" {
			out = append(out, raw)
			continue
		}
		var found []string
		if p, ok := u.User.Password(); ok {
			found = append(found, p)
		}
		for _, seg := range strings.Split(u.Path, "/") {
			if keyLike(seg) {
				found = append(found, seg)
			}
		}
		for _, vals := range u.Query() {
			for _, v := range vals {
				if keyLike(v) {
					found = append(found, v)
				}
			}
		}
		if len(found) == 0 {
			if _, tail, ok := strings.Cut(raw, u.Host); ok && tail != "" && tail != "/" {
				found = append(found, tail)
			}
		}
		out = append(out, found...)
	}
	return out
}

// FromDSN returns the password of a Postgres DSN, URL or key=value form.
func FromDSN(dsn string) []string {
	if u, err := url.Parse(dsn); err == nil && u.Scheme != "" {
		if p, ok := u.User.Password(); ok {
			return []string{p}
		}
		if p := u.Query().Get("password"); p != "" {
			return []string{p}
		}
		return nil
	}
	for _, field := range strings.Fields(dsn) {
		if v, ok := strings.CutPrefix(field, "password="); ok {
			return []string{strings.Trim(v, "'")}
		}
	}
	return nil
}

// keyLike: long enough and mixing letters with digits, so "ogrpc" or "eth-beacon-chain" stay
// readable and an API key does not.
func keyLike(s string) bool {
	if len(s) < 16 {
		return false
	}
	var letter, digit bool
	for _, r := range s {
		switch {
		case unicode.IsLetter(r):
			letter = true
		case unicode.IsDigit(r):
			digit = true
		}
	}
	return letter && digit
}

// Handler masks the message and every attribute before the wrapped handler sees them.
type Handler struct {
	next   slog.Handler
	masker *Masker
}

func NewHandler(next slog.Handler, masker *Masker) *Handler {
	return &Handler{next: next, masker: masker}
}

func (h *Handler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *Handler) Handle(ctx context.Context, r slog.Record) error {
	out := slog.NewRecord(r.Time, r.Level, h.masker.Mask(r.Message), r.PC)
	r.Attrs(func(a slog.Attr) bool {
		out.AddAttrs(h.maskAttr(a))
		return true
	})
	return h.next.Handle(ctx, out)
}

func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	masked := make([]slog.Attr, len(attrs))
	for i, a := range attrs {
		masked[i] = h.maskAttr(a)
	}
	return &Handler{next: h.next.WithAttrs(masked), masker: h.masker}
}

func (h *Handler) WithGroup(name string) slog.Handler {
	return &Handler{next: h.next.WithGroup(name), masker: h.masker}
}

func (h *Handler) maskAttr(a slog.Attr) slog.Attr {
	v := a.Value.Resolve()
	switch v.Kind() {
	case slog.KindString:
		return slog.String(a.Key, h.masker.Mask(v.String()))
	case slog.KindGroup:
		group := v.Group()
		masked := make([]any, len(group))
		for i, g := range group {
			masked[i] = h.maskAttr(g)
		}
		return slog.Group(a.Key, masked...)
	case slog.KindAny:
		// Rendered the way the text handler would; replaced only when a secret is in it, so values
		// without one keep their type.
		var s string
		if err, ok := v.Any().(error); ok {
			s = err.Error()
		} else {
			s = fmt.Sprintf("%+v", v.Any())
		}
		if masked := h.masker.Mask(s); masked != s {
			return slog.String(a.Key, masked)
		}
		return slog.Attr{Key: a.Key, Value: v}
	default:
		return slog.Attr{Key: a.Key, Value: v}
	}
}
