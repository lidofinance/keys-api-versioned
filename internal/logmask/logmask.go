// Package logmask replaces known secret values in every log record, whatever path the value took to
// get there: an error from a library, a wrapped URL, a panic message.
package logmask

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sort"
	"strings"
	"sync"
	"unicode"

	"github.com/jackc/pgx/v5/pgconn"
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

// FromDSN returns the password of a Postgres DSN, URL or key=value form, read with the driver's grammar.
func FromDSN(dsn string) []string {
	var found []string
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		found = fromDSNURL(dsn)
	} else {
		found = fromKeywordValue(dsn)
	}
	var out []string
	seen := map[string]bool{}
	for _, v := range found {
		if v != "" && !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}

// fromDSNURL: net/url ends userinfo at the last '@', the driver at the first. With an unencoded '@'
// in the password the driver puts its tail into the host, and lookup errors print it.
func fromDSNURL(dsn string) []string {
	var out []string
	if u, err := url.Parse(dsn); err == nil {
		if p, ok := u.User.Password(); ok {
			out = append(out, p)
		}
		if p := u.Query().Get("password"); p != "" {
			out = append(out, p)
		}
		if p := u.Query().Get("sslpassword"); p != "" {
			out = append(out, p)
		}
	}
	rest := dsn[strings.Index(dsn, "://")+3:]
	if end := strings.IndexAny(rest, "/?"); end >= 0 {
		rest = rest[:end]
	}
	if last := strings.LastIndex(rest, "@"); last >= 0 {
		userinfo := rest[:last]
		if _, password, ok := strings.Cut(userinfo, ":"); ok {
			out = append(out, password)
			if unescaped, err := url.PathUnescape(password); err == nil {
				out = append(out, unescaped)
			}
			for _, part := range strings.Split(password, "@") {
				out = append(out, part)
			}
		}
	}
	return out
}

// fromKeywordValue follows libpq: spaces around '=', single-quoted values, backslash escapes. Both
// the raw text and the value are returned, since an error may quote either.
func fromKeywordValue(dsn string) []string {
	var out []string
	s := dsn
	for {
		s = strings.TrimLeft(s, " \t\n\r\f\v")
		if s == "" {
			return out
		}
		eq := strings.IndexByte(s, '=')
		if eq < 0 {
			return out
		}
		key := strings.TrimSpace(s[:eq])
		s = strings.TrimLeft(s[eq+1:], " \t\n\r\f\v")
		var raw, value strings.Builder
		if strings.HasPrefix(s, "'") {
			raw.WriteByte('\'')
			i := 1
			for ; i < len(s); i++ {
				c := s[i]
				raw.WriteByte(c)
				if c == '\\' && i+1 < len(s) {
					i++
					raw.WriteByte(s[i])
					value.WriteByte(s[i])
					continue
				}
				if c == '\'' {
					break
				}
				value.WriteByte(c)
			}
			s = s[min(i+1, len(s)):]
		} else {
			i := 0
			for ; i < len(s) && !unicode.IsSpace(rune(s[i])); i++ {
				c := s[i]
				raw.WriteByte(c)
				if c == '\\' && i+1 < len(s) {
					i++
					raw.WriteByte(s[i])
					value.WriteByte(s[i])
					continue
				}
				value.WriteByte(c)
			}
			s = s[i:]
		}
		if key == "password" || key == "sslpassword" {
			out = append(out, raw.String(), strings.Trim(raw.String(), "'"), value.String())
		}
	}
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

// DSNParseError drops the connection string a driver parse error quotes: its own redaction misses
// spellings such as "password = value". The cause after it carries no credential.
func DSNParseError(err error) error {
	var parse *pgconn.ParseConfigError
	if !errors.As(err, &parse) {
		return err
	}
	if inner := parse.Unwrap(); inner != nil {
		return fmt.Errorf("invalid connection string: %w", inner)
	}
	return errors.New("invalid connection string")
}
