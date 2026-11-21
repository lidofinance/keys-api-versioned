package logmask

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	alchemyKey = "aBcD1234efGH5678ijKL9012"
	drpcKey    = "Zx9Yw8Vu7Ts6Rq5Po4Nm3Lk2"
	dbPassword = "s3cret-db-pass"
)

var providers = "https://eth-mainnet.example.com/v2/" + alchemyKey +
	",https://lb.example.org/ogrpc?network=ethereum&dkey=" + drpcKey

func newLogger(t *testing.T) (*slog.Logger, *bytes.Buffer, *Masker) {
	t.Helper()
	var buf bytes.Buffer
	m := &Masker{}
	m.Add(FromURLs(providers)...)
	m.Add(FromDSN("postgres://keys:" + dbPassword + "@db:5432/keys_api")...)
	return slog.New(NewHandler(slog.NewTextHandler(&buf, nil), m)), &buf, m
}

func assertClean(t *testing.T, out string) {
	t.Helper()
	for _, secret := range []string{alchemyKey, drpcKey, dbPassword} {
		if strings.Contains(out, secret) {
			t.Fatalf("secret %q leaked:\n%s", secret, out)
		}
	}
}

func TestMasksSecretsWhereverTheyAppear(t *testing.T) {
	log, buf, _ := newLogger(t)

	urlErr := &url.Error{Op: "Post", URL: "https://eth-mainnet.example.com/v2/" + alchemyKey, Err: errors.New("timeout")}
	log.Error("message with "+drpcKey,
		"err", fmt.Errorf("wrapped: %w", urlErr),
		"plain", "dkey="+drpcKey,
		"dsn_error", errors.New("auth failed for password "+dbPassword),
		slog.Group("nested", "url", "https://lb.example.org/ogrpc?network=ethereum&dkey="+drpcKey),
		"struct", struct{ Endpoint string }{"https://eth-mainnet.example.com/v2/" + alchemyKey},
	)
	log.With("bound", alchemyKey).Info("with")

	out := buf.String()
	assertClean(t, out)
	if !strings.Contains(out, Replacement) {
		t.Fatalf("no replacement token:\n%s", out)
	}
	if !strings.Contains(out, "eth-mainnet.example.com") || !strings.Contains(out, "network=ethereum") {
		t.Fatalf("host or non-secret query masked too:\n%s", out)
	}
}

func TestRotatedValuesAreMaskedAndOldOnesStay(t *testing.T) {
	log, buf, m := newLogger(t)
	const rotated = "N3wKeyAfterRotation00001"
	m.Add(FromURLs("https://eth-mainnet.example.com/v2/" + rotated)...)

	log.Info("x", "old", alchemyKey, "new", rotated)

	out := buf.String()
	assertClean(t, out)
	if strings.Contains(out, rotated) {
		t.Fatalf("rotated key leaked:\n%s", out)
	}
}

func TestLeavesOrdinaryValuesAlone(t *testing.T) {
	log, buf, _ := newLogger(t)
	log.Info("rpc", "method", "eth_call", "calls", 3, "took", "12ms", "path", "/ogrpc")

	out := buf.String()
	if strings.Contains(out, Replacement) {
		t.Fatalf("ordinary values masked:\n%s", out)
	}
}

func TestKeyLike(t *testing.T) {
	for s, want := range map[string]bool{
		"ogrpc":                  false,
		"eth-beacon-chain-hoodi": false,
		"v2":                     false,
		alchemyKey:               true,
		drpcKey:                  true,
	} {
		if got := keyLike(s); got != want {
			t.Errorf("keyLike(%q) = %v, want %v", s, got, want)
		}
	}
}

func TestFromDSN(t *testing.T) {
	for dsn, want := range map[string]string{
		"postgres://u:" + dbPassword + "@h/db":                      dbPassword,
		"postgres://u@h/db?password=" + dbPassword:                  dbPassword,
		"host=h user=u password=" + dbPassword + " sslmode=disable": dbPassword,
		"postgres://u@h/db":                                         "",
	} {
		got := strings.Join(FromDSN(dsn), "")
		if got != want {
			t.Errorf("FromDSN(%q) = %q, want %q", dsn, got, want)
		}
	}
}

func TestFromURLsFallsBackToTheWholeTail(t *testing.T) {
	got := FromURLs("https://node.example.com/rpc/shortkey?token=abc, not a url")
	want := []string{"/rpc/shortkey?token=abc", "not a url"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("FromURLs = %q, want %q", got, want)
	}
}

// Spellings the driver accepts that a whitespace split reads differently.
func TestFromDSNReadsTheDriversGrammar(t *testing.T) {
	cases := map[string]struct{ dsn, leaked string }{
		"spaces around =":    {"host=pg user=keys password = Zx81kP0qWm7Lr2 dbname=keys", "Zx81kP0qWm7Lr2"},
		"space before =":     {"host=pg password =Zx81kP0qWm7Lr2", "Zx81kP0qWm7Lr2"},
		"escaped space":      {`host=pg password=abcdef\ ghijkl dbname=keys`, `abcdef\ ghijkl`},
		"quoted":             {"host=pg password='Zx81 kP0qWm7Lr2'", "Zx81 kP0qWm7Lr2"},
		"unencoded @ in URL": {"postgres://keys:Qa7@Lm92XcVb4Tz@pg.invalid:5432/keys", "Lm92XcVb4Tz"},
		"encoded URL":        {"postgres://keys:Qa7%40Lm92XcVb4Tz@pg:5432/keys", "Qa7@Lm92XcVb4Tz"},
	}
	for name, c := range cases {
		var m Masker
		m.Add(FromDSN(c.dsn)...)
		if line := m.Mask("err: cannot parse `" + c.dsn + "`: lookup " + c.leaked + "@pg.invalid"); strings.Contains(line, c.leaked) {
			t.Errorf("%s: %q still shows %q", name, line, c.leaked)
		}
	}
}

// The driver's parse error quotes the whole connection string.
func TestDSNParseErrorDropsTheConnectionString(t *testing.T) {
	_, err := pgxpool.ParseConfig("host=pg user=keys password = Zx81kP0qWm7Lr2 sslmode=verify-full sslrootcert=/missing/ca.crt")
	if err == nil {
		t.Fatal("want a parse error for a missing CA file")
	}
	if got := DSNParseError(err).Error(); strings.Contains(got, "Zx81kP0qWm7Lr2") || !strings.Contains(got, "CA file") {
		t.Errorf("got %q: want the cause without the password", got)
	}
}
