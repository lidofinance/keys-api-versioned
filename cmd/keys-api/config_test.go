package main

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/metrics"
	"github.com/lidofinance/keys-api-versioned/internal/secrets"
)

func TestAnUnparsableOrOutOfRangeVariableFailsStartup(t *testing.T) {
	for key, value := range map[string]string{
		"PROVIDER_TIMEOUT_MS":         "12O00",
		"DATABASE_MAX_CONNS":          "-1",
		"PORT":                        "70000",
		"KEYS_API_BUILD_AT_FINALIZED": "yes please",
	} {
		t.Run(key, func(t *testing.T) {
			t.Setenv(key, value)
			err := validateEnv()
			if err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("%s=%q: got %v, want an error naming it", key, value, err)
			}
			if strings.Contains(err.Error(), value) {
				t.Errorf("the error echoes the value: %v", err)
			}
		})
	}

	t.Setenv("PROVIDER_TIMEOUT_MS", "12000")
	t.Setenv("KEYS_API_BUILD_AT_FINALIZED", "true")
	t.Setenv("RETAIN_VERSIONS", "0")
	if err := validateEnv(); err != nil {
		t.Fatalf("valid values: %v", err)
	}
}

func TestARotatedEndpointListIsValidatedBeforeTheSwap(t *testing.T) {
	t.Setenv("PROVIDERS_URLS", "https://old.example.com/v3/old-key")
	swapped := 0
	set := func(string) error { swapped++; return nil }

	for _, bad := range []string{"rpc.example.com/v3/secret-key", "https://ok.example.com, ftp://h/secret-key", " , "} {
		err := applyRotatedSecrets(map[string]string{"PROVIDERS_URLS": bad}, set, nil, discard(), metrics.New("keys_api"))
		if err == nil {
			t.Errorf("%q was accepted", bad)
		} else if strings.Contains(err.Error(), "secret-key") {
			t.Errorf("the error carries the URL: %v", err)
		}
	}
	if swapped != 0 {
		t.Fatalf("endpoints were swapped %d times for invalid lists", swapped)
	}

	good := "https://new.example.com/v3/new-key,http://10.0.0.1:8545"
	if err := applyRotatedSecrets(map[string]string{"PROVIDERS_URLS": good}, set, nil, discard(), metrics.New("keys_api")); err != nil {
		t.Fatal(err)
	}
	if swapped != 1 || os.Getenv("PROVIDERS_URLS") != good {
		t.Errorf("a valid list: swapped %d times, environment %q", swapped, os.Getenv("PROVIDERS_URLS"))
	}
}

func TestADatabaseURLRotationAsksForARestart(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://u:old@db/keys")
	restart := make(chan error, 1)

	same := map[string]string{"DATABASE_URL": "postgres://u:old@db/keys"}
	if err := applyRotatedSecrets(same, nil, restart, discard(), metrics.New("keys_api")); err != nil || len(restart) != 0 {
		t.Fatalf("an unchanged value: err %v, restart requested %t", err, len(restart) != 0)
	}

	rotated := map[string]string{"DATABASE_URL": "postgres://u:new@db/keys"}
	if err := applyRotatedSecrets(rotated, nil, restart, discard(), metrics.New("keys_api")); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-restart:
		if strings.Contains(err.Error(), "new@") {
			t.Errorf("the restart reason carries the DSN: %v", err)
		}
	default:
		t.Fatal("a rotated DATABASE_URL did not ask for a restart")
	}
}

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestStreamsDefaultToHalfThePoolAndMustStayBelowIt(t *testing.T) {
	t.Setenv("DATABASE_MAX_CONNS", "10")
	if got := maxStreams(); got != 5 {
		t.Errorf("maxStreams() = %d with a pool of 10, want 5", got)
	}
	t.Setenv("DATABASE_MAX_CONNS", "1")
	if got := maxStreams(); got != 1 {
		t.Errorf("maxStreams() = %d with a pool of 1, want 1", got)
	}
	// The default fills a pool of 1, and a dump then holds readiness and the writer for its duration.
	if err := validateEnv(); err == nil || !strings.Contains(err.Error(), "DATABASE_MAX_CONNS") {
		t.Errorf("a pool of 1 with the default stream cap: got %v, want an error", err)
	}
	t.Setenv("KEYS_API_MODE", "ingest")
	if err := validateEnv(); err != nil {
		t.Errorf("a pool of 1 in ingest mode, which serves no streams: %v", err)
	}
	t.Setenv("KEYS_API_MODE", "all")

	t.Setenv("DATABASE_MAX_CONNS", "10")
	t.Setenv("STREAM_MAX_CONCURRENT", "10")
	if err := validateEnv(); err == nil || !strings.Contains(err.Error(), "STREAM_MAX_CONCURRENT") {
		t.Errorf("streams at the pool size: got %v, want an error naming STREAM_MAX_CONCURRENT", err)
	}
	t.Setenv("STREAM_MAX_CONCURRENT", "9")
	if err := validateEnv(); err != nil {
		t.Errorf("streams below the pool size: %v", err)
	}
}

// A rotation carrying a value boot rejects is refused: restarting on it would crash-loop every replica.
func TestARotationBootWouldRejectDoesNotRestart(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://u:old@db/keys")
	t.Setenv("PROVIDERS_URLS", "http://node:8545")

	for name, values := range map[string]map[string]string{
		"a DSN that does not parse": {"DATABASE_URL": "<no value>"},
		"a placeholder endpoint list": {
			"DATABASE_URL":   "postgres://u:new@db/keys",
			"PROVIDERS_URLS": "<no value>",
		},
	} {
		restart := make(chan error, 1)
		err := applyRotatedSecrets(values, nil, restart, discard(), metrics.New("keys_api"))
		if err == nil || len(restart) != 0 {
			t.Errorf("%s: err %v, restart requested %t; want an error and no restart", name, err, len(restart) != 0)
		}
		if err != nil && strings.Contains(err.Error(), "new@") {
			t.Errorf("%s: the error carries the DSN: %v", name, err)
		}
	}
}

// At boot a rejected file value leaves the environment's own value in force.
func TestBootKeepsTheEnvironmentForARejectedFileValue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	if err := os.WriteFile(path, []byte(`{"DATABASE_URL":"postgres://u:p@db/keys","PROVIDERS_URLS":"<no value>"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PROVIDERS_URLS", "http://node:8545")
	t.Setenv("DATABASE_URL", "")

	applied, _, rejected, err := secrets.LoadIntoEnvChecked(path, checkSecret)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := rejected["PROVIDERS_URLS"]; !ok {
		t.Errorf("rejected %v, want PROVIDERS_URLS", rejected)
	}
	if os.Getenv("PROVIDERS_URLS") != "http://node:8545" {
		t.Errorf("PROVIDERS_URLS is %q, want the environment's", os.Getenv("PROVIDERS_URLS"))
	}
	if applied["DATABASE_URL"] == "" || os.Getenv("DATABASE_URL") != "postgres://u:p@db/keys" {
		t.Errorf("a valid DATABASE_URL was not applied: %v", applied)
	}
}
