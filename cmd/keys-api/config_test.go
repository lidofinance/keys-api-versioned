package main

import (
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/metrics"
)

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
