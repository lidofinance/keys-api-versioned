package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestUsageDoesNotPrintTheEndpointFromTheEnvironment(t *testing.T) {
	if os.Getenv("INGEST_USAGE_CHILD") == "1" {
		os.Args = []string{"ingest", "-h"}
		main()
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestUsageDoesNotPrintTheEndpointFromTheEnvironment$")
	cmd.Env = append(os.Environ(), "INGEST_USAGE_CHILD=1",
		"PROVIDERS_URLS=https://rpc.example.com/v3/secret-api-key")
	out, _ := cmd.CombinedOutput()
	if !strings.Contains(string(out), "-rpc") {
		t.Fatalf("the child did not print usage:\n%s", out)
	}
	if strings.Contains(string(out), "secret-api-key") {
		t.Errorf("-h printed the endpoint from PROVIDERS_URLS:\n%s", out)
	}
}
