package secrets

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// render replaces the file like the real writer: temp file renamed over the path (new inode).
func render(t *testing.T, path, contents string) {
	t.Helper()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(contents), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatalf("rename: %v", err)
	}
	// Nudge the mtime forward: some filesystems keep only second resolution.
	future := time.Now().Add(time.Second)
	if err := os.Chtimes(path, future, future); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
}

func TestReadAbsentFileIsNotAnError(t *testing.T) {
	values, err := Read(filepath.Join(t.TempDir(), "nothing-here"))
	if err != nil {
		t.Fatalf("absent file should read as no values, got %v", err)
	}
	if len(values) != 0 {
		t.Fatalf("expected no values, got %v", values)
	}
}

func TestReadStringifiesValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	render(t, path, `{"PROVIDERS_URLS":"http://el:8545","PROVIDER_TIMEOUT_MS":1500,"EMPTY":null}`)

	values, err := Read(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if values["PROVIDERS_URLS"] != "http://el:8545" {
		t.Fatalf("urls: %q", values["PROVIDERS_URLS"])
	}
	// A number in the secret must not reach a parser as a number, and a null must not
	// reach one as the string "null".
	if values["PROVIDER_TIMEOUT_MS"] != "1500" {
		t.Fatalf("timeout: %q", values["PROVIDER_TIMEOUT_MS"])
	}
	if _, ok := values["EMPTY"]; ok {
		t.Fatalf("a null value should be dropped, got %q", values["EMPTY"])
	}
}

func TestReadRejectsWhatIsNotAJSONObject(t *testing.T) {
	for name, contents := range map[string]string{
		"not json":   "nope",
		"json array": `["http://el:8545"]`,
		"json null":  "null",
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config")
			render(t, path, contents)
			if _, err := Read(path); err == nil {
				t.Fatal("expected an error rather than silent acceptance")
			}
		})
	}
}

func TestWatcherSeesARenameOverThePath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	render(t, path, `{"PROVIDERS_URLS":"http://one:8545"}`)

	var got map[string]string
	w := NewWatcher(path, time.Minute, func(values map[string]string) error {
		got = values
		return nil
	}, nil)

	if !w.CheckOnce() || got["PROVIDERS_URLS"] != "http://one:8545" {
		t.Fatalf("the first poll offers the file as it is, got %v", got)
	}
	if w.CheckOnce() {
		t.Fatal("an unchanged file is not a rotation")
	}

	render(t, path, `{"PROVIDERS_URLS":"http://two:8545"}`)
	if !w.CheckOnce() {
		t.Fatal("a rename over the path should be seen")
	}
	if got["PROVIDERS_URLS"] != "http://two:8545" {
		t.Fatalf("applied %v", got)
	}
}

func TestWatcherKeepsPreviousValuesOnABadRender(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	render(t, path, `{"PROVIDERS_URLS":"http://one:8545"}`)

	applied := 0
	reported := 0
	w := NewWatcher(path, time.Minute,
		func(map[string]string) error { applied++; return nil },
		func(error) { reported++ })

	render(t, path, "")
	if w.CheckOnce() {
		t.Fatal("a broken render must not count as applied")
	}
	if applied != 0 {
		t.Fatalf("onChange ran %d times", applied)
	}
	if reported != 1 {
		t.Fatalf("expected one report, got %d", reported)
	}

	// The mtime is remembered even for a bad file, so it is reported once, not every interval.
	if w.CheckOnce() || reported != 1 {
		t.Fatalf("a bad render should be reported once, got %d reports", reported)
	}
}

func TestWatcherKeepsPreviousValuesWhenApplyFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	render(t, path, `{"PROVIDERS_URLS":"http://one:8545"}`)

	reported := 0
	w := NewWatcher(path, time.Minute,
		func(map[string]string) error { return errors.New("cannot apply") },
		func(error) { reported++ })

	render(t, path, `{"PROVIDERS_URLS":"http://two:8545"}`)
	if w.CheckOnce() {
		t.Fatal("a failed apply must not count as applied")
	}
	if reported != 1 {
		t.Fatalf("expected the failure to be reported once, got %d", reported)
	}
}

func TestLoadIntoEnvPutsTheFileOverTheEnvironment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	render(t, path, `{"PROVIDERS_URLS":"http://from-file:8545"}`)
	t.Setenv("PROVIDERS_URLS", "http://from-env:8545")

	values, _, err := LoadIntoEnv(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(values) != 1 {
		t.Fatalf("expected the applied values back, got %v", values)
	}
	if os.Getenv("PROVIDERS_URLS") != "http://from-file:8545" {
		t.Fatalf("environment not overridden: %q", os.Getenv("PROVIDERS_URLS"))
	}
}

func TestLoadIntoEnvLeavesTheEnvironmentAloneWithoutAFile(t *testing.T) {
	t.Setenv("PROVIDERS_URLS", "http://from-env:8545")

	values, _, err := LoadIntoEnv(filepath.Join(t.TempDir(), "nothing-here"))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(values) != 0 {
		t.Fatalf("expected no values, got %v", values)
	}
	if os.Getenv("PROVIDERS_URLS") != "http://from-env:8545" {
		t.Fatalf("environment changed: %q", os.Getenv("PROVIDERS_URLS"))
	}
}

func TestTheFileSetsCredentialsOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	render(t, path, `{"PROVIDERS_URLS":"http://from-file:8545","KEYS_API_MODE":"ingest","LIDO_LOCATOR":"0x0"}`)
	t.Setenv("KEYS_API_MODE", "server")

	values, ignored, err := LoadIntoEnv(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if _, ok := values["PROVIDERS_URLS"]; !ok || len(values) != 1 {
		t.Errorf("applied %v, want PROVIDERS_URLS only", values)
	}
	if strings.Join(ignored, ",") != "KEYS_API_MODE,LIDO_LOCATOR" {
		t.Errorf("ignored %v, want KEYS_API_MODE and LIDO_LOCATOR", ignored)
	}
	if got := os.Getenv("KEYS_API_MODE"); got != "server" {
		t.Errorf("KEYS_API_MODE = %q: the file changed configuration it may not set", got)
	}
}

func TestAFileReadableByOthersIsReported(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	render(t, path, `{}`)
	for mode, loose := range map[os.FileMode]bool{0o600: false, 0o400: false, 0o640: true, 0o644: true} {
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
		if _, got := LooseMode(path); got != loose {
			t.Errorf("mode %o: loose = %t, want %t", mode, got, loose)
		}
	}
}

// An empty string is a template default for a field that is gone, like a null.
func TestAnEmptyValueIsSkippedLikeANull(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	render(t, path, `{"DATABASE_URL":"","PROVIDERS_URLS":"http://node:8545"}`)
	values, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := values["DATABASE_URL"]; ok || values["PROVIDERS_URLS"] == "" {
		t.Errorf("read %v, want the empty DATABASE_URL skipped", values)
	}
}
