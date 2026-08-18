// Package secrets reads the credentials this service is given as a file, and notices when
// they are rotated.
//
// The execution endpoints carry provider credentials, and in Kubernetes the OpenBao agent
// delivers them by writing a JSON file rather than by setting environment variables. The
// reason is not aesthetics: a process cannot be handed new environment variables from
// outside, so env-based delivery costs a pod restart per rotation — and until that restart
// happens the old credential keeps working, which makes a rotation nobody applied look
// exactly like a healthy deployment right up to the moment the provider revokes the key.
//
// The agent updates the file by writing a temp file and renaming it over the path. The
// rename is atomic, so a reader never sees half a file — but it also creates a new inode,
// which is why Watcher polls the path with os.Stat instead of watching the file. A watcher
// attached to the file itself (fsnotify) goes silent after the first rotation.
//
// An absent file is a normal state: it means "no agent here, read the environment", which
// is how the compose deployment runs. The contract is deliberately the same as
// ethereum-head-watcher's src/secrets.py, so one description covers both services.
package secrets

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// DefaultPath is where the agent is configured to render, and DefaultPollInterval is how
// often the path is stat'ed.
const (
	DefaultPath         = "/vault/secrets/config"
	DefaultPollInterval = 10 * time.Second
)

// Read returns the file's values, or an empty map when there is no usable file.
//
// Absent is not an error — it is the environment-only deployment. Present but unparseable
// is an error the caller reports and then treats the same way: refusing to start would turn
// a bad render of one key into an outage of the whole service.
func Read(path string) (map[string]string, error) {
	if path == "" {
		return nil, nil
	}

	blob, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	var raw map[string]any
	if err := json.Unmarshal(blob, &raw); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if raw == nil {
		return nil, fmt.Errorf("parse %s: not a JSON object", path)
	}

	values := make(map[string]string, len(raw))
	for key, value := range raw {
		switch v := value.(type) {
		case string:
			values[key] = v
		case nil:
			// A key rendered as null is a template that resolved to nothing. Dropped rather
			// than stored as "null", which would reach a URL parser as a hostname.
			continue
		default:
			values[key] = fmt.Sprint(v)
		}
	}
	return values, nil
}

// LoadIntoEnv makes the file the source of truth for everything read through the
// environment, before anything reads it.
//
// One place rather than a lookup at every call site: the values a rotation can change are
// the same ones the environment carries, and two sources consulted in different orders by
// different callers is how a service ends up half-rotated. Returns the values it applied, so
// the caller can tell later what a rotation actually changed.
func LoadIntoEnv(path string) (map[string]string, error) {
	values, err := Read(path)
	if err != nil {
		return nil, err
	}
	for key, value := range values {
		if err := os.Setenv(key, value); err != nil {
			return nil, fmt.Errorf("set %s from %s: %w", key, path, err)
		}
	}
	return values, nil
}

// Watcher re-reads the file when its mtime changes and hands the new values to a callback.
type Watcher struct {
	path     string
	interval time.Duration
	mtime    int64
	found    bool

	// OnChange applies the new values. An error means nothing was applied, and the
	// previous values stay in force.
	OnChange func(values map[string]string) error
	// OnError reports a bad render or a failed apply. Separate from a logger so the caller
	// decides what a failure costs — here it also moves a counter.
	OnError func(err error)
}

// NewWatcher starts from the file's current mtime, so the first poll after a start does not
// re-apply what the process was configured with.
func NewWatcher(path string, interval time.Duration, onChange func(map[string]string) error, onError func(error)) *Watcher {
	if interval <= 0 {
		interval = DefaultPollInterval
	}
	w := &Watcher{path: path, interval: interval, OnChange: onChange, OnError: onError}
	w.mtime, w.found = mtimeOf(path)
	return w
}

func mtimeOf(path string) (int64, bool) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, false
	}
	return info.ModTime().UnixNano(), true
}

// CheckOnce reports whether a change was seen and applied. Separate from Run so the
// behaviour is testable without waiting on a ticker.
func (w *Watcher) CheckOnce() bool {
	mtime, found := mtimeOf(w.path)
	if !found || (w.found && mtime == w.mtime) {
		return false
	}
	w.mtime, w.found = mtime, true

	values, err := Read(w.path)
	if err != nil {
		w.reportError(err)
		return false
	}
	if len(values) == 0 {
		// A rotation that renders to nothing is not something to apply over working values.
		// The mtime is remembered anyway — otherwise one broken template would report the
		// same error every interval, forever.
		w.reportError(fmt.Errorf("secrets file %s changed but has no usable values, keeping the previous ones", w.path))
		return false
	}

	if w.OnChange != nil {
		if err := w.OnChange(values); err != nil {
			w.reportError(fmt.Errorf("apply rotated secrets: %w", err))
			return false
		}
	}
	return true
}

// Run polls until the context is cancelled. Meant to be started in a goroutine.
func (w *Watcher) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.CheckOnce()
		}
	}
}

func (w *Watcher) reportError(err error) {
	if w.OnError != nil {
		w.OnError(err)
	}
}
