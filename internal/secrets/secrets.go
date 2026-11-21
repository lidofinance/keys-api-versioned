// Package secrets reads credentials delivered as a JSON file. The path is polled, never watched:
// the writer renames a temp file over it, so the inode changes on every rotation.
package secrets

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"
)

const (
	DefaultPath         = "/vault/secrets/config"
	DefaultPollInterval = 10 * time.Second
)

// Read returns an empty map when the file is absent (env-only deployment). A bad render is an
// error the caller reports but survives: refusing to start would turn one bad key into an outage.
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
			if v == "" {
				// Same as a null: a template default for a field that is gone.
				continue
			}
			values[key] = v
		case nil:
			// A null is a template that resolved to nothing; stored as "null" it would reach a URL parser.
			continue
		default:
			values[key] = fmt.Sprint(v)
		}
	}
	return values, nil
}

// Allowed: the file carries credentials only. Other configuration belongs in the environment,
// where a deploy shows it; whoever writes the file must not be able to set KEYS_API_MODE.
var Allowed = map[string]bool{"DATABASE_URL": true, "PROVIDERS_URLS": true}

// Split returns the allowed values and the sorted names of the others.
func Split(values map[string]string) (map[string]string, []string) {
	kept := make(map[string]string, len(values))
	var ignored []string
	for key, value := range values {
		if Allowed[key] {
			kept[key] = value
		} else {
			ignored = append(ignored, key)
		}
	}
	sort.Strings(ignored)
	return kept, ignored
}

// LoadIntoEnv makes the file the single source for env-read values, so a rotation cannot leave
// callers half-rotated. Returns what it applied and the keys it ignored.
func LoadIntoEnv(path string) (map[string]string, []string, error) {
	applied, ignored, _, err := LoadIntoEnvChecked(path, nil)
	return applied, ignored, err
}

// LoadIntoEnvChecked applies only the values check accepts; the environment keeps its own for the
// rest, whose errors come back by key.
func LoadIntoEnvChecked(path string, check func(key, value string) error) (map[string]string, []string, map[string]error, error) {
	values, err := Read(path)
	if err != nil {
		return nil, nil, nil, err
	}
	kept, ignored := Split(values)
	var rejected map[string]error
	for key, value := range kept {
		if check != nil {
			if err := check(key, value); err != nil {
				if rejected == nil {
					rejected = map[string]error{}
				}
				rejected[key] = err
				delete(kept, key)
				continue
			}
		}
		if err := os.Setenv(key, value); err != nil {
			return nil, nil, nil, fmt.Errorf("set %s from %s: %w", key, path, err)
		}
	}
	return kept, ignored, rejected, nil
}

// LooseMode reports a file readable by group or others.
func LooseMode(path string) (os.FileMode, bool) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, false
	}
	return info.Mode().Perm(), info.Mode().Perm()&0o077 != 0
}

type Watcher struct {
	path     string
	interval time.Duration
	mtime    int64
	found    bool

	// OnChange returning an error means nothing was applied; the previous values stay in force.
	OnChange func(values map[string]string) error
	OnError  func(err error)
}

// NewWatcher starts from the current mtime, so the first poll does not re-apply startup values.
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
		// The mtime is remembered anyway, so a broken file is reported once, not every interval.
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
