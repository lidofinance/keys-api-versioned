package e2e

import (
	"log/slog"
	"time"
)

// slogError is the level the service logs a failure at, including the one failure a
// response cannot carry itself: a stream that died after its headers went out.
const slogError = slog.LevelError

// Waits used across the suite. They are deadlines for something that has already
// happened — a request the harness knows finished, a lock the database has released —
// not sleeps that pace a test, so they can be generous without making the suite slow.
const (
	waitShort = 5 * time.Second
	waitLong  = 20 * time.Second
)
