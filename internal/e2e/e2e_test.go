package e2e

import (
	"log/slog"
	"time"
)

const slogError = slog.LevelError

// Deadlines for something that already happened, not pacing sleeps, so they can be generous.
const (
	waitShort = 5 * time.Second
	waitLong  = 20 * time.Second
)
