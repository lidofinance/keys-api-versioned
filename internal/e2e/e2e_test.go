package e2e

import "time"

// Waits used across the suite. They are deadlines for something that has already
// happened — a request the harness knows finished, a lock the database has released —
// not sleeps that pace a test, so they can be generous without making the suite slow.
const (
	waitShort = 5 * time.Second
	waitLong  = 20 * time.Second
)
