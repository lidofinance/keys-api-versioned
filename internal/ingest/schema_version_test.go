package ingest

import (
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/store"
)

// One entry per store.SchemaVersion, never edited: a layout change adds the next one. Without the
// bump an older writer cannot tell the store is newer, and drops it.
var schemaFingerprints = map[int]string{
	1: "5b63f4af6f0f8cbc73e4b4fd0286488e38f9208b68eeb02f090daa243b41bfb9",
}

func TestTheSchemaVersionMovesWithTheSchema(t *testing.T) {
	got := schemaFingerprint(schemaSQL)
	want, ok := schemaFingerprints[store.SchemaVersion]
	if !ok || got != want {
		t.Fatalf("schema.sql has fingerprint %s, and version %d pins %q: a layout change bumps "+
			"store.SchemaVersion and adds its fingerprint here", got, store.SchemaVersion, want)
	}
	for version := range schemaFingerprints {
		if version > store.SchemaVersion {
			t.Errorf("a fingerprint is pinned for version %d, above store.SchemaVersion %d", version, store.SchemaVersion)
		}
	}
}
