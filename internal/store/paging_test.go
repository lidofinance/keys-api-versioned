package store

import (
	"strings"
	"testing"
)

func TestKeyCursorRoundTrip(t *testing.T) {
	in := KeyCursor{Block: 25609605, ModuleID: 3, OperatorIndex: 608, KeyIndex: 41,
		Hash: "0x5595e30e2db14337c148e8f6441833caaf12f02df378c39708bacf3de752669e"}

	out, err := ParseKeyCursor(in.String())
	if err != nil {
		t.Fatalf("parse %q: %v", in.String(), err)
	}
	if out != in {
		t.Fatalf("round trip gave %+v, want %+v", out, in)
	}
}

func TestKeyCursorRejectsGarbage(t *testing.T) {
	// Client input: a silent zero value would restart the walk and the consumer would see duplicates.
	for _, s := range []string{"", "1", "1.2.3", "1.2.3.4", "1.2.3.4.5", "1.2.3.4.0x12", "1.2.3.4.5.6", "a.2.3.4", "1.2.3.x", "latest"} {
		if _, err := ParseKeyCursor(s); err == nil {
			t.Errorf("ParseKeyCursor(%q) accepted a malformed cursor", s)
		}
	}
}

// A cursor from before the hash would resume by height, on whichever branch a reorg left there.
func TestAKeyCursorWithoutAHashIsRefused(t *testing.T) {
	_, err := ParseKeyCursor("25609605.3.608.41")
	if err == nil || !strings.Contains(err.Error(), "start the walk again") {
		t.Fatalf("a hashless cursor: %v, want a refusal telling the consumer to restart", err)
	}
}
