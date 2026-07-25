package store

import "testing"

func TestKeyCursorRoundTrip(t *testing.T) {
	in := KeyCursor{Block: 25609605, ModuleID: 3, OperatorIndex: 608, KeyIndex: 41}

	out, err := ParseKeyCursor(in.String())
	if err != nil {
		t.Fatalf("parse %q: %v", in.String(), err)
	}
	if out != in {
		t.Fatalf("round trip gave %+v, want %+v", out, in)
	}
}

func TestKeyCursorRejectsGarbage(t *testing.T) {
	// A cursor comes from a client, so every one of these is reachable input. A
	// silent zero value would restart the walk from the top and the consumer would
	// see duplicates rather than an error.
	for _, s := range []string{"", "1", "1.2.3", "1.2.3.4.5", "a.2.3.4", "1.2.3.x", "latest"} {
		if _, err := ParseKeyCursor(s); err == nil {
			t.Errorf("ParseKeyCursor(%q) accepted a malformed cursor", s)
		}
	}
}
