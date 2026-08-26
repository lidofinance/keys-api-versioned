package eth

import (
	"context"
	"errors"
	"fmt"
	neturl "net/url"
	"strings"
	"testing"
)

func TestRedactErrStripsKeyKeepsCause(t *testing.T) {
	raw := &neturl.Error{
		Op:  "Post",
		URL: "https://provider.example/v2/SECRETKEY123",
		Err: context.Canceled,
	}

	got := redactErr(raw)

	if s := got.Error(); strings.Contains(s, "SECRETKEY123") {
		t.Fatalf("key survived redaction: %q", s)
	}
	if s := got.Error(); !strings.Contains(s, "provider.example") {
		t.Fatalf("host did not survive redaction: %q", s)
	}
	if !errors.Is(got, context.Canceled) {
		t.Fatal("wrapped cause lost: errors.Is(context.Canceled) is false")
	}
	// The wrap the fallback loop applies must stay redacted and unwrappable too.
	wrapped := fmt.Errorf("all 2 endpoint(s) failed, last: %w", got)
	if strings.Contains(wrapped.Error(), "SECRETKEY123") {
		t.Fatalf("key survived the wrap: %q", wrapped.Error())
	}
	if !errors.Is(wrapped, context.Canceled) {
		t.Fatal("wrap broke the cause chain")
	}
}

func TestRedactErrLeavesOtherErrorsAlone(t *testing.T) {
	plain := errors.New("no response for id 7 (eth_call)")
	if got := redactErr(plain); got != plain {
		t.Fatal("non-URL error was replaced")
	}
}
