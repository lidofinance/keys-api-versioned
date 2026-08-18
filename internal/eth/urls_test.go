package eth

import "testing"

func TestSetURLsReplacesTheFallbackList(t *testing.T) {
	c := NewClient("http://one:8545, http://two:8545", Options{})

	if got := c.endpoints(); len(got) != 2 || got[0] != "http://one:8545" || got[1] != "http://two:8545" {
		t.Fatalf("startup list parsed as %v", got)
	}

	if err := c.SetURLs("http://three:8545"); err != nil {
		t.Fatalf("rotation refused: %v", err)
	}
	if got := c.endpoints(); len(got) != 1 || got[0] != "http://three:8545" {
		t.Fatalf("after rotation %v", got)
	}
}

func TestSetURLsRefusesAnEmptyList(t *testing.T) {
	c := NewClient("http://one:8545", Options{})

	// A render that produced nothing must leave a working client alone rather than turn it
	// into one with nowhere to send.
	for _, urls := range []string{"", "  ", ",", ", ,"} {
		if err := c.SetURLs(urls); err == nil {
			t.Fatalf("%q was accepted", urls)
		}
	}
	if got := c.endpoints(); len(got) != 1 || got[0] != "http://one:8545" {
		t.Fatalf("previous endpoints not kept: %v", got)
	}
}

func TestEndpointsIsACopy(t *testing.T) {
	// The request loop walks the returned slice while making network calls; if it aliased
	// the client's own slice, a rotation would mutate a list mid-sync.
	c := NewClient("http://one:8545", Options{})
	got := c.endpoints()
	got[0] = "http://mutated:8545"
	if again := c.endpoints(); again[0] != "http://one:8545" {
		t.Fatalf("endpoints aliased the client's slice: %v", again)
	}
}
