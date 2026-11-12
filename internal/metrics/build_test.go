package metrics

import (
	"strings"
	"testing"
)

func TestTheCommitIsExportedBesideTheCanonicalBuildInfo(t *testing.T) {
	m := New("keys_api")
	m.SetBuildInfo("keys-api-versioned", "0.1.0", "dev", "1")
	m.SetBuildCommit("keys_api", "0.1.0", "abc1234")

	body := scrape(t, m)
	if !strings.Contains(body, `keys_api_build_info{commit="abc1234",version="0.1.0"} 1`) {
		t.Error("the scrape does not say which commit is running")
	}
	if !strings.Contains(body, `build_info{env="dev",name="keys-api-versioned",network="1",version="0.1.0"} 1`) {
		t.Error("the canonical build_info changed its labels")
	}
}
