package ingest_test

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lidofinance/keys-api-versioned/internal/ingest"
	"github.com/lidofinance/keys-api-versioned/internal/testpg"
	"github.com/lidofinance/keys-api-versioned/internal/testreg"
)

// The schema used to be appended as ?search_path=, which glued itself onto the last value of a
// keyword/value DSN and failed the connection.
func TestAKeywordValueDSNGetsTheChainSchema(t *testing.T) {
	base := testpg.DSN(t)
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	password, _ := u.User.Password()
	keyword := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		u.Hostname(), u.Port(), u.User.Username(), password, u.Path[1:])

	const chainID = 990011
	ctx := context.Background()
	s, err := ingest.NewPGStore(ctx, keyword, ingest.PGOptions{ChainID: chainID, Source: "test"})
	if err != nil {
		t.Fatalf("open with a keyword/value DSN: %v", err)
	}
	defer s.Close()
	defer dropSchema(t, base, "chain_990011")
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if s.Schema() != "chain_990011" {
		t.Errorf("schema %q, want chain_990011", s.Schema())
	}
}

// A database that is not up yet (a restart, a failover, both pods scheduled together) used to exit
// the process at boot.
func TestAnUnreachableDatabaseIsWaitedFor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()

	s, err := ingest.NewPGStore(ctx, "postgres://u:p@127.0.0.1:1/db?connect_timeout=1", ingest.PGOptions{Lazy: true})
	if err != nil {
		t.Fatalf("open a lazy store over a closed port: %v", err)
	}
	defer s.Close()

	retries := 0
	if err := s.WaitForDatabase(ctx, func(error, time.Duration) { retries++ }); err == nil {
		t.Fatal("connected to a closed port")
	}
	if retries < 2 {
		t.Errorf("%d retries before giving up at the deadline, want at least 2", retries)
	}
}

// Two image versions over one schema (a rollout, a rollback) used to drop and re-apply it in turn,
// statement by statement, failing each other mid-way.
func TestConcurrentMigrationsOfAChangedSchemaSucceed(t *testing.T) {
	base := testpg.DSN(t)
	ctx := context.Background()
	const chainID = 990012
	defer dropSchema(t, base, "chain_990012")

	first, err := ingest.NewPGStore(ctx, base, ingest.PGOptions{ChainID: chainID, Source: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if err := first.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	for round := 0; round < 5; round++ {
		// Each round's rebuild recreates the fence: first is a new writer again.
		if _, err := first.TakeEpoch(ctx); err != nil {
			t.Fatal(err)
		}
		testreg.Spec{Modules: 1, OperatorsPerModule: 1, KeysPerOperator: 1}.
			Publish(ctx, t, first, testreg.At(int64(1000+round)))
		if _, err := first.Pool().Exec(ctx, `update schema_fingerprint set fingerprint = 'older layout'`); err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		errs := make(chan error, 4)
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s, err := ingest.NewPGStore(ctx, base, ingest.PGOptions{ChainID: chainID, Source: "test"})
				if err != nil {
					errs <- err
					return
				}
				defer s.Close()
				errs <- s.Migrate(ctx)
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("round %d: a concurrent migration failed: %v", round, err)
			}
		}
		latest, err := first.Latest(ctx)
		if err != nil || latest != nil {
			t.Fatalf("round %d: after the rebuild the schema should be empty and usable: %+v, %v", round, latest, err)
		}
	}
}

func dropSchema(t *testing.T, dsn, schema string) {
	admin, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Logf("drop %s: %v", schema, err)
		return
	}
	defer admin.Close()
	_, _ = admin.Exec(context.Background(), `drop schema if exists `+schema+` cascade`)
}
