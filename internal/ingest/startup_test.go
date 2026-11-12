package ingest_test

import (
	"context"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lidofinance/keys-api-versioned/internal/ingest"
	"github.com/lidofinance/keys-api-versioned/internal/testpg"
	"github.com/lidofinance/keys-api-versioned/internal/testreg"
)

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
