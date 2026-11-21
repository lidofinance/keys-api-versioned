package ingest

import (
	"context"

	"github.com/jackc/pgx/v5"
)

func SchemaFingerprintForTest(sql string) string { return schemaFingerprint(sql) }

func SetPruneBeforeCommitForTest(f func()) (restore func()) {
	pruneBeforeCommit = f
	return func() { pruneBeforeCommit = nil }
}

func SetPruneClaimBatchForTest(n int) (restore func()) {
	previous := pruneClaimBatch
	pruneClaimBatch = n
	return func() { pruneClaimBatch = previous }
}

func (s *PGStore) BeginWriteForTest(ctx context.Context) (pgx.Tx, error) {
	return s.beginWrite(ctx, s.pool)
}
