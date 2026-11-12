package ingest

func SchemaFingerprintForTest(sql string) string { return schemaFingerprint(sql) }

func SetPruneBeforeCommitForTest(f func()) (restore func()) {
	pruneBeforeCommit = f
	return func() { pruneBeforeCommit = nil }
}
