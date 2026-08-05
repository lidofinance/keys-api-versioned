package ingest

// SchemaFingerprintForTest exposes the fingerprint to the external test package, which is
// where the rest of these tests live.
func SchemaFingerprintForTest(sql string) string { return schemaFingerprint(sql) }
