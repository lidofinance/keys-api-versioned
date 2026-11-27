package api

// ContractVersion is the canonical release whose contract /v1 serves, the one in
// testdata/canonical-openapi.json. /v1/status reports it as appVersion, and consumers refuse a
// service below their minimum: the council daemon wants 5.0.0 (semver), the oracle 1.5.0 (PEP 440,
// which rejects suffixes such as "-poc").
const ContractVersion = "6.0.0"
