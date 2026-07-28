# The service owns this database; its schema is created on boot.
DSN ?= postgres://postgres:postgres@127.0.0.1:55433/keys_api_app?sslmode=disable
PORT ?= 3010
PROVIDERS_URLS ?=
CHAIN_ID ?= 1
INTERVAL_MS ?= 12000
KEEP ?= 20
STATIC_FILE ?= testdata/registry.json

# The fork suite. FORK_BLOCK is pinned rather than tracking head: anvil caches the
# forked state on disk per block, and every scenario asserts preconditions that only
# hold at this one. docs/fork-tests.md says what re-pinning costs.
FORK_UPSTREAM_RPC ?=
FORK_BLOCK ?= 3290000

CANONICAL ?= http://127.0.0.1:3000
POC ?= http://127.0.0.1:$(PORT)
TARGET ?= $(POC)/v1/keys
PID ?=

.PHONY: build run run-static diff bench test test-e2e test-fork vet clean

build:
	go build -o bin/keys-api ./cmd/keys-api
	go build -o bin/diff ./cmd/diff
	go build -o bin/bench ./cmd/bench
	go build -o bin/ingest ./cmd/ingest

# Ingest plus API against a chain, in one process.
run: build
	DATABASE_URL="$(DSN)" PORT=$(PORT) PROVIDERS_URLS="$(PROVIDERS_URLS)" \
	CHAIN_ID=$(CHAIN_ID) UPDATE_KEYS_INTERVAL_MS=$(INTERVAL_MS) RETAIN_VERSIONS=$(KEEP) ./bin/keys-api

# The same service with no chain, from a registry file.
run-static: build
	DATABASE_URL="$(DSN)" PORT=$(PORT) KEYS_API_SOURCE=static \
	KEYS_API_STATIC_FILE=$(STATIC_FILE) UPDATE_KEYS_INTERVAL_MS=2000 ./bin/keys-api

# Response parity against the canonical service. -live tolerates the two of them
# being at different blocks, which they always are when both follow a chain.
diff: build
	./bin/diff -a $(CANONICAL) -b $(POC) -live

bench: build
	./bin/bench -url $(TARGET) -requests 5 -concurrency 1 -warmup 1 $(if $(PID),-rss-pid $(PID),)
	./bin/bench -url $(TARGET) -requests 8 -concurrency 4 -warmup 0 $(if $(PID),-rss-pid $(PID),)

# The writer's integration test needs a database it may drop tables in.
# Unit and component tests need nothing; the rest need a database they may create and
# drop schemas in, and no chain — the writer is driven directly, so the whole suite is
# hermetic.
test:
	go test ./...
	TEST_DATABASE_URL="$(subst keys_api_app,keys_api_test,$(DSN))" go test ./internal/ingest/ ./internal/e2e/ -count=1

test-e2e:
	TEST_DATABASE_URL="$(subst keys_api_app,keys_api_test,$(DSN))" go test ./internal/e2e/ -count=1 -v

# The fork suite: the real chain reader, the real loop, an anvil fork of Hoodi at a
# pinned block. Behind a build tag, so `go test ./...` neither runs it nor needs anvil.
#
# Its own database, because it writes versions at the real block numbers of the chain
# it forks and leases are keyed by block number across the whole database.
#
# The first run is slow and leans on the upstream node: anvil serves a cold read by
# pulling storage a slot at a time. After that it comes out of ~/.foundry/cache/rpc and
# a scenario costs a couple of seconds.
test-fork:
	FORK_UPSTREAM_RPC="$(FORK_UPSTREAM_RPC)" FORK_BLOCK="$(FORK_BLOCK)" \
	TEST_DATABASE_URL="$(subst keys_api_app,keys_api_fork,$(DSN))" \
	go test -tags fork ./internal/fork/ -count=1 -v

vet:
	go vet ./...

clean:
	rm -rf bin
