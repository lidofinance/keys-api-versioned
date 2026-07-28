#!/bin/sh
# Runs once, on first start of an empty data directory.
#
# The two services own separate schemas, so they get separate databases in one
# Postgres: `keys_api` is the canonical service's, created by the image from
# POSTGRES_DB, and `keys_api_app` is ours. Sharing one would put their migrations in
# each other's way for no benefit. `keys_api_test` is for `make test`.
set -eu

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
	create database keys_api_app;
	-- The writer's integration test drops and recreates its tables, so it gets a
	-- database of its own rather than sharing one with a running service.
	create database keys_api_test;
	-- The fork suite gets its own again, because it writes versions at the real block
	-- numbers of the chain it forks. Advisory locks — which is how a reader holds a
	-- version against retention — are keyed by block number and scoped to the
	-- database, so a fork test sharing keys_api_test would lease and prune blocks that
	-- another test believes are its own.
	create database keys_api_fork;
SQL
