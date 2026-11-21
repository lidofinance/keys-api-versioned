package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// A lease (shared advisory lock keyed by version block) stops retention deleting rows a slow
// reader still streams; it dies with the session, so there is no expiry. Reorgs: see stillOnChain.
//
// Two-int key: (class of this schema, block). The writer lock's class is a constant, so the two
// never collide, and other advisory locks in the database are not read as leases.
const leaseKey = `hashtext('keys-api lease ' || ` + SchemaKey + `), $1::int4`

// SchemaKey names the schema unqualified tables resolve to, so every spelling of one search_path
// takes one lock. Before that schema exists it is the raw search_path, which for chain_<id> is the same text.
const SchemaKey = `coalesce((current_schemas(false))[1], current_setting('search_path'))`

// LeaseVersion's lock lives on the caller's connection: release it before the connection
// returns to the pool.
func LeaseVersion(ctx context.Context, conn *pgxpool.Conn, block int64) (func(), error) {
	if _, err := conn.Exec(ctx, `select pg_advisory_lock_shared(`+leaseKey+`)`, block); err != nil {
		return nil, fmt.Errorf("lease version %d: %w", block, err)
	}
	return func() {
		// Not on ctx: a cancelled request still has to give the lock back.
		if _, err := conn.Exec(context.WithoutCancel(ctx),
			`select pg_advisory_unlock_shared(`+leaseKey+`)`, block); err != nil {
			// The lock goes with the connection anyway; at worst retention waits for that.
			_ = err
		}
	}, nil
}

// TryClaimForPrune takes a version's lease key exclusively until tx ends. False means a reader
// holds it; while true, a reader about to lease it waits for tx and then finds the version gone.
func TryClaimForPrune(ctx context.Context, tx pgx.Tx, block int64) (bool, error) {
	var got bool
	if err := tx.QueryRow(ctx, `select pg_try_advisory_xact_lock(`+leaseKey+`)`, block).Scan(&got); err != nil {
		return false, fmt.Errorf("claim version %d for pruning: %w", block, err)
	}
	return got, nil
}

// LeasedVersions lists blocks leased in this schema; pg_locks shows a two-int key as classid,
// objid with objsubid = 2.
func LeasedVersions(ctx context.Context, pool *pgxpool.Pool) ([]int64, error) {
	rows, err := pool.Query(ctx, `
		select distinct objid::bigint
		  from pg_locks
		 where locktype = 'advisory'
		   and objsubid = 2
		   and classid = hashtext('keys-api lease ' || `+SchemaKey+`)::oid
		   and granted
		   and database = (select oid from pg_database where datname = current_database())`)
	if err != nil {
		return nil, fmt.Errorf("read leases: %w", err)
	}
	defer rows.Close()

	var out []int64
	for rows.Next() {
		var block int64
		if err := rows.Scan(&block); err != nil {
			return nil, err
		}
		out = append(out, block)
	}
	return out, rows.Err()
}
