package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// A lease (shared advisory lock keyed by version block) stops retention deleting rows a slow
// reader still streams; it dies with the session, so there is no expiry. Reorgs: see stillOnChain.

// leaseVersion's lock lives on the caller's connection: release it before the connection
// returns to the pool.
func leaseVersion(ctx context.Context, conn *pgxpool.Conn, block int64) (func(), error) {
	if _, err := conn.Exec(ctx, `select pg_advisory_lock_shared($1)`, block); err != nil {
		return nil, fmt.Errorf("lease version %d: %w", block, err)
	}
	return func() {
		// Not on ctx: a cancelled request still has to give the lock back.
		if _, err := conn.Exec(context.WithoutCancel(ctx),
			`select pg_advisory_unlock_shared($1)`, block); err != nil {
			// The lock goes with the connection anyway; at worst retention waits for that.
			_ = err
		}
	}, nil
}

// A single-bigint advisory lock appears in pg_locks split across classid (high 32) and objid
// (low 32) with objsubid = 1; scoped to the current database since a cluster may hold several.
func LeasedVersions(ctx context.Context, pool *pgxpool.Pool) ([]int64, error) {
	rows, err := pool.Query(ctx, `
		select distinct (classid::bigint << 32) | objid::bigint
		  from pg_locks
		 where locktype = 'advisory'
		   and objsubid = 1
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
