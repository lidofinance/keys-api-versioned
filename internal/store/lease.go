package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// A lease says "a reader is still using this version, do not delete its rows yet".
//
// It exists because of an asymmetry in the range-versioned schema. A concurrent
// *write* is harmless: a reader selects by `from_block <= B and (to_block is null or
// to_block > B)`, so a version published at a later block neither hides an old row
// (it is closed at a block above B, which the predicate still accepts) nor shows a
// new one. The answer does not even depend on whether the writer's transaction
// committed mid-request. A *delete* is the opposite: a row that is gone matches no
// predicate, and retention deletes rows of versions that fall out of the window
// while a slow consumer may still be streaming one of them.
//
// The mechanism is a shared Postgres advisory lock keyed by the version's block
// number, which suits the problem better than a table of leases with timestamps:
//
//   - It is visible across processes, which matters because the reader may be a
//     separate replica from the writer that prunes — that separation is the point of
//     KEYS_API_MODE=server.
//   - It is released when the session ends, including when the holder crashes or its
//     connection drops, so there is no expiry to tune and no sweeper to write.
//   - Many readers share one version without contending, and the writer never waits:
//     it reads the held set and lowers its own boundary instead.
//
// What a lease deliberately does not cover is a reorg. When a version is taken off
// the chain its rows must stop being current, or the next version would inherit keys
// from the losing branch, so the rollback deletes them — and the reader is told,
// rather than being handed what is left. That telling is not automatic: it is
// appSnapshot.stillOnChain, re-checking the version's status at the start of every
// query that continues a response. Without it the remaining modules came back empty
// and the handler finished the JSON, which is the silent complete-looking answer from
// a dead branch that this whole design exists to rule out.
//
// How the reader learns depends only on how far the response had got: before the
// first flush it is a 409 naming the new head, after it the connection is aborted.
// Both are loud. What it is not is instant — the check runs per query, so a rollback
// during the long first module's stream is noticed when the next module starts. That
// is a latency, not a correctness gap: rows already streaming come from a portal
// opened before the delete, so what the client received is the pinned version's own
// content. Checking inside a stream would need a second connection, since pgx cannot
// query on one with open rows, and that is not worth two connections per request.

// leaseVersion takes a shared lease on a version and returns the release. The lock
// lives on the caller's connection, so it must be released before that connection
// goes back to the pool — otherwise it would outlive the request that took it.
func leaseVersion(ctx context.Context, conn *pgxpool.Conn, block int64) (func(), error) {
	if _, err := conn.Exec(ctx, `select pg_advisory_lock_shared($1)`, block); err != nil {
		return nil, fmt.Errorf("lease version %d: %w", block, err)
	}
	return func() {
		// Deliberately not on ctx: a cancelled request still has to give the lock back,
		// and an Exec on a cancelled context would not run.
		if _, err := conn.Exec(context.WithoutCancel(ctx),
			`select pg_advisory_unlock_shared($1)`, block); err != nil {
			// The lock is released when the connection closes in any case, so the worst
			// outcome of a failure here is retention waiting for that.
			_ = err
		}
	}, nil
}

// LeasedVersions reports the version blocks readers currently hold, so the writer
// can keep their rows. It is in this package because the lock key convention is the
// contract between the two sides and belongs in one place.
//
// Advisory locks taken with a single bigint appear in pg_locks split across
// classid (high 32 bits) and objid (low 32 bits), with objsubid = 1; the int-pair
// form uses objsubid = 2 and is not what leaseVersion takes. The query is scoped to
// the current database because the comparison stand runs two of them in one cluster.
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
