package platformops

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/orggate"
)

// beginOrganizationRuntimeTx opens a transaction that writes organization runtime state and takes
// that organization's gate as its first statement.
//
// The platform side owns the same runtime tables the request path does — quotas and counters,
// admission entries, outbox messages and their capacity guard, trigger firings — so a transaction
// here can collide with a run creation, a cancellation, or a worker advance for the same
// organization. Taking the gate first puts every writer in one order instead of relying on the row
// locks to happen to agree, and the gate is the same lock internal/postgres uses for the request
// path so the two sides serialize with each other.
//
// The caller must not already hold a runtime row lock and must not take a second organization's
// gate in the same transaction.
func beginOrganizationRuntimeTx(ctx context.Context, pool *pgxpool.Pool, organizationID string) (pgx.Tx, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	if err = orggate.LockTx(ctx, tx, organizationID); err != nil {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return nil, err
	}
	return tx, nil
}
