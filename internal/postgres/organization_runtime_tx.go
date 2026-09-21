package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/orggate"
)

// beginOrganizationRuntimeTx opens a transaction that writes organization runtime state.
//
// It is the only place a runtime transaction is opened, so the organization gate from
// internal/orggate is taken as that transaction's first statement for every entry point: run
// creation, cancel, rerun, suspend, resume, approval, deadline sweep, worker claim, and worker
// advance. Callers must not open a runtime transaction by calling BeginTx directly — a transaction
// that skips the gate can still take its row locks in the order that completes a cycle against one
// that holds it.
//
// The gate makes row-lock order inside the transaction irrelevant, so this helper says nothing
// about which statements the caller runs afterwards. It only requires the organization whose state
// the transaction touches, and it holds exactly one gate: a transaction that would need a second
// organization is split into one transaction per organization by its caller.
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
