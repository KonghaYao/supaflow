// Package orggate serializes organization-runtime writes behind one deterministic gate.
//
// Organization runtime state spans runs, admission_entries, node_attempts, worker_leases,
// outbox_messages, organization_work_queue, organization_quotas, and organization_quota_counters.
// Those tables guard themselves with row locks, and the row-lock order differs per entry point:
// run creation takes the quota row first and reaches the work-queue row through the work-hint
// trigger, while the worker advance path takes the work-queue row first and reaches the quota row
// through the outbox capacity guard. Nothing made those orders agree, so two transactions that meet
// twice in opposite directions are a lock cycle that Postgres resolves by aborting one of them with
// 40P01.
//
// A transaction that holds this gate for an organization may take any row lock in the runtime
// tables, because no other transaction for the same organization can be holding one at the same
// time. The gate is a transaction-scoped advisory lock on (namespace, organization) released by the
// transaction itself, so a rolled-back or retried attempt releases it with no cleanup path.
//
// Two rules make the ordering hold:
//
//   - The gate is taken as the transaction's first statement, before any row lock. Taking it later
//     would leave the cycle intact: a transaction already holding a runtime row lock and waiting
//     for the gate can be blocked by a gate holder waiting for that same row.
//   - One transaction never holds two gates. Organization-scoped work uses a single-organization
//     transaction per organization instead of one transaction spanning several organizations.
package orggate

import (
	"context"

	"github.com/jackc/pgx/v5"
)

const (
	// namespace separates this lock from the other advisory locks in the schema (api mutation
	// ledgers, workflow-version capacity, definition drafts, retention policy, node registry), so a
	// hash collision between two different concerns cannot make them block each other.
	namespace = "supaflow:organization-runtime"
	// lockSQL keys the lock on the namespace and the organization identifier. hashtextextended maps
	// both to one bigint the way every other advisory lock in this schema does.
	lockSQL = `SELECT pg_advisory_xact_lock(hashtextextended($1, hashtextextended($2, 0)))`
)

// LockTx takes the organization runtime gate inside tx.
//
// The caller must invoke it before any statement that takes a row lock, and must not take a second
// gate in the same transaction.
func LockTx(ctx context.Context, tx pgx.Tx, organizationID string) error {
	_, err := tx.Exec(ctx, lockSQL, namespace, organizationID)
	return err
}
