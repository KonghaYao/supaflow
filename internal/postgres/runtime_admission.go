package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

const (
	quotaRetryAfter = 5 * time.Second
)

type runAdmissionDecision struct {
	status string
}

// admitRunTx reserves active capacity or durable queue capacity in the caller's
// run-creation transaction. It never commits or opens a nested transaction.
func admitRunTx(ctx context.Context, tx pgx.Tx, run workflowruntime.Run) (runAdmissionDecision, error) {
	if run.Priority < -1000 || run.Priority > 1000 {
		return runAdmissionDecision{}, runtimeError(workflowruntime.CodeInvalidArgument, "priority must be between -1000 and 1000", nil)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO tenant_quotas (tenant_id, max_active_runs, max_active_workers, max_queue_depth)
		VALUES ($1, $2, $3, $4) ON CONFLICT (tenant_id) DO NOTHING`,
		run.TenantID, workflowruntime.DefaultTenantMaxActiveRuns,
		workflowruntime.DefaultTenantMaxActiveWorkers, workflowruntime.DefaultTenantMaxQueueDepth); err != nil {
		return runAdmissionDecision{}, fmt.Errorf("bootstrap run quota: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO tenant_quota_counters (tenant_id) VALUES ($1)
		ON CONFLICT (tenant_id) DO NOTHING`, run.TenantID); err != nil {
		return runAdmissionDecision{}, fmt.Errorf("bootstrap run quota counters: %w", err)
	}
	var maxActive, maxQueue, active, queued int
	if err := tx.QueryRow(ctx, `
		SELECT q.max_active_runs, q.max_queue_depth, c.active_runs, c.queued_runs
		FROM tenant_quotas q
		JOIN tenant_quota_counters c ON c.tenant_id = q.tenant_id
		WHERE q.tenant_id = $1 FOR UPDATE OF q, c`, run.TenantID).Scan(
		&maxActive, &maxQueue, &active, &queued); err != nil {
		return runAdmissionDecision{}, fmt.Errorf("lock run quota: %w", err)
	}
	status := "active"
	if active >= maxActive {
		if queued >= maxQueue {
			return runAdmissionDecision{}, &workflowruntime.Error{
				Code: workflowruntime.CodeQuotaExceeded, Message: "tenant run capacity is exhausted",
				RetryAfter: quotaRetryAfter,
			}
		}
		status = "queued"
	}
	entryID, err := NewID("adm")
	if err != nil {
		return runAdmissionDecision{}, fmt.Errorf("allocate run admission: %w", err)
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO admission_entries (
		    tenant_id, id, resource_type, resource_id, priority, status, admitted_at, created_at
		) VALUES ($1, $2, 'run', $3, $4, $5,
		          CASE WHEN $5 = 'active' THEN $6::timestamptz ELSE NULL END, $6)`,
		run.TenantID, entryID, run.ID, run.Priority, status, run.CreatedAt); err != nil {
		return runAdmissionDecision{}, fmt.Errorf("persist run admission: %w", err)
	}
	column := "active_runs"
	if status == "queued" {
		column = "queued_runs"
	}
	if _, err = tx.Exec(ctx, `UPDATE tenant_quota_counters SET `+column+` = `+column+` + 1,
		updated_at = $2 WHERE tenant_id = $1`, run.TenantID, run.CreatedAt); err != nil {
		return runAdmissionDecision{}, fmt.Errorf("increment run quota counter: %w", err)
	}
	return runAdmissionDecision{status: status}, nil
}

// releaseRunAdmissionTx releases capacity and promotes exactly one queued run
// under the tenant counter lock. Replays are no-ops and cannot double-decrement.
func releaseRunAdmissionTx(ctx context.Context, tx pgx.Tx, tenantID workflowruntime.TenantID, runID string, now time.Time) (string, error) {
	var active, queued int
	if err := tx.QueryRow(ctx, `
		SELECT active_runs, queued_runs FROM tenant_quota_counters
		WHERE tenant_id = $1 FOR UPDATE`, tenantID).Scan(&active, &queued); err != nil {
		return "", fmt.Errorf("lock run quota counters: %w", err)
	}
	var entryID, status string
	err := tx.QueryRow(ctx, `
		SELECT id, status FROM admission_entries
		WHERE tenant_id = $1 AND resource_type = 'run' AND resource_id = $2
		FOR UPDATE`, tenantID, runID).Scan(&entryID, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("run admission is missing")
	}
	if err != nil {
		return "", fmt.Errorf("lock run admission: %w", err)
	}
	if status == "released" {
		return "", nil
	}
	if (status == "active" && active <= 0) || (status == "queued" && queued <= 0) {
		return "", fmt.Errorf("run quota counter invariant violated")
	}
	if _, err = tx.Exec(ctx, `
		UPDATE admission_entries SET status = 'released', released_at = $3
		WHERE tenant_id = $1 AND id = $2 AND status = $4`, tenantID, entryID, now, status); err != nil {
		return "", fmt.Errorf("release run admission: %w", err)
	}
	if status == "active" {
		active--
	} else {
		queued--
	}
	promotedRunID := ""
	if status == "active" && queued > 0 {
		err = tx.QueryRow(ctx, `
			UPDATE admission_entries SET status = 'active', admitted_at = $2
			WHERE (tenant_id, id) = (
			    SELECT tenant_id, id FROM admission_entries
			    WHERE tenant_id = $1 AND resource_type = 'run' AND status = 'queued'
			    ORDER BY priority DESC, created_at, id
			    FOR UPDATE SKIP LOCKED LIMIT 1
			) RETURNING resource_id`, tenantID, now).Scan(&promotedRunID)
		if err == nil {
			active++
			queued--
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return "", fmt.Errorf("promote queued run: %w", err)
		}
	}
	if _, err = tx.Exec(ctx, `
		UPDATE tenant_quota_counters
		SET active_runs = $2, queued_runs = $3, updated_at = $4
		WHERE tenant_id = $1`, tenantID, active, queued, now); err != nil {
		return "", fmt.Errorf("persist run quota release: %w", err)
	}
	return promotedRunID, nil
}
