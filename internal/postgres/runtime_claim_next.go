package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

const dynamicClaimScanLimit = 100

const reconcileWorkHintsSQL = `
WITH candidates AS (
    SELECT a.organization_id,
           min(CASE WHEN a.status='running' THEN l.expires_at ELSE a.available_at END) AS ready_at
    FROM node_attempts a
    JOIN runs r ON r.organization_id=a.organization_id AND r.id=a.run_id
    JOIN admission_entries admission ON admission.organization_id=r.organization_id
      AND admission.resource_type='run' AND admission.resource_id=r.id AND admission.status='active'
    LEFT JOIN worker_leases l ON l.organization_id=a.organization_id AND l.attempt_id=a.id
    WHERE (r.deadline_at IS NULL OR r.deadline_at > $1)
      AND ((r.status IN ('pending','running') AND a.status IN ('pending','retry_ready'))
        OR (r.status IN ('running','cancelling') AND a.status='running' AND l.expires_at IS NOT NULL))
    GROUP BY a.organization_id
    ORDER BY min(CASE WHEN a.status='running' THEN l.expires_at ELSE a.available_at END)
    LIMIT 100
)
INSERT INTO organization_work_queue(organization_id,ready_at,updated_at)
SELECT organization_id,ready_at,$1 FROM candidates
ON CONFLICT (organization_id) DO UPDATE SET ready_at=EXCLUDED.ready_at,updated_at=EXCLUDED.updated_at`

// ClaimNext atomically selects a fair organization hint and claims one
// authoritative attempt. Queue eligibility, leases, and cursor use one DB clock.
func (s *RuntimeStore) ClaimNext(ctx context.Context, request workflowruntime.ClaimNextAttemptRequest) (*workflowruntime.WorkItem, error) {
	tx, err := s.repository.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, runtimeError(workflowruntime.CodeConflict, "could not claim next attempt", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var now time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return nil, runtimeError(workflowruntime.CodeConflict, "could not read database clock", err)
	}
	reconciled := false
	for scanned := 0; scanned < dynamicClaimScanLimit; scanned++ {
		var organizationID string
		err = tx.QueryRow(ctx, `SELECT organization_id FROM organization_work_queue
			WHERE ready_at <= $1 ORDER BY last_claimed_at NULLS FIRST,ready_at,organization_id
			FOR UPDATE SKIP LOCKED LIMIT 1`, now).Scan(&organizationID)
		if errors.Is(err, pgx.ErrNoRows) {
			if reconciled {
				return nil, nil
			}
			if _, err = tx.Exec(ctx, reconcileWorkHintsSQL, now); err != nil {
				return nil, runtimeError(workflowruntime.CodeConflict, "could not reconcile work hints", err)
			}
			reconciled = true
			continue
		}
		if err != nil {
			return nil, runtimeError(workflowruntime.CodeConflict, "could not select work organization", err)
		}
		work, claimErr := s.claimAttemptTx(ctx, tx, workflowruntime.ClaimAttemptRequest{
			OrganizationID: workflowruntime.OrganizationID(organizationID), WorkerID: request.WorkerID,
			LeaseDuration: request.LeaseDuration,
		}, now)
		if claimErr != nil {
			return nil, claimErr
		}
		if work == nil {
			if _, err = tx.Exec(ctx, `SELECT refresh_organization_work_hint($1)`, organizationID); err != nil {
				return nil, runtimeError(workflowruntime.CodeConflict, "could not clear stale work hint", err)
			}
			continue
		}
		if _, err = tx.Exec(ctx, `SELECT refresh_organization_work_hint($1)`, organizationID); err != nil {
			return nil, runtimeError(workflowruntime.CodeConflict, "could not refresh work hint", err)
		}
		if _, err = tx.Exec(ctx, `UPDATE organization_work_queue SET last_claimed_at=$2,updated_at=$2 WHERE organization_id=$1`, organizationID, now); err != nil {
			return nil, runtimeError(workflowruntime.CodeConflict, "could not advance work cursor", err)
		}
		if err = tx.Commit(ctx); err != nil {
			return nil, runtimeError(workflowruntime.CodeConflict, "could not commit next attempt claim", err)
		}
		return work, nil
	}
	return nil, nil
}
