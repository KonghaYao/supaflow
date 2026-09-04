package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

const selectRuntimeAttemptForClaimSQL = `
SELECT a.id, a.execution_id, a.tenant_id, a.run_id, a.node_state_id, a.node_id, a.executor,
       a.attempt_number, a.status, a.input, a.resume_data, a.available_at, a.started_at,
       a.finished_at, COALESCE(a.error->>'code', ''), a.created_at, a.updated_at
FROM node_attempts a
JOIN runs r ON r.tenant_id = a.tenant_id AND r.id = a.run_id
JOIN admission_entries admission
  ON admission.tenant_id = r.tenant_id
 AND admission.resource_type = 'run'
 AND admission.resource_id = r.id
 AND admission.status = 'active'
LEFT JOIN worker_leases l ON l.tenant_id = a.tenant_id AND l.attempt_id = a.id
WHERE a.tenant_id = $1 AND a.available_at <= $2
	AND (r.deadline_at IS NULL OR r.deadline_at > $2)
  AND (
      (r.status IN ('pending', 'running') AND a.status IN ('pending', 'retry_ready'))
      OR (r.status IN ('running', 'cancelling') AND a.status = 'running' AND l.expires_at <= $2)
  )
ORDER BY a.available_at, a.created_at, a.id
FOR UPDATE OF a SKIP LOCKED
LIMIT 1`

// ClaimAttempt claims one ready attempt or takes over an expired lease.
func (s *RuntimeStore) ClaimAttempt(ctx context.Context, request workflowruntime.ClaimAttemptRequest) (*workflowruntime.WorkItem, error) {
	request.Now = timeOrNow(request.Now)
	if _, err := s.ExpireRuns(ctx, request.TenantID, request.Now, 100); err != nil {
		return nil, err
	}
	tx, err := s.repository.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, runtimeError(workflowruntime.CodeConflict, "could not claim attempt", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var databaseNow time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&databaseNow); err != nil {
		return nil, runtimeError(workflowruntime.CodeConflict, "could not read database clock", err)
	}
	if err = resumeDueTimer(ctx, tx, request.TenantID, databaseNow); err != nil {
		return nil, runtimeError(workflowruntime.CodeConflict, "could not resume due timer", err)
	}
	var work workflowruntime.WorkItem
	var tenantID, attemptStatus string
	var nodeStateID string
	err = tx.QueryRow(ctx, selectRuntimeAttemptForClaimSQL, string(request.TenantID), databaseNow).Scan(
		&work.Attempt.ID, &work.Attempt.ExecutionID, &tenantID, &work.Attempt.RunID, &nodeStateID,
		&work.Attempt.NodeID, &work.Attempt.Executor, &work.Attempt.Number,
		&attemptStatus, &work.Attempt.Input, &work.Attempt.ResumeData, &work.Attempt.NextAttemptAt,
		&work.Attempt.StartedAt, &work.Attempt.FinishedAt, &work.Attempt.ErrorCode,
		&work.Attempt.CreatedAt, &work.Attempt.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, runtimeError(workflowruntime.CodeConflict, "could not select claimable attempt", err)
	}
	work.Attempt.TenantID = workflowruntime.TenantID(tenantID)
	work.Attempt.Status = runtimeAttemptStatus(attemptStatus)
	tookOver := attemptStatus == "running"
	work.Takeover = tookOver
	work.Lease = workflowruntime.Lease{
		TenantID: request.TenantID, RunID: work.Attempt.RunID,
		AttemptID: work.Attempt.ID, WorkerID: request.WorkerID,
		ExpiresAt: databaseNow.Add(request.LeaseDuration), UpdatedAt: databaseNow,
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO worker_leases (
		    attempt_id, tenant_id, run_id, worker_id, fencing_token, acquired_at, expires_at
		) VALUES ($1, $2, $3, $4, 1, $5, $6)
		ON CONFLICT (attempt_id) DO UPDATE SET
		    worker_id = EXCLUDED.worker_id,
		    fencing_token = worker_leases.fencing_token + 1,
		    acquired_at = EXCLUDED.acquired_at,
		    expires_at = EXCLUDED.expires_at
		WHERE worker_leases.tenant_id = EXCLUDED.tenant_id
		RETURNING fencing_token`, work.Attempt.ID, request.TenantID, work.Attempt.RunID,
		request.WorkerID, databaseNow, work.Lease.ExpiresAt,
	).Scan(&work.Lease.FencingToken)
	if err != nil {
		return nil, runtimeError(workflowruntime.CodeConflict, "could not acquire attempt lease", err)
	}
	if _, err = tx.Exec(ctx, `
		UPDATE node_attempts
		SET status = 'running', worker_id = $3, fencing_token = $4,
		    started_at = COALESCE(started_at, $5), updated_at = $5
		WHERE tenant_id = $1 AND id = $2`, request.TenantID, work.Attempt.ID,
		request.WorkerID, work.Lease.FencingToken, databaseNow); err != nil {
		return nil, runtimeError(workflowruntime.CodeConflict, "could not mark attempt running", err)
	}
	if _, err = tx.Exec(ctx, `
		UPDATE node_states SET status = 'running', updated_at = $3
		WHERE tenant_id = $1 AND id = $2`, request.TenantID, nodeStateID, databaseNow); err != nil {
		return nil, runtimeError(workflowruntime.CodeConflict, "could not mark node running", err)
	}
	var firstStart bool
	err = tx.QueryRow(ctx, `
		WITH pending_run AS (
		    SELECT tenant_id, id, started_at
		    FROM runs
		    WHERE tenant_id = $1 AND id = $2 AND status = 'pending'
		    FOR UPDATE
		)
		UPDATE runs AS r
		SET status = 'running', started_at = COALESCE(r.started_at, $3),
		    updated_at = $3, version = r.version + 1
		FROM pending_run AS pending
		WHERE r.tenant_id = pending.tenant_id AND r.id = pending.id
		RETURNING pending.started_at IS NULL`, request.TenantID, work.Attempt.RunID, databaseNow).Scan(&firstStart)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, runtimeError(workflowruntime.CodeConflict, "could not mark run running", err)
	}
	claimPayload, err := json.Marshal(map[string]any{
		"attemptId": work.Attempt.ID, "workerId": request.WorkerID,
		"fencingToken": work.Lease.FencingToken, "takeover": tookOver,
	})
	if err != nil {
		return nil, runtimeError(workflowruntime.CodeConflict, "could not encode claim event", err)
	}
	claimEvents := make([]workflowruntime.Event, 0, 3)
	if firstStart {
		claimEvents = append(claimEvents, workflowruntime.Event{Type: "run.started", Data: json.RawMessage(`{}`)})
	}
	claimEvents = append(claimEvents, workflowruntime.Event{Type: "attempt.claimed", Data: claimPayload})
	if tookOver {
		claimEvents = append(claimEvents, workflowruntime.Event{Type: "attempt.lease_taken_over", Data: claimPayload})
	}
	if _, err = appendRuntimeEvents(ctx, tx, request.TenantID, work.Attempt.RunID, claimEvents); err != nil {
		return nil, runtimeError(workflowruntime.CodeConflict, "could not persist claim events", err)
	}
	work.Run, err = scanRuntimeRun(tx.QueryRow(ctx, runtimeRunByIDSQL, string(request.TenantID), work.Attempt.RunID))
	if err != nil {
		return nil, runtimeError(workflowruntime.CodeConflict, "could not read claimed run", err)
	}
	err = tx.QueryRow(ctx, `
		SELECT tenant_id, run_id, node_id, status, attempt_count, latest_attempt_id, updated_at
		FROM node_states WHERE tenant_id = $1 AND id = $2`, request.TenantID, nodeStateID).Scan(
		&tenantID, &work.Node.RunID, &work.Node.NodeID, &attemptStatus,
		&work.Node.AttemptCount, &work.Node.LatestAttemptID, &work.Node.UpdatedAt,
	)
	if err != nil {
		return nil, runtimeError(workflowruntime.CodeConflict, "could not read claimed node", err)
	}
	work.Node.TenantID = workflowruntime.TenantID(tenantID)
	work.Node.Status = runtimeNodeStatus(attemptStatus)
	var snapshot workflowruntime.Snapshot
	var snapshotTenant string
	snapshotErr := tx.QueryRow(ctx, `
		SELECT id, tenant_id, run_id, attempt_id, sequence, state, created_at
		FROM run_snapshots
		WHERE tenant_id = $1 AND run_id = $2 AND attempt_id = $3
		ORDER BY sequence DESC LIMIT 1`, request.TenantID, work.Attempt.RunID, work.Attempt.ID).Scan(
		&snapshot.ID, &snapshotTenant, &snapshot.RunID, &snapshot.AttemptID,
		&snapshot.Sequence, &snapshot.Data, &snapshot.CreatedAt)
	if snapshotErr == nil {
		snapshot.TenantID = workflowruntime.TenantID(snapshotTenant)
		work.Snapshot = &snapshot
	} else if !errors.Is(snapshotErr, pgx.ErrNoRows) {
		return nil, runtimeError(workflowruntime.CodeConflict, "could not read run snapshot", snapshotErr)
	}
	work.Attempt.Status = workflowruntime.AttemptRunning
	work.Attempt.StartedAt = timePointerRuntime(databaseNow)
	work.Attempt.UpdatedAt = databaseNow
	if err = tx.Commit(ctx); err != nil {
		return nil, runtimeError(workflowruntime.CodeConflict, "could not commit attempt claim", err)
	}
	return &work, nil
}

// RenewLease extends a live lease with matching worker and fencing token.
func (s *RuntimeStore) RenewLease(ctx context.Context, request workflowruntime.RenewLeaseRequest) (workflowruntime.Lease, error) {
	if request.LeaseDuration <= 0 {
		return workflowruntime.Lease{}, runtimeError(workflowruntime.CodeInvalidArgument, "lease duration must be positive", nil)
	}
	lease := workflowruntime.Lease{
		TenantID: request.TenantID, RunID: request.RunID, AttemptID: request.AttemptID,
		WorkerID: request.WorkerID, FencingToken: request.FencingToken,
	}
	err := s.repository.pool.QueryRow(ctx, `
		WITH coordination_clock AS (SELECT clock_timestamp() AS current_time)
		UPDATE worker_leases lease
		SET expires_at = coordination_clock.current_time + $6 * interval '1 millisecond'
		FROM coordination_clock
		WHERE tenant_id = $1 AND run_id = $2 AND attempt_id = $3
		  AND worker_id = $4 AND fencing_token = $5
		  AND expires_at > coordination_clock.current_time
		RETURNING lease.expires_at, coordination_clock.current_time`,
		request.TenantID, request.RunID, request.AttemptID, request.WorkerID,
		request.FencingToken, request.LeaseDuration.Milliseconds()).Scan(&lease.ExpiresAt, &lease.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return workflowruntime.Lease{}, runtimeError(workflowruntime.CodeLeaseLost, "attempt lease was lost", ErrStaleLease)
		}
		return workflowruntime.Lease{}, runtimeError(workflowruntime.CodeConflict, "could not renew lease", err)
	}
	return lease, nil
}

func runtimeAttemptStatus(status string) workflowruntime.AttemptStatus {
	switch status {
	case "retry_ready", "pending":
		return workflowruntime.AttemptPending
	case "succeeded":
		return workflowruntime.AttemptSuccess
	case "timed_out", "error":
		return workflowruntime.AttemptError
	default:
		return workflowruntime.AttemptStatus(strings.ToUpper(status))
	}
}

func runtimeNodeStatus(status string) workflowruntime.NodeStatus {
	return workflowruntime.NodeStatus(strings.ToUpper(status))
}

func timePointerRuntime(value time.Time) *time.Time { return &value }

func validateRuntimeLease(ctx context.Context, tx pgx.Tx, tenantID workflowruntime.TenantID, runID, attemptID, workerID string, fencingToken uint64, _ time.Time) (string, string, error) {
	var nodeStateID, nodeID string
	err := tx.QueryRow(ctx, `
		SELECT a.node_state_id, a.node_id
		FROM node_attempts a
		JOIN worker_leases l ON l.tenant_id = a.tenant_id AND l.attempt_id = a.id
		WHERE a.tenant_id = $1 AND a.run_id = $2 AND a.id = $3
		  AND a.status = 'running' AND a.worker_id = $4 AND a.fencing_token = $5
		  AND l.worker_id = $4 AND l.fencing_token = $5 AND l.expires_at > clock_timestamp()
		FOR UPDATE OF a, l`, tenantID, runID, attemptID, workerID, fencingToken).Scan(&nodeStateID, &nodeID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", runtimeError(workflowruntime.CodeStaleFencingToken, "attempt lease is stale", ErrStaleLease)
	}
	if err != nil {
		return "", "", runtimeError(workflowruntime.CodeConflict, "could not validate attempt lease", err)
	}
	return nodeStateID, nodeID, nil
}
