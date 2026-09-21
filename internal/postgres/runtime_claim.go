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
SELECT a.id, a.execution_id, a.organization_id, a.run_id, a.node_state_id, a.node_id, a.executor,
       a.attempt_number, a.status, a.input, a.resume_data, a.available_at, a.started_at,
       a.finished_at, COALESCE(a.error->>'code', ''), a.created_at, a.updated_at
FROM node_attempts a
JOIN runs r ON r.organization_id = a.organization_id AND r.id = a.run_id
JOIN admission_entries admission
  ON admission.organization_id = r.organization_id
 AND admission.resource_type = 'run'
 AND admission.resource_id = r.id
 AND admission.status = 'active'
LEFT JOIN worker_leases l ON l.organization_id = a.organization_id AND l.attempt_id = a.id
WHERE a.organization_id = $1 AND a.available_at <= $2
	AND (r.deadline_at IS NULL OR r.deadline_at > $2)
  AND (
      (r.status IN ('pending', 'running') AND a.status IN ('pending', 'retry_ready'))
      OR (r.status IN ('running', 'cancelling') AND a.status = 'running' AND l.expires_at <= $2)
  )
ORDER BY a.available_at, a.created_at, a.id
FOR UPDATE OF a SKIP LOCKED
LIMIT 1`

// ClaimAttempt claims one ready attempt or takes over an expired lease.
//
// The worker retries this on its next tick rather than inside this call, so the attempt is not
// repeated here; what matters is that a rolled-back transaction is not reported as a conflict, which
// told whoever read the log that two workers were racing for the same attempt when the server had
// simply chosen this transaction as a deadlock victim.
func (s *RuntimeStore) ClaimAttempt(ctx context.Context, request workflowruntime.ClaimAttemptRequest) (*workflowruntime.WorkItem, error) {
	tx, err := beginOrganizationRuntimeTx(ctx, s.repository.pool, string(request.OrganizationID))
	if err != nil {
		return nil, runtimePersistenceError("could not claim attempt", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var databaseNow time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&databaseNow); err != nil {
		return nil, runtimePersistenceError("could not read database clock", err)
	}
	work, err := s.claimAttemptTx(ctx, tx, request, databaseNow)
	if err != nil || work == nil {
		return work, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, runtimePersistenceError("could not commit attempt claim", workflowruntime.MarkCommitOutcomeUnknown(err))
	}
	return work, nil
}

func (s *RuntimeStore) claimAttemptTx(ctx context.Context, tx pgx.Tx, request workflowruntime.ClaimAttemptRequest, databaseNow time.Time) (*workflowruntime.WorkItem, error) {
	var err error
	if request.LeaseDuration <= 0 {
		return nil, runtimeError(workflowruntime.CodeInvalidArgument, "lease duration must be positive", nil)
	}
	if err := resumeDueTimer(ctx, tx, request.OrganizationID, databaseNow); err != nil {
		return nil, runtimePersistenceError("could not resume due timer", err)
	}
	var work workflowruntime.WorkItem
	var organizationID, attemptStatus string
	var nodeStateID string
	err = tx.QueryRow(ctx, selectRuntimeAttemptForClaimSQL, string(request.OrganizationID), databaseNow).Scan(
		&work.Attempt.ID, &work.Attempt.ExecutionID, &organizationID, &work.Attempt.RunID, &nodeStateID,
		&work.Attempt.NodeID, &work.Attempt.Executor, &work.Attempt.Number,
		&attemptStatus, &work.Attempt.Input, &work.Attempt.ResumeData, &work.Attempt.NextAttemptAt,
		&work.Attempt.StartedAt, &work.Attempt.FinishedAt, &work.Attempt.ErrorCode,
		&work.Attempt.CreatedAt, &work.Attempt.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, runtimePersistenceError("could not select claimable attempt", err)
	}
	work.Attempt.OrganizationID = workflowruntime.OrganizationID(organizationID)
	work.Attempt.Status = runtimeAttemptStatus(attemptStatus)
	tookOver := attemptStatus == "running"
	work.Takeover = tookOver
	work.Lease = workflowruntime.Lease{
		OrganizationID: request.OrganizationID, RunID: work.Attempt.RunID,
		AttemptID: work.Attempt.ID, WorkerID: request.WorkerID,
		ExpiresAt: databaseNow.Add(request.LeaseDuration), UpdatedAt: databaseNow,
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO worker_leases (
		    attempt_id, organization_id, run_id, worker_id, fencing_token, acquired_at, expires_at
		) VALUES ($1, $2, $3, $4, 1, $5, $6)
		ON CONFLICT (attempt_id) DO UPDATE SET
		    worker_id = EXCLUDED.worker_id,
		    fencing_token = worker_leases.fencing_token + 1,
		    acquired_at = EXCLUDED.acquired_at,
		    expires_at = EXCLUDED.expires_at
		WHERE worker_leases.organization_id = EXCLUDED.organization_id
		RETURNING fencing_token`, work.Attempt.ID, request.OrganizationID, work.Attempt.RunID,
		request.WorkerID, databaseNow, work.Lease.ExpiresAt,
	).Scan(&work.Lease.FencingToken)
	if err != nil {
		return nil, runtimePersistenceError("could not acquire attempt lease", err)
	}
	if _, err = tx.Exec(ctx, `
		UPDATE node_attempts
		SET status = 'running', worker_id = $3, fencing_token = $4,
		    started_at = COALESCE(started_at, $5), updated_at = $5
		WHERE organization_id = $1 AND id = $2`, request.OrganizationID, work.Attempt.ID,
		request.WorkerID, work.Lease.FencingToken, databaseNow); err != nil {
		return nil, runtimePersistenceError("could not mark attempt running", err)
	}
	if _, err = tx.Exec(ctx, `
		UPDATE node_states SET status = 'running', updated_at = $3
		WHERE organization_id = $1 AND id = $2`, request.OrganizationID, nodeStateID, databaseNow); err != nil {
		return nil, runtimePersistenceError("could not mark node running", err)
	}
	var firstStart bool
	err = tx.QueryRow(ctx, `
		WITH pending_run AS (
		    SELECT organization_id, id, started_at
		    FROM runs
		    WHERE organization_id = $1 AND id = $2 AND status = 'pending'
		    FOR UPDATE
		)
		UPDATE runs AS r
		SET status = 'running', started_at = COALESCE(r.started_at, $3),
		    updated_at = $3, version = r.version + 1
		FROM pending_run AS pending
		WHERE r.organization_id = pending.organization_id AND r.id = pending.id
		RETURNING pending.started_at IS NULL`, request.OrganizationID, work.Attempt.RunID, databaseNow).Scan(&firstStart)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, runtimePersistenceError("could not mark run running", err)
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
	if _, err = appendRuntimeEvents(ctx, tx, request.OrganizationID, work.Attempt.RunID, claimEvents); err != nil {
		return nil, runtimePersistenceError("could not persist claim events", err)
	}
	work.Run, err = scanRuntimeRun(tx.QueryRow(ctx, runtimeRunByIDSQL, string(request.OrganizationID), work.Attempt.RunID))
	if err != nil {
		return nil, runtimePersistenceError("could not read claimed run", err)
	}
	err = tx.QueryRow(ctx, `
		SELECT organization_id, run_id, node_id, status, attempt_count, latest_attempt_id, updated_at
		FROM node_states WHERE organization_id = $1 AND id = $2`, request.OrganizationID, nodeStateID).Scan(
		&organizationID, &work.Node.RunID, &work.Node.NodeID, &attemptStatus,
		&work.Node.AttemptCount, &work.Node.LatestAttemptID, &work.Node.UpdatedAt,
	)
	if err != nil {
		return nil, runtimePersistenceError("could not read claimed node", err)
	}
	work.Node.OrganizationID = workflowruntime.OrganizationID(organizationID)
	work.Node.Status = runtimeNodeStatus(attemptStatus)
	var snapshot workflowruntime.Snapshot
	var snapshotOrganization string
	snapshotErr := tx.QueryRow(ctx, `
		SELECT id, organization_id, run_id, attempt_id, sequence, state, created_at
		FROM run_snapshots
		WHERE organization_id = $1 AND run_id = $2 AND attempt_id = $3
		ORDER BY sequence DESC LIMIT 1`, request.OrganizationID, work.Attempt.RunID, work.Attempt.ID).Scan(
		&snapshot.ID, &snapshotOrganization, &snapshot.RunID, &snapshot.AttemptID,
		&snapshot.Sequence, &snapshot.Data, &snapshot.CreatedAt)
	if snapshotErr == nil {
		snapshot.OrganizationID = workflowruntime.OrganizationID(snapshotOrganization)
		work.Snapshot = &snapshot
	} else if !errors.Is(snapshotErr, pgx.ErrNoRows) {
		return nil, runtimePersistenceError("could not read run snapshot", snapshotErr)
	}
	work.Attempt.Status = workflowruntime.AttemptRunning
	work.Attempt.StartedAt = timePointerRuntime(databaseNow)
	work.Attempt.UpdatedAt = databaseNow
	return &work, nil
}

// RenewLease extends a live lease with matching worker and fencing token.
func (s *RuntimeStore) RenewLease(ctx context.Context, request workflowruntime.RenewLeaseRequest) (workflowruntime.Lease, error) {
	if request.LeaseDuration <= 0 {
		return workflowruntime.Lease{}, runtimeError(workflowruntime.CodeInvalidArgument, "lease duration must be positive", nil)
	}
	lease := workflowruntime.Lease{
		OrganizationID: request.OrganizationID, RunID: request.RunID, AttemptID: request.AttemptID,
		WorkerID: request.WorkerID, FencingToken: request.FencingToken,
	}
	// Renewal writes worker_leases, whose work-hint trigger writes organization_work_queue, so it
	// takes the organization gate like every other runtime write even though it moves one row.
	tx, err := beginOrganizationRuntimeTx(ctx, s.repository.pool, string(request.OrganizationID))
	if err != nil {
		return workflowruntime.Lease{}, runtimePersistenceError("could not start lease renewal", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	err = tx.QueryRow(ctx, `
		WITH coordination_clock AS (SELECT clock_timestamp() AS current_time)
		UPDATE worker_leases lease
		SET expires_at = coordination_clock.current_time + $6 * interval '1 millisecond'
		FROM coordination_clock
		WHERE organization_id = $1 AND run_id = $2 AND attempt_id = $3
		  AND worker_id = $4 AND fencing_token = $5
		  AND expires_at > coordination_clock.current_time
		RETURNING lease.expires_at, coordination_clock.current_time`,
		request.OrganizationID, request.RunID, request.AttemptID, request.WorkerID,
		request.FencingToken, request.LeaseDuration.Milliseconds()).Scan(&lease.ExpiresAt, &lease.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return workflowruntime.Lease{}, runtimeError(workflowruntime.CodeLeaseLost, "attempt lease was lost", ErrStaleLease)
		}
		return workflowruntime.Lease{}, runtimePersistenceError("could not renew lease", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return workflowruntime.Lease{}, runtimePersistenceError("could not commit lease renewal", workflowruntime.MarkCommitOutcomeUnknown(err))
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

func validateRuntimeLease(ctx context.Context, tx pgx.Tx, organizationID workflowruntime.OrganizationID, runID, attemptID, workerID string, fencingToken uint64, _ time.Time) (string, string, error) {
	var nodeStateID, nodeID string
	err := tx.QueryRow(ctx, `
		SELECT a.node_state_id, a.node_id
		FROM node_attempts a
		JOIN worker_leases l ON l.organization_id = a.organization_id AND l.attempt_id = a.id
		WHERE a.organization_id = $1 AND a.run_id = $2 AND a.id = $3
		  AND a.status = 'running' AND a.worker_id = $4 AND a.fencing_token = $5
		  AND l.worker_id = $4 AND l.fencing_token = $5 AND l.expires_at > clock_timestamp()
		FOR UPDATE OF a, l`, organizationID, runID, attemptID, workerID, fencingToken).Scan(&nodeStateID, &nodeID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", runtimeError(workflowruntime.CodeStaleFencingToken, "attempt lease is stale", ErrStaleLease)
	}
	if err != nil {
		return "", "", runtimePersistenceError("could not validate attempt lease", err)
	}
	return nodeStateID, nodeID, nil
}
