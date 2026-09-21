package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

// CompleteAttempt atomically commits output, snapshot, events, successors, and
// the resulting run status behind the active fencing token.
func (s *RuntimeStore) CompleteAttempt(ctx context.Context, request workflowruntime.CompleteAttemptRequest) (workflowruntime.Run, error) {
	tx, err := beginOrganizationRuntimeTx(ctx, s.repository.pool, string(request.OrganizationID))
	if err != nil {
		return workflowruntime.Run{}, runtimeError(workflowruntime.CodeConflict, "could not complete attempt", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	request.Now = timeOrNow(request.Now)
	nodeStateID, _, err := validateRuntimeLease(ctx, tx, request.OrganizationID, request.RunID,
		request.AttemptID, request.WorkerID, request.FencingToken, request.Now)
	if err != nil {
		return workflowruntime.Run{}, err
	}
	var currentStatus string
	var cancelRequestedAt *time.Time
	if err = tx.QueryRow(ctx, `
		SELECT status, cancel_requested_at FROM runs
		WHERE organization_id = $1 AND id = $2 FOR UPDATE`, request.OrganizationID, request.RunID).Scan(
		&currentStatus, &cancelRequestedAt); err != nil {
		return workflowruntime.Run{}, runtimeError(workflowruntime.CodeConflict, "could not lock completing run", err)
	}
	if currentStatus != "running" && currentStatus != "cancelling" {
		return workflowruntime.Run{}, runtimeError(workflowruntime.CodeInvalidTransition, "run cannot accept attempt completion", nil)
	}
	if currentStatus == "cancelling" || cancelRequestedAt != nil {
		// Cancellation wins the completion race. Ignore a result that arrived
		// after durable intent and never let it enqueue successor side effects.
		request.RunStatus = workflowruntime.RunCancelled
		request.Output = nil
		request.Snapshot = nil
		request.Next = nil
		request.Events = nil
	}
	attemptStatus, nodeStatus := "succeeded", "success"
	if request.RunStatus == workflowruntime.RunCancelled {
		attemptStatus, nodeStatus = "cancelled", "cancelled"
	}
	if request.Output != nil {
		request.Output.OrganizationID = request.OrganizationID
		request.Output.RunID = request.RunID
		request.Output.AttemptID = request.AttemptID
		if err = persistRuntimeOutput(ctx, tx, nodeStateID, request.Output, request.Now); err != nil {
			return workflowruntime.Run{}, runtimeError(workflowruntime.CodeConflict, "could not persist attempt output", err)
		}
	}
	if _, err = tx.Exec(ctx, `
		UPDATE node_attempts
		SET status = $4, finished_at = $5, updated_at = $5
		WHERE organization_id = $1 AND run_id = $2 AND id = $3`,
		request.OrganizationID, request.RunID, request.AttemptID, attemptStatus, request.Now); err != nil {
		return workflowruntime.Run{}, runtimeError(workflowruntime.CodeConflict, "could not finish attempt", err)
	}
	var outputID any
	if request.Output != nil {
		outputID = request.Output.ID
	}
	if _, err = tx.Exec(ctx, `
		UPDATE node_states SET status = $3, output_id = $4, updated_at = $5
		WHERE organization_id = $1 AND id = $2`, request.OrganizationID, nodeStateID,
		nodeStatus, outputID, request.Now); err != nil {
		return workflowruntime.Run{}, runtimeError(workflowruntime.CodeConflict, "could not finish node", err)
	}
	if request.RunStatus == workflowruntime.RunCancelled {
		if err = terminateRemainingRunWorkTx(ctx, tx, request.OrganizationID, request.RunID, request.Now); err != nil {
			return workflowruntime.Run{}, runtimeError(workflowruntime.CodeConflict, "could not finish cancelled run work", err)
		}
	}
	for _, next := range request.Next {
		if err = enqueueDAGSuccessor(ctx, tx, request.OrganizationID, request.RunID, next, request.Now); err != nil {
			return workflowruntime.Run{}, runtimeError(workflowruntime.CodeConflict, "could not create successor attempt", err)
		}
	}
	status := request.RunStatus
	if status != workflowruntime.RunCancelled {
		status, err = remainingDAGRunStatus(ctx, tx, request.OrganizationID, request.RunID)
		if err != nil {
			return workflowruntime.Run{}, runtimeError(workflowruntime.CodeConflict, "could not inspect DAG completion", err)
		}
	}
	if _, err = tx.Exec(ctx, `
		UPDATE runs SET status = $3::text, updated_at = $4::timestamptz,
		    finished_at = CASE WHEN $3::text IN ('success', 'failed', 'error', 'cancelled') THEN $4::timestamptz ELSE NULL END,
		    output_id = CASE WHEN $3::text = 'success' THEN $5::text ELSE NULL END,
		    version = version + 1
		WHERE organization_id = $1 AND id = $2`, request.OrganizationID, request.RunID,
		dbRunStatus(status), request.Now, outputID); err != nil {
		return workflowruntime.Run{}, runtimeError(workflowruntime.CodeConflict, "could not advance run", err)
	}
	if isTerminalRuntimeStatus(status) {
		if _, err = releaseRunAdmissionTx(ctx, tx, request.OrganizationID, request.RunID, request.Now); err != nil {
			return workflowruntime.Run{}, runtimeError(workflowruntime.CodeConflict, "could not release run capacity", err)
		}
	}
	defaultEvents := []string{"node.completed"}
	if isTerminalRuntimeStatus(status) {
		defaultEvents = append(defaultEvents, runtimeTerminalEventType(status))
	}
	sequence, err := appendRuntimeEvents(ctx, tx, request.OrganizationID, request.RunID, request.Events, defaultEvents...)
	if err != nil {
		return workflowruntime.Run{}, runtimeError(workflowruntime.CodeConflict, "could not persist completion events", err)
	}
	if err = persistRuntimeSnapshot(ctx, tx, request.OrganizationID, request.RunID,
		request.AttemptID, request.Snapshot, sequence, request.Now); err != nil {
		return workflowruntime.Run{}, runtimeError(workflowruntime.CodeConflict, "could not persist completion snapshot", err)
	}
	if request.RunStatus != workflowruntime.RunCancelled {
		err = releaseTerminalLease(ctx, tx, request.OrganizationID, request.AttemptID)
	}
	if err != nil {
		return workflowruntime.Run{}, runtimeError(workflowruntime.CodeConflict, "could not release completed lease", err)
	}
	run, err := scanRuntimeRunProjection(tx.QueryRow(ctx, runtimeRunProjectionByIDSQL, string(request.OrganizationID), request.RunID))
	if err != nil {
		return workflowruntime.Run{}, runtimeError(workflowruntime.CodeConflict, "could not read completed run", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return workflowruntime.Run{}, runtimeError(workflowruntime.CodeConflict, "could not commit attempt completion", workflowruntime.MarkCommitOutcomeUnknown(err))
	}
	return run, nil
}

// terminateRemainingRunWorkTx makes the first terminal result the single
// terminalization point for every branch of a fan-out. Late workers are fenced
// by removal of all run leases and cannot leave unreachable live work.
func terminateRemainingRunWorkTx(ctx context.Context, tx pgx.Tx, organizationID workflowruntime.OrganizationID, runID string, now time.Time) error {
	if _, err := tx.Exec(ctx, `
		UPDATE node_attempts SET status='cancelled',finished_at=$3,updated_at=$3
		WHERE organization_id=$1 AND run_id=$2
		  AND status IN ('pending','retry_ready','running','suspended')`, organizationID, runID, now); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE node_states SET status='cancelled',updated_at=$3
		WHERE organization_id=$1 AND run_id=$2
		  AND status IN ('pending','ready','running','suspended')`, organizationID, runID, now); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE interrupts SET status='cancelled',resolved_at=$3
		WHERE organization_id=$1 AND run_id=$2 AND status='pending'`, organizationID, runID, now); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `DELETE FROM worker_leases WHERE organization_id=$1 AND run_id=$2`, organizationID, runID)
	return err
}
