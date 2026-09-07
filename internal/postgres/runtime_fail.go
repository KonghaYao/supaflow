package postgres

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

// FailAttempt atomically records an execution error and either schedules one
// bounded retry attempt or finalizes the run.
func (s *RuntimeStore) FailAttempt(ctx context.Context, request workflowruntime.FailAttemptRequest) (workflowruntime.Run, error) {
	tx, err := s.repository.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return workflowruntime.Run{}, runtimeError(workflowruntime.CodeConflict, "could not fail attempt", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	request.Now = timeOrNow(request.Now)
	nodeStateID, nodeID, err := validateRuntimeLease(ctx, tx, request.OrganizationID,
		request.RunID, request.AttemptID, request.WorkerID, request.FencingToken, request.Now)
	if err != nil {
		return workflowruntime.Run{}, err
	}
	var currentStatus string
	var cancelRequestedAt *time.Time
	if err = tx.QueryRow(ctx, `
		SELECT status, cancel_requested_at FROM runs
		WHERE organization_id = $1 AND id = $2 FOR UPDATE`, request.OrganizationID, request.RunID).Scan(
		&currentStatus, &cancelRequestedAt); err != nil {
		return workflowruntime.Run{}, runtimeError(workflowruntime.CodeConflict, "could not lock failing run", err)
	}
	if currentStatus != "running" && currentStatus != "cancelling" {
		return workflowruntime.Run{}, runtimeError(workflowruntime.CodeInvalidTransition, "run cannot accept attempt failure", nil)
	}
	if cancelRequestedAt != nil {
		request.RetryAt = nil
		request.TerminalStatus = workflowruntime.RunCancelled
	}
	var executor, executionID string
	var input json.RawMessage
	var attemptNumber int
	if err = tx.QueryRow(ctx, `
		SELECT executor, execution_id, input, attempt_number FROM node_attempts
		WHERE organization_id = $1 AND id = $2`, request.OrganizationID, request.AttemptID).Scan(
		&executor, &executionID, &input, &attemptNumber); err != nil {
		return workflowruntime.Run{}, runtimeError(workflowruntime.CodeConflict, "could not read failed attempt", err)
	}
	errorData, _ := json.Marshal(map[string]string{"code": request.ErrorCode})
	if _, err = tx.Exec(ctx, `
		UPDATE node_attempts SET status = 'failed', error = $3,
		    finished_at = $4, updated_at = $4
		WHERE organization_id = $1 AND id = $2`, request.OrganizationID, request.AttemptID,
		errorData, request.Now); err != nil {
		return workflowruntime.Run{}, runtimeError(workflowruntime.CodeConflict, "could not persist attempt failure", err)
	}
	status := request.TerminalStatus
	if request.RetryAt != nil {
		status = workflowruntime.RunRunning
		newAttemptID, idErr := NewID("att")
		if idErr != nil {
			return workflowruntime.Run{}, runtimeError(workflowruntime.CodeConflict, "could not allocate retry attempt", idErr)
		}
		if _, err = tx.Exec(ctx, `
			INSERT INTO node_attempts (
			    id, execution_id, organization_id, run_id, node_state_id, node_id, executor, input,
			    attempt_number, status, available_at, created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'retry_ready', $10, $11, $11)`,
			newAttemptID, executionID, request.OrganizationID, request.RunID, nodeStateID, nodeID,
			executor, input, attemptNumber+1, *request.RetryAt, request.Now); err != nil {
			return workflowruntime.Run{}, runtimeError(workflowruntime.CodeConflict, "could not persist retry attempt", err)
		}
		if _, err = tx.Exec(ctx, `
			UPDATE node_states SET status = 'pending', last_error = $3,
			    attempt_count = $4, latest_attempt_id = $5, updated_at = $6
			WHERE organization_id = $1 AND id = $2`, request.OrganizationID, nodeStateID,
			errorData, attemptNumber+1, newAttemptID, request.Now); err != nil {
			return workflowruntime.Run{}, runtimeError(workflowruntime.CodeConflict, "could not schedule retry node", err)
		}
	} else {
		if status == "" {
			status = workflowruntime.RunFailed
		}
		nodeStatus := "failed"
		if status == workflowruntime.RunError {
			nodeStatus = "error"
		} else if status == workflowruntime.RunCancelled {
			nodeStatus = "cancelled"
		}
		if _, err = tx.Exec(ctx, `
			UPDATE node_states SET status = $3, last_error = $4, updated_at = $5
			WHERE organization_id = $1 AND id = $2`, request.OrganizationID, nodeStateID,
			nodeStatus, errorData, request.Now); err != nil {
			return workflowruntime.Run{}, runtimeError(workflowruntime.CodeConflict, "could not fail node", err)
		}
		if err = terminateRemainingRunWorkTx(ctx, tx, request.OrganizationID, request.RunID, request.Now); err != nil {
			return workflowruntime.Run{}, runtimeError(workflowruntime.CodeConflict, "could not finish terminal run work", err)
		}
		if err = cancelSubworkflowDescendantsWithReasonTx(ctx, tx, request.OrganizationID, request.RunID, request.Now, "parent_terminal"); err != nil {
			return workflowruntime.Run{}, runtimeError(workflowruntime.CodeConflict, "could not cancel terminal run descendants", err)
		}
	}
	if _, err = tx.Exec(ctx, `
		UPDATE runs SET status = $3::text, updated_at = $4::timestamptz,
		    finished_at = CASE WHEN $3::text IN ('failed', 'error', 'cancelled') THEN $4::timestamptz ELSE NULL END,
		    version = version + 1
		WHERE organization_id = $1 AND id = $2`, request.OrganizationID, request.RunID,
		dbRunStatus(status), request.Now); err != nil {
		return workflowruntime.Run{}, runtimeError(workflowruntime.CodeConflict, "could not update failed run", err)
	}
	if request.RetryAt == nil {
		if _, err = releaseRunAdmissionTx(ctx, tx, request.OrganizationID, request.RunID, request.Now); err != nil {
			return workflowruntime.Run{}, runtimeError(workflowruntime.CodeConflict, "could not release run capacity", err)
		}
	}
	defaultEvents := []string{"node.failed"}
	if request.RetryAt != nil {
		defaultEvents = append(defaultEvents, "node.retry_scheduled")
	} else {
		defaultEvents = append(defaultEvents, runtimeTerminalEventType(status))
	}
	if _, err = appendRuntimeEvents(ctx, tx, request.OrganizationID, request.RunID,
		request.Events, defaultEvents...); err != nil {
		return workflowruntime.Run{}, runtimeError(workflowruntime.CodeConflict, "could not persist failure events", err)
	}
	if request.RetryAt != nil {
		err = releaseTerminalLease(ctx, tx, request.OrganizationID, request.AttemptID)
	}
	if err != nil {
		return workflowruntime.Run{}, runtimeError(workflowruntime.CodeConflict, "could not release failed lease", err)
	}
	run, err := scanRuntimeRun(tx.QueryRow(ctx, runtimeRunByIDSQL, string(request.OrganizationID), request.RunID))
	if err != nil {
		return workflowruntime.Run{}, runtimeError(workflowruntime.CodeConflict, "could not read failed run", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return workflowruntime.Run{}, runtimeError(workflowruntime.CodeConflict, "could not commit attempt failure", err)
	}
	return run, nil
}
