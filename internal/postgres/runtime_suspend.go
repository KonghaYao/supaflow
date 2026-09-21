package postgres

import (
	"context"
	"encoding/json"
	"time"

	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

// SuspendAttempt atomically stores a single-use interrupt and snapshot while
// retaining the lease row so the next claim increments its fencing token.
func (s *RuntimeStore) SuspendAttempt(ctx context.Context, request workflowruntime.SuspendAttemptRequest) (workflowruntime.Interrupt, error) {
	tx, err := beginOrganizationRuntimeTx(ctx, s.repository.pool, string(request.OrganizationID))
	if err != nil {
		return workflowruntime.Interrupt{}, runtimeError(workflowruntime.CodeConflict, "could not suspend attempt", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	request.Now = timeOrNow(request.Now)
	nodeStateID, nodeID, err := validateRuntimeLease(ctx, tx, request.OrganizationID,
		request.RunID, request.AttemptID, request.WorkerID, request.FencingToken, request.Now)
	if err != nil {
		return workflowruntime.Interrupt{}, err
	}
	var runStatus string
	var cancelRequestedAt *time.Time
	if err = tx.QueryRow(ctx, `
		SELECT status, cancel_requested_at FROM runs
		WHERE organization_id = $1 AND id = $2 FOR UPDATE`, request.OrganizationID, request.RunID).Scan(
		&runStatus, &cancelRequestedAt); err != nil {
		return workflowruntime.Interrupt{}, runtimeError(workflowruntime.CodeConflict, "could not lock suspending run", err)
	}
	if runStatus != "running" || cancelRequestedAt != nil {
		return workflowruntime.Interrupt{}, runtimeError(workflowruntime.CodeInvalidTransition, "run cannot be suspended", nil)
	}
	interrupt := request.Interrupt
	interrupt.OrganizationID = request.OrganizationID
	interrupt.RunID = request.RunID
	interrupt.AttemptID = request.AttemptID
	interrupt.NodeID = nodeID
	interrupt.Status = workflowruntime.InterruptPending
	if interrupt.CreatedAt.IsZero() {
		interrupt.CreatedAt = request.Now
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO interrupts (
		    id, organization_id, run_id, node_state_id, attempt_id, kind, status,
		    prompt, resume_token_hash, expires_at, auto_resume, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7, $8, $9, $10, $11)`,
		interrupt.ID, request.OrganizationID, request.RunID, nodeStateID, request.AttemptID,
		interrupt.Kind, json.RawMessage(`{}`), interrupt.TokenDigest[:], interrupt.ExpiresAt,
		interrupt.AutoResume, interrupt.CreatedAt); err != nil {
		return workflowruntime.Interrupt{}, runtimeError(workflowruntime.CodeConflict, "could not persist interrupt", err)
	}
	if _, err = tx.Exec(ctx, `
		UPDATE node_attempts SET status = 'suspended', updated_at = $3
		WHERE organization_id = $1 AND id = $2`, request.OrganizationID, request.AttemptID, request.Now); err != nil {
		return workflowruntime.Interrupt{}, runtimeError(workflowruntime.CodeConflict, "could not suspend node attempt", err)
	}
	if _, err = tx.Exec(ctx, `
		UPDATE node_states SET status = 'suspended', updated_at = $3
		WHERE organization_id = $1 AND id = $2`, request.OrganizationID, nodeStateID, request.Now); err != nil {
		return workflowruntime.Interrupt{}, runtimeError(workflowruntime.CodeConflict, "could not suspend node", err)
	}
	if _, err = tx.Exec(ctx, `
		UPDATE runs SET status = CASE WHEN EXISTS (
		    SELECT 1 FROM node_attempts
		    WHERE organization_id = $1 AND run_id = $2 AND status IN ('pending','retry_ready','running')
		) THEN 'running' ELSE 'suspended' END, updated_at = $3, version = version + 1
		WHERE organization_id = $1 AND id = $2`, request.OrganizationID, request.RunID, request.Now); err != nil {
		return workflowruntime.Interrupt{}, runtimeError(workflowruntime.CodeConflict, "could not suspend run", err)
	}
	eventType := "approval.requested"
	if interrupt.Kind == "wait" {
		eventType = "wait.started"
	}
	if interrupt.Kind == "webhook" {
		eventType = "webhook.waiting"
	}
	sequence, err := appendRuntimeEvents(ctx, tx, request.OrganizationID, request.RunID, request.Events, eventType)
	if err != nil {
		return workflowruntime.Interrupt{}, runtimeError(workflowruntime.CodeConflict, "could not persist suspension events", err)
	}
	if err = persistRuntimeSnapshot(ctx, tx, request.OrganizationID, request.RunID,
		request.AttemptID, request.Snapshot, sequence, request.Now); err != nil {
		return workflowruntime.Interrupt{}, runtimeError(workflowruntime.CodeConflict, "could not persist suspension snapshot", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return workflowruntime.Interrupt{}, runtimeError(workflowruntime.CodeConflict, "could not commit suspension", err)
	}
	return interrupt, nil
}
