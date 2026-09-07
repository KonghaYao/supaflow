package postgres

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pazhou/supaflow/internal/apimutation"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

type approvalResponseSnapshot struct {
	RunID             string                    `json:"runId"`
	PlanID            string                    `json:"planId"`
	PlanHash          string                    `json:"planHash"`
	Status            workflowruntime.RunStatus `json:"status"`
	DeadlineAt        *time.Time                `json:"deadlineAt,omitempty"`
	CancelRequestedAt *time.Time                `json:"cancelRequestedAt,omitempty"`
	CreatedAt         time.Time                 `json:"createdAt"`
	UpdatedAt         time.Time                 `json:"updatedAt"`
	Version           uint64                    `json:"version"`
}

// RequestCancel persists cancellation intent and immediately finalizes a run
// when no attempt is executing.
func (s *RuntimeStore) RequestCancel(ctx context.Context, organizationID workflowruntime.OrganizationID, runID string, now time.Time) (workflowruntime.CancelRunResult, error) {
	tx, err := s.repository.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return workflowruntime.CancelRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not request cancellation", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err = LockAPIMutationTx(ctx, tx); err != nil {
		if errors.Is(err, ErrIdempotencyConflict) {
			return workflowruntime.CancelRunResult{}, runtimeError(workflowruntime.CodeIdempotencyConflict, "idempotency key was used for a different request", err)
		}
		return workflowruntime.CancelRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not lock cancellation operation", err)
	}
	run, err := scanRuntimeRun(tx.QueryRow(ctx, runtimeRunByIDSQL+` FOR UPDATE`, string(organizationID), runID))
	if errors.Is(err, pgx.ErrNoRows) {
		return workflowruntime.CancelRunResult{}, runtimeError(workflowruntime.CodeNotFound, "run not found", err)
	}
	if err != nil {
		return workflowruntime.CancelRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not lock run", err)
	}
	if run.CancelRequestedAt != nil || run.Status == workflowruntime.RunCancelled {
		if err = CompleteAPIMutationTx(ctx, tx, runID, map[string]any{"runId": runID}); err != nil {
			return workflowruntime.CancelRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not complete cancellation operation", err)
		}
		if err = tx.Commit(ctx); err != nil {
			return workflowruntime.CancelRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not commit cancellation replay", err)
		}
		apimutation.MarkCommitted(ctx)
		return workflowruntime.CancelRunResult{Run: run, AlreadyRequested: true}, nil
	}
	if isTerminalRuntimeStatus(run.Status) {
		return workflowruntime.CancelRunResult{}, runtimeError(workflowruntime.CodeInvalidTransition, "terminal run cannot be cancelled", nil)
	}
	now = timeOrNow(now)
	var active bool
	if err = tx.QueryRow(ctx, `
		SELECT EXISTS (
		    SELECT 1 FROM node_attempts
		    WHERE organization_id = $1 AND run_id = $2 AND status = 'running'
		)`, string(organizationID), runID).Scan(&active); err != nil {
		return workflowruntime.CancelRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not inspect active attempts", err)
	}
	status := workflowruntime.RunCancelled
	if active {
		status = workflowruntime.RunCancelling
	}
	if _, err = tx.Exec(ctx, `
		UPDATE runs SET cancel_requested_at = $3::timestamptz, status = $4::text, updated_at = $3::timestamptz,
		    finished_at = CASE WHEN $4::text = 'cancelled' THEN $3::timestamptz ELSE finished_at END,
		    version = version + 1
		WHERE organization_id = $1 AND id = $2`, string(organizationID), runID, now, dbRunStatus(status)); err != nil {
		return workflowruntime.CancelRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not persist cancellation", err)
	}
	if !active {
		if _, err = tx.Exec(ctx, `
			UPDATE node_attempts SET status = 'cancelled', finished_at = $3, updated_at = $3
			WHERE organization_id = $1 AND run_id = $2 AND status IN ('pending', 'retry_ready', 'suspended')`,
			string(organizationID), runID, now); err != nil {
			return workflowruntime.CancelRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not cancel idle attempts", err)
		}
		if _, err = tx.Exec(ctx, `
			UPDATE node_states SET status = 'cancelled', updated_at = $3
			WHERE organization_id = $1 AND run_id = $2 AND status IN ('pending', 'ready', 'suspended')`,
			string(organizationID), runID, now); err != nil {
			return workflowruntime.CancelRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not cancel idle nodes", err)
		}
		if _, err = tx.Exec(ctx, `
			UPDATE interrupts SET status = 'cancelled', resolved_at = $3
			WHERE organization_id = $1 AND run_id = $2 AND status = 'pending'`,
			string(organizationID), runID, now); err != nil {
			return workflowruntime.CancelRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not cancel pending interrupts", err)
		}
		if _, err = releaseRunAdmissionTx(ctx, tx, organizationID, runID, now); err != nil {
			return workflowruntime.CancelRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not release run capacity", err)
		}
	}
	for _, eventType := range cancellationEventTypes(active) {
		if _, err = appendEventTx(ctx, tx, string(organizationID), runID, eventType, json.RawMessage(`{}`)); err != nil {
			return workflowruntime.CancelRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not append cancellation event", err)
		}
		if err = insertOutboxTx(ctx, tx, string(organizationID), "run", runID, eventType, json.RawMessage(`{}`)); err != nil {
			return workflowruntime.CancelRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not enqueue cancellation event", err)
		}
	}
	if err = cancelSubworkflowDescendantsTx(ctx, tx, organizationID, runID, now); err != nil {
		return workflowruntime.CancelRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not cancel subworkflow descendants", err)
	}
	if err = CompleteAPIMutationTx(ctx, tx, runID, map[string]any{"runId": runID}); err != nil {
		return workflowruntime.CancelRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not complete cancellation operation", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return workflowruntime.CancelRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not commit cancellation", err)
	}
	apimutation.MarkCommitted(ctx)
	updated, err := s.GetRun(ctx, organizationID, runID)
	if err != nil {
		return workflowruntime.CancelRunResult{}, err
	}
	return workflowruntime.CancelRunResult{Run: updated, AlreadyRequested: false}, nil
}

func cancellationEventTypes(active bool) []string {
	if active {
		return []string{"run.cancel_requested"}
	}
	return []string{"run.cancel_requested", "run.cancelled"}
}

func isTerminalRuntimeStatus(status workflowruntime.RunStatus) bool {
	return status == workflowruntime.RunSuccess || status == workflowruntime.RunFailed ||
		status == workflowruntime.RunError || status == workflowruntime.RunCancelled
}

// ResolveInterrupt consumes an approval token exactly once and makes its
// suspended attempt runnable in the same transaction.
func (s *RuntimeStore) ResolveInterrupt(ctx context.Context, request workflowruntime.ResolveInterruptRequest) (workflowruntime.ResolveInterruptResult, error) {
	if request.DecidedBy == "" || len(request.IdempotencyKey) < 8 || len(request.IdempotencyKey) > 128 {
		return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeInvalidArgument, "approval actor and idempotency key are required", nil)
	}
	tx, err := s.repository.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeConflict, "could not resolve approval", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err = LockAPIMutationTx(ctx, tx); err != nil {
		if errors.Is(err, ErrIdempotencyConflict) {
			return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeIdempotencyConflict, "idempotency key was used for a different request", err)
		}
		return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeConflict, "could not lock approval operation", err)
	}
	if _, err = tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, hashtextextended($2, 0)))`,
		string(request.OrganizationID), request.IdempotencyKey,
	); err != nil {
		return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeConflict, "could not lock approval idempotency key", err)
	}
	var existingHash []byte
	var existingInterruptID string
	err = tx.QueryRow(ctx, `
		SELECT request_hash, interrupt_id FROM approval_decisions
		WHERE organization_id = $1 AND idempotency_key = $2`, request.OrganizationID, request.IdempotencyKey,
	).Scan(&existingHash, &existingInterruptID)
	if err == nil {
		if len(existingHash) != len(request.RequestHash) || subtle.ConstantTimeCompare(existingHash, request.RequestHash[:]) != 1 {
			return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeIdempotencyConflict, "idempotency key was used for a different request", nil)
		}
		result, replayErr := resolvedInterruptResultTx(ctx, tx, request.OrganizationID, existingInterruptID)
		if replayErr != nil {
			return workflowruntime.ResolveInterruptResult{}, replayErr
		}
		if err = CompleteAPIMutationTx(ctx, tx, request.InterruptID, map[string]any{"runId": result.Run.ID}); err != nil {
			return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeConflict, "could not complete approval operation", err)
		}
		if err = tx.Commit(ctx); err != nil {
			return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeConflict, "could not commit approval replay", err)
		}
		apimutation.MarkCommitted(ctx)
		return result, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeConflict, "could not inspect approval idempotency key", err)
	}
	var databaseNow time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&databaseNow); err != nil {
		return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeConflict, "could not read database clock", err)
	}
	var interrupt workflowruntime.Interrupt
	var organization string
	var digest []byte
	var status string
	var attemptStatus, runStatus string
	err = tx.QueryRow(ctx, `
		SELECT i.id, i.organization_id, i.run_id, a.node_id, i.attempt_id, i.kind,
		       i.resume_token_hash, i.expires_at, i.resolved_at, i.status, i.created_at,
		       a.status, r.status
		FROM interrupts i
		JOIN node_attempts a ON a.organization_id = i.organization_id AND a.id = i.attempt_id
		JOIN runs r ON r.organization_id = i.organization_id AND r.id = i.run_id
		WHERE i.organization_id = $1 AND i.id = $2
		FOR UPDATE OF i, a, r`, string(request.OrganizationID), request.InterruptID).Scan(
		&interrupt.ID, &organization, &interrupt.RunID, &interrupt.NodeID,
		&interrupt.AttemptID, &interrupt.Kind, &digest, &interrupt.ExpiresAt,
		&interrupt.ResolvedAt, &status, &interrupt.CreatedAt, &attemptStatus, &runStatus,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeApprovalInvalid, "approval token is invalid", nil)
	}
	if err != nil {
		return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeConflict, "could not lock approval", err)
	}
	if len(digest) != len(request.TokenDigest) || subtle.ConstantTimeCompare(digest, request.TokenDigest[:]) != 1 {
		return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeApprovalInvalid, "approval token is invalid", nil)
	}
	interrupt.OrganizationID = workflowruntime.OrganizationID(organization)
	if interrupt.Kind != "approval" && interrupt.Kind != "human_approval" {
		return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeApprovalInvalid, "approval token is invalid", nil)
	}
	if status != "pending" || interrupt.ResolvedAt != nil {
		return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeApprovalConsumed, "approval was already resolved", nil)
	}
	if !databaseNow.Before(interrupt.ExpiresAt) {
		return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeApprovalExpired, "approval token has expired", nil)
	}
	if attemptStatus != "suspended" || (runStatus != "suspended" && runStatus != "running") {
		return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeInvalidTransition, "approval run cannot resume an interrupt", nil)
	}
	decision, err := json.Marshal(request.Decision)
	if err != nil {
		return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeInvalidArgument, "approval decision is invalid", err)
	}
	decisionName := "reject"
	if request.Decision.Approved {
		decisionName = "approve"
	}
	decisionID, err := NewID("decision")
	if err != nil {
		return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeConflict, "could not allocate approval decision", err)
	}
	if _, err = tx.Exec(ctx, `
		UPDATE interrupts SET status = 'resolved', resolved_at = $3
		WHERE organization_id = $1 AND id = $2 AND status = 'pending'`, request.OrganizationID, request.InterruptID, databaseNow); err != nil {
		return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeConflict, "could not consume approval", err)
	}
	if _, err = tx.Exec(ctx, `
		UPDATE node_attempts SET status = 'pending', resume_data = $4,
		    available_at = $3, updated_at = $3
		WHERE organization_id = $1 AND id = $2 AND status = 'suspended'`, request.OrganizationID, interrupt.AttemptID, databaseNow, decision); err != nil {
		return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeConflict, "could not resume attempt", err)
	}
	if _, err = tx.Exec(ctx, `
		UPDATE node_states SET status = 'pending', updated_at = $3
		WHERE organization_id = $1 AND run_id = $2 AND node_id = $4`, request.OrganizationID, interrupt.RunID, databaseNow, interrupt.NodeID); err != nil {
		return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeConflict, "could not resume node", err)
	}
	if _, err = tx.Exec(ctx, `
		UPDATE runs SET status = CASE WHEN status = 'suspended' THEN 'pending' ELSE status END,
		    updated_at = $3, version = version + 1
		WHERE organization_id = $1 AND id = $2 AND status IN ('suspended','running')`, request.OrganizationID, interrupt.RunID, databaseNow); err != nil {
		return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeConflict, "could not resume run", err)
	}
	interrupt.ResolvedAt = &databaseNow
	interrupt.Status = workflowruntime.InterruptResolved
	interrupt.Decision = decision
	if _, err = appendEventTx(ctx, tx, string(request.OrganizationID), interrupt.RunID, "approval.resolved", decision); err != nil {
		return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeConflict, "could not append approval event", err)
	}
	if err = insertOutboxTx(ctx, tx, string(request.OrganizationID), "run", interrupt.RunID, "approval.resolved", decision); err != nil {
		return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeConflict, "could not enqueue approval event", err)
	}
	run, err := scanRuntimeRun(tx.QueryRow(ctx, runtimeRunByIDSQL, string(request.OrganizationID), interrupt.RunID))
	if err != nil {
		return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeConflict, "could not read resumed run", err)
	}
	responseSnapshot, err := json.Marshal(approvalResponseSnapshot{
		RunID: run.ID, PlanID: run.Plan.ID, PlanHash: run.Plan.Hash, Status: run.Status,
		DeadlineAt:        run.DeadlineAt,
		CancelRequestedAt: run.CancelRequestedAt, CreatedAt: run.CreatedAt,
		UpdatedAt: run.UpdatedAt, Version: run.Version,
	})
	if err != nil {
		return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeConflict, "could not encode approval response", err)
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO approval_decisions (
		    id, organization_id, interrupt_id, decision, decision_data, decided_by, comment, decided_at,
		    idempotency_key, request_hash, response_snapshot
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		decisionID, request.OrganizationID, request.InterruptID, decisionName, decision,
		request.DecidedBy, request.Decision.Comment, request.Now, request.IdempotencyKey,
		request.RequestHash[:], responseSnapshot); err != nil {
		return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeConflict, "could not persist approval", err)
	}
	storedResult, err := resolvedInterruptResultTx(ctx, tx, request.OrganizationID, request.InterruptID)
	if err != nil {
		return workflowruntime.ResolveInterruptResult{}, err
	}
	if err = CompleteAPIMutationTx(ctx, tx, request.InterruptID, map[string]any{"runId": storedResult.Run.ID}); err != nil {
		return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeConflict, "could not complete approval operation", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeConflict, "could not commit approval", err)
	}
	apimutation.MarkCommitted(ctx)
	return storedResult, nil
}

func resolvedInterruptResultTx(ctx context.Context, tx pgx.Tx, organizationID workflowruntime.OrganizationID, interruptID string) (workflowruntime.ResolveInterruptResult, error) {
	var interrupt workflowruntime.Interrupt
	var storedOrganization, status string
	var digest []byte
	var responseSnapshotJSON []byte
	err := tx.QueryRow(ctx, `
		SELECT i.id, i.organization_id, i.run_id, a.node_id, i.attempt_id, i.kind,
		       i.resume_token_hash, i.expires_at, i.resolved_at, i.status, i.created_at,
		       d.decision_data, d.response_snapshot
		FROM approval_decisions d
		JOIN interrupts i ON i.organization_id = d.organization_id AND i.id = d.interrupt_id
		JOIN node_attempts a ON a.organization_id = i.organization_id AND a.id = i.attempt_id
		WHERE d.organization_id = $1 AND d.interrupt_id = $2`, organizationID, interruptID).Scan(
		&interrupt.ID, &storedOrganization, &interrupt.RunID, &interrupt.NodeID, &interrupt.AttemptID,
		&interrupt.Kind, &digest, &interrupt.ExpiresAt, &interrupt.ResolvedAt, &status,
		&interrupt.CreatedAt, &interrupt.Decision, &responseSnapshotJSON,
	)
	if err != nil {
		return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeConflict, "could not replay approval result", err)
	}
	interrupt.OrganizationID = workflowruntime.OrganizationID(storedOrganization)
	interrupt.Status = workflowruntime.InterruptResolved
	copy(interrupt.TokenDigest[:], digest)
	run, err := scanRuntimeRun(tx.QueryRow(ctx, runtimeRunByIDSQL, string(organizationID), interrupt.RunID))
	if err != nil {
		return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeConflict, "could not read replayed run", err)
	}
	var snapshot approvalResponseSnapshot
	if err = json.Unmarshal(responseSnapshotJSON, &snapshot); err != nil {
		return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeConflict, "could not decode approval response", err)
	}
	if snapshot.RunID == "" || snapshot.PlanID == "" || snapshot.PlanHash == "" || snapshot.Status == "" ||
		snapshot.CreatedAt.IsZero() || snapshot.UpdatedAt.IsZero() || snapshot.RunID != interrupt.RunID {
		return workflowruntime.ResolveInterruptResult{}, runtimeError(workflowruntime.CodeConflict, "approval response snapshot is invalid", nil)
	}
	run.ID, run.Plan.ID, run.Plan.Hash, run.Status = snapshot.RunID, snapshot.PlanID, snapshot.PlanHash, snapshot.Status
	run.DeadlineAt, run.CancelRequestedAt, run.CreatedAt, run.UpdatedAt, run.Version = snapshot.DeadlineAt, snapshot.CancelRequestedAt, snapshot.CreatedAt, snapshot.UpdatedAt, snapshot.Version
	return workflowruntime.ResolveInterruptResult{Interrupt: interrupt, Run: run}, nil
}
