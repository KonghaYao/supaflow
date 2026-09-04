package postgres

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

const maxPostgresWebhookResumeBytes = 256 << 10

// ResumeWebhook consumes a webhook interrupt independently from approvals and
// records replay-safe response evidence in one transaction.
func (s *RuntimeStore) ResumeWebhook(ctx context.Context, request workflowruntime.ResumeWebhookRequest) (workflowruntime.ResumeWebhookResult, error) {
	if err := s.repository.requirePool(); err != nil {
		return workflowruntime.ResumeWebhookResult{}, err
	}
	if len(request.IdempotencyKey) < 8 || len(request.IdempotencyKey) > 128 || len(request.Data) > maxPostgresWebhookResumeBytes || !json.Valid(request.Data) {
		return workflowruntime.ResumeWebhookResult{}, runtimeError(workflowruntime.CodeInvalidArgument, "webhook resume request is invalid", nil)
	}
	request.Now = timeOrNow(request.Now)
	tx, err := s.repository.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return workflowruntime.ResumeWebhookResult{}, runtimeError(workflowruntime.CodeConflict, "could not resume webhook", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, hashtextextended($2, 0)))`, string(request.TenantID), request.IdempotencyKey); err != nil {
		return workflowruntime.ResumeWebhookResult{}, runtimeError(workflowruntime.CodeConflict, "could not lock webhook idempotency key", err)
	}
	var existingHash []byte
	var existingInterruptID string
	err = tx.QueryRow(ctx, `SELECT request_hash, interrupt_id FROM webhook_resumptions
		WHERE tenant_id=$1 AND idempotency_key=$2`, request.TenantID, request.IdempotencyKey).Scan(&existingHash, &existingInterruptID)
	if err == nil {
		if len(existingHash) != len(request.RequestHash) || subtle.ConstantTimeCompare(existingHash, request.RequestHash[:]) != 1 {
			return workflowruntime.ResumeWebhookResult{}, runtimeError(workflowruntime.CodeIdempotencyConflict, "idempotency key was used for a different request", nil)
		}
		result, replayErr := webhookResumeResultTx(ctx, tx, request.TenantID, existingInterruptID)
		if replayErr != nil {
			return workflowruntime.ResumeWebhookResult{}, replayErr
		}
		return result, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return workflowruntime.ResumeWebhookResult{}, runtimeError(workflowruntime.CodeConflict, "could not inspect webhook idempotency key", err)
	}
	interrupt, attemptStatus, runStatus, err := lockWebhookInterrupt(ctx, tx, request)
	if err != nil {
		return workflowruntime.ResumeWebhookResult{}, err
	}
	if interrupt.Status != workflowruntime.InterruptPending || interrupt.ResolvedAt != nil {
		return workflowruntime.ResumeWebhookResult{}, runtimeError(workflowruntime.CodeApprovalConsumed, "webhook resume token was already consumed", nil)
	}
	if !request.Now.Before(interrupt.ExpiresAt) {
		return workflowruntime.ResumeWebhookResult{}, runtimeError(workflowruntime.CodeApprovalExpired, "webhook resume token has expired", nil)
	}
	if attemptStatus != "suspended" || (runStatus != "suspended" && runStatus != "running") {
		return workflowruntime.ResumeWebhookResult{}, runtimeError(workflowruntime.CodeInvalidTransition, "run cannot resume webhook interrupt", nil)
	}
	if _, err = tx.Exec(ctx, `UPDATE interrupts SET status='resolved',resolved_at=$3
		WHERE tenant_id=$1 AND id=$2 AND status='pending'`, request.TenantID, request.InterruptID, request.Now); err != nil {
		return workflowruntime.ResumeWebhookResult{}, runtimeError(workflowruntime.CodeConflict, "could not consume webhook token", err)
	}
	if _, err = tx.Exec(ctx, `UPDATE node_attempts SET status='pending',resume_data=$4,available_at=$3,updated_at=$3
		WHERE tenant_id=$1 AND id=$2 AND status='suspended'`, request.TenantID, request.AttemptID, request.Now, request.Data); err != nil {
		return workflowruntime.ResumeWebhookResult{}, runtimeError(workflowruntime.CodeConflict, "could not resume webhook attempt", err)
	}
	if _, err = tx.Exec(ctx, `UPDATE node_states SET status='pending',updated_at=$4
		WHERE tenant_id=$1 AND run_id=$2 AND node_id=$3`, request.TenantID, request.RunID, request.NodeID, request.Now); err != nil {
		return workflowruntime.ResumeWebhookResult{}, runtimeError(workflowruntime.CodeConflict, "could not resume webhook node", err)
	}
	if _, err = tx.Exec(ctx, `UPDATE runs SET status=CASE WHEN status='suspended' THEN 'pending' ELSE status END,
		updated_at=$3,version=version+1 WHERE tenant_id=$1 AND id=$2 AND status IN ('suspended','running')`, request.TenantID, request.RunID, request.Now); err != nil {
		return workflowruntime.ResumeWebhookResult{}, runtimeError(workflowruntime.CodeConflict, "could not resume webhook run", err)
	}
	if _, err = appendRuntimeEvents(ctx, tx, request.TenantID, request.RunID, []workflowruntime.Event{{Type: "webhook.resumed", Data: request.Data}}); err != nil {
		return workflowruntime.ResumeWebhookResult{}, runtimeError(workflowruntime.CodeConflict, "could not persist webhook resume event", err)
	}
	auditID, err := NewID("audit")
	if err != nil {
		return workflowruntime.ResumeWebhookResult{}, runtimeError(workflowruntime.CodeConflict, "could not allocate webhook audit", err)
	}
	metadata, _ := json.Marshal(map[string]any{"runId": request.RunID, "nodeId": request.NodeID, "payloadBytes": len(request.Data)})
	if _, err = tx.Exec(ctx, `INSERT INTO audit_records
		(id,tenant_id,actor_id,action,resource_type,resource_id,outcome,metadata,created_at)
		VALUES ($1,$2,'external_webhook','webhook.resume','interrupt',$3,'allowed',$4,$5)`, auditID, request.TenantID, request.InterruptID, metadata, request.Now); err != nil {
		return workflowruntime.ResumeWebhookResult{}, runtimeError(workflowruntime.CodeConflict, "could not persist webhook audit", err)
	}
	run, err := scanRuntimeRun(tx.QueryRow(ctx, runtimeRunByIDSQL, string(request.TenantID), request.RunID))
	if err != nil {
		return workflowruntime.ResumeWebhookResult{}, runtimeError(workflowruntime.CodeConflict, "could not read resumed webhook run", err)
	}
	snapshot, err := encodeRunResponseSnapshot(run)
	if err != nil {
		return workflowruntime.ResumeWebhookResult{}, err
	}
	resumeID, err := NewID("resume")
	if err != nil {
		return workflowruntime.ResumeWebhookResult{}, runtimeError(workflowruntime.CodeConflict, "could not allocate webhook resume", err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO webhook_resumptions
		(id,tenant_id,interrupt_id,idempotency_key,request_hash,resume_data,response_snapshot,created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, resumeID, request.TenantID, request.InterruptID,
		request.IdempotencyKey, request.RequestHash[:], request.Data, snapshot, request.Now); err != nil {
		return workflowruntime.ResumeWebhookResult{}, runtimeError(workflowruntime.CodeConflict, "could not persist webhook resume", err)
	}
	stored, err := webhookResumeResultTx(ctx, tx, request.TenantID, request.InterruptID)
	if err != nil {
		return workflowruntime.ResumeWebhookResult{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return workflowruntime.ResumeWebhookResult{}, runtimeError(workflowruntime.CodeConflict, "could not commit webhook resume", err)
	}
	return stored, nil
}

func lockWebhookInterrupt(ctx context.Context, tx pgx.Tx, request workflowruntime.ResumeWebhookRequest) (workflowruntime.Interrupt, string, string, error) {
	var interrupt workflowruntime.Interrupt
	var tenant, status, attemptStatus, runStatus string
	var digest []byte
	err := tx.QueryRow(ctx, `SELECT i.id,i.tenant_id,i.run_id,a.node_id,i.attempt_id,i.kind,
		i.resume_token_hash,i.expires_at,i.resolved_at,i.status,i.created_at,a.status,r.status
		FROM interrupts i JOIN node_attempts a ON a.tenant_id=i.tenant_id AND a.id=i.attempt_id
		JOIN runs r ON r.tenant_id=i.tenant_id AND r.id=i.run_id
		WHERE i.tenant_id=$1 AND i.id=$2 FOR UPDATE OF i,a,r`, request.TenantID, request.InterruptID).Scan(
		&interrupt.ID, &tenant, &interrupt.RunID, &interrupt.NodeID, &interrupt.AttemptID,
		&interrupt.Kind, &digest, &interrupt.ExpiresAt, &interrupt.ResolvedAt, &status,
		&interrupt.CreatedAt, &attemptStatus, &runStatus)
	identityMismatch := err == nil && (interrupt.Kind != "webhook" || interrupt.RunID != request.RunID || interrupt.NodeID != request.NodeID || interrupt.AttemptID != request.AttemptID || len(digest) != len(request.TokenDigest) || subtle.ConstantTimeCompare(digest, request.TokenDigest[:]) != 1)
	if errors.Is(err, pgx.ErrNoRows) || identityMismatch {
		return workflowruntime.Interrupt{}, "", "", runtimeError(workflowruntime.CodeApprovalInvalid, "webhook resume token is invalid", nil)
	}
	if err != nil {
		return workflowruntime.Interrupt{}, "", "", runtimeError(workflowruntime.CodeConflict, "could not lock webhook interrupt", err)
	}
	interrupt.TenantID, interrupt.Status = workflowruntime.TenantID(tenant), workflowruntime.InterruptStatus(strings.ToUpper(status))
	copy(interrupt.TokenDigest[:], digest)
	return interrupt, attemptStatus, runStatus, nil
}

func encodeRunResponseSnapshot(run workflowruntime.Run) ([]byte, error) {
	value, err := json.Marshal(approvalResponseSnapshot{RunID: run.ID, PlanID: run.Plan.ID, PlanHash: run.Plan.Hash,
		Status: run.Status, DeadlineAt: run.DeadlineAt, CancelRequestedAt: run.CancelRequestedAt,
		CreatedAt: run.CreatedAt, UpdatedAt: run.UpdatedAt, Version: run.Version})
	if err != nil {
		return nil, runtimeError(workflowruntime.CodeConflict, "could not encode webhook response", err)
	}
	return value, nil
}

func webhookResumeResultTx(ctx context.Context, tx pgx.Tx, tenantID workflowruntime.TenantID, interruptID string) (workflowruntime.ResumeWebhookResult, error) {
	var interrupt workflowruntime.Interrupt
	var tenant string
	var digest, response []byte
	err := tx.QueryRow(ctx, `SELECT i.id,i.tenant_id,i.run_id,a.node_id,i.attempt_id,i.kind,
		i.resume_token_hash,i.expires_at,i.resolved_at,i.created_at,w.resume_data,w.response_snapshot
		FROM webhook_resumptions w JOIN interrupts i ON i.tenant_id=w.tenant_id AND i.id=w.interrupt_id
		JOIN node_attempts a ON a.tenant_id=i.tenant_id AND a.id=i.attempt_id
		WHERE w.tenant_id=$1 AND w.interrupt_id=$2`, tenantID, interruptID).Scan(
		&interrupt.ID, &tenant, &interrupt.RunID, &interrupt.NodeID, &interrupt.AttemptID,
		&interrupt.Kind, &digest, &interrupt.ExpiresAt, &interrupt.ResolvedAt,
		&interrupt.CreatedAt, &interrupt.Decision, &response)
	if err != nil {
		return workflowruntime.ResumeWebhookResult{}, runtimeError(workflowruntime.CodeConflict, "could not replay webhook resume", err)
	}
	interrupt.TenantID, interrupt.Status = workflowruntime.TenantID(tenant), workflowruntime.InterruptResolved
	copy(interrupt.TokenDigest[:], digest)
	run, err := scanRuntimeRun(tx.QueryRow(ctx, runtimeRunByIDSQL, string(tenantID), interrupt.RunID))
	if err != nil {
		return workflowruntime.ResumeWebhookResult{}, runtimeError(workflowruntime.CodeConflict, "could not read replayed webhook run", err)
	}
	var snapshot approvalResponseSnapshot
	if err = json.Unmarshal(response, &snapshot); err != nil || snapshot.RunID != interrupt.RunID || snapshot.Status == "" {
		return workflowruntime.ResumeWebhookResult{}, runtimeError(workflowruntime.CodeConflict, "webhook response snapshot is invalid", err)
	}
	run.ID, run.Plan.ID, run.Plan.Hash, run.Status = snapshot.RunID, snapshot.PlanID, snapshot.PlanHash, snapshot.Status
	run.DeadlineAt, run.CancelRequestedAt, run.CreatedAt = snapshot.DeadlineAt, snapshot.CancelRequestedAt, snapshot.CreatedAt
	run.UpdatedAt, run.Version = snapshot.UpdatedAt, snapshot.Version
	return workflowruntime.ResumeWebhookResult{Interrupt: interrupt, Run: run}, nil
}
