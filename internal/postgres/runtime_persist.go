package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

func appendRuntimeEvents(ctx context.Context, tx pgx.Tx, tenantID workflowruntime.TenantID, runID string, events []workflowruntime.Event, defaults ...string) (uint64, error) {
	if len(events) == 0 {
		for _, eventType := range defaults {
			events = append(events, workflowruntime.Event{Type: eventType, Data: json.RawMessage(`{}`)})
		}
	}
	var last uint64
	for _, input := range events {
		payload := input.Data
		if len(payload) == 0 {
			payload = json.RawMessage(`{}`)
		}
		event, err := appendEventTx(ctx, tx, string(tenantID), runID, input.Type, payload)
		if err != nil {
			return 0, err
		}
		last = uint64(event.Sequence)
		if err = insertOutboxTx(ctx, tx, string(tenantID), "run", runID, input.Type, payload); err != nil {
			return 0, err
		}
	}
	return last, nil
}

func runtimeTerminalEventType(status workflowruntime.RunStatus) string {
	switch status {
	case workflowruntime.RunSuccess:
		return "run.succeeded"
	case workflowruntime.RunFailed:
		return "run.failed"
	case workflowruntime.RunError:
		return "run.error"
	case workflowruntime.RunCancelled:
		return "run.cancelled"
	default:
		return ""
	}
}

func persistRuntimeOutput(ctx context.Context, tx pgx.Tx, nodeStateID string, output *workflowruntime.Output, now time.Time) error {
	if output == nil {
		return nil
	}
	if output.ID == "" {
		var err error
		output.ID, err = NewID("output")
		if err != nil {
			return err
		}
	}
	if output.ContentType == "" {
		output.ContentType = "application/json"
	}
	var inline any
	var objectRef any
	if output.ObjectRef != "" {
		objectRef = output.ObjectRef
	} else {
		if len(output.Inline) == 0 {
			output.Inline = json.RawMessage(`null`)
		}
		inline = output.Inline
	}
	sensitivity := "internal"
	if output.Sensitive {
		sensitivity = "sensitive"
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('supaflow:retention:' || $1, 0))`, output.TenantID); err != nil {
		return fmt.Errorf("lock tenant retention policy: %w", err)
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO node_outputs (
		    id, tenant_id, run_id, node_state_id, attempt_id, content_type,
		    inline_value, object_ref, size_bytes, content_hash, sensitivity, created_at, retention_until
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
		    $12::timestamptz + COALESCE((SELECT output_days FROM retention_policies WHERE tenant_id=$2), 30) * interval '1 day')`,
		output.ID, output.TenantID, output.RunID, nodeStateID, output.AttemptID,
		output.ContentType, inline, objectRef, output.Size, decodeHash(output.SHA256),
		sensitivity, now)
	if err != nil {
		return fmt.Errorf("persist runtime output: %w", err)
	}
	return nil
}

func persistRuntimeSnapshot(ctx context.Context, tx pgx.Tx, tenantID workflowruntime.TenantID, runID, attemptID string, snapshot *workflowruntime.Snapshot, sequence uint64, now time.Time) error {
	if snapshot == nil {
		return nil
	}
	if snapshot.ID == "" {
		var err error
		snapshot.ID, err = NewID("snapshot")
		if err != nil {
			return err
		}
	}
	if snapshot.Sequence == 0 {
		snapshot.Sequence = sequence
	}
	snapshot.TenantID = tenantID
	snapshot.RunID = runID
	snapshot.AttemptID = attemptID
	snapshot.CreatedAt = now
	digest := sha256.Sum256(snapshot.Data)
	_, err := tx.Exec(ctx, `
		INSERT INTO run_snapshots (
		    id, tenant_id, run_id, attempt_id, sequence, state, state_hash, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		snapshot.ID, tenantID, runID, attemptID, snapshot.Sequence, snapshot.Data,
		digest[:], now)
	if err != nil {
		return fmt.Errorf("persist runtime snapshot: %w", err)
	}
	return nil
}

func createSuccessorAttempt(ctx context.Context, tx pgx.Tx, tenantID workflowruntime.TenantID, runID string, next workflowruntime.NewAttempt, now time.Time) error {
	var nodeStateID string
	var attemptNumber int
	err := tx.QueryRow(ctx, `
		SELECT id, attempt_count + 1 FROM node_states
		WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
		FOR UPDATE`, tenantID, runID, next.NodeID).Scan(&nodeStateID, &attemptNumber)
	if err == pgx.ErrNoRows {
		nodeStateID, err = NewID("node")
		if err != nil {
			return err
		}
		attemptNumber = 1
		if _, err = tx.Exec(ctx, `
			INSERT INTO node_states (
			    id, tenant_id, run_id, node_id, status, attempt_count, created_at, updated_at
			) VALUES ($1, $2, $3, $4, 'pending', 1, $5, $5)`,
			nodeStateID, tenantID, runID, next.NodeID, now); err != nil {
			return fmt.Errorf("insert successor node: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("lock successor node: %w", err)
	}
	attemptID, err := NewID("att")
	if err != nil {
		return err
	}
	availableAt := next.NextAttemptAt
	if availableAt.IsZero() {
		availableAt = now
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO node_attempts (
		    id, execution_id, tenant_id, run_id, node_state_id, node_id, executor, input,
		    attempt_number, status, available_at, created_at, updated_at
		) VALUES ($1, $1, $2, $3, $4, $5, $6, $7, $8, 'pending', $9, $10, $10)`,
		attemptID, tenantID, runID, nodeStateID, next.NodeID, next.Executor,
		next.Input, attemptNumber, availableAt, now); err != nil {
		return fmt.Errorf("insert successor attempt: %w", err)
	}
	if _, err = tx.Exec(ctx, `
		UPDATE node_states
		SET status = 'pending', attempt_count = $4, latest_attempt_id = $5, updated_at = $6
		WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3`,
		tenantID, runID, next.NodeID, attemptNumber, attemptID, now); err != nil {
		return fmt.Errorf("update successor node: %w", err)
	}
	return nil
}

func releaseTerminalLease(ctx context.Context, tx pgx.Tx, tenantID workflowruntime.TenantID, attemptID string) error {
	_, err := tx.Exec(ctx, `DELETE FROM worker_leases WHERE tenant_id = $1 AND attempt_id = $2`, tenantID, attemptID)
	if err != nil {
		return fmt.Errorf("release terminal lease: %w", err)
	}
	return nil
}
