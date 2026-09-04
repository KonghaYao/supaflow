package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

func resumeDueTimer(ctx context.Context, tx pgx.Tx, tenantID workflowruntime.TenantID, now time.Time) error {
	var interruptID, runID, attemptID, nodeStateID string
	err := tx.QueryRow(ctx, `
		SELECT id, run_id, attempt_id, node_state_id FROM interrupts
		WHERE tenant_id=$1 AND status='pending' AND auto_resume=true AND expires_at <= $2
		ORDER BY expires_at, id FOR UPDATE SKIP LOCKED LIMIT 1`, tenantID, now).Scan(&interruptID, &runID, &attemptID, &nodeStateID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	decision := json.RawMessage(`{"expired":true}`)
	if _, err = tx.Exec(ctx, `UPDATE interrupts SET status='resolved', resolved_at=$3
		WHERE tenant_id=$1 AND id=$2`, tenantID, interruptID, now); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE node_attempts SET status='pending', resume_data=$3, available_at=$4, updated_at=$4
		WHERE tenant_id=$1 AND id=$2 AND status='suspended'`, tenantID, attemptID, decision, now); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE node_states SET status='pending', updated_at=$3
		WHERE tenant_id=$1 AND id=$2`, tenantID, nodeStateID, now); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE runs SET status='running', updated_at=$3, version=version+1
		WHERE tenant_id=$1 AND id=$2 AND status='suspended'`, tenantID, runID, now); err != nil {
		return err
	}
	_, err = appendRuntimeEvents(ctx, tx, tenantID, runID, nil, "wait.resumed")
	return err
}

func enqueueDAGSuccessor(ctx context.Context, tx pgx.Tx, tenantID workflowruntime.TenantID, runID string, next workflowruntime.NewAttempt, now time.Time) error {
	if (next.EdgeID == "" || len(next.RequiredEdges) <= 1) && !next.AggregateInput {
		return createSuccessorAttempt(ctx, tx, tenantID, runID, next, now)
	}
	if len(next.RequiredEdges) <= 1 {
		input, inputErr := aggregateDAGInput(next)
		if inputErr != nil {
			return inputErr
		}
		next.Input = input
		return createSuccessorAttempt(ctx, tx, tenantID, runID, next, now)
	}
	required, err := json.Marshal(next.RequiredEdges)
	if err != nil {
		return fmt.Errorf("encode required edges: %w", err)
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO dag_joins (tenant_id, run_id, node_id, executor, required_edges, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $6)
		ON CONFLICT (tenant_id, run_id, node_id) DO NOTHING`, tenantID, runID, next.NodeID, next.Executor, required, now); err != nil {
		return fmt.Errorf("create join barrier: %w", err)
	}
	var executor string
	var requiredMatches bool
	var scheduled bool
	if err = tx.QueryRow(ctx, `
		SELECT executor, required_edges = $4::jsonb, scheduled FROM dag_joins
		WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3 FOR UPDATE`,
		tenantID, runID, next.NodeID, required).Scan(&executor, &requiredMatches, &scheduled); err != nil {
		return fmt.Errorf("lock join barrier: %w", err)
	}
	if executor != next.Executor || !requiredMatches {
		return fmt.Errorf("join barrier plan mismatch")
	}
	if scheduled {
		return nil
	}
	availableAt := next.NextAttemptAt
	if availableAt.IsZero() {
		availableAt = now
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO dag_edge_inputs (
		    tenant_id, run_id, node_id, edge_id, source_node_id, source_port,
		    target_port, value, available_at, created_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (tenant_id, run_id, node_id, edge_id) DO NOTHING`, tenantID, runID,
		next.NodeID, next.EdgeID, next.SourceNodeID, next.SourcePort, next.TargetPort,
		next.Input, availableAt, now); err != nil {
		return fmt.Errorf("persist edge input: %w", err)
	}
	var count int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM dag_edge_inputs
		WHERE tenant_id=$1 AND run_id=$2 AND node_id=$3`, tenantID, runID, next.NodeID).Scan(&count); err != nil {
		return err
	}
	if count < len(next.RequiredEdges) {
		return nil
	}
	var input []byte
	var readyAt time.Time
	if err = tx.QueryRow(ctx, `
		SELECT jsonb_object_agg(CASE WHEN target_port='' THEN source_node_id ELSE target_port END, value), max(available_at)
		FROM dag_edge_inputs WHERE tenant_id=$1 AND run_id=$2 AND node_id=$3`,
		tenantID, runID, next.NodeID).Scan(&input, &readyAt); err != nil {
		return fmt.Errorf("assemble join input: %w", err)
	}
	if len(next.StaticInputs) > 0 {
		var values map[string]json.RawMessage
		if err = json.Unmarshal(input, &values); err != nil {
			return fmt.Errorf("decode joined input: %w", err)
		}
		for port, literal := range next.StaticInputs {
			if _, duplicate := values[port]; duplicate {
				return fmt.Errorf("duplicate target input port")
			}
			values[port] = literal
		}
		input, err = json.Marshal(values)
		if err != nil {
			return fmt.Errorf("encode joined input: %w", err)
		}
	}
	tag, err := tx.Exec(ctx, `UPDATE dag_joins SET scheduled=true, updated_at=$4
		WHERE tenant_id=$1 AND run_id=$2 AND node_id=$3 AND scheduled=false`, tenantID, runID, next.NodeID, now)
	if err != nil || tag.RowsAffected() != 1 {
		return fmt.Errorf("claim ready join: %w", err)
	}
	next.Input, next.NextAttemptAt = input, readyAt
	return createSuccessorAttempt(ctx, tx, tenantID, runID, next, now)
}

func aggregateDAGInput(next workflowruntime.NewAttempt) ([]byte, error) {
	values := make(map[string]json.RawMessage, len(next.StaticInputs)+1)
	for port, value := range next.StaticInputs {
		values[port] = value
	}
	port := next.TargetPort
	if port == "" {
		port = next.SourceNodeID
	}
	if port == "" {
		port = "input"
	}
	if _, duplicate := values[port]; duplicate {
		return nil, fmt.Errorf("duplicate target input port")
	}
	values[port] = next.Input
	return json.Marshal(values)
}

func remainingDAGRunStatus(ctx context.Context, tx pgx.Tx, tenantID workflowruntime.TenantID, runID string) (workflowruntime.RunStatus, error) {
	var runnable, suspended, waitingJoin bool
	err := tx.QueryRow(ctx, `SELECT
		EXISTS (SELECT 1 FROM node_attempts WHERE tenant_id=$1 AND run_id=$2 AND status IN ('pending','retry_ready','running')),
		EXISTS (SELECT 1 FROM node_attempts WHERE tenant_id=$1 AND run_id=$2 AND status='suspended'),
		EXISTS (SELECT 1 FROM dag_joins WHERE tenant_id=$1 AND run_id=$2 AND scheduled=false)`, tenantID, runID).Scan(&runnable, &suspended, &waitingJoin)
	if err != nil {
		return "", err
	}
	if runnable || waitingJoin {
		return workflowruntime.RunRunning, nil
	}
	if suspended {
		return workflowruntime.RunSuspended, nil
	}
	return workflowruntime.RunSuccess, nil
}
