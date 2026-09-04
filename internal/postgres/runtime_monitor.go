package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

// ListRuns reads a bounded recent-run projection using tenant in every join and predicate.
func (s *RuntimeStore) ListRuns(ctx context.Context, tenantID workflowruntime.TenantID, filter workflowruntime.RunFilter) ([]workflowruntime.RunSummary, error) {
	rows, err := s.repository.pool.Query(ctx, `
		SELECT r.id,r.execution_plan_id,r.plan_hash,r.plan_entry_node_id,r.plan_executor,
		       encode(r.request_hash,'hex'),r.input,r.idempotency_key,r.status,r.priority,r.deadline_at,r.cancel_requested_at,
		       r.created_at,r.updated_at,r.version,r.workflow_id,
		       o.id,o.inline_value,o.content_type,o.size_bytes,encode(o.content_hash,'hex'),
		       o.sensitivity,(o.object_ref IS NOT NULL),o.retention_status,
		       l.source_run_id,l.from_node_id,l.created_at
		FROM runs r
		LEFT JOIN node_outputs o ON o.tenant_id=r.tenant_id AND o.id=r.output_id
		LEFT JOIN run_rerun_lineage l ON l.tenant_id=r.tenant_id AND l.run_id=r.id
		WHERE r.tenant_id=$1 AND ($2='' OR r.workflow_id=$2) AND ($3='' OR r.status=$3)
		ORDER BY r.created_at DESC,r.id DESC LIMIT $4`, string(tenantID), filter.WorkflowID, dbRunStatus(filter.Status), filter.Limit)
	if err != nil {
		return nil, runtimeError(workflowruntime.CodeConflict, "could not list runs", err)
	}
	defer rows.Close()
	items := make([]workflowruntime.RunSummary, 0)
	for rows.Next() {
		item, scanErr := scanRuntimeRunSummary(rows, tenantID)
		if scanErr != nil {
			return nil, runtimeError(workflowruntime.CodeConflict, "could not decode run", scanErr)
		}
		items = append(items, item)
	}
	if err = rows.Err(); err != nil {
		return nil, runtimeError(workflowruntime.CodeConflict, "could not iterate runs", err)
	}
	return items, nil
}

func scanRuntimeRunSummary(row rowScanner, tenantID workflowruntime.TenantID) (workflowruntime.RunSummary, error) {
	var item workflowruntime.RunSummary
	var status string
	var outputID, contentType, outputHash, sensitivity, retentionStatus *string
	var outputSize *int64
	var inline json.RawMessage
	var external *bool
	var sourceRunID, fromNodeID *string
	var rerunCreatedAt *time.Time
	err := row.Scan(&item.Run.ID, &item.Run.Plan.ID, &item.Run.Plan.Hash, &item.Run.Plan.EntryNodeID,
		&item.Run.Plan.Executor, &item.Run.RequestFingerprint, &item.Run.Input, &item.Run.IdempotencyKey,
		&status, &item.Run.Priority, &item.Run.DeadlineAt, &item.Run.CancelRequestedAt, &item.Run.CreatedAt, &item.Run.UpdatedAt, &item.Run.Version,
		&item.WorkflowID, &outputID, &inline, &contentType, &outputSize, &outputHash, &sensitivity,
		&external, &retentionStatus, &sourceRunID, &fromNodeID, &rerunCreatedAt)
	item.Run.TenantID, item.Run.Status = tenantID, runtimeRunStatus(status)
	if err == nil && outputID != nil {
		item.Run.Output = &workflowruntime.RunOutput{Inline: inline, ContentType: *contentType, Size: *outputSize,
			SHA256: *outputHash, Sensitive: *sensitivity == "sensitive", External: *external, RetentionStatus: *retentionStatus}
	}
	if err == nil && sourceRunID != nil {
		item.Run.Rerun = &workflowruntime.RerunLineage{SourceRunID: *sourceRunID, CreatedAt: *rerunCreatedAt}
		if fromNodeID != nil {
			item.Run.Rerun.FromNodeID = *fromNodeID
		}
	}
	return item, err
}

// GetRunDetails reads node states, ordered attempts, and safe output values for one run.
func (s *RuntimeStore) GetRunDetails(ctx context.Context, tenantID workflowruntime.TenantID, runID string) (workflowruntime.RunDetails, error) {
	run, err := s.GetRun(ctx, tenantID, runID)
	if err != nil {
		return workflowruntime.RunDetails{}, err
	}
	rows, err := s.repository.pool.Query(ctx, `
		SELECT n.node_id,n.status,n.attempt_count,COALESCE(n.latest_attempt_id,''),n.updated_at,
		       a.id,a.executor,a.attempt_number,a.status,a.input,a.available_at,a.started_at,a.finished_at,
		       COALESCE(a.error->>'code',''),a.created_at,a.updated_at,
		       o.id,o.inline_value,o.content_type,o.size_bytes,encode(o.content_hash,'hex'),
		       o.sensitivity,(o.object_ref IS NOT NULL),o.retention_status
		FROM node_states n
		LEFT JOIN node_attempts a ON a.tenant_id=n.tenant_id AND a.run_id=n.run_id AND a.node_state_id=n.id
		LEFT JOIN node_outputs o ON o.tenant_id=n.tenant_id AND o.run_id=n.run_id AND o.id=n.output_id
		WHERE n.tenant_id=$1 AND n.run_id=$2
		ORDER BY n.created_at,n.node_id,a.attempt_number`, string(tenantID), runID)
	if err != nil {
		return workflowruntime.RunDetails{}, runtimeError(workflowruntime.CodeConflict, "could not read run diagnostics", err)
	}
	defer rows.Close()
	byNode := make(map[string]*workflowruntime.NodeDiagnostic)
	order := make([]string, 0)
	for rows.Next() {
		var nodeID, nodeStatus, latestAttempt string
		var attemptID, executor, attemptStatus, errorCode *string
		var attemptNumber *int
		var input json.RawMessage
		var availableAt, attemptCreatedAt, attemptUpdatedAt time.Time
		var startedAt, finishedAt *time.Time
		var attemptCount int
		var nodeUpdatedAt time.Time
		var outputID, contentType, outputHash, sensitivity, retentionStatus *string
		var inline json.RawMessage
		var outputSize *int64
		var external *bool
		if err = rows.Scan(&nodeID, &nodeStatus, &attemptCount, &latestAttempt, &nodeUpdatedAt,
			&attemptID, &executor, &attemptNumber, &attemptStatus, &input, &availableAt, &startedAt, &finishedAt,
			&errorCode, &attemptCreatedAt, &attemptUpdatedAt, &outputID, &inline, &contentType, &outputSize,
			&outputHash, &sensitivity, &external, &retentionStatus); err != nil {
			return workflowruntime.RunDetails{}, runtimeError(workflowruntime.CodeConflict, "could not decode diagnostics", err)
		}
		node := byNode[nodeID]
		if node == nil {
			node = &workflowruntime.NodeDiagnostic{Node: workflowruntime.NodeState{TenantID: tenantID, RunID: runID,
				NodeID: nodeID, Status: runtimeNodeStatus(nodeStatus), AttemptCount: attemptCount,
				LatestAttemptID: latestAttempt, UpdatedAt: nodeUpdatedAt}, Attempts: []workflowruntime.AttemptDiagnostic{}}
			if outputID != nil {
				node.Output = &workflowruntime.RunOutput{Inline: inline, ContentType: *contentType, Size: *outputSize,
					SHA256: *outputHash, Sensitive: *sensitivity == "sensitive", External: *external, RetentionStatus: *retentionStatus}
			}
			byNode[nodeID], order = node, append(order, nodeID)
		}
		if attemptID != nil {
			attempt := workflowruntime.Attempt{TenantID: tenantID, ID: *attemptID, RunID: runID, NodeID: nodeID,
				Executor: *executor, Number: *attemptNumber, Status: runtimeAttemptStatus(*attemptStatus), Input: input,
				NextAttemptAt: availableAt, StartedAt: startedAt, FinishedAt: finishedAt,
				ErrorCode: *errorCode, CreatedAt: attemptCreatedAt, UpdatedAt: attemptUpdatedAt}
			node.Attempts = append(node.Attempts, workflowruntime.AttemptDiagnostic{Attempt: attempt})
		}
	}
	if err = rows.Err(); err != nil {
		return workflowruntime.RunDetails{}, runtimeError(workflowruntime.CodeConflict, "could not iterate diagnostics", err)
	}
	nodes := make([]workflowruntime.NodeDiagnostic, 0, len(order))
	for _, nodeID := range order {
		nodes = append(nodes, *byNode[nodeID])
	}
	var cursor uint64
	if err = s.repository.pool.QueryRow(ctx, `SELECT next_event_sequence FROM runs WHERE tenant_id=$1 AND id=$2`, string(tenantID), runID).Scan(&cursor); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return workflowruntime.RunDetails{}, runtimeError(workflowruntime.CodeConflict, "could not read event cursor", err)
	}
	return workflowruntime.RunDetails{Run: run, Nodes: nodes, LastEventCursor: cursor}, nil
}

var _ workflowruntime.MonitorStore = (*RuntimeStore)(nil)
