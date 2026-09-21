package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

// CreateSubworkflow atomically persists a child run and immutable lineage. A
// rollback therefore cannot leave executable child work without a parent.
func (s *RuntimeStore) CreateSubworkflow(ctx context.Context, request workflowruntime.CreateSubworkflowRunRequest) (workflowruntime.CreateRunResult, error) {
	if err := s.repository.requirePool(); err != nil {
		return workflowruntime.CreateRunResult{}, err
	}
	link := request.Link
	if link.OrganizationID == "" || link.ParentRunID == "" || link.ParentNodeID == "" || link.ParentAttemptID == "" || link.ParentExecutionID == "" || request.Run.OrganizationID != link.OrganizationID {
		return workflowruntime.CreateRunResult{}, runtimeError(workflowruntime.CodeInvalidArgument, "subworkflow lineage is incomplete", nil)
	}
	tx, err := beginOrganizationRuntimeTx(ctx, s.repository.pool, string(request.Run.OrganizationID))
	if err != nil {
		return workflowruntime.CreateRunResult{}, runtimePersistenceError("could not start subworkflow", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var parentAttemptID, parentExecutionID string
	err = tx.QueryRow(ctx, `SELECT a.id,a.execution_id FROM node_attempts a
		JOIN runs r ON r.organization_id=a.organization_id AND r.id=a.run_id
		WHERE a.organization_id=$1 AND a.id=$2 AND a.run_id=$3 AND a.node_id=$4
		  AND r.status IN ('pending','running','suspended') AND r.cancel_requested_at IS NULL
		FOR UPDATE OF r,a`,
		link.OrganizationID, link.ParentAttemptID, link.ParentRunID, link.ParentNodeID).Scan(&parentAttemptID, &parentExecutionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return workflowruntime.CreateRunResult{}, runtimeError(workflowruntime.CodeNotFound, "parent attempt not found", err)
	}
	if err != nil {
		return workflowruntime.CreateRunResult{}, runtimePersistenceError("could not lock parent attempt", err)
	}
	if parentExecutionID != link.ParentExecutionID {
		return workflowruntime.CreateRunResult{}, runtimeError(workflowruntime.CodeConflict, "parent execution identity does not match attempt", nil)
	}
	result, err := createRuntimeRunTx(ctx, tx, request.Run)
	if err != nil {
		return workflowruntime.CreateRunResult{}, err
	}
	link.ChildRunID = result.Run.ID
	if link.CreatedAt.IsZero() {
		link.CreatedAt = request.Run.CreatedAt
	}
	tag, err := tx.Exec(ctx, `INSERT INTO subworkflow_links (
		organization_id,parent_run_id,parent_node_id,parent_attempt_id,parent_execution_id,child_run_id,created_at
	) VALUES ($1,$2,$3,$4,$5,$6,$7)
	ON CONFLICT (organization_id,parent_execution_id) DO NOTHING`, link.OrganizationID, link.ParentRunID,
		link.ParentNodeID, link.ParentAttemptID, link.ParentExecutionID, link.ChildRunID, link.CreatedAt)
	if err != nil {
		return workflowruntime.CreateRunResult{}, runtimePersistenceError("could not persist subworkflow lineage", err)
	}
	if tag.RowsAffected() == 0 {
		var childRunID string
		if err = tx.QueryRow(ctx, `SELECT child_run_id FROM subworkflow_links
			WHERE organization_id=$1 AND parent_execution_id=$2`, link.OrganizationID, link.ParentExecutionID).Scan(&childRunID); err != nil {
			return workflowruntime.CreateRunResult{}, runtimePersistenceError("could not verify subworkflow lineage", err)
		}
		if childRunID != link.ChildRunID {
			return workflowruntime.CreateRunResult{}, runtimeError(workflowruntime.CodeConflict, "parent execution already links another child", nil)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return workflowruntime.CreateRunResult{}, runtimePersistenceError("could not commit subworkflow", err)
	}
	return result, nil
}

func descendantRunIDsTx(ctx context.Context, tx pgx.Tx, organizationID workflowruntime.OrganizationID, parentRunID string) ([]string, error) {
	rows, err := tx.Query(ctx, `WITH RECURSIVE descendants(id) AS (
		SELECT child_run_id FROM subworkflow_links WHERE organization_id=$1 AND parent_run_id=$2
		UNION
		SELECT l.child_run_id FROM subworkflow_links l
		JOIN descendants d ON d.id=l.parent_run_id WHERE l.organization_id=$1
	)
	SELECT r.id FROM runs r JOIN descendants d ON d.id=r.id
	WHERE r.organization_id=$1 AND r.status IN ('pending','running','suspended','cancelling')
	ORDER BY r.id FOR UPDATE OF r`, organizationID, parentRunID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// cancelSubworkflowDescendantsTx invalidates descendant leases immediately;
// workers can no longer commit after a parent cancellation is durable.
func cancelSubworkflowDescendantsTx(ctx context.Context, tx pgx.Tx, organizationID workflowruntime.OrganizationID, parentRunID string, now time.Time) error {
	return cancelSubworkflowDescendantsWithReasonTx(ctx, tx, organizationID, parentRunID, now, "parent_cancelled")
}

// cancelSubworkflowDescendantsWithReasonTx recursively terminalizes child
// runs when their parent can no longer observe or safely join their results.
func cancelSubworkflowDescendantsWithReasonTx(ctx context.Context, tx pgx.Tx, organizationID workflowruntime.OrganizationID, parentRunID string, now time.Time, reason string) error {
	ids, err := descendantRunIDsTx(ctx, tx, organizationID, parentRunID)
	if err != nil {
		return err
	}
	for _, runID := range ids {
		tag, updateErr := tx.Exec(ctx, `UPDATE runs SET cancel_requested_at=COALESCE(cancel_requested_at,$3),
			status='cancelled',finished_at=$3,updated_at=$3,version=version+1
			WHERE organization_id=$1 AND id=$2 AND status IN ('pending','running','suspended','cancelling')`, organizationID, runID, now)
		if updateErr != nil {
			return updateErr
		}
		if tag.RowsAffected() == 0 {
			continue
		}
		if _, updateErr = tx.Exec(ctx, `UPDATE node_attempts SET status='cancelled',finished_at=$3,updated_at=$3
			WHERE organization_id=$1 AND run_id=$2 AND status IN ('pending','retry_ready','running','suspended')`, organizationID, runID, now); updateErr != nil {
			return updateErr
		}
		if _, updateErr = tx.Exec(ctx, `UPDATE node_states SET status='cancelled',updated_at=$3
			WHERE organization_id=$1 AND run_id=$2 AND status IN ('pending','ready','running','suspended')`, organizationID, runID, now); updateErr != nil {
			return updateErr
		}
		if _, updateErr = tx.Exec(ctx, `UPDATE interrupts SET status='cancelled',resolved_at=$3
			WHERE organization_id=$1 AND run_id=$2 AND status='pending'`, organizationID, runID, now); updateErr != nil {
			return updateErr
		}
		if _, updateErr = tx.Exec(ctx, `DELETE FROM worker_leases WHERE organization_id=$1 AND run_id=$2`, organizationID, runID); updateErr != nil {
			return updateErr
		}
		if _, updateErr = releaseRunAdmissionTx(ctx, tx, organizationID, runID, now); updateErr != nil {
			return updateErr
		}
		payload, marshalErr := json.Marshal(map[string]string{"reason": reason})
		if marshalErr != nil {
			return marshalErr
		}
		if _, updateErr = appendRuntimeEvents(ctx, tx, organizationID, runID, []workflowruntime.Event{
			{Type: "run.cancel_requested", Data: payload},
			{Type: "run.cancelled", Data: payload},
		}); updateErr != nil {
			return updateErr
		}
	}
	return nil
}
