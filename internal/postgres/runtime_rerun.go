package postgres

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/pazhou/supaflow/internal/apimutation"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

// RerunRun atomically creates a fresh run, lineage, entry attempt, event, and outbox.
func (s *RuntimeStore) RerunRun(ctx context.Context, request workflowruntime.RerunRunRequest) (workflowruntime.CreateRunResult, error) {
	if err := s.repository.requirePool(); err != nil {
		return workflowruntime.CreateRunResult{}, err
	}
	tx, err := beginOrganizationRuntimeTx(ctx, s.repository.pool, string(request.Run.OrganizationID))
	if err != nil {
		return workflowruntime.CreateRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not start rerun", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err = LockAPIMutationTx(ctx, tx); err != nil {
		if errors.Is(err, ErrIdempotencyConflict) {
			return workflowruntime.CreateRunResult{}, runtimeError(workflowruntime.CodeIdempotencyConflict, "idempotency key was used for a different request", err)
		}
		return workflowruntime.CreateRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not lock rerun operation", err)
	}

	if replay, found, lookupErr := rerunReplay(ctx, tx, request); lookupErr != nil {
		return workflowruntime.CreateRunResult{}, lookupErr
	} else if found {
		if err = CompleteAPIMutationTx(ctx, tx, replay.ID, map[string]any{"runId": replay.ID}); err != nil {
			return workflowruntime.CreateRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not complete rerun operation", err)
		}
		if err = tx.Commit(ctx); err != nil {
			return workflowruntime.CreateRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not commit rerun replay", err)
		}
		apimutation.MarkCommitted(ctx)
		return workflowruntime.CreateRunResult{Run: replay}, nil
	}

	source, err := scanRuntimeRun(tx.QueryRow(ctx, `SELECT `+runtimeRunColumns+` FROM runs
		WHERE organization_id = $1 AND id = $2 FOR SHARE`, request.Run.OrganizationID, request.SourceRunID))
	if errors.Is(err, pgx.ErrNoRows) {
		return workflowruntime.CreateRunResult{}, runtimeError(workflowruntime.CodeNotFound, "source run not found", err)
	}
	if err != nil {
		return workflowruntime.CreateRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not read source run", err)
	}
	if !workflowruntime.IsTerminalRun(source.Status) {
		return workflowruntime.CreateRunResult{}, runtimeError(workflowruntime.CodeInvalidTransition, "only terminal runs can be rerun", nil)
	}

	input := source.Input
	if request.FromNodeID != "" {
		err = tx.QueryRow(ctx, `SELECT input FROM node_attempts
			WHERE organization_id = $1 AND run_id = $2 AND node_id = $3
			ORDER BY attempt_number DESC LIMIT 1`, request.Run.OrganizationID, source.ID, request.FromNodeID).Scan(&input)
		if errors.Is(err, pgx.ErrNoRows) {
			return workflowruntime.CreateRunResult{}, runtimeError(workflowruntime.CodeNotFound, "source node attempt not found", err)
		}
		if err != nil {
			return workflowruntime.CreateRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not read source node input", err)
		}
	}
	run := request.Run
	run.Plan.ID, run.Plan.Hash, run.Input = source.Plan.ID, source.Plan.Hash, input
	stored, err := scanRuntimeRun(tx.QueryRow(ctx, createRuntimeRunSQL,
		run.ID, string(run.OrganizationID), run.IdempotencyKey, decodeHash(run.RequestFingerprint), run.Input,
		run.Plan.EntryNodeID, run.Plan.Executor, dbRunStatus(run.Status), run.CreatedAt, run.Version, run.Plan.ID, run.Plan.Hash, run.Priority, run.DeadlineAt))
	if errors.Is(err, pgx.ErrNoRows) {
		if replay, found, replayErr := rerunReplay(ctx, tx, request); replayErr != nil {
			return workflowruntime.CreateRunResult{}, replayErr
		} else if found {
			if err = CompleteAPIMutationTx(ctx, tx, replay.ID, map[string]any{"runId": replay.ID}); err != nil {
				return workflowruntime.CreateRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not complete rerun operation", err)
			}
			if err = tx.Commit(ctx); err != nil {
				return workflowruntime.CreateRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not commit rerun replay", err)
			}
			apimutation.MarkCommitted(ctx)
			return workflowruntime.CreateRunResult{Run: replay}, nil
		}
		return workflowruntime.CreateRunResult{}, runtimeError(workflowruntime.CodeNotFound, "execution plan not found", err)
	}
	if err != nil {
		return workflowruntime.CreateRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not persist rerun", err)
	}
	admission, admissionErr := admitRunTx(ctx, tx, run)
	if admissionErr != nil {
		var runtimeAdmissionErr *workflowruntime.Error
		if errors.As(admissionErr, &runtimeAdmissionErr) {
			return workflowruntime.CreateRunResult{}, admissionErr
		}
		return workflowruntime.CreateRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not admit rerun", admissionErr)
	}
	var fromNode any
	if request.FromNodeID != "" {
		fromNode = request.FromNodeID
	}
	_, err = tx.Exec(ctx, `INSERT INTO run_rerun_lineage (organization_id, run_id, source_run_id, from_node_id, created_at)
		VALUES ($1, $2, $3, $4, $5)`, run.OrganizationID, run.ID, source.ID, fromNode, run.CreatedAt)
	if err != nil {
		return workflowruntime.CreateRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not persist rerun lineage", err)
	}
	payload, _ := json.Marshal(map[string]any{"sourceRunId": source.ID, "fromNodeId": fromNode})
	if _, err = appendEventTx(ctx, tx, string(run.OrganizationID), run.ID, "run.created", payload); err != nil {
		return workflowruntime.CreateRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not persist rerun event", err)
	}
	if err = insertOutboxTx(ctx, tx, string(run.OrganizationID), "run", run.ID, "run.created", payload); err != nil {
		return workflowruntime.CreateRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not persist rerun delivery", err)
	}
	admissionPayload, _ := json.Marshal(map[string]any{"priority": run.Priority, "status": admission.status})
	admissionEvent := runtimeAdmissionEvent(admission.status)
	if _, err = appendEventTx(ctx, tx, string(run.OrganizationID), run.ID, admissionEvent, admissionPayload); err != nil {
		return workflowruntime.CreateRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not persist rerun admission event", err)
	}
	if err = insertOutboxTx(ctx, tx, string(run.OrganizationID), "run", run.ID, admissionEvent, admissionPayload); err != nil {
		return workflowruntime.CreateRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not persist rerun admission delivery", err)
	}
	if err = seedRuntimeAttempt(ctx, tx, run); err != nil {
		return workflowruntime.CreateRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not seed rerun attempt", err)
	}
	if err = CompleteAPIMutationTx(ctx, tx, stored.ID, map[string]any{"runId": stored.ID}); err != nil {
		return workflowruntime.CreateRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not complete rerun operation", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return workflowruntime.CreateRunResult{}, runtimeError(workflowruntime.CodeConflict, "could not commit rerun", err)
	}
	apimutation.MarkCommitted(ctx)
	stored.Input = input
	stored.Rerun = &workflowruntime.RerunLineage{SourceRunID: source.ID, FromNodeID: request.FromNodeID, CreatedAt: run.CreatedAt}
	return workflowruntime.CreateRunResult{Run: stored, Created: true}, nil
}

func rerunReplay(ctx context.Context, tx pgx.Tx, request workflowruntime.RerunRunRequest) (workflowruntime.Run, bool, error) {
	run, err := scanRuntimeRunProjection(tx.QueryRow(ctx, runtimeRunProjectionByIdempotencySQL,
		string(request.Run.OrganizationID), request.Run.IdempotencyKey))
	if errors.Is(err, pgx.ErrNoRows) {
		return workflowruntime.Run{}, false, nil
	}
	if err != nil {
		return workflowruntime.Run{}, false, runtimeError(workflowruntime.CodeConflict, "could not inspect rerun idempotency", err)
	}
	if subtle.ConstantTimeCompare([]byte(run.RequestFingerprint), []byte(request.Run.RequestFingerprint)) != 1 {
		return workflowruntime.Run{}, false, runtimeError(workflowruntime.CodeIdempotencyConflict, "idempotency key was used for a different request", ErrIdempotencyConflict)
	}
	return run, true, nil
}
