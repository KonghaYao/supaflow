package postgres

import (
	"context"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/apimutation"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

// RuntimeStore adapts PostgreSQL rows and transactions to runtime.Store.
type RuntimeStore struct {
	repository *Repository
}

// NewRuntimeStore constructs the production durable runtime Store adapter.
func NewRuntimeStore(pool *pgxpool.Pool) *RuntimeStore {
	return &RuntimeStore{repository: NewRepository(pool)}
}

var _ workflowruntime.Store = (*RuntimeStore)(nil)

func runtimeError(code workflowruntime.ErrorCode, message string, cause error) error {
	return &workflowruntime.Error{Code: code, Message: message, Cause: cause}
}

func dbRunStatus(status workflowruntime.RunStatus) string { return strings.ToLower(string(status)) }

func runtimeRunStatus(status string) workflowruntime.RunStatus {
	return workflowruntime.RunStatus(strings.ToUpper(status))
}

const createRuntimeRunSQL = `
INSERT INTO runs (
    id, organization_id, workflow_id, workflow_version_id, execution_plan_id,
    idempotency_key, request_hash, input, plan_hash, plan_entry_node_id, plan_executor,
    priority, status, deadline_at, created_at, updated_at, version
)
SELECT $1, p.organization_id, p.workflow_id, p.workflow_version_id, p.id,
       $3, $4, $5, $12, $6, $7, $13, $8, $14, $9, $9, $10
FROM execution_plans p
WHERE p.organization_id = $2 AND p.id = $11 AND encode(p.plan_hash, 'hex') = $12
ON CONFLICT (organization_id, idempotency_key) DO NOTHING
RETURNING id, organization_id, execution_plan_id, plan_hash, plan_entry_node_id, plan_executor,
          encode(request_hash, 'hex'), input, idempotency_key, status, priority,
          deadline_at, cancel_requested_at, created_at, updated_at, version`

// CreateRun atomically enforces organization idempotency and seeds entry work.
//
// One attempt is the whole transaction; a deadlock or serialization failure rolls all of it back, so
// the retry reopens the transaction instead of continuing on the aborted one. Run creation carries
// the caller's idempotency key and its ledger entry, so repeating the attempt cannot create a second
// run: whichever attempt commits is the run, and the rest replay it.
func (s *RuntimeStore) CreateRun(ctx context.Context, request workflowruntime.CreateRunRequest) (workflowruntime.CreateRunResult, error) {
	if err := s.repository.requirePool(); err != nil {
		return workflowruntime.CreateRunResult{}, err
	}
	var result workflowruntime.CreateRunResult
	err := retryOnTransient(ctx, func(ctx context.Context) error {
		attempt, attemptErr := s.createRunTx(ctx, request)
		if attemptErr == nil {
			result = attempt
		}
		return attemptErr
	})
	if err != nil {
		return workflowruntime.CreateRunResult{}, err
	}
	return result, nil
}

// createRunTx is one complete attempt of the run-creation transaction. It returns classified errors
// so the retry wrapper only sees transient ones, and so nothing that already concluded something
// about the request is retried on the strength of a wrapped cause.
func (s *RuntimeStore) createRunTx(ctx context.Context, request workflowruntime.CreateRunRequest) (workflowruntime.CreateRunResult, error) {
	tx, err := beginOrganizationRuntimeTx(ctx, s.repository.pool, string(request.Run.OrganizationID))
	if err != nil {
		return workflowruntime.CreateRunResult{}, runtimePersistenceError("could not start run", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err = LockAPIMutationTx(ctx, tx); err != nil {
		if errors.Is(err, ErrIdempotencyConflict) {
			return workflowruntime.CreateRunResult{}, runtimeError(workflowruntime.CodeIdempotencyConflict, "idempotency key was used for a different request", err)
		}
		return workflowruntime.CreateRunResult{}, runtimePersistenceError("could not lock run operation", err)
	}
	result, err := createRuntimeRunTx(ctx, tx, request.Run)
	if err != nil {
		return workflowruntime.CreateRunResult{}, err
	}
	if err = CompleteAPIMutationTx(ctx, tx, result.Run.ID, map[string]any{"runId": result.Run.ID}); err != nil {
		if errors.Is(err, ErrIdempotencyConflict) {
			// 幂等键被另一个 body 用过：这是语义结论，重试不会改变它，也仍然报 conflict。
			return workflowruntime.CreateRunResult{}, runtimePersistenceError("could not complete run operation", err)
		}
		return workflowruntime.CreateRunResult{}, runtimePersistenceError("could not complete run operation", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return workflowruntime.CreateRunResult{}, runtimePersistenceError("could not commit run", workflowruntime.MarkCommitOutcomeUnknown(err))
	}
	apimutation.MarkCommitted(ctx)
	return result, nil
}

func createRuntimeRunTx(ctx context.Context, tx pgx.Tx, run workflowruntime.Run) (workflowruntime.CreateRunResult, error) {
	stored, err := scanRuntimeRun(tx.QueryRow(ctx, createRuntimeRunSQL,
		run.ID, string(run.OrganizationID), run.IdempotencyKey, decodeHash(run.RequestFingerprint),
		run.Input, run.Plan.EntryNodeID, run.Plan.Executor, dbRunStatus(run.Status),
		run.CreatedAt, run.Version, run.Plan.ID, run.Plan.Hash, run.Priority, run.DeadlineAt,
	))
	created := err == nil
	if errors.Is(err, pgx.ErrNoRows) {
		stored, err = scanRuntimeRunProjection(tx.QueryRow(ctx, runtimeRunProjectionByIdempotencySQL,
			string(run.OrganizationID), run.IdempotencyKey))
		if err == nil && subtle.ConstantTimeCompare(
			[]byte(stored.RequestFingerprint), []byte(run.RequestFingerprint),
		) != 1 {
			return workflowruntime.CreateRunResult{}, runtimeError(
				workflowruntime.CodeIdempotencyConflict,
				"idempotency key was used for a different request", ErrIdempotencyConflict,
			)
		}
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return workflowruntime.CreateRunResult{}, runtimeError(workflowruntime.CodeNotFound, "execution plan not found", err)
	}
	if err != nil {
		return workflowruntime.CreateRunResult{}, runtimePersistenceError("could not persist run", err)
	}
	if created {
		admission, admissionErr := admitRunTx(ctx, tx, run)
		if admissionErr != nil {
			var runtimeAdmissionErr *workflowruntime.Error
			if errors.As(admissionErr, &runtimeAdmissionErr) {
				return workflowruntime.CreateRunResult{}, admissionErr
			}
			return workflowruntime.CreateRunResult{}, runtimePersistenceError("could not admit run", admissionErr)
		}
		if _, err = appendEventTx(ctx, tx, string(run.OrganizationID), run.ID, "run.created", json.RawMessage(`{}`)); err != nil {
			return workflowruntime.CreateRunResult{}, runtimePersistenceError("could not persist run event", err)
		}
		if err = insertOutboxTx(ctx, tx, string(run.OrganizationID), "run", run.ID, "run.created", json.RawMessage(`{}`)); err != nil {
			return workflowruntime.CreateRunResult{}, runtimePersistenceError("could not persist run delivery", err)
		}
		admissionPayload, _ := json.Marshal(map[string]any{"priority": run.Priority, "status": admission.status})
		admissionEvent := runtimeAdmissionEvent(admission.status)
		if _, err = appendEventTx(ctx, tx, string(run.OrganizationID), run.ID, admissionEvent, admissionPayload); err != nil {
			return workflowruntime.CreateRunResult{}, runtimePersistenceError("could not persist admission event", err)
		}
		if err = insertOutboxTx(ctx, tx, string(run.OrganizationID), "run", run.ID, admissionEvent, admissionPayload); err != nil {
			return workflowruntime.CreateRunResult{}, runtimePersistenceError("could not persist admission delivery", err)
		}
		if run.Plan.EntryNodeID != "" {
			if err = seedRuntimeAttempt(ctx, tx, run); err != nil {
				return workflowruntime.CreateRunResult{}, runtimePersistenceError("could not seed entry attempt", err)
			}
		}
	}
	return workflowruntime.CreateRunResult{Run: stored, Created: created}, nil
}

func runtimeAdmissionEvent(status string) string {
	if status == "queued" {
		return "run.queued"
	}
	return "run.admitted"
}

const runtimeRunColumns = `
id, organization_id, execution_plan_id, plan_hash, plan_entry_node_id, plan_executor,
encode(request_hash, 'hex'), input, idempotency_key, status,
priority, deadline_at, cancel_requested_at, created_at, updated_at, version`

const runtimeRunByIDSQL = `SELECT ` + runtimeRunColumns + ` FROM runs
WHERE organization_id = $1 AND id = $2`

const runtimeRunProjectionColumns = `
r.id, r.organization_id, r.execution_plan_id, r.plan_hash, r.plan_entry_node_id, r.plan_executor,
encode(r.request_hash, 'hex'), r.input, r.idempotency_key, r.status,
r.priority, r.deadline_at, r.cancel_requested_at, r.created_at, r.updated_at, r.version,
o.id, o.inline_value, o.content_type, o.size_bytes, encode(o.content_hash, 'hex'),
o.sensitivity, (o.object_ref IS NOT NULL), o.retention_status,
l.source_run_id, l.from_node_id, l.created_at`

const runtimeRunProjectionByIdempotencySQL = `SELECT ` + runtimeRunProjectionColumns + `
FROM runs r LEFT JOIN node_outputs o ON o.organization_id = r.organization_id AND o.id = r.output_id
LEFT JOIN run_rerun_lineage l ON l.organization_id = r.organization_id AND l.run_id = r.id
WHERE r.organization_id = $1 AND r.idempotency_key = $2`

const runtimeRunProjectionByIDSQL = `SELECT ` + runtimeRunProjectionColumns + `
FROM runs r LEFT JOIN node_outputs o ON o.organization_id = r.organization_id AND o.id = r.output_id
LEFT JOIN run_rerun_lineage l ON l.organization_id = r.organization_id AND l.run_id = r.id
WHERE r.organization_id = $1 AND r.id = $2`

func scanRuntimeRun(row rowScanner) (workflowruntime.Run, error) {
	var run workflowruntime.Run
	var organizationID, status string
	err := row.Scan(&run.ID, &organizationID, &run.Plan.ID, &run.Plan.Hash, &run.Plan.EntryNodeID,
		&run.Plan.Executor, &run.RequestFingerprint, &run.Input, &run.IdempotencyKey,
		&status, &run.Priority, &run.DeadlineAt, &run.CancelRequestedAt, &run.CreatedAt, &run.UpdatedAt, &run.Version)
	run.OrganizationID = workflowruntime.OrganizationID(organizationID)
	run.Status = runtimeRunStatus(status)
	return run, err
}

func scanRuntimeRunProjection(row rowScanner) (workflowruntime.Run, error) {
	var run workflowruntime.Run
	var organizationID, status string
	var outputID, contentType, outputHash, sensitivity, retentionStatus *string
	var outputSize *int64
	var inline json.RawMessage
	var external *bool
	var sourceRunID, fromNodeID *string
	var rerunCreatedAt *time.Time
	err := row.Scan(&run.ID, &organizationID, &run.Plan.ID, &run.Plan.Hash, &run.Plan.EntryNodeID,
		&run.Plan.Executor, &run.RequestFingerprint, &run.Input, &run.IdempotencyKey,
		&status, &run.Priority, &run.DeadlineAt, &run.CancelRequestedAt, &run.CreatedAt, &run.UpdatedAt, &run.Version,
		&outputID, &inline, &contentType, &outputSize, &outputHash, &sensitivity, &external, &retentionStatus,
		&sourceRunID, &fromNodeID, &rerunCreatedAt)
	run.OrganizationID = workflowruntime.OrganizationID(organizationID)
	run.Status = runtimeRunStatus(status)
	if err == nil && outputID != nil {
		run.Output = &workflowruntime.RunOutput{
			Inline: inline, ContentType: *contentType, Size: *outputSize, RetentionStatus: *retentionStatus,
			SHA256: *outputHash, Sensitive: *sensitivity == "sensitive", External: *external,
		}
	}
	if err == nil && sourceRunID != nil {
		run.Rerun = &workflowruntime.RerunLineage{SourceRunID: *sourceRunID, CreatedAt: *rerunCreatedAt}
		if fromNodeID != nil {
			run.Rerun.FromNodeID = *fromNodeID
		}
	}
	return run, err
}

func seedRuntimeAttempt(ctx context.Context, tx pgx.Tx, run workflowruntime.Run) error {
	nodeStateID, err := NewID("node")
	if err != nil {
		return err
	}
	attemptID, err := NewID("att")
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO node_states (
		    id, organization_id, run_id, node_id, status, attempt_count, latest_attempt_id,
		    created_at, updated_at
		) VALUES ($1, $2, $3, $4, 'pending', 1, $5, $6, $6)`,
		nodeStateID, run.OrganizationID, run.ID, run.Plan.EntryNodeID, attemptID, run.CreatedAt,
	); err != nil {
		return fmt.Errorf("insert entry node state: %w", err)
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO node_attempts (
		    id, execution_id, organization_id, run_id, node_state_id, node_id, executor, input,
		    attempt_number, status, available_at, created_at, updated_at
		) VALUES ($1, $1, $2, $3, $4, $5, $6, $7, 1, 'pending', $8, $8, $8)`,
		attemptID, run.OrganizationID, run.ID, nodeStateID, run.Plan.EntryNodeID,
		run.Plan.Executor, run.Input, run.CreatedAt,
	); err != nil {
		return fmt.Errorf("insert entry node attempt: %w", err)
	}
	return nil
}

// GetRun returns a run only within organizationID.
func (s *RuntimeStore) GetRun(ctx context.Context, organizationID workflowruntime.OrganizationID, runID string) (workflowruntime.Run, error) {
	if err := s.repository.requirePool(); err != nil {
		return workflowruntime.Run{}, err
	}
	run, err := scanRuntimeRunProjection(s.repository.pool.QueryRow(ctx, runtimeRunProjectionByIDSQL, string(organizationID), runID))
	if errors.Is(err, pgx.ErrNoRows) {
		return workflowruntime.Run{}, runtimeError(workflowruntime.CodeNotFound, "run not found", err)
	}
	if err != nil {
		return workflowruntime.Run{}, runtimePersistenceError("could not read run", err)
	}
	return run, nil
}

// ListEvents returns organization-scoped events strictly after afterCursor.
func (s *RuntimeStore) ListEvents(ctx context.Context, organizationID workflowruntime.OrganizationID, runID string, afterCursor uint64, limit int) ([]workflowruntime.Event, error) {
	events, err := s.repository.ListEvents(ctx, string(organizationID), runID, int64(afterCursor), limit)
	if err != nil {
		return nil, runtimePersistenceError("could not list run events", err)
	}
	result := make([]workflowruntime.Event, 0, len(events))
	for _, event := range events {
		result = append(result, workflowruntime.Event{
			OrganizationID: organizationID, RunID: event.RunID, Cursor: uint64(event.Sequence),
			Type: event.Type, Data: event.Payload, At: event.CreatedAt,
		})
	}
	return result, nil
}

// ListInterrupts returns unresolved interrupts for a organization-scoped run.
func (s *RuntimeStore) ListInterrupts(ctx context.Context, organizationID workflowruntime.OrganizationID, runID string) ([]workflowruntime.Interrupt, error) {
	rows, err := s.repository.pool.Query(ctx, `
		SELECT i.id, i.organization_id, i.run_id, a.node_id, i.attempt_id, i.kind, i.status,
		       i.resume_token_hash, COALESCE(i.expires_at, 'infinity'::timestamptz),
		       i.resolved_at, COALESCE(d.decision_data, 'null'::jsonb), i.created_at
		FROM interrupts i
		JOIN node_attempts a ON a.organization_id = i.organization_id AND a.id = i.attempt_id
		LEFT JOIN approval_decisions d ON d.organization_id = i.organization_id AND d.interrupt_id = i.id
		WHERE i.organization_id = $1 AND i.run_id = $2 AND i.status = 'pending'
		ORDER BY i.created_at, i.id`, string(organizationID), runID)
	if err != nil {
		return nil, runtimePersistenceError("could not list interrupts", err)
	}
	defer rows.Close()
	result := make([]workflowruntime.Interrupt, 0)
	for rows.Next() {
		var item workflowruntime.Interrupt
		var storedOrganization, status string
		var digest []byte
		if err := rows.Scan(&item.ID, &storedOrganization, &item.RunID, &item.NodeID,
			&item.AttemptID, &item.Kind, &status, &digest, &item.ExpiresAt, &item.ResolvedAt,
			&item.Decision, &item.CreatedAt); err != nil {
			return nil, runtimePersistenceError("could not decode interrupt", err)
		}
		item.OrganizationID = workflowruntime.OrganizationID(storedOrganization)
		item.Status = workflowruntime.InterruptStatus(strings.ToUpper(status))
		copy(item.TokenDigest[:], digest)
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, runtimePersistenceError("could not iterate interrupts", err)
	}
	return result, nil
}

func decodeHash(value string) []byte {
	decoded, err := hex.DecodeString(value)
	if err == nil {
		return decoded
	}
	return []byte(value)
}

func timeOrNow(value time.Time) time.Time {
	if value.IsZero() {
		return time.Now().UTC()
	}
	return value
}
