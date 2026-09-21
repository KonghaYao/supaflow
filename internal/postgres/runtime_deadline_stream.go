package postgres

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

const (
	maxRuntimeStreamChunkBytes = 64 << 10
	maxRuntimeStreamBytes      = 1 << 20
	maxRuntimeStreamChunks     = 4096
)

// ExpireRuns terminalizes a bounded due batch and invalidates every active
// fencing lease in the same transaction.
func (s *RuntimeStore) ExpireRuns(ctx context.Context, organizationID workflowruntime.OrganizationID, now time.Time, limit int) (int, error) {
	if err := s.repository.requirePool(); err != nil {
		return 0, err
	}
	if limit < 1 || limit > 1000 {
		return 0, runtimeError(workflowruntime.CodeInvalidArgument, "expiry limit must be between 1 and 1000", nil)
	}
	now = timeOrNow(now)
	tx, err := beginOrganizationRuntimeTx(ctx, s.repository.pool, string(organizationID))
	if err != nil {
		return 0, runtimePersistenceError("could not start run expiry", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return 0, runtimePersistenceError("could not read deadline clock", err)
	}
	rows, err := tx.Query(ctx, `SELECT id FROM runs
		WHERE organization_id=$1 AND deadline_at IS NOT NULL AND deadline_at <= $2
		  AND status IN ('pending','running','suspended','cancelling')
		ORDER BY deadline_at,id FOR UPDATE SKIP LOCKED LIMIT $3`, organizationID, now, limit)
	if err != nil {
		return 0, runtimePersistenceError("could not claim expired runs", err)
	}
	runIDs := make([]string, 0, limit)
	for rows.Next() {
		var runID string
		if err = rows.Scan(&runID); err != nil {
			rows.Close()
			return 0, runtimePersistenceError("could not decode expired run", err)
		}
		runIDs = append(runIDs, runID)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return 0, runtimePersistenceError("could not iterate expired runs", err)
	}
	payload := json.RawMessage(`{"code":"run_timeout"}`)
	for _, runID := range runIDs {
		if err = expireRunTx(ctx, tx, organizationID, runID, now, payload); err != nil {
			return 0, err
		}
	}
	expired := len(runIDs)
	remaining := limit - expired
	if remaining > 0 {
		approvalCount, approvalErr := expireApprovalInterruptsTx(ctx, tx, organizationID, now, remaining)
		if approvalErr != nil {
			return 0, approvalErr
		}
		expired += approvalCount
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, runtimePersistenceError("could not commit run expiry", err)
	}
	return expired, nil
}

type expiredApproval struct {
	id          string
	runID       string
	attemptID   string
	nodeStateID string
	nodeID      string
}

// expireApprovalInterruptsTx gives approval expiry the same bounded, durable
// finalization semantics as a run deadline. The locked interrupt determines
// the one node reported as timed out; all sibling work is cancelled.
func expireApprovalInterruptsTx(ctx context.Context, tx pgx.Tx, organizationID workflowruntime.OrganizationID, now time.Time, limit int) (int, error) {
	rows, err := tx.Query(ctx, `SELECT i.id,i.run_id,i.attempt_id,i.node_state_id,a.node_id
		FROM interrupts i
		JOIN node_attempts a ON a.organization_id=i.organization_id AND a.id=i.attempt_id
		JOIN runs r ON r.organization_id=i.organization_id AND r.id=i.run_id
		WHERE i.organization_id=$1 AND i.kind IN ('approval','human_approval','webhook')
		  AND i.status='pending' AND i.expires_at <= $2
		  AND r.status IN ('pending','running','suspended','cancelling')
		ORDER BY i.expires_at,i.id FOR UPDATE OF i,r SKIP LOCKED LIMIT $3`, organizationID, now, limit)
	if err != nil {
		return 0, runtimePersistenceError("could not claim expired approvals", err)
	}
	defer rows.Close()
	approvals := make([]expiredApproval, 0, limit)
	for rows.Next() {
		var approval expiredApproval
		if err = rows.Scan(&approval.id, &approval.runID, &approval.attemptID, &approval.nodeStateID, &approval.nodeID); err != nil {
			return 0, runtimePersistenceError("could not decode expired approval", err)
		}
		approvals = append(approvals, approval)
	}
	if err = rows.Err(); err != nil {
		return 0, runtimePersistenceError("could not iterate expired approvals", err)
	}
	expired := 0
	for _, approval := range approvals {
		changed, finalizeErr := expireApprovalTx(ctx, tx, organizationID, approval, now)
		if finalizeErr != nil {
			return 0, finalizeErr
		}
		if changed {
			expired++
		}
	}
	return expired, nil
}

func expireApprovalTx(ctx context.Context, tx pgx.Tx, organizationID workflowruntime.OrganizationID, approval expiredApproval, now time.Time) (bool, error) {
	payload, _ := json.Marshal(map[string]string{"code": "node_timeout", "nodeId": approval.nodeID})
	tag, err := tx.Exec(ctx, `UPDATE runs SET status='error', finished_at=$3, updated_at=$3, version=version+1
		WHERE organization_id=$1 AND id=$2 AND status IN ('pending','running','suspended','cancelling')`, organizationID, approval.runID, now)
	if err != nil {
		return false, runtimePersistenceError("could not finalize approval run", err)
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}
	if _, err = tx.Exec(ctx, `UPDATE node_attempts SET
		status=CASE WHEN id=$3 THEN 'error' ELSE 'cancelled' END,
		error=CASE WHEN id=$3 THEN $5::jsonb ELSE error END,
		finished_at=$4, updated_at=$4
		WHERE organization_id=$1 AND run_id=$2 AND status IN ('pending','retry_ready','running','suspended')`,
		organizationID, approval.runID, approval.attemptID, now, payload); err != nil {
		return false, runtimePersistenceError("could not finalize approval attempts", err)
	}
	if _, err = tx.Exec(ctx, `UPDATE node_states SET
		status=CASE WHEN id=$3 THEN 'error' ELSE 'cancelled' END,
		last_error=CASE WHEN id=$3 THEN $5::jsonb ELSE last_error END,
		updated_at=$4
		WHERE organization_id=$1 AND run_id=$2 AND status IN ('pending','ready','running','suspended')`,
		organizationID, approval.runID, approval.nodeStateID, now, payload); err != nil {
		return false, runtimePersistenceError("could not finalize approval nodes", err)
	}
	if _, err = tx.Exec(ctx, `UPDATE interrupts SET
		status=CASE WHEN id=$3 THEN 'expired' ELSE 'cancelled' END, resolved_at=$4
		WHERE organization_id=$1 AND run_id=$2 AND status='pending'`, organizationID, approval.runID, approval.id, now); err != nil {
		return false, runtimePersistenceError("could not close expired approval interrupts", err)
	}
	if _, err = tx.Exec(ctx, `DELETE FROM worker_leases WHERE organization_id=$1 AND run_id=$2`, organizationID, approval.runID); err != nil {
		return false, runtimePersistenceError("could not invalidate approval leases", err)
	}
	if _, err = releaseRunAdmissionTx(ctx, tx, organizationID, approval.runID, now); err != nil {
		return false, runtimePersistenceError("could not release expired approval capacity", err)
	}
	if err = cancelSubworkflowDescendantsWithReasonTx(ctx, tx, organizationID, approval.runID, now, "parent_terminal"); err != nil {
		return false, err
	}
	if _, err = appendRuntimeEvents(ctx, tx, organizationID, approval.runID, []workflowruntime.Event{
		{Type: "node.timed_out", Data: payload}, {Type: "run.error", Data: payload},
	}); err != nil {
		return false, runtimePersistenceError("could not persist approval timeout events", err)
	}
	return true, nil
}

// ExpireRun terminalizes exactly one organization-scoped run when its deadline is due.
func (s *RuntimeStore) ExpireRun(ctx context.Context, organizationID workflowruntime.OrganizationID, runID string, now time.Time) (bool, error) {
	if err := s.repository.requirePool(); err != nil {
		return false, err
	}
	if organizationID == "" || runID == "" {
		return false, runtimeError(workflowruntime.CodeInvalidArgument, "organization and run are required", nil)
	}
	now = timeOrNow(now)
	tx, err := beginOrganizationRuntimeTx(ctx, s.repository.pool, string(organizationID))
	if err != nil {
		return false, runtimePersistenceError("could not start targeted run expiry", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return false, runtimePersistenceError("could not read deadline clock", err)
	}
	var found string
	err = tx.QueryRow(ctx, `SELECT id FROM runs
		WHERE organization_id=$1 AND id=$2 AND deadline_at IS NOT NULL AND deadline_at <= $3
		  AND status IN ('pending','running','suspended','cancelling')
		FOR UPDATE`, organizationID, runID, now).Scan(&found)
	if errors.Is(err, pgx.ErrNoRows) {
		var exists bool
		if lookupErr := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM runs WHERE organization_id=$1 AND id=$2)`, organizationID, runID).Scan(&exists); lookupErr != nil {
			return false, runtimePersistenceError("could not inspect targeted run expiry", lookupErr)
		}
		if !exists {
			return false, runtimeError(workflowruntime.CodeNotFound, "run not found", nil)
		}
		if err = tx.Commit(ctx); err != nil {
			return false, runtimePersistenceError("could not commit targeted run expiry", err)
		}
		return false, nil
	}
	if err != nil {
		return false, runtimePersistenceError("could not lock targeted run expiry", err)
	}
	payload := json.RawMessage(`{"code":"run_timeout"}`)
	if err = expireRunTx(ctx, tx, organizationID, found, now, payload); err != nil {
		return false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return false, runtimePersistenceError("could not commit targeted run expiry", err)
	}
	return true, nil
}

func expireRunTx(ctx context.Context, tx pgx.Tx, organizationID workflowruntime.OrganizationID, runID string, now time.Time, payload json.RawMessage) error {
	descendants, err := descendantRunIDsTx(ctx, tx, organizationID, runID)
	if err != nil {
		return runtimePersistenceError("could not lock subworkflow descendants", err)
	}
	if err = expireSingleRunTx(ctx, tx, organizationID, runID, now, payload); err != nil {
		return err
	}
	for _, childRunID := range descendants {
		if err = expireSingleRunTx(ctx, tx, organizationID, childRunID, now, payload); err != nil {
			return err
		}
	}
	return nil
}

func expireSingleRunTx(ctx context.Context, tx pgx.Tx, organizationID workflowruntime.OrganizationID, runID string, now time.Time, payload json.RawMessage) error {
	tag, err := tx.Exec(ctx, `UPDATE runs SET status='error', finished_at=$3, updated_at=$3, version=version+1
		WHERE organization_id=$1 AND id=$2 AND status IN ('pending','running','suspended','cancelling')`, organizationID, runID, now)
	if err != nil {
		return runtimePersistenceError("could not timeout run", err)
	}
	if tag.RowsAffected() == 0 {
		return nil
	}
	if _, err := tx.Exec(ctx, `UPDATE node_attempts
		SET status='timed_out', error=$4, finished_at=$3, updated_at=$3
		WHERE organization_id=$1 AND run_id=$2 AND status IN ('pending','retry_ready','running','suspended')`, organizationID, runID, now, payload); err != nil {
		return runtimePersistenceError("could not timeout attempts", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE node_states SET status='error', last_error=$4, updated_at=$3
		WHERE organization_id=$1 AND run_id=$2 AND status IN ('pending','ready','running','suspended')`, organizationID, runID, now, payload); err != nil {
		return runtimePersistenceError("could not timeout nodes", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE interrupts SET status='cancelled', resolved_at=$3
		WHERE organization_id=$1 AND run_id=$2 AND status='pending'`, organizationID, runID, now); err != nil {
		return runtimePersistenceError("could not close timed out interrupts", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM worker_leases WHERE organization_id=$1 AND run_id=$2`, organizationID, runID); err != nil {
		return runtimePersistenceError("could not invalidate timed out leases", err)
	}
	if _, err := releaseRunAdmissionTx(ctx, tx, organizationID, runID, now); err != nil {
		return runtimePersistenceError("could not release timed out run capacity", err)
	}
	if _, err := appendRuntimeEvents(ctx, tx, organizationID, runID, []workflowruntime.Event{
		{Type: "run.timed_out", Data: payload}, {Type: "run.error", Data: payload},
	}); err != nil {
		return runtimePersistenceError("could not persist timeout events", err)
	}
	return nil
}

// AppendStreamChunk validates fencing and atomically writes its event/outbox.
func (s *RuntimeStore) AppendStreamChunk(ctx context.Context, request workflowruntime.AppendStreamChunkRequest) (workflowruntime.Event, error) {
	if err := s.repository.requirePool(); err != nil {
		return workflowruntime.Event{}, err
	}
	if request.Sequence < 1 || request.Sequence > maxRuntimeStreamChunks || len(request.Chunk) == 0 || len(request.Chunk) > maxRuntimeStreamChunkBytes || !json.Valid(request.Chunk) {
		return workflowruntime.Event{}, runtimeError(workflowruntime.CodeInvalidArgument, "stream chunk must be valid JSON and no larger than 64 KiB", nil)
	}
	request.Now = timeOrNow(request.Now)
	tx, err := beginOrganizationRuntimeTx(ctx, s.repository.pool, string(request.OrganizationID))
	if err != nil {
		return workflowruntime.Event{}, runtimePersistenceError("could not start stream commit", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	_, nodeID, err := validateRuntimeLease(ctx, tx, request.OrganizationID, request.RunID, request.AttemptID, request.WorkerID, request.FencingToken, request.Now)
	if err != nil {
		return workflowruntime.Event{}, err
	}
	digest := sha256.Sum256(request.Chunk)
	if existing, found, lookupErr := findStreamChunk(ctx, tx, request, digest); lookupErr != nil || found {
		return existing, lookupErr
	}
	var chunkCount int
	err = tx.QueryRow(ctx, `INSERT INTO attempt_stream_state
		(organization_id,attempt_id,run_id,chunk_count,total_bytes,updated_at)
		SELECT $1,$2,$3,1,$4,$5 WHERE $6=1
		ON CONFLICT (organization_id,attempt_id) DO UPDATE SET
		 chunk_count=attempt_stream_state.chunk_count+1,
		 total_bytes=attempt_stream_state.total_bytes+EXCLUDED.total_bytes,
		 updated_at=EXCLUDED.updated_at
		WHERE attempt_stream_state.run_id=EXCLUDED.run_id
		  AND attempt_stream_state.chunk_count+1 = $6
		  AND attempt_stream_state.chunk_count < $7
		  AND attempt_stream_state.total_bytes+EXCLUDED.total_bytes <= $8
		RETURNING chunk_count`, request.OrganizationID, request.AttemptID, request.RunID,
		len(request.Chunk), request.Now, request.Sequence, maxRuntimeStreamChunks, maxRuntimeStreamBytes).Scan(&chunkCount)
	if errors.Is(err, pgx.ErrNoRows) {
		if existing, found, lookupErr := findStreamChunk(ctx, tx, request, digest); lookupErr != nil || found {
			return existing, lookupErr
		}
		return workflowruntime.Event{}, runtimeError(workflowruntime.CodeConflict, "stream sequence is not contiguous or attempt stream limit was exceeded", nil)
	}
	if err != nil {
		return workflowruntime.Event{}, runtimePersistenceError("could not reserve stream chunk", err)
	}
	payload, err := json.Marshal(map[string]any{
		"attemptId": request.AttemptID, "nodeId": nodeID, "sequence": request.Sequence,
		"chunk": json.RawMessage(request.Chunk),
	})
	if err != nil {
		return workflowruntime.Event{}, runtimePersistenceError("could not encode stream chunk", err)
	}
	event, err := appendEventTx(ctx, tx, string(request.OrganizationID), request.RunID, "stream.chunk", payload)
	if err != nil {
		return workflowruntime.Event{}, runtimePersistenceError("could not persist stream chunk", err)
	}
	if err = insertOutboxTx(ctx, tx, string(request.OrganizationID), "run", request.RunID, "stream.chunk", payload); err != nil {
		return workflowruntime.Event{}, runtimePersistenceError("could not enqueue stream chunk", err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO attempt_stream_chunks
		(organization_id,attempt_id,run_id,chunk_sequence,content_hash,event_sequence,created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`, request.OrganizationID, request.AttemptID, request.RunID,
		request.Sequence, digest[:], event.Sequence, request.Now); err != nil {
		return workflowruntime.Event{}, runtimePersistenceError("could not record stream idempotency", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return workflowruntime.Event{}, runtimePersistenceError("could not commit stream chunk", err)
	}
	return workflowruntime.Event{OrganizationID: request.OrganizationID, RunID: request.RunID, Cursor: uint64(event.Sequence), Type: event.Type, Data: event.Payload, At: event.CreatedAt}, nil
}

func findStreamChunk(ctx context.Context, tx pgx.Tx, request workflowruntime.AppendStreamChunkRequest, digest [32]byte) (workflowruntime.Event, bool, error) {
	var event Event
	var storedDigest []byte
	err := tx.QueryRow(ctx, `SELECT e.id,e.organization_id,e.run_id,e.sequence,e.event_type,e.payload,e.created_at,c.content_hash
		FROM attempt_stream_chunks c JOIN run_events e
		  ON e.organization_id=c.organization_id AND e.run_id=c.run_id AND e.sequence=c.event_sequence
		WHERE c.organization_id=$1 AND c.attempt_id=$2 AND c.run_id=$3 AND c.chunk_sequence=$4`,
		request.OrganizationID, request.AttemptID, request.RunID, request.Sequence,
	).Scan(&event.ID, &event.OrganizationID, &event.RunID, &event.Sequence, &event.Type, &event.Payload, &event.CreatedAt, &storedDigest)
	if errors.Is(err, pgx.ErrNoRows) {
		return workflowruntime.Event{}, false, nil
	}
	if err != nil {
		return workflowruntime.Event{}, false, runtimePersistenceError("could not read stream idempotency", err)
	}
	if subtle.ConstantTimeCompare(storedDigest, digest[:]) != 1 {
		return workflowruntime.Event{}, false, runtimeError(workflowruntime.CodeConflict, "stream sequence was used for different content", nil)
	}
	return workflowruntime.Event{
		OrganizationID: workflowruntime.OrganizationID(event.OrganizationID), RunID: event.RunID, Cursor: uint64(event.Sequence),
		Type: event.Type, Data: event.Payload, At: event.CreatedAt,
	}, true, nil
}
