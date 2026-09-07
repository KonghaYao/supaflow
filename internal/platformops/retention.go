package platformops

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RetentionPolicy defines organization output/event horizons and archival behavior.
type RetentionPolicy struct {
	OrganizationID      string `json:"-"`
	EventDays           int    `json:"eventDays"`
	OutputDays          int    `json:"outputDays"`
	ArchiveBeforeDelete bool   `json:"archiveBeforeDelete"`
}

// OrganizationExport is a secret-free point-in-time organization data export.
type OrganizationExport struct {
	ID             string     `json:"id"`
	OrganizationID string     `json:"-"`
	Status         string     `json:"status"`
	ObjectID       string     `json:"objectId,omitempty"`
	ExpiresAt      *time.Time `json:"expiresAt,omitempty"`
	CreatedAt      time.Time  `json:"createdAt"`
	CompletedAt    *time.Time `json:"completedAt,omitempty"`
}

// RetentionSweep reports bounded policy enforcement work.
type RetentionSweep struct {
	EventsRemoved         int
	ObjectsArchived       int
	ObjectsDeleted        int
	InlineOutputsArchived int
	InlineOutputsDeleted  int
}

// Retention manages policy, archival, deletion, and organization export metadata.
type Retention struct {
	pool      *pgxpool.Pool
	objects   ObjectStore
	maxExport int64
}

// NewRetention creates a retention service with an export size bound.
func NewRetention(pool *pgxpool.Pool, objects ObjectStore, maxExportBytes int64) (*Retention, error) {
	if pool == nil || objects == nil || maxExportBytes <= 0 {
		return nil, ErrInvalidArgument
	}
	return &Retention{pool: pool, objects: objects, maxExport: maxExportBytes}, nil
}

// ArchiveExpired marks expired active objects archived under a row lock. The
// database clock is authoritative so a skewed control-plane replica cannot
// archive data before its retention horizon.
func (r *Retention) ArchiveExpired(ctx context.Context, organizationID string, limit int) (int, error) {
	if organizationID == "" || limit <= 0 || limit > 1000 {
		return 0, ErrInvalidArgument
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("begin managed object archive: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var now time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return 0, fmt.Errorf("read database clock for managed object archive: %w", err)
	}
	tag, err := tx.Exec(ctx, `
		WITH expired AS (
		    SELECT organization_id,id FROM managed_objects
		    WHERE organization_id=$1 AND status='active' AND retention_until <= $2
		    ORDER BY retention_until,id FOR UPDATE SKIP LOCKED LIMIT $3
		)
		UPDATE managed_objects o SET status='archived',archived_at=$2
		FROM expired WHERE o.organization_id=expired.organization_id AND o.id=expired.id`, organizationID, now, limit)
	if err != nil {
		return 0, fmt.Errorf("archive expired objects: %w", err)
	}
	count := int(tag.RowsAffected())
	if count > 0 {
		metadata, _ := json.Marshal(map[string]any{"count": count, "retentionAt": now})
		if _, err = insertAuditTx(ctx, tx, AuditRecord{OrganizationID: organizationID, ActorID: "system:retention",
			Action: "retention.objects.archive", ResourceType: "managed_object", ResourceID: "batch",
			Outcome: AuditAllowed, Metadata: metadata}); err != nil {
			return 0, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit managed object archive: %w", err)
	}
	return count, nil
}

// DeleteArchived idempotently removes archived bytes after the requested grace
// period, measured from the database clock.
func (r *Retention) DeleteArchived(ctx context.Context, organizationID string, grace time.Duration, limit int) (int, error) {
	if organizationID == "" || grace < 0 || limit <= 0 || limit > 1000 {
		return 0, ErrInvalidArgument
	}
	rows, err := r.pool.Query(ctx, `
		SELECT id,kind FROM managed_objects
		WHERE organization_id=$1 AND status='archived'
		  AND archived_at <= clock_timestamp() - make_interval(secs => $2)
		ORDER BY archived_at,id LIMIT $3`, organizationID, grace.Seconds(), limit)
	if err != nil {
		return 0, fmt.Errorf("select archived objects: %w", err)
	}
	type target struct{ id, kind string }
	targets := make([]target, 0, limit)
	for rows.Next() {
		var item target
		if err = rows.Scan(&item.id, &item.kind); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan archived object: %w", err)
		}
		targets = append(targets, item)
	}
	rows.Close()
	deleted := 0
	for _, item := range targets {
		audit := AuditRecord{OrganizationID: organizationID, ActorID: "system:retention",
			Action: "retention.objects.delete", ResourceType: "managed_object", ResourceID: item.id,
			Outcome: AuditAllowed}
		var claimed bool
		if claimed, err = deleteManagedObject(ctx, r.pool, r.objects, organizationID, item.id, item.kind, &audit); err != nil {
			return deleted, err
		}
		if claimed {
			deleted++
		}
	}
	return deleted, nil
}

// SweepOrganization enforces one organization policy without unbounded scans.
func (r *Retention) SweepOrganization(ctx context.Context, organizationID string, now time.Time, limit int) (RetentionSweep, error) {
	if organizationID == "" || now.IsZero() || limit <= 0 || limit > 1000 {
		return RetentionSweep{}, ErrInvalidArgument
	}
	result := RetentionSweep{}
	var err error
	result.EventsRemoved, err = r.sweepEventsAndBackfill(ctx, organizationID, limit)
	if err != nil {
		return result, err
	}
	result.ObjectsArchived, err = r.ArchiveExpired(ctx, organizationID, limit)
	if err != nil {
		return result, err
	}
	result.InlineOutputsArchived, err = r.ArchiveNodeOutputs(ctx, organizationID, "system:retention", limit)
	if err != nil {
		return result, err
	}
	result.ObjectsDeleted, err = r.DeleteArchived(ctx, organizationID, 24*time.Hour, limit)
	if err != nil {
		return result, err
	}
	result.InlineOutputsDeleted, err = r.DeleteArchivedNodeOutputs(ctx, organizationID, "system:retention", 24*time.Hour, limit)
	return result, err
}

// ArchiveEvents writes a redacted immutable archive before deleting bounded terminal-run events.
func (r *Retention) ArchiveEvents(ctx context.Context, organizationID, actorID string, before time.Time, limit int) (ManagedObject, int, error) {
	if organizationID == "" || actorID == "" || before.IsZero() || limit <= 0 || limit > 10000 {
		return ManagedObject{}, 0, ErrInvalidArgument
	}
	objectID, err := newID("obj")
	if err != nil {
		return ManagedObject{}, 0, err
	}
	upload, err := beginObjectUpload(ctx, r.pool, r.objects, objectUploadSpec{
		organizationID: organizationID, id: objectID, kind: "archive", key: objectID, contentType: "application/json",
	})
	if err != nil {
		return ManagedObject{}, 0, err
	}
	committed := false
	defer func() {
		if committed {
			_ = upload.closeHeartbeat()
		} else {
			_ = upload.abort()
		}
	}()
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ManagedObject{}, 0, fmt.Errorf("begin event archive: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	rows, err := tx.Query(ctx, `
		SELECT e.id,e.run_id,e.sequence,e.event_type,e.payload,e.created_at
		FROM run_events e JOIN runs r ON r.organization_id=e.organization_id AND r.id=e.run_id
		WHERE e.organization_id=$1 AND e.created_at < $2
		  AND r.status IN ('success','failed','error','cancelled')
		ORDER BY e.created_at,e.id FOR UPDATE OF e SKIP LOCKED LIMIT $3`, organizationID, before, limit)
	if err != nil {
		return ManagedObject{}, 0, fmt.Errorf("select event archive: %w", err)
	}
	archiveFile, err := os.CreateTemp("", "supaflow-event-archive-*.json")
	if err != nil {
		rows.Close()
		return ManagedObject{}, 0, fmt.Errorf("create bounded event archive spool: %w", err)
	}
	archivePath := archiveFile.Name()
	defer func() {
		_ = archiveFile.Close()
		_ = os.Remove(archivePath)
	}()
	bounded := &boundedJSONWriter{writer: archiveFile, remaining: r.maxExport}
	if _, err = bounded.Write([]byte(`{"schemaVersion":1,"events":[`)); err != nil {
		rows.Close()
		return ManagedObject{}, 0, err
	}
	ids := make([]string, 0, limit)
	for rows.Next() {
		var id, runID, eventType string
		var sequence int64
		var payload json.RawMessage
		var createdAt time.Time
		if err = rows.Scan(&id, &runID, &sequence, &eventType, &payload, &createdAt); err != nil {
			rows.Close()
			return ManagedObject{}, 0, fmt.Errorf("scan event archive: %w", err)
		}
		redacted, redactErr := redactJSON(payload)
		if redactErr != nil {
			rows.Close()
			return ManagedObject{}, 0, ErrInvalidArgument
		}
		record, encodeErr := json.Marshal(map[string]any{
			"id": id, "runId": runID, "sequence": sequence, "type": eventType,
			"payload": json.RawMessage(redacted), "createdAt": createdAt,
		})
		if encodeErr != nil {
			rows.Close()
			return ManagedObject{}, 0, fmt.Errorf("encode event archive row: %w", encodeErr)
		}
		if len(ids) > 0 {
			if _, err = bounded.Write([]byte{','}); err != nil {
				rows.Close()
				return ManagedObject{}, 0, err
			}
		}
		if _, err = bounded.Write(record); err != nil {
			rows.Close()
			return ManagedObject{}, 0, err
		}
		ids = append(ids, id)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return ManagedObject{}, 0, fmt.Errorf("iterate event archive: %w", err)
	}
	rows.Close()
	if len(ids) == 0 {
		if err = tx.Commit(ctx); err != nil {
			return ManagedObject{}, 0, fmt.Errorf("commit empty event archive: %w", err)
		}
		return ManagedObject{}, 0, nil
	}
	if _, err = bounded.Write([]byte(`]}`)); err != nil {
		return ManagedObject{}, 0, err
	}
	if _, err = archiveFile.Seek(0, 0); err != nil {
		return ManagedObject{}, 0, fmt.Errorf("rewind event archive spool: %w", err)
	}
	stored, err := upload.put(ctx, archiveFile, r.maxExport)
	if err != nil {
		return ManagedObject{}, 0, err
	}
	if err = upload.publishTx(ctx, tx); err != nil {
		return ManagedObject{}, 0, err
	}
	digest, _ := hex.DecodeString(stored.SHA256)
	metadata := ManagedObject{OrganizationID: organizationID, ID: objectID, Kind: "archive", ObjectKey: stored.Key, ContentType: "application/json", Size: stored.Size, SHA256: stored.SHA256, Status: "archived"}
	err = tx.QueryRow(ctx, `
		INSERT INTO managed_objects (organization_id,id,kind,object_key,content_type,size_bytes,content_hash,status,archived_at)
		VALUES ($1,$2,'archive',$3,'application/json',$4,$5,'archived',now()) RETURNING created_at`,
		organizationID, objectID, stored.Key, stored.Size, digest).Scan(&metadata.CreatedAt)
	if err != nil {
		return ManagedObject{}, 0, fmt.Errorf("register event archive: %w", err)
	}
	tag, err := tx.Exec(ctx, `DELETE FROM run_events WHERE organization_id=$1 AND id=ANY($2)`, organizationID, ids)
	if err != nil {
		return ManagedObject{}, 0, fmt.Errorf("delete archived events: %w", err)
	}
	if _, err = insertAuditTx(ctx, tx, AuditRecord{
		OrganizationID: organizationID, ActorID: actorID, Action: "retention.events.archive",
		ResourceType: "managed_object", ResourceID: objectID, Outcome: AuditAllowed,
	}); err != nil {
		return ManagedObject{}, 0, err
	}
	if err = upload.completeTx(ctx, tx); err != nil {
		return ManagedObject{}, 0, err
	}
	if err = tx.Commit(ctx); err != nil {
		return ManagedObject{}, 0, fmt.Errorf("commit event archive: %w", err)
	}
	committed = true
	return metadata, int(tag.RowsAffected()), nil
}

// ExportOrganization creates a consistent JSON export without ciphertext, tokens, or secret values.
func (r *Retention) ExportOrganization(ctx context.Context, organizationID, actorID string, expiresAt time.Time) (OrganizationExport, error) {
	if organizationID == "" || actorID == "" || expiresAt.IsZero() {
		return OrganizationExport{}, ErrInvalidArgument
	}
	exportID, stableOperation := MutationResourceID(ctx, "export", "organization-export")
	if stableOperation {
		existing, found, err := r.findOrganizationExport(ctx, organizationID, exportID)
		if err != nil {
			return OrganizationExport{}, err
		}
		if found {
			replayTx, beginErr := r.pool.BeginTx(ctx, pgx.TxOptions{})
			if beginErr != nil {
				return OrganizationExport{}, fmt.Errorf("begin organization export replay: %w", beginErr)
			}
			defer func() { _ = replayTx.Rollback(context.WithoutCancel(ctx)) }()
			audited, auditErr := recordAllowedMutationTx(ctx, replayTx, exportID)
			if auditErr != nil {
				return OrganizationExport{}, fmt.Errorf("audit organization export replay: %w", auditErr)
			}
			if commitErr := replayTx.Commit(ctx); commitErr != nil {
				return OrganizationExport{}, fmt.Errorf("commit organization export replay: %w", commitErr)
			}
			if audited {
				markMutationCommitted(ctx)
			}
			return existing, nil
		}
	}
	if !stableOperation {
		generatedID, generateErr := newID("export")
		if generateErr != nil {
			return OrganizationExport{}, generateErr
		}
		exportID = generatedID
	}
	objectID, err := mutationResourceIDOrRandom(ctx, "obj", "organization-export-object")
	if err != nil {
		return OrganizationExport{}, err
	}
	upload, err := beginObjectUpload(ctx, r.pool, r.objects, objectUploadSpec{
		organizationID: organizationID, id: objectID, kind: "organization_export", key: exportID,
		contentType: "application/json", retentionUntil: &expiresAt,
	})
	if err != nil {
		return OrganizationExport{}, err
	}
	committed := false
	defer func() {
		if committed {
			_ = upload.closeHeartbeat()
		} else {
			_ = upload.abort()
		}
	}()
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return OrganizationExport{}, fmt.Errorf("begin organization export snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var exportedAt time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&exportedAt); err != nil {
		return OrganizationExport{}, fmt.Errorf("read database clock for organization export: %w", err)
	}
	exportFile, err := os.CreateTemp("", "supaflow-organization-export-*.json")
	if err != nil {
		return OrganizationExport{}, fmt.Errorf("create bounded organization export spool: %w", err)
	}
	exportPath := exportFile.Name()
	defer func() {
		_ = exportFile.Close()
		_ = os.Remove(exportPath)
	}()
	if err = streamOrganizationExport(ctx, tx, exportFile, r.maxExport, organizationID, exportedAt); err != nil {
		return OrganizationExport{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return OrganizationExport{}, fmt.Errorf("commit organization export snapshot: %w", err)
	}
	if _, err = exportFile.Seek(0, 0); err != nil {
		return OrganizationExport{}, fmt.Errorf("rewind organization export spool: %w", err)
	}
	stored, err := upload.put(ctx, exportFile, r.maxExport)
	if err != nil {
		return OrganizationExport{}, err
	}
	digest, _ := hex.DecodeString(stored.SHA256)
	writeTx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return OrganizationExport{}, err
	}
	defer func() { _ = writeTx.Rollback(context.WithoutCancel(ctx)) }()
	if err = upload.publishTx(ctx, writeTx); err != nil {
		return OrganizationExport{}, err
	}
	if _, err = writeTx.Exec(ctx, `
		INSERT INTO managed_objects (organization_id,id,kind,object_key,content_type,size_bytes,content_hash,retention_until)
		VALUES ($1,$2,'organization_export',$3,'application/json',$4,$5,$6)`, organizationID, objectID,
		stored.Key, stored.Size, digest, expiresAt); err != nil {
		return OrganizationExport{}, fmt.Errorf("register organization export object: %w", err)
	}
	result := OrganizationExport{ID: exportID, OrganizationID: organizationID, Status: "ready", ObjectID: objectID, ExpiresAt: &expiresAt}
	err = writeTx.QueryRow(ctx, `
		INSERT INTO organization_exports (organization_id,id,requested_by,status,object_id,expires_at,completed_at)
		VALUES ($1,$2,$3,'ready',$4,$5,now()) RETURNING created_at,completed_at`, organizationID,
		exportID, actorID, objectID, expiresAt).Scan(&result.CreatedAt, &result.CompletedAt)
	if err != nil {
		return OrganizationExport{}, fmt.Errorf("record organization export: %w", err)
	}
	audited, err := recordAllowedMutationTx(ctx, writeTx, exportID)
	if err != nil {
		return OrganizationExport{}, fmt.Errorf("audit organization export mutation: %w", err)
	}
	if !audited {
		if _, err = insertAuditTx(ctx, writeTx, AuditRecord{
			OrganizationID: organizationID, ActorID: actorID, Action: "organization.export",
			ResourceType: "organization_export", ResourceID: exportID, Outcome: AuditAllowed,
		}); err != nil {
			return OrganizationExport{}, err
		}
	}
	if err = upload.completeTx(ctx, writeTx); err != nil {
		return OrganizationExport{}, err
	}
	if err = writeTx.Commit(ctx); err != nil {
		return OrganizationExport{}, fmt.Errorf("commit organization export: %w", err)
	}
	if audited {
		markMutationCommitted(ctx)
	}
	committed = true
	return result, nil
}

func (r *Retention) findOrganizationExport(ctx context.Context, organizationID, exportID string) (OrganizationExport, bool, error) {
	var result OrganizationExport
	err := r.pool.QueryRow(ctx, `
		SELECT id,organization_id,status,object_id,created_at,completed_at,expires_at
		FROM organization_exports WHERE organization_id=$1 AND id=$2`, organizationID, exportID).Scan(
		&result.ID, &result.OrganizationID, &result.Status, &result.ObjectID,
		&result.CreatedAt, &result.CompletedAt, &result.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return OrganizationExport{}, false, nil
	}
	if err != nil {
		return OrganizationExport{}, false, fmt.Errorf("recover organization export: %w", err)
	}
	return result, true, nil
}
