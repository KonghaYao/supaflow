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

// RetentionPolicy defines tenant output/event horizons and archival behavior.
type RetentionPolicy struct {
	TenantID            string `json:"-"`
	EventDays           int    `json:"eventDays"`
	OutputDays          int    `json:"outputDays"`
	ArchiveBeforeDelete bool   `json:"archiveBeforeDelete"`
}

// TenantExport is a secret-free point-in-time tenant data export.
type TenantExport struct {
	ID          string     `json:"id"`
	TenantID    string     `json:"-"`
	Status      string     `json:"status"`
	ObjectID    string     `json:"objectId,omitempty"`
	ExpiresAt   *time.Time `json:"expiresAt,omitempty"`
	CreatedAt   time.Time  `json:"createdAt"`
	CompletedAt *time.Time `json:"completedAt,omitempty"`
}

// RetentionSweep reports bounded policy enforcement work.
type RetentionSweep struct {
	EventsRemoved         int
	ObjectsArchived       int
	ObjectsDeleted        int
	InlineOutputsArchived int
	InlineOutputsDeleted  int
}

// Retention manages policy, archival, deletion, and tenant export metadata.
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
func (r *Retention) ArchiveExpired(ctx context.Context, tenantID string, limit int) (int, error) {
	if tenantID == "" || limit <= 0 || limit > 1000 {
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
		    SELECT tenant_id,id FROM managed_objects
		    WHERE tenant_id=$1 AND status='active' AND retention_until <= $2
		    ORDER BY retention_until,id FOR UPDATE SKIP LOCKED LIMIT $3
		)
		UPDATE managed_objects o SET status='archived',archived_at=$2
		FROM expired WHERE o.tenant_id=expired.tenant_id AND o.id=expired.id`, tenantID, now, limit)
	if err != nil {
		return 0, fmt.Errorf("archive expired objects: %w", err)
	}
	count := int(tag.RowsAffected())
	if count > 0 {
		metadata, _ := json.Marshal(map[string]any{"count": count, "retentionAt": now})
		if _, err = insertAuditTx(ctx, tx, AuditRecord{TenantID: tenantID, ActorID: "system:retention",
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
func (r *Retention) DeleteArchived(ctx context.Context, tenantID string, grace time.Duration, limit int) (int, error) {
	if tenantID == "" || grace < 0 || limit <= 0 || limit > 1000 {
		return 0, ErrInvalidArgument
	}
	rows, err := r.pool.Query(ctx, `
		SELECT id,kind FROM managed_objects
		WHERE tenant_id=$1 AND status='archived'
		  AND archived_at <= clock_timestamp() - make_interval(secs => $2)
		ORDER BY archived_at,id LIMIT $3`, tenantID, grace.Seconds(), limit)
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
		audit := AuditRecord{TenantID: tenantID, ActorID: "system:retention",
			Action: "retention.objects.delete", ResourceType: "managed_object", ResourceID: item.id,
			Outcome: AuditAllowed}
		var claimed bool
		if claimed, err = deleteManagedObject(ctx, r.pool, r.objects, tenantID, item.id, item.kind, &audit); err != nil {
			return deleted, err
		}
		if claimed {
			deleted++
		}
	}
	return deleted, nil
}

// SweepTenant enforces one tenant policy without unbounded scans.
func (r *Retention) SweepTenant(ctx context.Context, tenantID string, now time.Time, limit int) (RetentionSweep, error) {
	if tenantID == "" || now.IsZero() || limit <= 0 || limit > 1000 {
		return RetentionSweep{}, ErrInvalidArgument
	}
	result := RetentionSweep{}
	var err error
	result.EventsRemoved, err = r.sweepEventsAndBackfill(ctx, tenantID, limit)
	if err != nil {
		return result, err
	}
	result.ObjectsArchived, err = r.ArchiveExpired(ctx, tenantID, limit)
	if err != nil {
		return result, err
	}
	result.InlineOutputsArchived, err = r.ArchiveNodeOutputs(ctx, tenantID, "system:retention", limit)
	if err != nil {
		return result, err
	}
	result.ObjectsDeleted, err = r.DeleteArchived(ctx, tenantID, 24*time.Hour, limit)
	if err != nil {
		return result, err
	}
	result.InlineOutputsDeleted, err = r.DeleteArchivedNodeOutputs(ctx, tenantID, "system:retention", 24*time.Hour, limit)
	return result, err
}

// ArchiveEvents writes a redacted immutable archive before deleting bounded terminal-run events.
func (r *Retention) ArchiveEvents(ctx context.Context, tenantID, actorID string, before time.Time, limit int) (ManagedObject, int, error) {
	if tenantID == "" || actorID == "" || before.IsZero() || limit <= 0 || limit > 10000 {
		return ManagedObject{}, 0, ErrInvalidArgument
	}
	objectID, err := newID("obj")
	if err != nil {
		return ManagedObject{}, 0, err
	}
	upload, err := beginObjectUpload(ctx, r.pool, r.objects, objectUploadSpec{
		tenantID: tenantID, id: objectID, kind: "archive", key: objectID, contentType: "application/json",
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
		FROM run_events e JOIN runs r ON r.tenant_id=e.tenant_id AND r.id=e.run_id
		WHERE e.tenant_id=$1 AND e.created_at < $2
		  AND r.status IN ('success','failed','error','cancelled')
		ORDER BY e.created_at,e.id FOR UPDATE OF e SKIP LOCKED LIMIT $3`, tenantID, before, limit)
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
	metadata := ManagedObject{TenantID: tenantID, ID: objectID, Kind: "archive", ObjectKey: stored.Key, ContentType: "application/json", Size: stored.Size, SHA256: stored.SHA256, Status: "archived"}
	err = tx.QueryRow(ctx, `
		INSERT INTO managed_objects (tenant_id,id,kind,object_key,content_type,size_bytes,content_hash,status,archived_at)
		VALUES ($1,$2,'archive',$3,'application/json',$4,$5,'archived',now()) RETURNING created_at`,
		tenantID, objectID, stored.Key, stored.Size, digest).Scan(&metadata.CreatedAt)
	if err != nil {
		return ManagedObject{}, 0, fmt.Errorf("register event archive: %w", err)
	}
	tag, err := tx.Exec(ctx, `DELETE FROM run_events WHERE tenant_id=$1 AND id=ANY($2)`, tenantID, ids)
	if err != nil {
		return ManagedObject{}, 0, fmt.Errorf("delete archived events: %w", err)
	}
	if _, err = insertAuditTx(ctx, tx, AuditRecord{
		TenantID: tenantID, ActorID: actorID, Action: "retention.events.archive",
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

// ExportTenant creates a consistent JSON export without ciphertext, tokens, or secret values.
func (r *Retention) ExportTenant(ctx context.Context, tenantID, actorID string, expiresAt time.Time) (TenantExport, error) {
	if tenantID == "" || actorID == "" || expiresAt.IsZero() {
		return TenantExport{}, ErrInvalidArgument
	}
	exportID, stableOperation := MutationResourceID(ctx, "export", "tenant-export")
	if stableOperation {
		existing, found, err := r.findTenantExport(ctx, tenantID, exportID)
		if err != nil {
			return TenantExport{}, err
		}
		if found {
			replayTx, beginErr := r.pool.BeginTx(ctx, pgx.TxOptions{})
			if beginErr != nil {
				return TenantExport{}, fmt.Errorf("begin tenant export replay: %w", beginErr)
			}
			defer func() { _ = replayTx.Rollback(context.WithoutCancel(ctx)) }()
			audited, auditErr := recordAllowedMutationTx(ctx, replayTx, exportID)
			if auditErr != nil {
				return TenantExport{}, fmt.Errorf("audit tenant export replay: %w", auditErr)
			}
			if commitErr := replayTx.Commit(ctx); commitErr != nil {
				return TenantExport{}, fmt.Errorf("commit tenant export replay: %w", commitErr)
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
			return TenantExport{}, generateErr
		}
		exportID = generatedID
	}
	objectID, err := mutationResourceIDOrRandom(ctx, "obj", "tenant-export-object")
	if err != nil {
		return TenantExport{}, err
	}
	upload, err := beginObjectUpload(ctx, r.pool, r.objects, objectUploadSpec{
		tenantID: tenantID, id: objectID, kind: "tenant_export", key: exportID,
		contentType: "application/json", retentionUntil: &expiresAt,
	})
	if err != nil {
		return TenantExport{}, err
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
		return TenantExport{}, fmt.Errorf("begin tenant export snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var exportedAt time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&exportedAt); err != nil {
		return TenantExport{}, fmt.Errorf("read database clock for tenant export: %w", err)
	}
	exportFile, err := os.CreateTemp("", "supaflow-tenant-export-*.json")
	if err != nil {
		return TenantExport{}, fmt.Errorf("create bounded tenant export spool: %w", err)
	}
	exportPath := exportFile.Name()
	defer func() {
		_ = exportFile.Close()
		_ = os.Remove(exportPath)
	}()
	if err = streamTenantExport(ctx, tx, exportFile, r.maxExport, tenantID, exportedAt); err != nil {
		return TenantExport{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return TenantExport{}, fmt.Errorf("commit tenant export snapshot: %w", err)
	}
	if _, err = exportFile.Seek(0, 0); err != nil {
		return TenantExport{}, fmt.Errorf("rewind tenant export spool: %w", err)
	}
	stored, err := upload.put(ctx, exportFile, r.maxExport)
	if err != nil {
		return TenantExport{}, err
	}
	digest, _ := hex.DecodeString(stored.SHA256)
	writeTx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return TenantExport{}, err
	}
	defer func() { _ = writeTx.Rollback(context.WithoutCancel(ctx)) }()
	if err = upload.publishTx(ctx, writeTx); err != nil {
		return TenantExport{}, err
	}
	if _, err = writeTx.Exec(ctx, `
		INSERT INTO managed_objects (tenant_id,id,kind,object_key,content_type,size_bytes,content_hash,retention_until)
		VALUES ($1,$2,'tenant_export',$3,'application/json',$4,$5,$6)`, tenantID, objectID,
		stored.Key, stored.Size, digest, expiresAt); err != nil {
		return TenantExport{}, fmt.Errorf("register tenant export object: %w", err)
	}
	result := TenantExport{ID: exportID, TenantID: tenantID, Status: "ready", ObjectID: objectID, ExpiresAt: &expiresAt}
	err = writeTx.QueryRow(ctx, `
		INSERT INTO tenant_exports (tenant_id,id,requested_by,status,object_id,expires_at,completed_at)
		VALUES ($1,$2,$3,'ready',$4,$5,now()) RETURNING created_at,completed_at`, tenantID,
		exportID, actorID, objectID, expiresAt).Scan(&result.CreatedAt, &result.CompletedAt)
	if err != nil {
		return TenantExport{}, fmt.Errorf("record tenant export: %w", err)
	}
	audited, err := recordAllowedMutationTx(ctx, writeTx, exportID)
	if err != nil {
		return TenantExport{}, fmt.Errorf("audit tenant export mutation: %w", err)
	}
	if !audited {
		if _, err = insertAuditTx(ctx, writeTx, AuditRecord{
			TenantID: tenantID, ActorID: actorID, Action: "tenant.export",
			ResourceType: "tenant_export", ResourceID: exportID, Outcome: AuditAllowed,
		}); err != nil {
			return TenantExport{}, err
		}
	}
	if err = upload.completeTx(ctx, writeTx); err != nil {
		return TenantExport{}, err
	}
	if err = writeTx.Commit(ctx); err != nil {
		return TenantExport{}, fmt.Errorf("commit tenant export: %w", err)
	}
	if audited {
		markMutationCommitted(ctx)
	}
	committed = true
	return result, nil
}

func (r *Retention) findTenantExport(ctx context.Context, tenantID, exportID string) (TenantExport, bool, error) {
	var result TenantExport
	err := r.pool.QueryRow(ctx, `
		SELECT id,tenant_id,status,object_id,created_at,completed_at,expires_at
		FROM tenant_exports WHERE tenant_id=$1 AND id=$2`, tenantID, exportID).Scan(
		&result.ID, &result.TenantID, &result.Status, &result.ObjectID,
		&result.CreatedAt, &result.CompletedAt, &result.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return TenantExport{}, false, nil
	}
	if err != nil {
		return TenantExport{}, false, fmt.Errorf("recover tenant export: %w", err)
	}
	return result, true, nil
}
