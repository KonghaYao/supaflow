package platformops

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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

// ArchiveExpired marks expired active objects archived under a row lock.
func (r *Retention) ArchiveExpired(ctx context.Context, tenantID string, now time.Time, limit int) (int, error) {
	if tenantID == "" || now.IsZero() || limit <= 0 || limit > 1000 {
		return 0, ErrInvalidArgument
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("begin managed object archive: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
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

// DeleteArchived idempotently removes archived bytes then tombstones metadata.
func (r *Retention) DeleteArchived(ctx context.Context, tenantID string, archivedBefore time.Time, limit int) (int, error) {
	if tenantID == "" || archivedBefore.IsZero() || limit <= 0 || limit > 1000 {
		return 0, ErrInvalidArgument
	}
	rows, err := r.pool.Query(ctx, `
		SELECT id,kind FROM managed_objects
		WHERE tenant_id=$1 AND status='archived' AND archived_at <= $2
		ORDER BY archived_at,id LIMIT $3`, tenantID, archivedBefore, limit)
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
	policy, err := r.backfillOutputRetention(ctx, tenantID, limit)
	if err != nil {
		return RetentionSweep{}, err
	}
	result := RetentionSweep{}
	if policy.ArchiveBeforeDelete {
		_, result.EventsRemoved, err = r.ArchiveEvents(ctx, tenantID, "system:retention", now.AddDate(0, 0, -policy.EventDays), limit)
	} else {
		tag, deleteErr := r.pool.Exec(ctx, `DELETE FROM run_events WHERE (tenant_id,id) IN (
			SELECT e.tenant_id,e.id FROM run_events e JOIN runs run ON run.tenant_id=e.tenant_id AND run.id=e.run_id
			WHERE e.tenant_id=$1 AND e.created_at < $2 AND run.status IN ('success','failed','error','cancelled')
			ORDER BY e.created_at,e.id LIMIT $3)`, tenantID, now.AddDate(0, 0, -policy.EventDays), limit)
		err = deleteErr
		result.EventsRemoved = int(tag.RowsAffected())
	}
	if err != nil {
		return result, err
	}
	result.ObjectsArchived, err = r.ArchiveExpired(ctx, tenantID, now, limit)
	if err != nil {
		return result, err
	}
	result.InlineOutputsArchived, err = r.ArchiveNodeOutputs(ctx, tenantID, "system:retention", now, limit)
	if err != nil {
		return result, err
	}
	result.ObjectsDeleted, err = r.DeleteArchived(ctx, tenantID, now.Add(-24*time.Hour), limit)
	if err != nil {
		return result, err
	}
	result.InlineOutputsDeleted, err = r.DeleteArchivedNodeOutputs(ctx, tenantID, "system:retention", now.Add(-24*time.Hour), limit)
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
	ids := make([]string, 0, limit)
	records := make([]map[string]any, 0, limit)
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
		ids = append(ids, id)
		records = append(records, map[string]any{"id": id, "runId": runID, "sequence": sequence, "type": eventType, "payload": redacted, "createdAt": createdAt})
	}
	rows.Close()
	if len(ids) == 0 {
		if err = tx.Commit(ctx); err != nil {
			return ManagedObject{}, 0, fmt.Errorf("commit empty event archive: %w", err)
		}
		return ManagedObject{}, 0, nil
	}
	contents, err := json.Marshal(map[string]any{"schemaVersion": 1, "events": records})
	if err != nil || int64(len(contents)) > r.maxExport {
		return ManagedObject{}, 0, ErrRateLimited
	}
	stored, err := upload.put(ctx, bytes.NewReader(contents), r.maxExport)
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
	exportData := map[string]any{"schemaVersion": 1, "tenantId": tenantID, "exportedAt": time.Now().UTC()}
	for name, query := range exportQueries() {
		records, queryErr := exportRows(ctx, tx, query, tenantID)
		if queryErr != nil {
			return TenantExport{}, queryErr
		}
		exportData[name] = records
	}
	if err = tx.Commit(ctx); err != nil {
		return TenantExport{}, fmt.Errorf("commit tenant export snapshot: %w", err)
	}
	contents, err := json.Marshal(exportData)
	if err != nil {
		return TenantExport{}, fmt.Errorf("encode tenant export: %w", err)
	}
	contents, err = redactJSON(contents)
	if err != nil {
		return TenantExport{}, fmt.Errorf("redact tenant export: %w", err)
	}
	if int64(len(contents)) > r.maxExport {
		return TenantExport{}, ErrRateLimited
	}
	stored, err := upload.put(ctx, bytes.NewReader(contents), r.maxExport)
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
	if _, err = insertAuditTx(ctx, writeTx, AuditRecord{
		TenantID: tenantID, ActorID: actorID, Action: "tenant.export",
		ResourceType: "tenant_export", ResourceID: exportID, Outcome: AuditAllowed,
	}); err != nil {
		return TenantExport{}, err
	}
	if err = upload.completeTx(ctx, writeTx); err != nil {
		return TenantExport{}, err
	}
	if err = writeTx.Commit(ctx); err != nil {
		return TenantExport{}, fmt.Errorf("commit tenant export: %w", err)
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

func exportQueries() map[string]string {
	return map[string]string{
		"workflows":   `SELECT id,slug,name,description,status,created_by,created_at,updated_at FROM workflows WHERE tenant_id=$1 ORDER BY id`,
		"versions":    `SELECT id,workflow_id,version,definition,encode(definition_hash,'hex'),published_by,published_at FROM workflow_versions WHERE tenant_id=$1 ORDER BY workflow_id,version`,
		"runs":        `SELECT id,workflow_id,workflow_version_id,execution_plan_id,status,created_at,updated_at,finished_at FROM runs WHERE tenant_id=$1 ORDER BY created_at,id`,
		"events":      `SELECT id,run_id,sequence,event_type,payload,created_at FROM run_events WHERE tenant_id=$1 ORDER BY run_id,sequence`,
		"credentials": `SELECT id,name,provider,purpose,status,current_version,created_by,created_at,updated_at,revoked_at FROM credential_refs WHERE tenant_id=$1 ORDER BY id`,
		"audit":       `SELECT id,actor_id,action,resource_type,resource_id,outcome,metadata,created_at FROM audit_records WHERE tenant_id=$1 ORDER BY created_at,id`,
	}
}

func exportRows(ctx context.Context, tx pgx.Tx, query, tenantID string) ([]map[string]any, error) {
	rows, err := tx.Query(ctx, query, tenantID)
	if err != nil {
		return nil, fmt.Errorf("query tenant export: %w", err)
	}
	defer rows.Close()
	fields := rows.FieldDescriptions()
	result := make([]map[string]any, 0)
	for rows.Next() {
		values, valuesErr := rows.Values()
		if valuesErr != nil {
			return nil, fmt.Errorf("read tenant export row: %w", valuesErr)
		}
		record := make(map[string]any, len(values))
		for index, value := range values {
			record[fields[index].Name] = value
		}
		result = append(result, record)
	}
	if err = rows.Err(); err != nil && !errors.Is(err, context.Canceled) {
		return nil, fmt.Errorf("iterate tenant export: %w", err)
	}
	return result, err
}
