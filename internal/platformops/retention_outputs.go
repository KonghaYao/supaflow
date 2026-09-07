package platformops

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ArchiveNodeOutputs marks expired runtime output metadata archived and writes
// durable audit evidence in the same transaction. Payload bytes remain
// available during the archive grace period.
func (r *Retention) ArchiveNodeOutputs(ctx context.Context, organizationID, actorID string, limit int) (int, error) {
	if organizationID == "" || actorID == "" || limit <= 0 || limit > 1000 {
		return 0, ErrInvalidArgument
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("begin node output archive: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var now time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return 0, fmt.Errorf("read database clock for node output archive: %w", err)
	}
	tag, err := tx.Exec(ctx, `
		WITH expired AS (
			SELECT organization_id,id FROM node_outputs
			WHERE organization_id=$1 AND retention_status='active' AND retention_until <= $2
			ORDER BY retention_until,id FOR UPDATE SKIP LOCKED LIMIT $3
		)
		UPDATE node_outputs output SET retention_status='archived',archived_at=$2
		FROM expired WHERE output.organization_id=expired.organization_id AND output.id=expired.id`, organizationID, now, limit)
	if err != nil {
		return 0, fmt.Errorf("archive node outputs: %w", err)
	}
	count := int(tag.RowsAffected())
	if count > 0 {
		metadata, _ := json.Marshal(map[string]any{"count": count, "retentionAt": now})
		if _, err = insertAuditTx(ctx, tx, AuditRecord{
			OrganizationID: organizationID, ActorID: actorID, Action: "retention.outputs.archive",
			ResourceType: "node_output", ResourceID: "batch", Outcome: AuditAllowed, Metadata: metadata,
		}); err != nil {
			return 0, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit node output archive: %w", err)
	}
	return count, nil
}

// DeleteArchivedNodeOutputs removes inline bytes and tombstones metadata. An
// external output is tombstoned only after its managed object is deleted.
func (r *Retention) DeleteArchivedNodeOutputs(ctx context.Context, organizationID, actorID string, grace time.Duration, limit int) (int, error) {
	if organizationID == "" || actorID == "" || grace < 0 || limit <= 0 || limit > 1000 {
		return 0, ErrInvalidArgument
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("begin node output deletion: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	tag, err := tx.Exec(ctx, `
		WITH expired AS (
			SELECT output.organization_id,output.id FROM node_outputs output
			LEFT JOIN managed_objects object
			  ON object.organization_id=output.organization_id AND object.id=output.object_ref
			WHERE output.organization_id=$1 AND output.retention_status='archived'
			  AND output.archived_at <= clock_timestamp() - make_interval(secs => $2)
			  AND (output.inline_value IS NOT NULL OR object.status='deleted')
			ORDER BY output.archived_at,output.id FOR UPDATE OF output SKIP LOCKED LIMIT $3
		)
		UPDATE node_outputs output SET retention_status='deleted',inline_value=NULL,object_ref=NULL,deleted_at=now()
		FROM expired WHERE output.organization_id=expired.organization_id AND output.id=expired.id`, organizationID, grace.Seconds(), limit)
	if err != nil {
		return 0, fmt.Errorf("tombstone node outputs: %w", err)
	}
	count := int(tag.RowsAffected())
	if count > 0 {
		metadata, _ := json.Marshal(map[string]any{"count": count, "grace": grace.String()})
		if _, err = insertAuditTx(ctx, tx, AuditRecord{
			OrganizationID: organizationID, ActorID: actorID, Action: "retention.outputs.delete",
			ResourceType: "node_output", ResourceID: "batch", Outcome: AuditAllowed, Metadata: metadata,
		}); err != nil {
			return 0, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit node output deletion: %w", err)
	}
	return count, nil
}
