package platformops

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// deleteManagedObject first commits a durable, non-readable deletion state.
// The supervisor can therefore finish physical cleanup after cancellation,
// process death, or an ambiguous client-side database result.
func deleteManagedObject(ctx context.Context, pool *pgxpool.Pool, objects ObjectStore, organizationID, objectID, kind string, audit *AuditRecord) (bool, error) {
	if pool == nil || objects == nil || organizationID == "" || objectID == "" || !validObjectKind(kind) {
		return false, ErrInvalidArgument
	}
	objectKey, claimed, err := markObjectDeleting(ctx, pool, organizationID, objectID, kind, audit)
	if err != nil {
		return false, err
	}
	if audit != nil && !claimed {
		return false, nil
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), objectUploadCleanupTimeout)
	defer cancel()
	if err = objects.Delete(cleanupCtx, organizationID, objectKey); err != nil {
		return claimed, fmt.Errorf("delete managed object bytes: %w", err)
	}
	if _, err = pool.Exec(cleanupCtx, `UPDATE managed_objects
		SET status='deleted',deleted_at=COALESCE(deleted_at,now())
		WHERE organization_id=$1 AND id=$2 AND kind=$3 AND status='deleting'`,
		organizationID, objectID, kind); err != nil {
		return claimed, fmt.Errorf("complete managed object deletion: %w", err)
	}
	return claimed, nil
}

func markObjectDeleting(ctx context.Context, pool *pgxpool.Pool, organizationID, objectID, kind string, audit *AuditRecord) (string, bool, error) {
	if audit != nil {
		if err := validateAuditRecord(*audit); err != nil || audit.OrganizationID != organizationID || audit.ResourceID != objectID {
			return "", false, ErrInvalidArgument
		}
		if audit.ID == "" {
			digest := sha256.Sum256([]byte(organizationID + "\x00" + audit.Action + "\x00" + audit.ResourceType + "\x00" + objectID))
			audit.ID = "audit_del_" + hex.EncodeToString(digest[:16])
		}
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return "", false, fmt.Errorf("begin managed object deletion: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var objectKey, status string
	err = tx.QueryRow(ctx, `SELECT object_key,status FROM managed_objects
		WHERE organization_id=$1 AND id=$2 AND kind=$3 FOR UPDATE`, organizationID, objectID, kind).Scan(&objectKey, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, ErrNotFound
	}
	if err != nil {
		return "", false, fmt.Errorf("lock managed object deletion: %w", err)
	}
	if status != "active" && status != "archived" && status != "deleting" && status != "deleted" {
		return "", false, ErrInvalidArgument
	}
	claimed := status == "active" || status == "archived"
	if claimed {
		if _, err = tx.Exec(ctx, `UPDATE managed_objects SET status='deleting'
			WHERE organization_id=$1 AND id=$2 AND kind=$3`, organizationID, objectID, kind); err != nil {
			return "", false, fmt.Errorf("mark managed object deleting: %w", err)
		}
		if audit != nil {
			if _, err = insertAuditTx(ctx, tx, *audit); err != nil {
				return "", false, err
			}
		}
	}
	if err = tx.Commit(ctx); err == nil {
		return objectKey, claimed, nil
	}
	// Resolve an ambiguous commit. A committed deleting/deleted state proves
	// that any requested audit was committed in the same transaction.
	resolveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), objectUploadCleanupTimeout)
	defer cancel()
	var resolvedKey, resolvedStatus string
	var auditCommitted bool
	resolveErr := pool.QueryRow(resolveCtx, `SELECT object_key,status,
		$4='' OR EXISTS (SELECT 1 FROM audit_records WHERE organization_id=$1 AND id=$4)
		FROM managed_objects WHERE organization_id=$1 AND id=$2 AND kind=$3`,
		organizationID, objectID, kind, auditID(audit)).Scan(&resolvedKey, &resolvedStatus, &auditCommitted)
	if resolveErr == nil && (resolvedStatus == "deleting" || resolvedStatus == "deleted") {
		return resolvedKey, claimed && auditCommitted, nil
	}
	return "", false, errors.Join(fmt.Errorf("commit managed object deletion intent: %w", err), resolveErr)
}

func auditID(record *AuditRecord) string {
	if record == nil {
		return ""
	}
	return record.ID
}

func recoverManagedObjectDeletions(ctx context.Context, pool *pgxpool.Pool, objects ObjectStore, organizationID string, limit int) (int, error) {
	if pool == nil || objects == nil || organizationID == "" || limit <= 0 || limit > 1000 {
		return 0, ErrInvalidArgument
	}
	rows, err := pool.Query(ctx, `SELECT id,object_key FROM managed_objects
		WHERE organization_id=$1 AND status='deleting' ORDER BY id LIMIT $2`, organizationID, limit)
	if err != nil {
		return 0, fmt.Errorf("select interrupted object deletions: %w", err)
	}
	type target struct{ id, key string }
	targets := make([]target, 0, limit)
	for rows.Next() {
		var item target
		if err = rows.Scan(&item.id, &item.key); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan interrupted object deletion: %w", err)
		}
		targets = append(targets, item)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate interrupted object deletions: %w", err)
	}
	recovered := 0
	var failures []error
	for _, item := range targets {
		if err = objects.Delete(ctx, organizationID, item.key); err != nil {
			failures = append(failures, fmt.Errorf("recover managed object deletion: %w", err))
			continue
		}
		tag, updateErr := pool.Exec(ctx, `UPDATE managed_objects
			SET status='deleted',deleted_at=COALESCE(deleted_at,now())
			WHERE organization_id=$1 AND id=$2 AND status='deleting'`, organizationID, item.id)
		if updateErr != nil {
			failures = append(failures, updateErr)
			continue
		}
		recovered += int(tag.RowsAffected())
	}
	return recovered, errors.Join(failures...)
}
