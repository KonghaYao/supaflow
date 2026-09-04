package platformops

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	objectUploadLease          = 30 * time.Second
	objectUploadCleanupTimeout = 10 * time.Second
)

type objectUploadSpec struct {
	tenantID, id, runID, kind, key, contentType string
	retentionUntil                              *time.Time
}

type objectUpload struct {
	objectUploadSpec
	pool       *pgxpool.Pool
	uploads    uploadAborter
	ownerToken string
	cancel     context.CancelFunc
	done       chan error
	closeOnce  sync.Once
	closeErr   error
}

type uploadAborter interface {
	StageUpload(context.Context, string, string, string, io.Reader, int64) (StoredObject, error)
	PublishUpload(context.Context, string, string, string) error
	AbortUpload(context.Context, string, string, string, bool) error
}

// beginObjectUpload durably records ownership before any filesystem bytes are
// written. Expired owners are reclaimed only by the recovery coordinator.
func beginObjectUpload(ctx context.Context, pool *pgxpool.Pool, objects ObjectStore, spec objectUploadSpec) (*objectUpload, error) {
	if pool == nil || objects == nil || spec.tenantID == "" || spec.id == "" || spec.key == "" || spec.contentType == "" || !validObjectKind(spec.kind) {
		return nil, ErrInvalidArgument
	}
	uploadStore, ok := objects.(uploadAborter)
	if !ok {
		return nil, errors.New("object store does not support fenced uploads")
	}
	ownerToken, err := newID("upload")
	if err != nil {
		return nil, err
	}
	var claimed string
	err = pool.QueryRow(ctx, `
		INSERT INTO object_upload_intents
		(tenant_id,id,run_id,kind,object_key,content_type,retention_until,owner_token,lease_expires_at)
		SELECT $1,$2,NULLIF($3,''),$4,$5,$6,$7,$8,now() + $9 * interval '1 millisecond'
		WHERE NOT EXISTS (
			SELECT 1 FROM managed_objects WHERE tenant_id=$1 AND (id=$2 OR object_key=$5)
		)
		ON CONFLICT DO NOTHING
		RETURNING owner_token`, spec.tenantID, spec.id, spec.runID, spec.kind, spec.key,
		spec.contentType, spec.retentionUntil, ownerToken, objectUploadLease.Milliseconds()).Scan(&claimed)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrConflict
	}
	if err != nil {
		return nil, fmt.Errorf("claim object upload intent: %w", err)
	}
	upload := &objectUpload{objectUploadSpec: spec, pool: pool, ownerToken: claimed, uploads: uploadStore}
	upload.startHeartbeat()
	return upload, nil
}

func validObjectKind(kind string) bool {
	switch kind {
	case "output", "archive", "tenant_export", "backup":
		return true
	default:
		return false
	}
}

func (u *objectUpload) startHeartbeat() {
	ctx, cancel := context.WithCancel(context.Background())
	u.cancel = cancel
	u.done = make(chan error, 1)
	go func() {
		ticker := time.NewTicker(objectUploadLease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				u.done <- nil
				return
			case <-ticker.C:
				tag, err := u.pool.Exec(ctx, `
					UPDATE object_upload_intents
					SET lease_expires_at=now() + $4 * interval '1 millisecond',updated_at=now()
					WHERE tenant_id=$1 AND id=$2 AND owner_token=$3`,
					u.tenantID, u.id, u.ownerToken, objectUploadLease.Milliseconds())
				if err != nil {
					u.done <- fmt.Errorf("renew object upload intent: %w", err)
					return
				}
				if tag.RowsAffected() == 0 {
					u.done <- nil
					return
				}
			}
		}
	}()
}

func (u *objectUpload) closeHeartbeat() error {
	u.closeOnce.Do(func() {
		u.cancel()
		u.closeErr = <-u.done
	})
	return u.closeErr
}

func (u *objectUpload) put(ctx context.Context, source io.Reader, maxBytes int64) (StoredObject, error) {
	stored, err := u.uploads.StageUpload(ctx, u.tenantID, u.key, u.ownerToken, source, maxBytes)
	if err != nil {
		return StoredObject{}, err
	}
	select {
	case heartbeatErr := <-u.done:
		u.closeOnce.Do(func() { u.closeErr = heartbeatErr })
		if heartbeatErr != nil {
			return StoredObject{}, heartbeatErr
		}
	default:
	}
	return stored, nil
}

// publishTx fences the owner under a row lock before making staged bytes
// visible. The caller must register metadata and complete the intent in this
// same transaction.
func (u *objectUpload) publishTx(ctx context.Context, tx pgx.Tx) error {
	var owned bool
	err := tx.QueryRow(ctx, `SELECT true FROM object_upload_intents
		WHERE tenant_id=$1 AND id=$2 AND owner_token=$3 FOR UPDATE`,
		u.tenantID, u.id, u.ownerToken).Scan(&owned)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrStaleLease
	}
	if err != nil {
		return fmt.Errorf("fence object upload publication: %w", err)
	}
	if !owned {
		return ErrStaleLease
	}
	if err = u.uploads.PublishUpload(ctx, u.tenantID, u.key, u.ownerToken); err != nil {
		return err
	}
	return nil
}

// completeTx couples the managed-object transaction to removal of the durable
// intent. A stale or recovered owner is fenced before the caller can commit.
func (u *objectUpload) completeTx(ctx context.Context, tx pgx.Tx) error {
	tag, err := tx.Exec(ctx, `DELETE FROM object_upload_intents
		WHERE tenant_id=$1 AND id=$2 AND owner_token=$3`, u.tenantID, u.id, u.ownerToken)
	if err != nil {
		return fmt.Errorf("complete object upload intent: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrStaleLease
	}
	return nil
}

func (u *objectUpload) abort() error {
	heartbeatErr := u.closeHeartbeat()
	ctx, cancel := context.WithTimeout(context.Background(), objectUploadCleanupTimeout)
	defer cancel()
	tx, err := u.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		stageErr := u.uploads.AbortUpload(ctx, u.tenantID, u.key, u.ownerToken, false)
		return errors.Join(heartbeatErr, stageErr, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var managedExists bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM managed_objects WHERE tenant_id=$1 AND id=$2 AND object_key=$3
	)`, u.tenantID, u.id, u.key).Scan(&managedExists); err != nil {
		return errors.Join(heartbeatErr, err)
	}
	var ownsIntent bool
	err = tx.QueryRow(ctx, `SELECT true FROM object_upload_intents
		WHERE tenant_id=$1 AND id=$2 AND owner_token=$3 FOR UPDATE`,
		u.tenantID, u.id, u.ownerToken).Scan(&ownsIntent)
	if errors.Is(err, pgx.ErrNoRows) {
		err = nil
	} else if err != nil {
		return errors.Join(heartbeatErr, err)
	}
	removeFinal := ownsIntent && !managedExists
	fileErr := u.uploads.AbortUpload(ctx, u.tenantID, u.key, u.ownerToken, removeFinal)
	if fileErr == nil && ownsIntent {
		_, err = tx.Exec(ctx, `DELETE FROM object_upload_intents
			WHERE tenant_id=$1 AND id=$2 AND owner_token=$3`, u.tenantID, u.id, u.ownerToken)
	}
	if err == nil {
		err = tx.Commit(ctx)
	}
	return errors.Join(heartbeatErr, fileErr, err)
}

// recoverObjectUploads locks one expired intent at a time using PostgreSQL's
// clock, removes that immutable owner's bytes, and deletes the intent in the
// same transaction. A recovery-process crash rolls the row change back, so the
// original owner token remains available to the next recovery pass.
func recoverObjectUploads(ctx context.Context, pool *pgxpool.Pool, objects ObjectStore, tenantID string, limit int) (int, error) {
	if pool == nil || objects == nil || tenantID == "" || limit <= 0 || limit > 1000 {
		return 0, ErrInvalidArgument
	}
	uploadStore, ok := objects.(uploadAborter)
	if !ok {
		return 0, errors.New("object store does not support fenced uploads")
	}
	if _, err := pool.Exec(ctx, `DELETE FROM object_upload_intents intent
		USING managed_objects object
		WHERE intent.tenant_id=$1 AND object.tenant_id=intent.tenant_id
		  AND object.id=intent.id AND object.object_key=intent.object_key`, tenantID); err != nil {
		return 0, fmt.Errorf("discard completed upload intents: %w", err)
	}
	recovered := 0
	for recovered < limit {
		tx, beginErr := pool.BeginTx(ctx, pgx.TxOptions{})
		if beginErr != nil {
			return recovered, fmt.Errorf("begin stale object upload recovery: %w", beginErr)
		}
		var id, key, ownerToken string
		err := tx.QueryRow(ctx, `
			SELECT id,object_key,owner_token FROM object_upload_intents
			WHERE tenant_id=$1 AND lease_expires_at <= now()
			ORDER BY lease_expires_at,id FOR UPDATE SKIP LOCKED LIMIT 1`, tenantID).
			Scan(&id, &key, &ownerToken)
		if errors.Is(err, pgx.ErrNoRows) {
			_ = tx.Rollback(context.WithoutCancel(ctx))
			break
		}
		if err != nil {
			_ = tx.Rollback(context.WithoutCancel(ctx))
			return recovered, fmt.Errorf("lock stale object upload: %w", err)
		}
		if err = uploadStore.AbortUpload(ctx, tenantID, key, ownerToken, true); err != nil {
			_ = tx.Rollback(context.WithoutCancel(ctx))
			return recovered, err
		}
		tag, deleteErr := tx.Exec(ctx, `DELETE FROM object_upload_intents
			WHERE tenant_id=$1 AND id=$2 AND owner_token=$3`, tenantID, id, ownerToken)
		if deleteErr != nil || tag.RowsAffected() != 1 {
			_ = tx.Rollback(context.WithoutCancel(ctx))
			if deleteErr != nil {
				return recovered, fmt.Errorf("delete recovered object upload: %w", deleteErr)
			}
			return recovered, ErrStaleLease
		}
		if err = tx.Commit(ctx); err != nil {
			return recovered, fmt.Errorf("commit stale object upload recovery: %w", err)
		}
		recovered++
	}
	return recovered, nil
}

// RecoverObjects repairs expired uploads and interrupted physical deletions for
// one tenant. All upload fencing decisions use PostgreSQL's clock.
func (r *Retention) RecoverObjects(ctx context.Context, tenantID string, limit int) (int, error) {
	if r == nil {
		return 0, ErrInvalidArgument
	}
	uploads, uploadErr := recoverObjectUploads(ctx, r.pool, r.objects, tenantID, limit)
	deletions, deletionErr := recoverManagedObjectDeletions(ctx, r.pool, r.objects, tenantID, limit)
	return uploads + deletions, errors.Join(uploadErr, deletionErr)
}
