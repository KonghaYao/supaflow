package platformops

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ListTriggers returns bounded tenant-scoped trigger definitions.
func (s *PostgresTriggerStore) ListTriggers(ctx context.Context, request ListTriggersRequest) ([]Trigger, error) {
	var enabled any
	if request.Enabled != nil {
		enabled = *request.Enabled
	}
	rows, err := s.pool.Query(ctx, `
		SELECT tenant_id,id,public_hash,workflow_id,kind,name,status,config,
		       schedule_interval_seconds,COALESCE(misfire_policy,''),next_fire_at,
		       COALESCE(signing_credential_id,''),COALESCE(event_type,''),created_by,created_at,updated_at
		FROM triggers
		WHERE tenant_id=$1 AND ($2='' OR workflow_id=$2) AND ($3='' OR kind=$3)
		  AND ($4::boolean IS NULL OR (status='enabled')=$4)
		ORDER BY created_at DESC,id DESC LIMIT $5`, request.TenantID, request.WorkflowID, request.Kind, enabled, request.Limit)
	if err != nil {
		return nil, triggerError("list triggers", err)
	}
	defer rows.Close()
	items := make([]Trigger, 0)
	for rows.Next() {
		item, scanErr := scanTrigger(rows)
		if scanErr != nil {
			return nil, triggerError("scan trigger", scanErr)
		}
		items = append(items, item)
	}
	if err = rows.Err(); err != nil {
		return nil, triggerError("iterate triggers", err)
	}
	return items, nil
}

// UpdateTrigger atomically applies an optimistic, idempotent configuration change.
func (s *PostgresTriggerStore) UpdateTrigger(ctx context.Context, request UpdateTriggerRequest) (Trigger, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Trigger{}, triggerError("begin update trigger", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	replayed, found, err := claimTriggerMutation(ctx, tx, request.TenantID, request.IdempotencyKey, "update", request.RequestHash, request.TriggerID)
	if err != nil {
		return Trigger{}, err
	}
	if found {
		audited, auditErr := recordAllowedMutationTx(ctx, tx, replayed.ID)
		if auditErr != nil {
			return Trigger{}, triggerError("audit replayed trigger update", auditErr)
		}
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return Trigger{}, triggerError("commit replayed trigger update", commitErr)
		}
		if audited {
			markMutationCommitted(ctx)
		}
		return replayed, nil
	}
	var interval any
	if request.ScheduleInterval > 0 {
		interval = int64(request.ScheduleInterval / time.Second)
	}
	trigger, err := scanTrigger(tx.QueryRow(ctx, `
		UPDATE triggers SET name=$4,config=$5,schedule_interval_seconds=$6,misfire_policy=$7,
		       next_fire_at=$8,signing_credential_id=NULLIF($9,''),event_type=NULLIF($10,''),updated_at=now()
		WHERE tenant_id=$1 AND id=$2 AND updated_at=$3
		RETURNING tenant_id,id,public_hash,workflow_id,kind,name,status,config,
		          schedule_interval_seconds,COALESCE(misfire_policy,''),next_fire_at,
		          COALESCE(signing_credential_id,''),COALESCE(event_type,''),created_by,created_at,updated_at`,
		request.TenantID, request.TriggerID, request.ExpectedUpdatedAt, request.Name, request.Config, interval,
		nullableString(string(request.MisfirePolicy)), request.FirstFireAt, request.SigningCredentialID, request.EventType))
	if errors.Is(err, pgx.ErrNoRows) {
		return Trigger{}, ErrConflict
	}
	if err != nil {
		return Trigger{}, triggerError("update trigger", err)
	}
	if err = completeTriggerMutation(ctx, tx, request.TenantID, request.IdempotencyKey, trigger); err != nil {
		return Trigger{}, err
	}
	audited, err := recordAllowedMutationTx(ctx, tx, trigger.ID)
	if err != nil {
		return Trigger{}, triggerError("audit trigger update", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return Trigger{}, triggerError("commit update trigger", err)
	}
	if audited {
		markMutationCommitted(ctx)
	}
	return trigger, nil
}

// SetTriggerEnabledIdempotently atomically changes state at an expected revision.
func (s *PostgresTriggerStore) SetTriggerEnabledIdempotently(ctx context.Context, request ChangeTriggerStateRequest) (Trigger, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Trigger{}, triggerError("begin trigger state change", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	replayed, found, err := claimTriggerMutation(ctx, tx, request.TenantID, request.IdempotencyKey, "state", request.RequestHash, request.TriggerID)
	if err != nil {
		return Trigger{}, err
	}
	if found {
		audited, auditErr := recordAllowedMutationTx(ctx, tx, replayed.ID)
		if auditErr != nil {
			return Trigger{}, triggerError("audit replayed trigger state change", auditErr)
		}
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return Trigger{}, triggerError("commit replayed trigger state change", commitErr)
		}
		if audited {
			markMutationCommitted(ctx)
		}
		return replayed, nil
	}
	status := "disabled"
	if request.Enabled {
		status = "enabled"
	}
	trigger, err := scanTrigger(tx.QueryRow(ctx, `
		UPDATE triggers SET status=$4,updated_at=clock_timestamp() WHERE tenant_id=$1 AND id=$2 AND updated_at=$3
		RETURNING tenant_id,id,public_hash,workflow_id,kind,name,status,config,
		          schedule_interval_seconds,COALESCE(misfire_policy,''),next_fire_at,
		          COALESCE(signing_credential_id,''),COALESCE(event_type,''),created_by,created_at,updated_at`,
		request.TenantID, request.TriggerID, request.ExpectedUpdatedAt, status))
	if errors.Is(err, pgx.ErrNoRows) {
		return Trigger{}, ErrConflict
	}
	if err != nil {
		return Trigger{}, triggerError("change trigger state", err)
	}
	if err = completeTriggerMutation(ctx, tx, request.TenantID, request.IdempotencyKey, trigger); err != nil {
		return Trigger{}, err
	}
	audited, err := recordAllowedMutationTx(ctx, tx, trigger.ID)
	if err != nil {
		return Trigger{}, triggerError("audit trigger state change", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return Trigger{}, triggerError("commit trigger state", err)
	}
	if audited {
		markMutationCommitted(ctx)
	}
	return trigger, nil
}

// DeleteTrigger atomically and idempotently deletes one optimistic revision.
func (s *PostgresTriggerStore) DeleteTrigger(ctx context.Context, request DeleteTriggerRequest) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return triggerError("begin delete trigger", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	_, found, err := claimTriggerMutation(ctx, tx, request.TenantID, request.IdempotencyKey, "delete", request.RequestHash, request.TriggerID)
	if err != nil || found {
		if err != nil {
			return err
		}
		audited, auditErr := recordAllowedMutationTx(ctx, tx, request.TriggerID)
		if auditErr != nil {
			return triggerError("audit replayed trigger deletion", auditErr)
		}
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return triggerError("commit replayed trigger deletion", commitErr)
		}
		if audited {
			markMutationCommitted(ctx)
		}
		return nil
	}
	tag, err := tx.Exec(ctx, `DELETE FROM triggers WHERE tenant_id=$1 AND id=$2 AND updated_at=$3`, request.TenantID, request.TriggerID, request.ExpectedUpdatedAt)
	if err != nil {
		return triggerError("delete trigger", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	if err = completeTriggerMutation(ctx, tx, request.TenantID, request.IdempotencyKey, Trigger{}); err != nil {
		return err
	}
	audited, err := recordAllowedMutationTx(ctx, tx, request.TriggerID)
	if err != nil {
		return triggerError("audit trigger deletion", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return triggerError("commit delete trigger", err)
	}
	if audited {
		markMutationCommitted(ctx)
	}
	return nil
}

func claimTriggerMutation(ctx context.Context, tx pgx.Tx, tenantID, key, action string, hash [32]byte, triggerID string) (Trigger, bool, error) {
	tag, err := tx.Exec(ctx, `
		INSERT INTO trigger_mutations (tenant_id,idempotency_key,operation,request_hash,trigger_id)
		VALUES ($1,$2,$3,$4,$5) ON CONFLICT (tenant_id,idempotency_key) DO NOTHING`, tenantID, key, action, hash[:], triggerID)
	if err != nil {
		return Trigger{}, false, triggerError("claim trigger mutation", err)
	}
	if tag.RowsAffected() == 1 {
		return Trigger{}, false, nil
	}
	var storedAction string
	var storedHash []byte
	var result json.RawMessage
	err = tx.QueryRow(ctx, `SELECT operation,request_hash,result FROM trigger_mutations WHERE tenant_id=$1 AND idempotency_key=$2`, tenantID, key).Scan(&storedAction, &storedHash, &result)
	if err != nil {
		return Trigger{}, false, triggerError("read trigger mutation", err)
	}
	if storedAction != action || subtle.ConstantTimeCompare(storedHash, hash[:]) != 1 {
		return Trigger{}, false, ErrConflict
	}
	var trigger Trigger
	if len(result) > 0 && string(result) != "null" {
		if err = json.Unmarshal(result, &trigger); err != nil {
			return Trigger{}, false, fmt.Errorf("decode trigger mutation result: %w", err)
		}
		trigger.TenantID = tenantID
	}
	return trigger, true, nil
}

func completeTriggerMutation(ctx context.Context, tx pgx.Tx, tenantID, key string, trigger Trigger) error {
	result, err := json.Marshal(trigger)
	if err != nil {
		return triggerError("encode trigger mutation", err)
	}
	_, err = tx.Exec(ctx, `UPDATE trigger_mutations SET result=$3 WHERE tenant_id=$1 AND idempotency_key=$2`, tenantID, key, result)
	if err != nil {
		return triggerError("complete trigger mutation", err)
	}
	return nil
}

var _ TriggerLifecycleStore = (*PostgresTriggerStore)(nil)
