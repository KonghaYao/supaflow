package postgres

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/pazhou/supaflow/internal/apimutation"
)

// LockAPIMutationTx serializes one stable operation and rejects key reuse with
// a different request before any domain rows are changed.
func LockAPIMutationTx(ctx context.Context, tx pgx.Tx) error {
	operation, ok := apimutation.FromContext(ctx)
	if !ok {
		return nil
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, hashtextextended($2, 0)))`, operation.OrganizationID, operation.ID); err != nil {
		return fmt.Errorf("lock API mutation: %w", err)
	}
	var storedHash []byte
	err := tx.QueryRow(ctx, `SELECT request_hash FROM api_mutation_ledger WHERE organization_id=$1 AND operation_id=$2`, operation.OrganizationID, operation.ID).Scan(&storedHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect API mutation: %w", err)
	}
	if len(storedHash) != len(operation.RequestHash) || subtle.ConstantTimeCompare(storedHash, operation.RequestHash[:]) != 1 {
		return ErrIdempotencyConflict
	}
	return nil
}

// CompleteAPIMutationTx stores operation completion and its allowed audit fact
// in the caller's domain transaction. Replays do not duplicate evidence.
func CompleteAPIMutationTx(ctx context.Context, tx pgx.Tx, resourceID string, response any) error {
	operation, ok := apimutation.FromContext(ctx)
	if !ok {
		return nil
	}
	if resourceID == "" {
		resourceID = operation.ResourceID
	}
	snapshot, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("encode API mutation completion: %w", err)
	}
	if string(snapshot) == "null" {
		snapshot = []byte(`{}`)
	}
	command, err := tx.Exec(ctx, `
		INSERT INTO api_mutation_ledger
		    (operation_id,organization_id,action,idempotency_key,request_hash,resource_type,resource_id,response_snapshot)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT (organization_id,action,idempotency_key) DO NOTHING`, operation.ID, operation.OrganizationID,
		operation.Action, operation.IdempotencyKey, operation.RequestHash[:], operation.ResourceType, resourceID, snapshot)
	if err != nil {
		return fmt.Errorf("complete API mutation: %w", err)
	}
	if command.RowsAffected() == 0 {
		var storedHash []byte
		if err = tx.QueryRow(ctx, `SELECT request_hash FROM api_mutation_ledger WHERE organization_id=$1 AND action=$2 AND idempotency_key=$3`,
			operation.OrganizationID, operation.Action, operation.IdempotencyKey).Scan(&storedHash); err != nil {
			return fmt.Errorf("read API mutation completion: %w", err)
		}
		if len(storedHash) != len(operation.RequestHash) || subtle.ConstantTimeCompare(storedHash, operation.RequestHash[:]) != 1 {
			return ErrIdempotencyConflict
		}
		return nil
	}
	metadata, _ := json.Marshal(map[string]string{"operationId": operation.ID, "requestIdHash": operation.RequestIDHash})
	auditID, err := NewID("audit")
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO audit_records
		    (id,organization_id,actor_id,action,resource_type,resource_id,outcome,metadata,operation_id)
		VALUES ($1,$2,$3,$4,$5,$6,'allowed',$7,$8)`, auditID, operation.OrganizationID, operation.ActorID,
		operation.Action, operation.ResourceType, resourceID, metadata, operation.ID); err != nil {
		return fmt.Errorf("append API mutation audit: %w", err)
	}
	return nil
}
