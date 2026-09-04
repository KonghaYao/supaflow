package platformops

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// AuditIdentifier hashes untrusted correlation values before durable evidence storage.
func AuditIdentifier(value string) string {
	if value == "" {
		return ""
	}
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

// AuditOutcome classifies an authorization or execution decision.
type AuditOutcome string

const (
	AuditAllowed AuditOutcome = "allowed"
	AuditDenied  AuditOutcome = "denied"
	AuditFailed  AuditOutcome = "failed"
)

// AuditRecord is immutable security evidence with redacted metadata.
type AuditRecord struct {
	ID           string          `json:"id"`
	TenantID     string          `json:"-"`
	ActorID      string          `json:"actorId"`
	Action       string          `json:"action"`
	ResourceType string          `json:"resourceType"`
	ResourceID   string          `json:"resourceId"`
	Outcome      AuditOutcome    `json:"outcome"`
	Metadata     json.RawMessage `json:"metadata"`
	CreatedAt    time.Time       `json:"createdAt"`
}

// AuditQuery is always scoped to one authenticated tenant.
type AuditQuery struct {
	TenantID     string
	CanViewAudit bool
	ResourceType string
	ResourceID   string
	After        time.Time
	Limit        int
}

// AuditService writes and permission-gates immutable audit records.
type AuditService struct{ pool *pgxpool.Pool }

// NewAuditService creates a PostgreSQL audit service.
func NewAuditService(pool *pgxpool.Pool) (*AuditService, error) {
	if pool == nil {
		return nil, ErrInvalidArgument
	}
	return &AuditService{pool: pool}, nil
}

// Record appends immutable audit evidence after recursively redacting secret-like fields.
func (s *AuditService) Record(ctx context.Context, record AuditRecord) (AuditRecord, error) {
	if err := validateAuditRecord(record); err != nil {
		return AuditRecord{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return AuditRecord{}, fmt.Errorf("begin audit record: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	record, err = insertAuditTx(ctx, tx, record)
	if err != nil {
		return AuditRecord{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return AuditRecord{}, fmt.Errorf("commit audit record: %w", err)
	}
	return record, nil
}

// Query returns tenant-scoped audit evidence only to an authorized caller.
func (s *AuditService) Query(ctx context.Context, query AuditQuery) ([]AuditRecord, error) {
	if query.TenantID == "" || !query.CanViewAudit {
		return nil, ErrPermissionDenied
	}
	if query.Limit <= 0 || query.Limit > 500 {
		return nil, ErrInvalidArgument
	}
	var after any
	if !query.After.IsZero() {
		after = query.After
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id,tenant_id,actor_id,action,resource_type,resource_id,outcome,metadata,created_at
		FROM audit_records
		WHERE tenant_id=$1 AND ($2='' OR resource_type=$2) AND ($3='' OR resource_id=$3)
		  AND ($4::timestamptz IS NULL OR created_at > $4)
		ORDER BY created_at,id LIMIT $5`, query.TenantID, query.ResourceType, query.ResourceID, after, query.Limit)
	if err != nil {
		return nil, fmt.Errorf("query audit records: %w", err)
	}
	defer rows.Close()
	records := make([]AuditRecord, 0, query.Limit)
	for rows.Next() {
		var record AuditRecord
		if err = rows.Scan(&record.ID, &record.TenantID, &record.ActorID, &record.Action,
			&record.ResourceType, &record.ResourceID, &record.Outcome, &record.Metadata, &record.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan audit record: %w", err)
		}
		records = append(records, record)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("read audit records: %w", err)
	}
	return records, nil
}

func insertAuditTx(ctx context.Context, tx pgx.Tx, record AuditRecord) (AuditRecord, error) {
	if record.ID == "" {
		id, err := newID("audit")
		if err != nil {
			return AuditRecord{}, err
		}
		record.ID = id
	}
	metadata, err := redactJSON(record.Metadata)
	if err != nil {
		return AuditRecord{}, ErrInvalidArgument
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO audit_records (id,tenant_id,actor_id,action,resource_type,resource_id,outcome,metadata)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		RETURNING created_at`, record.ID, record.TenantID, record.ActorID, record.Action,
		record.ResourceType, record.ResourceID, record.Outcome, metadata).Scan(&record.CreatedAt)
	if err != nil {
		return AuditRecord{}, fmt.Errorf("append audit record: %w", err)
	}
	record.Metadata = metadata
	return record, nil
}

func insertAuditOnceTx(ctx context.Context, tx pgx.Tx, record AuditRecord) error {
	if err := validateAuditRecord(record); err != nil || record.ID == "" {
		return ErrInvalidArgument
	}
	metadata, err := redactJSON(record.Metadata)
	if err != nil {
		return ErrInvalidArgument
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO audit_records (id,tenant_id,actor_id,action,resource_type,resource_id,outcome,metadata)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (id) DO NOTHING`, record.ID,
		record.TenantID, record.ActorID, record.Action, record.ResourceType, record.ResourceID, record.Outcome, metadata)
	if err != nil {
		return fmt.Errorf("append idempotent audit record: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var existing AuditRecord
	var metadataMatches bool
	if err = tx.QueryRow(ctx, `SELECT tenant_id,actor_id,action,resource_type,resource_id,outcome,metadata=$2::jsonb
		FROM audit_records WHERE id=$1`, record.ID, metadata).Scan(&existing.TenantID, &existing.ActorID,
		&existing.Action, &existing.ResourceType, &existing.ResourceID, &existing.Outcome, &metadataMatches); err != nil {
		return fmt.Errorf("read idempotent audit record: %w", err)
	}
	if existing.TenantID != record.TenantID || existing.ActorID != record.ActorID ||
		existing.Action != record.Action || existing.ResourceType != record.ResourceType ||
		existing.ResourceID != record.ResourceID || existing.Outcome != record.Outcome || !metadataMatches {
		return ErrConflict
	}
	return nil
}

func validateAuditRecord(record AuditRecord) error {
	if record.TenantID == "" || record.ActorID == "" || record.Action == "" || record.ResourceType == "" || record.ResourceID == "" {
		return ErrInvalidArgument
	}
	if record.Outcome != AuditAllowed && record.Outcome != AuditDenied && record.Outcome != AuditFailed {
		return ErrInvalidArgument
	}
	return nil
}

func redactJSON(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 {
		return json.RawMessage(`{}`), nil
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	redactValue(value)
	return json.Marshal(value)
}

func redactValue(value any) {
	switch typed := value.(type) {
	case map[string]any:
		for key, nested := range typed {
			if sensitiveKey(key) {
				typed[key] = "[REDACTED]"
			} else {
				redactValue(nested)
			}
		}
	case []any:
		for _, nested := range typed {
			redactValue(nested)
		}
	}
}

func sensitiveKey(key string) bool {
	normalized := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(key, "-", ""), "_", ""))
	for _, marker := range []string{"secret", "token", "password", "authorization", "apikey", "connectionstring"} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return false
}
