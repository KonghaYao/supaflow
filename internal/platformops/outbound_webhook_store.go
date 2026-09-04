package platformops

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/netpolicy"
)

// CreateOutboundWebhookRequest contains safe subscription metadata only.
type CreateOutboundWebhookRequest struct {
	TenantID            string
	ActorID             string
	Name                string
	EndpointURL         string
	EventTypes          []string
	SigningCredentialID string
	Timeout             time.Duration
}

// OutboundWebhookRepository persists subscription lifecycle and receipts.
type OutboundWebhookRepository struct{ pool *pgxpool.Pool }

// NewOutboundWebhookRepository creates a PostgreSQL outbound webhook repository.
func NewOutboundWebhookRepository(pool *pgxpool.Pool) *OutboundWebhookRepository {
	return &OutboundWebhookRepository{pool: pool}
}

// Create persists an enabled tenant subscription referencing encrypted credentials.
func (s *OutboundWebhookRepository) Create(ctx context.Context, request CreateOutboundWebhookRequest) (OutboundWebhookSubscription, error) {
	if s == nil || s.pool == nil || validateOutboundWebhookRequest(request) != nil {
		return OutboundWebhookSubscription{}, ErrInvalidArgument
	}
	id, err := mutationResourceIDOrRandom(ctx, "sub", "subscription")
	if err != nil {
		return OutboundWebhookSubscription{}, err
	}
	events := normalizeEventTypes(request.EventTypes)
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return OutboundWebhookSubscription{}, fmt.Errorf("begin outbound webhook subscription: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var subscription OutboundWebhookSubscription
	err = tx.QueryRow(ctx, `
		INSERT INTO outbound_webhook_subscriptions (
		    tenant_id,id,name,endpoint_url,event_types,signing_credential_id,status,timeout_ms,created_by
		)
		SELECT $1,$2,$3,$4,$5,$6,'enabled',$7,$8
		FROM credential_refs c
		WHERE c.tenant_id=$1 AND c.id=$6 AND c.status='active' AND c.purpose='outbound_webhook_signing'
		ON CONFLICT (tenant_id,id) DO NOTHING
		RETURNING tenant_id,id,name,endpoint_url,event_types,signing_credential_id,status,
		          timeout_ms,created_by,created_at,updated_at`,
		request.TenantID, id, request.Name, request.EndpointURL, events,
		request.SigningCredentialID, request.Timeout.Milliseconds(), request.ActorID).Scan(
		&subscription.TenantID, &subscription.ID, &subscription.Name, &subscription.EndpointURL,
		&subscription.EventTypes, &subscription.SigningCredentialID, &subscription.Status,
		&subscription.TimeoutMS, &subscription.CreatedBy, &subscription.CreatedAt, &subscription.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		if _, stable := MutationResourceID(ctx, "sub", "subscription"); stable {
			replayed, replayErr := scanOutboundWebhook(tx.QueryRow(ctx, `
				SELECT tenant_id,id,name,endpoint_url,event_types,signing_credential_id,status,
				       timeout_ms,created_by,created_at,updated_at
				FROM outbound_webhook_subscriptions WHERE tenant_id=$1 AND id=$2`, request.TenantID, id))
			if replayErr == nil {
				if commitErr := tx.Commit(ctx); commitErr != nil {
					return OutboundWebhookSubscription{}, fmt.Errorf("commit outbound webhook replay: %w", commitErr)
				}
				replayed.Timeout = time.Duration(replayed.TimeoutMS) * time.Millisecond
				return replayed, nil
			}
		}
		return OutboundWebhookSubscription{}, ErrCredentialForbidden
	}
	if err != nil {
		return OutboundWebhookSubscription{}, fmt.Errorf("create outbound webhook subscription: %w", err)
	}
	subscription.Timeout = time.Duration(subscription.TimeoutMS) * time.Millisecond
	if _, err = insertAuditTx(ctx, tx, AuditRecord{
		TenantID: request.TenantID, ActorID: request.ActorID, Action: "webhook_subscription.create",
		ResourceType: "webhook_subscription", ResourceID: subscription.ID, Outcome: AuditAllowed,
	}); err != nil {
		return OutboundWebhookSubscription{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return OutboundWebhookSubscription{}, fmt.Errorf("commit outbound webhook subscription: %w", err)
	}
	return subscription, nil
}

// List returns safe tenant-scoped subscription metadata, excluding soft-deleted rows.
func (s *OutboundWebhookRepository) List(ctx context.Context, tenantID string, limit int) ([]OutboundWebhookSubscription, error) {
	if s == nil || s.pool == nil || tenantID == "" || limit <= 0 || limit > 500 {
		return nil, ErrInvalidArgument
	}
	rows, err := s.pool.Query(ctx, `
		SELECT tenant_id,id,name,endpoint_url,event_types,signing_credential_id,status,
		       timeout_ms,created_by,created_at,updated_at
		FROM outbound_webhook_subscriptions
		WHERE tenant_id=$1 AND status <> 'deleted' ORDER BY created_at,id LIMIT $2`, tenantID, limit)
	if err != nil {
		return nil, fmt.Errorf("list outbound webhook subscriptions: %w", err)
	}
	defer rows.Close()
	result := make([]OutboundWebhookSubscription, 0)
	for rows.Next() {
		subscription, scanErr := scanOutboundWebhook(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, subscription)
	}
	return result, rows.Err()
}

// SetEnabled changes one active subscription lifecycle state with a tenant predicate.
func (s *OutboundWebhookRepository) SetEnabled(ctx context.Context, tenantID, subscriptionID, actorID string, enabled bool, now time.Time) (OutboundWebhookSubscription, error) {
	if tenantID == "" || subscriptionID == "" || actorID == "" || now.IsZero() {
		return OutboundWebhookSubscription{}, ErrInvalidArgument
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return OutboundWebhookSubscription{}, fmt.Errorf("begin outbound webhook state change: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	status := "disabled"
	if enabled {
		status = "enabled"
	}
	row := tx.QueryRow(ctx, `
		UPDATE outbound_webhook_subscriptions SET status=$3,updated_at=$4
		WHERE tenant_id=$1 AND id=$2 AND status <> 'deleted' AND status <> $3
		RETURNING tenant_id,id,name,endpoint_url,event_types,signing_credential_id,status,
		          timeout_ms,created_by,created_at,updated_at`, tenantID, subscriptionID, status, now)
	subscription, err := scanOutboundWebhook(row)
	if errors.Is(err, pgx.ErrNoRows) {
		subscription, err = scanOutboundWebhook(tx.QueryRow(ctx, `
			SELECT tenant_id,id,name,endpoint_url,event_types,signing_credential_id,status,
			       timeout_ms,created_by,created_at,updated_at
			FROM outbound_webhook_subscriptions WHERE tenant_id=$1 AND id=$2 AND status=$3`, tenantID, subscriptionID, status))
		if errors.Is(err, pgx.ErrNoRows) {
			return OutboundWebhookSubscription{}, ErrNotFound
		}
		if err != nil {
			return OutboundWebhookSubscription{}, err
		}
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return OutboundWebhookSubscription{}, fmt.Errorf("commit outbound webhook state replay: %w", commitErr)
		}
		subscription.Timeout = time.Duration(subscription.TimeoutMS) * time.Millisecond
		return subscription, nil
	}
	if err != nil {
		return OutboundWebhookSubscription{}, err
	}
	if _, err = insertAuditTx(ctx, tx, AuditRecord{
		TenantID: tenantID, ActorID: actorID, Action: "webhook_subscription." + status,
		ResourceType: "webhook_subscription", ResourceID: subscriptionID, Outcome: AuditAllowed,
	}); err != nil {
		return OutboundWebhookSubscription{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return OutboundWebhookSubscription{}, fmt.Errorf("commit outbound webhook state change: %w", err)
	}
	return subscription, nil
}

// Delete soft-deletes a subscription so historical receipts retain referential integrity.
func (s *OutboundWebhookRepository) Delete(ctx context.Context, tenantID, subscriptionID, actorID string, now time.Time) error {
	if tenantID == "" || subscriptionID == "" || actorID == "" || now.IsZero() {
		return ErrInvalidArgument
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin outbound webhook deletion: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	tag, err := tx.Exec(ctx, `
		UPDATE outbound_webhook_subscriptions SET status='deleted',deleted_at=$3,updated_at=$3
		WHERE tenant_id=$1 AND id=$2 AND status <> 'deleted'`, tenantID, subscriptionID, now)
	if err != nil {
		return fmt.Errorf("delete outbound webhook subscription: %w", err)
	}
	if tag.RowsAffected() != 1 {
		if _, recovery := MutationResourceID(ctx, "sub", "delete:"+subscriptionID); recovery {
			var exists bool
			if queryErr := tx.QueryRow(ctx, `SELECT EXISTS (
				SELECT 1 FROM outbound_webhook_subscriptions WHERE tenant_id=$1 AND id=$2 AND status='deleted'
			)`, tenantID, subscriptionID).Scan(&exists); queryErr != nil {
				return fmt.Errorf("inspect outbound webhook deletion replay: %w", queryErr)
			}
			if exists {
				return tx.Commit(ctx)
			}
		}
		return ErrNotFound
	}
	if _, err = insertAuditTx(ctx, tx, AuditRecord{
		TenantID: tenantID, ActorID: actorID, Action: "webhook_subscription.delete",
		ResourceType: "webhook_subscription", ResourceID: subscriptionID, Outcome: AuditAllowed,
	}); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit outbound webhook deletion: %w", err)
	}
	return nil
}

// ListEnabled returns subscriptions selecting one exact event topic or wildcard.
func (s *OutboundWebhookRepository) ListEnabled(ctx context.Context, tenantID, topic string) ([]OutboundWebhookSubscription, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT tenant_id,id,name,endpoint_url,event_types,signing_credential_id,status,
		       timeout_ms,created_by,created_at,updated_at
		FROM outbound_webhook_subscriptions
		WHERE tenant_id=$1 AND status='enabled' AND ($2=ANY(event_types) OR '*'=ANY(event_types))
		ORDER BY created_at,id`, tenantID, topic)
	if err != nil {
		return nil, fmt.Errorf("select outbound webhook subscriptions: %w", err)
	}
	defer rows.Close()
	result := make([]OutboundWebhookSubscription, 0)
	for rows.Next() {
		subscription, scanErr := scanOutboundWebhook(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, subscription)
	}
	return result, rows.Err()
}

// BeginDelivery creates or advances a stable receipt and reports prior completion.
func (s *OutboundWebhookRepository) BeginDelivery(ctx context.Context, tenantID, subscriptionID, messageID, requestID string, now time.Time) (bool, error) {
	var delivered bool
	err := s.pool.QueryRow(ctx, `
		INSERT INTO outbound_webhook_receipts (
		    tenant_id,subscription_id,message_id,request_id,status,attempts,first_attempt_at,last_attempt_at
		) VALUES ($1,$2,$3,$4,'pending',1,$5,$5)
		ON CONFLICT (tenant_id,subscription_id,message_id) DO UPDATE
		SET attempts=CASE WHEN outbound_webhook_receipts.status='pending'
		                  THEN outbound_webhook_receipts.attempts+1 ELSE outbound_webhook_receipts.attempts END,
		    last_attempt_at=CASE WHEN outbound_webhook_receipts.status='pending'
		                         THEN EXCLUDED.last_attempt_at ELSE outbound_webhook_receipts.last_attempt_at END
		RETURNING status='delivered'`, tenantID, subscriptionID, messageID, requestID, now).Scan(&delivered)
	if err != nil {
		return false, fmt.Errorf("begin outbound webhook delivery: %w", err)
	}
	return delivered, nil
}

// CompleteDelivery marks a pending stable receipt delivered.
func (s *OutboundWebhookRepository) CompleteDelivery(ctx context.Context, tenantID, subscriptionID, messageID string, responseStatus int, now time.Time) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE outbound_webhook_receipts
		SET status='delivered',response_status=$4,last_error=NULL,delivered_at=$5,last_attempt_at=$5
		WHERE tenant_id=$1 AND subscription_id=$2 AND message_id=$3 AND status='pending'`,
		tenantID, subscriptionID, messageID, responseStatus, now)
	if err != nil {
		return fmt.Errorf("complete outbound webhook delivery: %w", err)
	}
	return nil
}

// FailWebhookDelivery stores only a redacted bounded diagnostic for a pending receipt.
func (s *OutboundWebhookRepository) FailWebhookDelivery(ctx context.Context, tenantID, subscriptionID, messageID, diagnostic string, now time.Time) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE outbound_webhook_receipts SET last_error=$4,last_attempt_at=$5
		WHERE tenant_id=$1 AND subscription_id=$2 AND message_id=$3 AND status='pending'`,
		tenantID, subscriptionID, messageID, RedactDiagnostic(diagnostic), now)
	if err != nil {
		return fmt.Errorf("record outbound webhook failure: %w", err)
	}
	return nil
}

func scanOutboundWebhook(row interface{ Scan(...any) error }) (OutboundWebhookSubscription, error) {
	var subscription OutboundWebhookSubscription
	err := row.Scan(&subscription.TenantID, &subscription.ID, &subscription.Name,
		&subscription.EndpointURL, &subscription.EventTypes, &subscription.SigningCredentialID,
		&subscription.Status, &subscription.TimeoutMS, &subscription.CreatedBy,
		&subscription.CreatedAt, &subscription.UpdatedAt)
	if err != nil {
		return OutboundWebhookSubscription{}, err
	}
	subscription.Timeout = time.Duration(subscription.TimeoutMS) * time.Millisecond
	return subscription, nil
}

func validateOutboundWebhookRequest(request CreateOutboundWebhookRequest) error {
	if request.TenantID == "" || request.ActorID == "" || request.Name == "" || request.SigningCredentialID == "" ||
		request.Timeout < 100*time.Millisecond || request.Timeout > 10*time.Second || len(request.EventTypes) == 0 || len(request.EventTypes) > 128 {
		return ErrInvalidArgument
	}
	parsed, err := url.Parse(request.EndpointURL)
	if err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" || parsed.User != nil || parsed.Fragment != "" || parsed.RawQuery != "" {
		return ErrInvalidArgument
	}
	if ip := net.ParseIP(parsed.Hostname()); ip != nil && !isPublicIP(ip) {
		return ErrInvalidArgument
	}
	for _, eventType := range request.EventTypes {
		if !validEventType(eventType) {
			return ErrInvalidArgument
		}
	}
	return nil
}

func validEventType(value string) bool {
	if value == "*" {
		return true
	}
	if value == "" || len(value) > 128 {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z') && !(char >= '0' && char <= '9') && char != '.' && char != '_' && char != '-' {
			return false
		}
	}
	return true
}

func normalizeEventTypes(values []string) []string {
	unique := make(map[string]struct{}, len(values))
	for _, value := range values {
		unique[value] = struct{}{}
	}
	result := make([]string, 0, len(unique))
	for value := range unique {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func isPublicIP(ip net.IP) bool { return netpolicy.IsPublicIP(ip) }

func hostAllowed(host string, allowlist []string) bool {
	if len(allowlist) == 0 {
		return true
	}
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	for _, allowed := range allowlist {
		allowed = strings.ToLower(strings.TrimSpace(strings.TrimSuffix(allowed, ".")))
		if allowed != "" && (host == allowed || strings.HasSuffix(host, "."+allowed)) {
			return true
		}
	}
	return false
}
