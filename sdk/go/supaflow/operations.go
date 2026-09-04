package supaflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"time"
)

// CapacitySnapshot reports tenant-owned runtime pressure and delivery backlog.
type CapacitySnapshot struct {
	ActiveRuns, QueuedRuns, ActiveWorkers, OutboxBacklog, DLQDepth int
	OldestOutboxAge                                                time.Duration
}

// Quota configures hard tenant concurrency and queue bounds.
type Quota struct {
	MaxActiveRuns    int `json:"maxActiveRuns"`
	MaxActiveWorkers int `json:"maxActiveWorkers"`
	MaxQueueDepth    int `json:"maxQueueDepth"`
}

// Credential is secret-free tenant credential metadata.
type Credential struct {
	ID             string     `json:"id"`
	Name           string     `json:"name"`
	Provider       string     `json:"provider"`
	Purpose        string     `json:"purpose"`
	Status         string     `json:"status"`
	CurrentVersion int        `json:"currentVersion"`
	CreatedAt      time.Time  `json:"createdAt"`
	UpdatedAt      time.Time  `json:"updatedAt"`
	RevokedAt      *time.Time `json:"revokedAt,omitempty"`
}

// WriteCredential supplies a write-only secret to credential create/rotate.
type WriteCredential struct {
	Name    string `json:"name"`
	Purpose string `json:"purpose"`
	Secret  string `json:"secret"`
}

// RetentionPolicy configures tenant event/output horizons.
type RetentionPolicy struct {
	EventDays           int  `json:"eventDays"`
	OutputDays          int  `json:"outputDays"`
	ArchiveBeforeDelete bool `json:"archiveBeforeDelete"`
}

// TenantExport is safe metadata for a point-in-time tenant export.
type TenantExport struct {
	ID          string     `json:"id"`
	Status      string     `json:"status"`
	ObjectID    string     `json:"objectId,omitempty"`
	ExpiresAt   *time.Time `json:"expiresAt,omitempty"`
	CreatedAt   time.Time  `json:"createdAt"`
	CompletedAt *time.Time `json:"completedAt,omitempty"`
}

// AuditRecord is a recursively redacted control-plane mutation record.
type AuditRecord struct {
	ID, ActorID, Action, ResourceType, ResourceID, Outcome string
	Metadata                                               json.RawMessage
	CreatedAt                                              time.Time
}

// DeadLetter is a failed outbound delivery safe projection.
type DeadLetter struct {
	ID, OriginalMessageID, AggregateType, AggregateID, Topic, LastError string
	Payload                                                             json.RawMessage
	Attempts                                                            int
	FailedAt                                                            time.Time
	RequeuedAt                                                          *time.Time
}

// Capacity reads tenant-scoped operational pressure.
func (c *Client) Capacity(ctx context.Context) (CapacitySnapshot, error) {
	var result CapacitySnapshot
	err := c.request(ctx, http.MethodGet, "/ops/capacity", "", nil, &result)
	return result, err
}

// ConfigureQuota updates tenant concurrency and queue limits.
func (c *Client) ConfigureQuota(ctx context.Context, quota Quota, idempotencyKey string) error {
	return c.request(ctx, http.MethodPost, "/ops/quotas", idempotencyKey, quota, nil)
}

// ListAudit returns bounded tenant audit records.
func (c *Client) ListAudit(ctx context.Context) ([]AuditRecord, error) {
	var result []AuditRecord
	err := c.request(ctx, http.MethodGet, "/ops/audit", "", nil, &result)
	return result, err
}

// ListDeadLetters returns bounded tenant dead letters.
func (c *Client) ListDeadLetters(ctx context.Context) ([]DeadLetter, error) {
	var result []DeadLetter
	err := c.request(ctx, http.MethodGet, "/ops/dlq", "", nil, &result)
	return result, err
}

// RequeueDeadLetter requests another bounded delivery attempt.
func (c *Client) RequeueDeadLetter(ctx context.Context, messageID, idempotencyKey string) error {
	return c.request(ctx, http.MethodPost, "/ops/dlq/"+url.PathEscape(messageID)+"/requeue", idempotencyKey, nil, nil)
}

// DrainWorker prevents new claims while allowing owned attempts to finish.
func (c *Client) DrainWorker(ctx context.Context, workerID, idempotencyKey string) error {
	return c.request(ctx, http.MethodPost, "/ops/workers/"+url.PathEscape(workerID)+"/drain", idempotencyKey, nil, nil)
}

// CreateCredential stores a new encrypted credential and returns safe metadata.
func (c *Client) CreateCredential(ctx context.Context, request WriteCredential, idempotencyKey string) (Credential, error) {
	var result Credential
	err := c.request(ctx, http.MethodPost, "/ops/credentials", idempotencyKey, request, &result)
	return result, err
}

// RotateCredential replaces a credential using optimistic version matching.
func (c *Client) RotateCredential(ctx context.Context, credentialID string, version int, secret, idempotencyKey string) (Credential, error) {
	var result Credential
	err := c.request(ctx, http.MethodPost, "/ops/credentials/"+url.PathEscape(credentialID)+"/rotate", idempotencyKey, map[string]any{"version": version, "secret": secret}, &result)
	return result, err
}

// RevokeCredential prevents future attempt grants and returns safe metadata.
func (c *Client) RevokeCredential(ctx context.Context, credentialID, idempotencyKey string) (Credential, error) {
	var result Credential
	err := c.request(ctx, http.MethodPost, "/ops/credentials/"+url.PathEscape(credentialID)+"/revoke", idempotencyKey, nil, &result)
	return result, err
}

// ConfigureRetention updates tenant event/output retention horizons.
func (c *Client) ConfigureRetention(ctx context.Context, policy RetentionPolicy, idempotencyKey string) error {
	return c.request(ctx, http.MethodPost, "/ops/retention", idempotencyKey, policy, nil)
}

// ExportTenant requests a secret-free tenant export.
func (c *Client) ExportTenant(ctx context.Context, idempotencyKey string) (TenantExport, error) {
	var result TenantExport
	err := c.request(ctx, http.MethodPost, "/ops/exports", idempotencyKey, nil, &result)
	return result, err
}
