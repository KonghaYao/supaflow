package platformops

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// TriggerKind identifies the external source that can create a firing.
type TriggerKind string

const (
	TriggerAPI      TriggerKind = "api"
	TriggerManual   TriggerKind = "manual"
	TriggerSchedule TriggerKind = "schedule"
	TriggerWebhook  TriggerKind = "webhook"
	TriggerEvent    TriggerKind = "event"
)

// MisfirePolicy defines how delayed schedules handle missed occurrences.
type MisfirePolicy string

const (
	MisfireSkip     MisfirePolicy = "skip"
	MisfireFireOnce MisfirePolicy = "fire_once"
	MisfireCatchUp  MisfirePolicy = "catch_up"
)

// Trigger is a organization-scoped durable trigger definition. It never contains secrets.
type Trigger struct {
	OrganizationID      string          `json:"-"`
	ID                  string          `json:"id"`
	PublicHash          string          `json:"publicHash"`
	WorkflowID          string          `json:"workflowId"`
	Kind                TriggerKind     `json:"kind"`
	Name                string          `json:"name"`
	Enabled             bool            `json:"enabled"`
	Config              json.RawMessage `json:"config"`
	ScheduleInterval    time.Duration   `json:"scheduleInterval,omitempty"`
	MisfirePolicy       MisfirePolicy   `json:"misfirePolicy,omitempty"`
	NextFireAt          *time.Time      `json:"nextFireAt,omitempty"`
	SigningCredentialID string          `json:"signingCredentialId,omitempty"`
	EventType           string          `json:"eventType,omitempty"`
	CreatedBy           string          `json:"createdBy"`
	CreatedAt           time.Time       `json:"createdAt"`
	UpdatedAt           time.Time       `json:"updatedAt"`
}

// Firing is one idempotent trigger occurrence queued through the outbox.
type Firing struct {
	OrganizationID    string          `json:"-"`
	ID                string          `json:"id"`
	TriggerID         string          `json:"triggerId"`
	WorkflowID        string          `json:"workflowId"`
	WorkflowVersionID string          `json:"workflowVersionId"`
	ExecutionPlanID   string          `json:"executionPlanId"`
	PlanHash          string          `json:"planHash"`
	DeduplicationKey  string          `json:"deduplicationKey"`
	SourceTime        *time.Time      `json:"sourceTime,omitempty"`
	Payload           json.RawMessage `json:"payload"`
	CreatedAt         time.Time       `json:"createdAt"`
}

// CreateTriggerRequest contains validated trigger configuration.
type CreateTriggerRequest struct {
	OrganizationID      string
	ActorID             string
	WorkflowID          string
	Kind                TriggerKind
	Name                string
	Config              json.RawMessage
	ScheduleInterval    time.Duration
	MisfirePolicy       MisfirePolicy
	FirstFireAt         *time.Time
	SigningCredentialID string
	EventType           string
	IdempotencyKey      string
	RequestHash         [sha256.Size]byte
}

// FireRequest describes one API, manual, or event occurrence.
type FireRequest struct {
	OrganizationID   string
	TriggerID        string
	ExpectedKind     TriggerKind
	DeduplicationKey string
	SourceTime       *time.Time
	Payload          json.RawMessage
	// StableReplaySourceTime permits a server-assigned effective timestamp to
	// differ on retry while preserving the timestamp stored by the first commit.
	StableReplaySourceTime bool
}

// WebhookRequest is the untrusted signed webhook envelope.
type WebhookRequest struct {
	OrganizationID string
	TriggerID      string
	RequestID      string
	Timestamp      time.Time
	Signature      string
	Body           []byte
	Now            time.Time
}

// PublicWebhookRequest identifies a webhook by its non-secret public route hash.
type PublicWebhookRequest struct {
	PublicHash string
	RequestID  string
	Timestamp  time.Time
	Signature  string
	Body       []byte
	Now        time.Time
}

// ScheduleOptions bound scheduler catch-up work per polling transaction.
type ScheduleOptions struct {
	Now          time.Time
	Limit        int
	MaxCatchUp   int
	MisfireGrace time.Duration
}

// TriggerStore is the durable transaction seam used by TriggerService.
type TriggerStore interface {
	CreateTrigger(context.Context, CreateTriggerRequest) (Trigger, error)
	GetTrigger(context.Context, string, string) (Trigger, error)
	ResolvePublicWebhook(context.Context, string) (Trigger, error)
	SetTriggerEnabled(context.Context, string, string, bool, time.Time) (Trigger, error)
	Fire(context.Context, FireRequest) (Firing, bool, error)
	FireWebhook(context.Context, FireRequest, string, [sha256.Size]byte, time.Time) (Firing, bool, error)
	FireDueSchedules(context.Context, ScheduleOptions) ([]Firing, error)
	FireEvent(context.Context, string, string, string, json.RawMessage, time.Time) ([]Firing, error)
}

// ReceivePublicWebhook resolves server-owned organization context from publicHash before verification.
func (s *TriggerService) ReceivePublicWebhook(ctx context.Context, request PublicWebhookRequest) (Firing, bool, error) {
	trigger, err := s.ResolvePublicWebhook(ctx, request.PublicHash)
	if err != nil {
		return Firing{}, false, err
	}
	return s.ReceiveWebhook(ctx, WebhookRequest{
		OrganizationID: trigger.OrganizationID, TriggerID: trigger.ID, RequestID: request.RequestID,
		Timestamp: request.Timestamp, Signature: request.Signature, Body: request.Body, Now: request.Now,
	})
}

// ResolvePublicWebhook converts an untrusted public route into server-owned
// organization and trigger identity. Callers may use the resolved organization only for
// request-scoped attribution before delegating signature verification here.
func (s *TriggerService) ResolvePublicWebhook(ctx context.Context, publicHash string) (Trigger, error) {
	if publicHash == "" {
		return Trigger{}, ErrInvalidArgument
	}
	return s.store.ResolvePublicWebhook(ctx, publicHash)
}

// TriggerSecretResolver resolves only webhook signing material for one trigger.
type TriggerSecretResolver interface {
	ResolveTriggerSecret(context.Context, string, string) ([]byte, error)
}

// TriggerService validates untrusted trigger inputs before durable transactions.
type TriggerService struct {
	store         TriggerStore
	secrets       TriggerSecretResolver
	webhookWindow time.Duration
}

// NewTriggerService creates a trigger service with a bounded signature window.
func NewTriggerService(store TriggerStore, secrets TriggerSecretResolver, webhookWindow time.Duration) (*TriggerService, error) {
	if store == nil || webhookWindow <= 0 {
		return nil, ErrInvalidArgument
	}
	return &TriggerService{store: store, secrets: secrets, webhookWindow: webhookWindow}, nil
}

// Create validates and persists one trigger.
func (s *TriggerService) Create(ctx context.Context, request CreateTriggerRequest) (Trigger, error) {
	if err := validateTriggerRequest(request); err != nil {
		return Trigger{}, err
	}
	if request.IdempotencyKey != "" {
		if !validMutationKey(request.IdempotencyKey) {
			return Trigger{}, ErrInvalidArgument
		}
		request.RequestHash = triggerMutationHash("create", request)
	}
	return s.store.CreateTrigger(ctx, request)
}

// SetEnabled changes whether a trigger accepts new firings.
func (s *TriggerService) SetEnabled(ctx context.Context, organizationID, triggerID string, enabled bool, now time.Time) (Trigger, error) {
	if organizationID == "" || triggerID == "" {
		return Trigger{}, ErrInvalidArgument
	}
	return s.store.SetTriggerEnabled(ctx, organizationID, triggerID, enabled, now)
}

// Fire creates an idempotent API or manual firing.
func (s *TriggerService) Fire(ctx context.Context, request FireRequest) (Firing, bool, error) {
	if request.ExpectedKind != TriggerAPI && request.ExpectedKind != TriggerManual {
		return Firing{}, false, ErrInvalidArgument
	}
	if err := validateFireRequest(request); err != nil {
		return Firing{}, false, err
	}
	return s.store.Fire(ctx, request)
}

// ReceiveWebhook verifies timestamp and signature before atomically consuming replay identity and firing.
func (s *TriggerService) ReceiveWebhook(ctx context.Context, request WebhookRequest) (Firing, bool, error) {
	if s.secrets == nil || request.OrganizationID == "" || request.TriggerID == "" || request.RequestID == "" || request.Now.IsZero() {
		return Firing{}, false, ErrInvalidArgument
	}
	if request.Timestamp.Before(request.Now.Add(-s.webhookWindow)) || request.Timestamp.After(request.Now.Add(s.webhookWindow)) {
		return Firing{}, false, ErrSignature
	}
	secret, err := s.secrets.ResolveTriggerSecret(ctx, request.OrganizationID, request.TriggerID)
	if err != nil {
		return Firing{}, false, err
	}
	defer clearBytes(secret)
	expected := webhookSignature(secret, request.RequestID, request.Timestamp, request.Body)
	provided := strings.TrimPrefix(strings.TrimSpace(request.Signature), "v1=")
	providedBytes, decodeErr := hex.DecodeString(provided)
	if decodeErr != nil || !hmac.Equal(expected, providedBytes) {
		return Firing{}, false, ErrSignature
	}
	digest := sha256.Sum256(request.Body)
	payload := json.RawMessage(request.Body)
	if !json.Valid(payload) {
		payload, _ = json.Marshal(map[string]string{"bodySHA256": hex.EncodeToString(digest[:])})
	}
	return s.store.FireWebhook(ctx, FireRequest{
		OrganizationID: request.OrganizationID, TriggerID: request.TriggerID, ExpectedKind: TriggerWebhook,
		DeduplicationKey: request.RequestID, SourceTime: &request.Timestamp, Payload: payload,
	}, request.RequestID, digest, request.Timestamp.Add(s.webhookWindow))
}

// Tick atomically claims due schedules and creates bounded idempotent firings.
func (s *TriggerService) Tick(ctx context.Context, options ScheduleOptions) ([]Firing, error) {
	if options.Now.IsZero() || options.Limit <= 0 || options.Limit > 1000 ||
		options.MaxCatchUp <= 0 || options.MaxCatchUp > 100 || options.MisfireGrace < 0 {
		return nil, ErrInvalidArgument
	}
	return s.store.FireDueSchedules(ctx, options)
}

// ReceiveEvent creates firings for matching enabled event triggers.
func (s *TriggerService) ReceiveEvent(ctx context.Context, organizationID, eventType, eventID string, payload json.RawMessage, at time.Time) ([]Firing, error) {
	if organizationID == "" || eventType == "" || eventID == "" || at.IsZero() || !validJSONObject(payload) {
		return nil, ErrInvalidArgument
	}
	return s.store.FireEvent(ctx, organizationID, eventType, eventID, payload, at)
}

// SignWebhook is provided for SDK/tests; production senders implement the same versioned scheme.
func SignWebhook(secret []byte, requestID string, timestamp time.Time, body []byte) string {
	return "v1=" + hex.EncodeToString(webhookSignature(secret, requestID, timestamp, body))
}

func webhookSignature(secret []byte, requestID string, timestamp time.Time, body []byte) []byte {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte("v1."))
	_, _ = mac.Write([]byte(requestID))
	_, _ = mac.Write([]byte("."))
	_, _ = mac.Write([]byte(strconv.FormatInt(timestamp.Unix(), 10)))
	_, _ = mac.Write([]byte("."))
	_, _ = mac.Write(body)
	return mac.Sum(nil)
}

func validateTriggerRequest(request CreateTriggerRequest) error {
	if request.OrganizationID == "" || request.ActorID == "" || request.WorkflowID == "" || request.Name == "" || !validJSONObject(request.Config) {
		return ErrInvalidArgument
	}
	switch request.Kind {
	case TriggerAPI, TriggerManual:
		return nil
	case TriggerSchedule:
		if request.ScheduleInterval <= 0 || request.FirstFireAt == nil || (request.MisfirePolicy != MisfireSkip && request.MisfirePolicy != MisfireFireOnce && request.MisfirePolicy != MisfireCatchUp) {
			return ErrInvalidArgument
		}
	case TriggerWebhook:
		if request.SigningCredentialID == "" {
			return ErrInvalidArgument
		}
	case TriggerEvent:
		if request.EventType == "" {
			return ErrInvalidArgument
		}
	default:
		return ErrInvalidArgument
	}
	return nil
}

func validateFireRequest(request FireRequest) error {
	if request.OrganizationID == "" || request.TriggerID == "" || len(request.DeduplicationKey) == 0 || len(request.DeduplicationKey) > 255 || !validJSONObject(request.Payload) {
		return ErrInvalidArgument
	}
	return nil
}

func validJSONObject(value json.RawMessage) bool {
	if len(value) == 0 {
		return true
	}
	var object map[string]json.RawMessage
	return json.Unmarshal(value, &object) == nil
}

func clearBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

func triggerError(operation string, err error) error {
	// triggers 上的 UNIQUE (organization_id, workflow_id, name) 让同名重复创建以 23505 失败，
	// 两个外键（workflow_id、signing_credential_id）让指向不存在父行的创建以 23503 失败：
	// 前者改名即可、后者换一个存在的 id 即可，都是调用方能自己修正的输入冲突，此前它们冒成
	// 500 internal_error，调用方既看不出原因也会去重试一个永远不会成功的请求。public_hash 的
	// 唯一约束理论上也能给 23505，那是服务端极低概率的随机碰撞，一并报成 conflict 而不为一条
	// 无法人为触发的分支增加第三种归类。
	if violation := integrityViolationError(err); violation != nil {
		return violation
	}
	var databaseError *pgconn.PgError
	if errors.As(err, &databaseError) && databaseError.Code == "53400" {
		return &RateLimitError{RetryAfter: 5 * time.Second, Reason: "trigger ingress capacity exhausted"}
	}
	return fmt.Errorf("%s: %w", operation, err)
}
