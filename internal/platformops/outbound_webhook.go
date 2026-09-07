package platformops

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const maxOutboundWebhookBody = 1 << 20

// OutboundWebhookSubscription is safe management metadata for one event sink.
type OutboundWebhookSubscription struct {
	OrganizationID      string        `json:"-"`
	ID                  string        `json:"id"`
	Name                string        `json:"name"`
	EndpointURL         string        `json:"endpointUrl"`
	EventTypes          []string      `json:"eventTypes"`
	SigningCredentialID string        `json:"signingCredentialId"`
	Status              string        `json:"status"`
	Timeout             time.Duration `json:"-"`
	TimeoutMS           int           `json:"timeoutMs"`
	CreatedBy           string        `json:"createdBy"`
	CreatedAt           time.Time     `json:"createdAt"`
	UpdatedAt           time.Time     `json:"updatedAt"`
}

// OutboundWebhookRequest is the bounded transport command sent to an HTTP adapter.
type OutboundWebhookRequest struct {
	URL       string
	RequestID string
	Timestamp time.Time
	EventType string
	Signature string
	Body      []byte
	Timeout   time.Duration
	Headers   map[string]string
}

// OutboundWebhookStore owns subscription lookup and replay-safe delivery receipts.
type OutboundWebhookStore interface {
	ListEnabled(context.Context, string, string) ([]OutboundWebhookSubscription, error)
	BeginDelivery(context.Context, string, string, string, string, time.Time) (bool, error)
	CompleteDelivery(context.Context, string, string, string, int, time.Time) error
	FailWebhookDelivery(context.Context, string, string, string, string, time.Time) error
}

// OutboundWebhookSecretResolver exposes only the signing secret needed for one subscription.
type OutboundWebhookSecretResolver interface {
	ResolveOutboundWebhookSecret(context.Context, string, string) ([]byte, error)
}

// OutboundWebhookClient performs one bounded external HTTP side effect.
type OutboundWebhookClient interface {
	Send(context.Context, OutboundWebhookRequest) (int, error)
}

// OutboundWebhookDelivery fans a runtime outbox message out to matching subscriptions.
type OutboundWebhookDelivery struct {
	store   OutboundWebhookStore
	secrets OutboundWebhookSecretResolver
	client  OutboundWebhookClient
	now     func() time.Time
}

// NewOutboundWebhookDelivery validates the event delivery dependencies.
func NewOutboundWebhookDelivery(store OutboundWebhookStore, secrets OutboundWebhookSecretResolver, client OutboundWebhookClient) (*OutboundWebhookDelivery, error) {
	if store == nil || secrets == nil || client == nil {
		return nil, ErrInvalidArgument
	}
	return &OutboundWebhookDelivery{store: store, secrets: secrets, client: client, now: func() time.Time { return time.Now().UTC() }}, nil
}

// Deliver sends one outbox event to every matching organization subscription.
func (d *OutboundWebhookDelivery) Deliver(ctx context.Context, message DeliveryMessage) error {
	if message.ID == "" || message.OrganizationID == "" || message.Topic == "" || !json.Valid(message.Payload) {
		return ErrInvalidArgument
	}
	subscriptions, err := d.store.ListEnabled(ctx, message.OrganizationID, message.Topic)
	if err != nil {
		return fmt.Errorf("list outbound webhook subscriptions: %w", err)
	}
	var failures []error
	for _, subscription := range subscriptions {
		if err = d.deliverSubscription(ctx, subscription, message); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (d *OutboundWebhookDelivery) deliverSubscription(ctx context.Context, subscription OutboundWebhookSubscription, message DeliveryMessage) error {
	now := d.now().UTC()
	requestID := outboundRequestID(subscription.ID, message.ID)
	delivered, err := d.store.BeginDelivery(ctx, message.OrganizationID, subscription.ID, message.ID, requestID, now)
	if err != nil {
		return fmt.Errorf("begin outbound webhook receipt: %w", err)
	}
	if delivered {
		return nil
	}
	body, err := json.Marshal(map[string]any{
		"id": message.ID, "type": message.Topic, "aggregateType": message.AggregateType,
		"aggregateId": message.AggregateID, "data": json.RawMessage(message.Payload),
	})
	if err != nil || len(body) > maxOutboundWebhookBody {
		return ErrInvalidArgument
	}
	secret, err := d.secrets.ResolveOutboundWebhookSecret(ctx, message.OrganizationID, subscription.SigningCredentialID)
	if err != nil {
		return fmt.Errorf("resolve outbound webhook signing credential: %w", err)
	}
	defer clearBytes(secret)
	request := OutboundWebhookRequest{
		URL: subscription.EndpointURL, RequestID: requestID, Timestamp: now,
		EventType: message.Topic, Body: body, Timeout: subscription.Timeout, Headers: message.TraceHeaders,
	}
	request.Signature = SignOutboundWebhook(secret, now, requestID, body)
	status, sendErr := d.client.Send(ctx, request)
	if sendErr == nil && status >= 200 && status < 300 {
		if err = d.store.CompleteDelivery(ctx, message.OrganizationID, subscription.ID, message.ID, status, d.now().UTC()); err != nil {
			return fmt.Errorf("complete outbound webhook receipt: %w", err)
		}
		return nil
	}
	diagnostic := "unexpected outbound webhook response"
	if sendErr != nil {
		diagnostic = sendErr.Error()
	}
	diagnostic = RedactDiagnostic(diagnostic)
	if err = d.store.FailWebhookDelivery(ctx, message.OrganizationID, subscription.ID, message.ID, diagnostic, d.now().UTC()); err != nil {
		return fmt.Errorf("record outbound webhook failure: %w", err)
	}
	return fmt.Errorf("outbound webhook delivery failed: %s", diagnostic)
}

// SignOutboundWebhook signs timestamp, stable request identity, and exact body bytes.
func SignOutboundWebhook(secret []byte, timestamp time.Time, requestID string, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(fmt.Sprintf("%d.%s.", timestamp.Unix(), requestID)))
	_, _ = mac.Write(body)
	return "v1=" + hex.EncodeToString(mac.Sum(nil))
}

// VerifyOutboundWebhookSignature compares an outbound signature in constant time.
func VerifyOutboundWebhookSignature(secret []byte, timestamp time.Time, requestID string, body []byte, signature string) bool {
	expected := SignOutboundWebhook(secret, timestamp, requestID, body)
	return subtle.ConstantTimeCompare([]byte(expected), []byte(strings.TrimSpace(signature))) == 1
}

func outboundRequestID(subscriptionID, messageID string) string {
	digest := sha256.Sum256([]byte(subscriptionID + "\x00" + messageID))
	return "wh_" + hex.EncodeToString(digest[:16])
}

func containsEventType(values []string, expected string) bool {
	for _, value := range values {
		if value == expected || value == "*" {
			return true
		}
	}
	return false
}
