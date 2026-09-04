package supaflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// Trigger is safe tenant-owned trigger metadata without signing secrets.
type Trigger struct {
	ID                  string          `json:"id"`
	PublicHash          string          `json:"publicHash"`
	WorkflowID          string          `json:"workflowId"`
	Kind                string          `json:"kind"`
	Name                string          `json:"name"`
	Enabled             bool            `json:"enabled"`
	Config              json.RawMessage `json:"config"`
	ScheduleInterval    time.Duration   `json:"scheduleInterval,omitempty"`
	MisfirePolicy       string          `json:"misfirePolicy,omitempty"`
	NextFireAt          *time.Time      `json:"nextFireAt,omitempty"`
	SigningCredentialID string          `json:"signingCredentialId,omitempty"`
	EventType           string          `json:"eventType,omitempty"`
	CreatedBy           string          `json:"createdBy"`
	CreatedAt           time.Time       `json:"createdAt"`
	UpdatedAt           time.Time       `json:"updatedAt"`
}

// TriggerInput configures a new trigger.
type TriggerInput struct {
	WorkflowID              string          `json:"workflowId"`
	Kind                    string          `json:"kind"`
	Name                    string          `json:"name"`
	Config                  json.RawMessage `json:"config"`
	ScheduleIntervalSeconds int64           `json:"scheduleIntervalSeconds,omitempty"`
	MisfirePolicy           string          `json:"misfirePolicy,omitempty"`
	FirstFireAt             *time.Time      `json:"firstFireAt,omitempty"`
	SigningCredentialID     string          `json:"signingCredentialId,omitempty"`
	EventType               string          `json:"eventType,omitempty"`
}

// TriggerUpdate changes mutable configuration at an optimistic revision.
type TriggerUpdate struct {
	Name                    string          `json:"name"`
	Config                  json.RawMessage `json:"config"`
	ScheduleIntervalSeconds int64           `json:"scheduleIntervalSeconds,omitempty"`
	MisfirePolicy           string          `json:"misfirePolicy,omitempty"`
	FirstFireAt             *time.Time      `json:"firstFireAt,omitempty"`
	SigningCredentialID     string          `json:"signingCredentialId,omitempty"`
	EventType               string          `json:"eventType,omitempty"`
	ExpectedUpdatedAt       time.Time       `json:"expectedUpdatedAt"`
}

// TriggerFilter selects a bounded trigger list.
type TriggerFilter struct {
	WorkflowID, Kind string
	Enabled          *bool
	Limit            int
}

// ListTriggers returns tenant-owned triggers; tenant selection is intentionally absent.
func (c *Client) ListTriggers(ctx context.Context, filter TriggerFilter) ([]Trigger, error) {
	query := url.Values{}
	if filter.WorkflowID != "" {
		query.Set("workflowId", filter.WorkflowID)
	}
	if filter.Kind != "" {
		query.Set("kind", filter.Kind)
	}
	if filter.Enabled != nil {
		query.Set("enabled", strconv.FormatBool(*filter.Enabled))
	}
	if filter.Limit > 0 {
		query.Set("limit", strconv.Itoa(filter.Limit))
	}
	path := "/ops/triggers"
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}
	var result []Trigger
	err := c.request(ctx, http.MethodGet, path, "", nil, &result)
	return result, err
}

// GetTrigger returns one tenant-owned trigger.
func (c *Client) GetTrigger(ctx context.Context, triggerID string) (Trigger, error) {
	var result Trigger
	err := c.request(ctx, http.MethodGet, "/ops/triggers/"+url.PathEscape(triggerID), "", nil, &result)
	return result, err
}

// CreateTrigger idempotently creates a trigger.
func (c *Client) CreateTrigger(ctx context.Context, input TriggerInput, key string) (Trigger, error) {
	var result Trigger
	err := c.request(ctx, http.MethodPost, "/ops/triggers", key, input, &result)
	return result, err
}

// UpdateTrigger idempotently changes mutable trigger configuration.
func (c *Client) UpdateTrigger(ctx context.Context, triggerID string, input TriggerUpdate, key string) (Trigger, error) {
	var result Trigger
	err := c.request(ctx, http.MethodPut, "/ops/triggers/"+url.PathEscape(triggerID), key, input, &result)
	return result, err
}

// SetTriggerEnabled idempotently changes trigger state at an optimistic revision.
func (c *Client) SetTriggerEnabled(ctx context.Context, triggerID string, enabled bool, expected time.Time, key string) (Trigger, error) {
	var result Trigger
	err := c.request(ctx, http.MethodPost, "/ops/triggers/"+url.PathEscape(triggerID)+"/state", key,
		map[string]any{"enabled": enabled, "expectedUpdatedAt": expected}, &result)
	return result, err
}

// DeleteTrigger idempotently deletes an optimistic trigger revision.
func (c *Client) DeleteTrigger(ctx context.Context, triggerID string, expected time.Time, key string) error {
	path := "/ops/triggers/" + url.PathEscape(triggerID) + "?expectedUpdatedAt=" + url.QueryEscape(expected.Format(time.RFC3339Nano))
	return c.request(ctx, http.MethodDelete, path, key, nil, nil)
}

// FireTriggerRequest creates an API/manual firing.
type FireTriggerRequest struct {
	Kind           string          `json:"kind"`
	IdempotencyKey string          `json:"idempotencyKey"`
	Payload        json.RawMessage `json:"payload"`
}

// FireTrigger queues an idempotent trigger occurrence.
func (c *Client) FireTrigger(ctx context.Context, triggerID string, input FireTriggerRequest) error {
	return c.request(ctx, http.MethodPost, "/ops/triggers/"+url.PathEscape(triggerID)+"/fire", input.IdempotencyKey, input, nil)
}

// ReceivePlatformEvent idempotently fans an event out to configured triggers.
func (c *Client) ReceivePlatformEvent(ctx context.Context, eventType, eventID string, payload json.RawMessage, idempotencyKey string) error {
	return c.request(ctx, http.MethodPost, "/ops/events/"+url.PathEscape(eventType), idempotencyKey,
		map[string]any{"eventId": eventID, "payload": payload}, nil)
}
