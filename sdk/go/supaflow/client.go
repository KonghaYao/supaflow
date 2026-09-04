// Package supaflow is the stable Go client for Supaflow's tenant-scoped API.
package supaflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const maxResponseBody = 4 << 20

// Client authenticates every request with one opaque API key. Tenant identity
// intentionally has no client option and is always derived by the server.
type Client struct {
	baseURL *url.URL
	apiKey  string
	http    *http.Client
}

// NewClient constructs a bounded API client.
func NewClient(baseURL, apiKey string, httpClient *http.Client) (*Client, error) {
	parsed, err := url.Parse(strings.TrimRight(baseURL, "/"))
	if err != nil || !safeBaseURL(parsed) || len(strings.TrimSpace(apiKey)) < 24 {
		return nil, errors.New("valid base URL and API key are required")
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	client := *httpClient
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return errors.New("supaflow: redirects are disabled")
	}
	return &Client{baseURL: parsed, apiKey: strings.TrimSpace(apiKey), http: &client}, nil
}

// APIError is a stable RFC 9457 response from the control plane.
type APIError struct {
	Status int    `json:"status"`
	Code   string `json:"code"`
	Title  string `json:"title"`
}

// Error returns a stable, secret-free API failure description.
func (e *APIError) Error() string { return fmt.Sprintf("supaflow: %s (%s)", e.Title, e.Code) }

func safeBaseURL(parsed *url.URL) bool {
	if parsed == nil || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
		return false
	}
	if parsed.Scheme == "https" {
		return true
	}
	host := strings.Trim(strings.ToLower(parsed.Hostname()), "[]")
	ip := net.ParseIP(host)
	return parsed.Scheme == "http" && (host == "localhost" || ip != nil && ip.IsLoopback())
}

// VersionSummary resolves a workflow version to an immutable execution plan.
type VersionSummary struct {
	WorkflowID  string    `json:"workflowId"`
	Version     uint64    `json:"version"`
	VersionID   string    `json:"versionId"`
	PlanID      string    `json:"planId"`
	PlanHash    string    `json:"planHash"`
	PublishedAt time.Time `json:"publishedAt"`
}

// StartRunRequest starts a run using an immutable plan identity.
type StartRunRequest struct {
	PlanID    string          `json:"planId"`
	PlanHash  string          `json:"planHash"`
	Input     json.RawMessage `json:"input"`
	Priority  int             `json:"priority,omitempty"`
	TimeoutMS int64           `json:"timeoutMs,omitempty"`
}

// RerunRequest optionally restarts execution at one node in the source plan.
type RerunRequest struct {
	FromNodeID string `json:"fromNodeId,omitempty"`
	Priority   int    `json:"priority,omitempty"`
	TimeoutMS  int64  `json:"timeoutMs,omitempty"`
}

// Run is the stable control-plane projection.
type Run struct {
	ID                string          `json:"id"`
	PlanID            string          `json:"planId"`
	PlanHash          string          `json:"planHash"`
	Status            string          `json:"status"`
	Priority          int             `json:"priority"`
	DeadlineAt        *time.Time      `json:"deadlineAt,omitempty"`
	Output            json.RawMessage `json:"output,omitempty"`
	OutputMetadata    *OutputMetadata `json:"outputMetadata,omitempty"`
	Rerun             *RerunLineage   `json:"rerun,omitempty"`
	CancelRequestedAt *time.Time      `json:"cancelRequestedAt,omitempty"`
	CreatedAt         time.Time       `json:"createdAt"`
	UpdatedAt         time.Time       `json:"updatedAt"`
}

// RerunLineage identifies the terminal source used to create a rerun.
type RerunLineage struct {
	SourceRunID string    `json:"sourceRunId"`
	FromNodeID  string    `json:"fromNodeId,omitempty"`
	CreatedAt   time.Time `json:"createdAt"`
}

// OutputMetadata describes how a terminal run result was stored without
// exposing an internal object-store locator.
type OutputMetadata struct {
	ContentType     string        `json:"contentType"`
	SizeBytes       int64         `json:"sizeBytes"`
	SHA256          string        `json:"sha256"`
	Sensitive       bool          `json:"sensitive"`
	Storage         OutputStorage `json:"storage"`
	DownloadURL     string        `json:"downloadUrl,omitempty"`
	RetentionStatus string        `json:"retentionStatus,omitempty"`
}

// OutputStorage is the safe storage class of a terminal run output.
type OutputStorage string

const (
	// OutputStorageInline means Run.Output contains the JSON value.
	OutputStorageInline OutputStorage = "inline"
	// OutputStorageObject means the result is external and its internal locator is hidden.
	OutputStorageObject OutputStorage = "object"
)

// Event is one ordered run event delivered through SSE.
type Event struct {
	Cursor uint64          `json:"cursor"`
	Type   string          `json:"type"`
	Data   json.RawMessage `json:"data,omitempty"`
	At     time.Time       `json:"at"`
}

// OutboundWebhookSubscription is safe metadata for one runtime event sink.
type OutboundWebhookSubscription struct {
	ID                  string    `json:"id"`
	Name                string    `json:"name"`
	EndpointURL         string    `json:"endpointUrl"`
	EventTypes          []string  `json:"eventTypes"`
	SigningCredentialID string    `json:"signingCredentialId"`
	Status              string    `json:"status"`
	TimeoutMS           int       `json:"timeoutMs"`
	CreatedBy           string    `json:"createdBy"`
	CreatedAt           time.Time `json:"createdAt"`
	UpdatedAt           time.Time `json:"updatedAt"`
}

// CreateOutboundWebhookSubscription creates a secret-reference-only event sink.
type CreateOutboundWebhookSubscription struct {
	Name                string   `json:"name"`
	EndpointURL         string   `json:"endpointUrl"`
	EventTypes          []string `json:"eventTypes"`
	SigningCredentialID string   `json:"signingCredentialId"`
	TimeoutMS           int      `json:"timeoutMs"`
}

// ResolveLatestVersion gets the plan ID/hash needed by StartRun.
func (c *Client) ResolveLatestVersion(ctx context.Context, workflowID string) (VersionSummary, error) {
	return c.ResolveVersion(ctx, workflowID, 0)
}

// ResolveVersion gets a specific immutable plan, or the latest when version is zero.
func (c *Client) ResolveVersion(ctx context.Context, workflowID string, version uint64) (VersionSummary, error) {
	path := "/v1/workflows/" + url.PathEscape(workflowID) + "/versions/latest"
	if version > 0 {
		path = "/v1/workflows/" + url.PathEscape(workflowID) + "/versions/" + strconv.FormatUint(version, 10)
	}
	var result VersionSummary
	err := c.request(ctx, http.MethodGet, path, "", nil, &result)
	return result, err
}

// StartRun idempotently starts a run.
func (c *Client) StartRun(ctx context.Context, request StartRunRequest, idempotencyKey string) (Run, error) {
	var result Run
	err := c.request(ctx, http.MethodPost, "/v1/runs", idempotencyKey, request, &result)
	return result, err
}

// GetRun reads the current run projection.
func (c *Client) GetRun(ctx context.Context, runID string) (Run, error) {
	var result Run
	err := c.request(ctx, http.MethodGet, "/v1/runs/"+url.PathEscape(runID), "", nil, &result)
	return result, err
}

// Rerun idempotently creates a new run from a terminal historical run.
func (c *Client) Rerun(ctx context.Context, runID string, request RerunRequest, idempotencyKey string) (Run, error) {
	var result Run
	err := c.request(ctx, http.MethodPost, "/v1/runs/"+url.PathEscape(runID)+"/rerun", idempotencyKey, request, &result)
	return result, err
}

// CancelRun idempotently persists cancellation intent.
func (c *Client) CancelRun(ctx context.Context, runID, idempotencyKey string) (Run, error) {
	var result Run
	err := c.request(ctx, http.MethodPost, "/v1/runs/"+url.PathEscape(runID)+"/cancel", idempotencyKey, nil, &result)
	return result, err
}

// ListOutboundWebhookSubscriptions lists safe tenant-scoped event sinks.
func (c *Client) ListOutboundWebhookSubscriptions(ctx context.Context) ([]OutboundWebhookSubscription, error) {
	var result []OutboundWebhookSubscription
	err := c.request(ctx, http.MethodGet, "/ops/webhook-subscriptions", "", nil, &result)
	return result, err
}

// CreateOutboundWebhookSubscription creates an event sink using an encrypted credential reference.
func (c *Client) CreateOutboundWebhookSubscription(ctx context.Context, request CreateOutboundWebhookSubscription, idempotencyKey string) (OutboundWebhookSubscription, error) {
	var result OutboundWebhookSubscription
	err := c.request(ctx, http.MethodPost, "/ops/webhook-subscriptions", idempotencyKey, request, &result)
	return result, err
}

// SetOutboundWebhookSubscriptionEnabled changes delivery lifecycle state.
func (c *Client) SetOutboundWebhookSubscriptionEnabled(ctx context.Context, subscriptionID string, enabled bool, idempotencyKey string) (OutboundWebhookSubscription, error) {
	action := "disable"
	if enabled {
		action = "enable"
	}
	var result OutboundWebhookSubscription
	err := c.request(ctx, http.MethodPost, "/ops/webhook-subscriptions/"+url.PathEscape(subscriptionID)+"/"+action, idempotencyKey, nil, &result)
	return result, err
}

// DeleteOutboundWebhookSubscription soft-deletes an event sink.
func (c *Client) DeleteOutboundWebhookSubscription(ctx context.Context, subscriptionID, idempotencyKey string) error {
	return c.request(ctx, http.MethodDelete, "/ops/webhook-subscriptions/"+url.PathEscape(subscriptionID), idempotencyKey, nil, nil)
}

func (c *Client) request(ctx context.Context, method, path, idempotencyKey string, input, output any) error {
	var body io.Reader
	if input != nil {
		encoded, err := json.Marshal(input)
		if err != nil {
			return fmt.Errorf("supaflow: encode request: %w", err)
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, c.baseURL.String()+path, body)
	if err != nil {
		return fmt.Errorf("supaflow: create request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("X-API-Key", c.apiKey)
	if input != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	response, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("supaflow: send request: %w", err)
	}
	defer response.Body.Close()
	limited := io.LimitReader(response.Body, maxResponseBody+1)
	payload, err := io.ReadAll(limited)
	if err != nil {
		return fmt.Errorf("supaflow: read response: %w", err)
	}
	if len(payload) > maxResponseBody {
		return errors.New("supaflow: response exceeds size limit")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		problem := &APIError{Status: response.StatusCode, Code: "request_failed", Title: response.Status}
		_ = json.Unmarshal(payload, problem)
		return problem
	}
	if output != nil && len(payload) > 0 {
		if err := json.Unmarshal(payload, output); err != nil {
			return fmt.Errorf("supaflow: decode response: %w", err)
		}
	}
	return nil
}
