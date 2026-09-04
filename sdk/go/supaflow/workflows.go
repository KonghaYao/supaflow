package supaflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// WorkflowDraft is the optimistic FlowGram design document projection.
type WorkflowDraft struct {
	ID         string          `json:"id"`
	WorkflowID string          `json:"workflowId"`
	Document   json.RawMessage `json:"document"`
	Revision   uint64          `json:"revision"`
	UpdatedBy  string          `json:"updatedBy"`
	CreatedAt  time.Time       `json:"createdAt"`
	UpdatedAt  time.Time       `json:"updatedAt"`
}

// PublishedPlan is immutable workflow publication metadata and its compiled plan.
type PublishedPlan struct {
	ID          string          `json:"id"`
	WorkflowID  string          `json:"workflowId"`
	Version     uint64          `json:"version"`
	Hash        string          `json:"hash"`
	Plan        json.RawMessage `json:"plan"`
	PublishedAt time.Time       `json:"publishedAt"`
}

// WorkflowVersion is one immutable publication and its canonical definition.
type WorkflowVersion struct {
	VersionSummary
	Definition json.RawMessage `json:"definition"`
}

// VersionChange is one deterministic canonical-definition difference.
type VersionChange struct {
	Path   string          `json:"path"`
	Before json.RawMessage `json:"before,omitempty"`
	After  json.RawMessage `json:"after,omitempty"`
}

// VersionDiff compares two immutable versions of one workflow.
type VersionDiff struct {
	WorkflowID string          `json:"workflowId"`
	From       uint64          `json:"from"`
	To         uint64          `json:"to"`
	Changes    []VersionChange `json:"changes"`
}

// PreviewStep describes one bounded side-effect-free draft simulation step.
type PreviewStep struct {
	NodeID       string          `json:"nodeId"`
	Type         string          `json:"type"`
	Status       string          `json:"status"`
	SelectedPort string          `json:"selectedPort,omitempty"`
	Output       json.RawMessage `json:"output,omitempty"`
}

// DryRun is the deterministic side-effect-free draft simulation result.
type DryRun struct {
	Mode     string          `json:"mode"`
	PlanHash string          `json:"planHash"`
	Steps    []PreviewStep   `json:"steps"`
	Output   json.RawMessage `json:"output"`
}

// ExecutorBinding identifies the immutable runner contract for a node type.
type ExecutorBinding struct {
	Kind                 string `json:"kind"`
	MinimumRunnerVersion string `json:"minimumRunnerVersion"`
}

// NodeTest is one bounded side-effect-free node simulation result.
type NodeTest struct {
	Mode         string          `json:"mode"`
	NodeID       string          `json:"nodeId"`
	NodeType     string          `json:"nodeType"`
	Executor     ExecutorBinding `json:"executor"`
	Status       string          `json:"status"`
	SelectedPort string          `json:"selectedPort,omitempty"`
	Output       json.RawMessage `json:"output,omitempty"`
}

// NodeType is the discoverable immutable node-registry projection. Schemas and
// capability maps remain raw JSON so SDK upgrades do not weaken server pinning.
type NodeType struct {
	Type               string                     `json:"type"`
	Version            string                     `json:"version"`
	Capabilities       json.RawMessage            `json:"capabilities"`
	DefaultTimeoutMS   int64                      `json:"defaultTimeoutMs"`
	MaximumOutputBytes int64                      `json:"maximumOutputBytes"`
	Permissions        json.RawMessage            `json:"permissions"`
	Executor           ExecutorBinding            `json:"executor"`
	Inputs             map[string]json.RawMessage `json:"inputs"`
	Outputs            map[string]json.RawMessage `json:"outputs"`
	ConfigSchema       json.RawMessage            `json:"configSchema"`
	SensitiveFields    []string                   `json:"sensitiveFields"`
	Migrations         []json.RawMessage          `json:"migrations"`
}

// ListWorkflowDrafts returns tenant-owned drafts.
func (c *Client) ListWorkflowDrafts(ctx context.Context) ([]WorkflowDraft, error) {
	var result []WorkflowDraft
	err := c.request(ctx, http.MethodGet, "/v1/workflows", "", nil, &result)
	return result, err
}

// GetWorkflowDraft returns one tenant-owned draft.
func (c *Client) GetWorkflowDraft(ctx context.Context, workflowID string) (WorkflowDraft, error) {
	var result WorkflowDraft
	err := c.request(ctx, http.MethodGet, "/v1/workflows/"+url.PathEscape(workflowID), "", nil, &result)
	return result, err
}

// CreateWorkflowDraft idempotently creates a draft.
func (c *Client) CreateWorkflowDraft(ctx context.Context, workflowID string, document json.RawMessage, key string) (WorkflowDraft, error) {
	var result WorkflowDraft
	err := c.request(ctx, http.MethodPost, "/v1/workflows", key, map[string]any{"workflowId": workflowID, "document": document}, &result)
	return result, err
}

// UpdateWorkflowDraft idempotently saves an optimistic revision.
func (c *Client) UpdateWorkflowDraft(ctx context.Context, workflowID string, document json.RawMessage, revision uint64, key string) (WorkflowDraft, error) {
	var result WorkflowDraft
	err := c.request(ctx, http.MethodPut, "/v1/workflows/"+url.PathEscape(workflowID), key,
		map[string]any{"document": document, "expectedRevision": revision}, &result)
	return result, err
}

// DeleteWorkflowDraft idempotently deletes an optimistic revision.
func (c *Client) DeleteWorkflowDraft(ctx context.Context, workflowID string, revision uint64, key string) error {
	return c.request(ctx, http.MethodDelete, "/v1/workflows/"+url.PathEscape(workflowID), key,
		map[string]any{"expectedRevision": revision}, nil)
}

// CompileWorkflowDraft validates and compiles the current draft without publishing.
func (c *Client) CompileWorkflowDraft(ctx context.Context, workflowID string) (json.RawMessage, error) {
	var result json.RawMessage
	err := c.request(ctx, http.MethodPost, "/v1/workflows/"+url.PathEscape(workflowID)+"/compile", "", nil, &result)
	return result, err
}

// PublishWorkflowDraft idempotently publishes the current draft.
func (c *Client) PublishWorkflowDraft(ctx context.Context, workflowID, key string) (PublishedPlan, error) {
	var result PublishedPlan
	err := c.request(ctx, http.MethodPost, "/v1/workflows/"+url.PathEscape(workflowID)+"/publish-draft", key, nil, &result)
	return result, err
}

// ListWorkflowVersions returns immutable versions newest first.
func (c *Client) ListWorkflowVersions(ctx context.Context, workflowID string) ([]VersionSummary, error) {
	var result []VersionSummary
	err := c.request(ctx, http.MethodGet, "/v1/workflows/"+url.PathEscape(workflowID)+"/versions", "", nil, &result)
	return result, err
}

// GetWorkflowVersion returns one immutable version and canonical definition.
func (c *Client) GetWorkflowVersion(ctx context.Context, workflowID string, version uint64) (WorkflowVersion, error) {
	var result WorkflowVersion
	err := c.request(ctx, http.MethodGet, "/v1/workflows/"+url.PathEscape(workflowID)+"/versions/"+strconv.FormatUint(version, 10), "", nil, &result)
	return result, err
}

// DiffWorkflowVersions returns a deterministic canonical-definition diff.
func (c *Client) DiffWorkflowVersions(ctx context.Context, workflowID string, from, to uint64) (VersionDiff, error) {
	query := url.Values{}
	query.Set("from", strconv.FormatUint(from, 10))
	query.Set("to", strconv.FormatUint(to, 10))
	var result VersionDiff
	err := c.request(ctx, http.MethodGet, "/v1/workflows/"+url.PathEscape(workflowID)+"/versions/diff?"+query.Encode(), "", nil, &result)
	return result, err
}

// RepublishWorkflowVersion idempotently publishes a historical definition.
func (c *Client) RepublishWorkflowVersion(ctx context.Context, workflowID string, version uint64, key string) (PublishedPlan, error) {
	var result PublishedPlan
	err := c.request(ctx, http.MethodPost, "/v1/workflows/"+url.PathEscape(workflowID)+"/versions/"+strconv.FormatUint(version, 10)+"/republish", key, nil, &result)
	return result, err
}

// DryRunWorkflowDraft simulates the current draft without production effects.
func (c *Client) DryRunWorkflowDraft(ctx context.Context, workflowID string, sampleInput json.RawMessage) (DryRun, error) {
	var result DryRun
	err := c.request(ctx, http.MethodPost, "/v1/workflows/"+url.PathEscape(workflowID)+"/dry-run", "", map[string]any{"sampleInput": sampleInput}, &result)
	return result, err
}

// TestWorkflowNode simulates one resolved draft node without production effects.
func (c *Client) TestWorkflowNode(ctx context.Context, workflowID, nodeID string, sampleInput json.RawMessage) (NodeTest, error) {
	var result NodeTest
	err := c.request(ctx, http.MethodPost, "/v1/workflows/"+url.PathEscape(workflowID)+"/node-test", "", map[string]any{"nodeId": nodeID, "sampleInput": sampleInput}, &result)
	return result, err
}

// ListNodeTypes returns the backend's immutable node-registry projection.
func (c *Client) ListNodeTypes(ctx context.Context) ([]NodeType, error) {
	var result []NodeType
	err := c.request(ctx, http.MethodGet, "/v1/node-types", "", nil, &result)
	return result, err
}
