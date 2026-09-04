package supaflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// RunFilter selects a bounded recent-run list.
type RunFilter struct {
	WorkflowID, Status string
	Limit              int
}

// RunSummary adds workflow identity to a Run list item.
type RunSummary struct {
	Run
	WorkflowID string `json:"workflowId"`
}

// AttemptDiagnostic exposes safe execution timing, input, and stable error code.
type AttemptDiagnostic struct {
	ID            string          `json:"id"`
	Number        int             `json:"number"`
	Executor      string          `json:"executor"`
	Status        string          `json:"status"`
	Input         json.RawMessage `json:"input"`
	ErrorCode     string          `json:"errorCode,omitempty"`
	NextAttemptAt time.Time       `json:"nextAttemptAt"`
	StartedAt     *time.Time      `json:"startedAt,omitempty"`
	FinishedAt    *time.Time      `json:"finishedAt,omitempty"`
}

// NodeDiagnostic is a logical node monitor projection.
type NodeDiagnostic struct {
	NodeID          string              `json:"nodeId"`
	Status          string              `json:"status"`
	AttemptCount    int                 `json:"attemptCount"`
	LatestAttemptID string              `json:"latestAttemptId"`
	UpdatedAt       time.Time           `json:"updatedAt"`
	Attempts        []AttemptDiagnostic `json:"attempts"`
	Output          json.RawMessage     `json:"output,omitempty"`
	OutputMetadata  *OutputMetadata     `json:"outputMetadata,omitempty"`
}

// RunDetails is the safe detailed monitor projection.
type RunDetails struct {
	Run             Run              `json:"run"`
	Input           json.RawMessage  `json:"input"`
	Nodes           []NodeDiagnostic `json:"nodes"`
	LastEventCursor uint64           `json:"lastEventCursor"`
}

// ListRuns returns recent tenant-owned runs.
func (c *Client) ListRuns(ctx context.Context, filter RunFilter) ([]RunSummary, error) {
	query := url.Values{}
	if filter.WorkflowID != "" {
		query.Set("workflowId", filter.WorkflowID)
	}
	if filter.Status != "" {
		query.Set("status", filter.Status)
	}
	if filter.Limit > 0 {
		query.Set("limit", strconv.Itoa(filter.Limit))
	}
	path := "/v1/runs"
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}
	var result []RunSummary
	err := c.request(ctx, http.MethodGet, path, "", nil, &result)
	return result, err
}

// GetRunDetails returns safe node attempts and diagnostics.
func (c *Client) GetRunDetails(ctx context.Context, runID string) (RunDetails, error) {
	var result RunDetails
	err := c.request(ctx, http.MethodGet, "/v1/runs/"+url.PathEscape(runID)+"/details", "", nil, &result)
	return result, err
}
