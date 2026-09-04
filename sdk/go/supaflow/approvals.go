package supaflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"time"
)

// Interrupt is a pending external action; tokens are returned only to authorized approvers.
type Interrupt struct {
	ID          string     `json:"id"`
	RunID       string     `json:"runId"`
	NodeID      string     `json:"nodeId"`
	Kind        string     `json:"kind"`
	ExpiresAt   time.Time  `json:"expiresAt"`
	CreatedAt   time.Time  `json:"createdAt"`
	ResolvedAt  *time.Time `json:"resolvedAt,omitempty"`
	ResumeToken string     `json:"resumeToken,omitempty"`
}

// ApprovalDecision resolves an approval interrupt.
type ApprovalDecision struct {
	ResumeToken string          `json:"resumeToken"`
	Decision    string          `json:"decision"`
	Comment     string          `json:"comment,omitempty"`
	Data        json.RawMessage `json:"data,omitempty"`
}

// ListInterrupts returns pending actions for one run.
func (c *Client) ListInterrupts(ctx context.Context, runID string) ([]Interrupt, error) {
	var result []Interrupt
	err := c.request(ctx, http.MethodGet, "/v1/runs/"+url.PathEscape(runID)+"/interrupts", "", nil, &result)
	return result, err
}

// ResolveInterrupt idempotently consumes one approval token.
func (c *Client) ResolveInterrupt(ctx context.Context, interruptID string, decision ApprovalDecision, key string) (Run, error) {
	var result Run
	err := c.request(ctx, http.MethodPost, "/v1/interrupts/"+url.PathEscape(interruptID)+"/resolve", key, decision, &result)
	return result, err
}
