package supaflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// ResumeWebhook invokes a public single-use callback. The token is carried in
// Authorization so it is not exposed through URLs, bodies, or tenant headers.
func (c *Client) ResumeWebhook(ctx context.Context, interruptID, resumeToken, idempotencyKey string, timestamp time.Time, data json.RawMessage) (Run, error) {
	encoded, err := json.Marshal(data)
	if err != nil {
		return Run{}, fmt.Errorf("supaflow: encode webhook resume: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL.String()+"/hooks/resume/"+url.PathEscape(interruptID), bytes.NewReader(encoded))
	if err != nil {
		return Run{}, fmt.Errorf("supaflow: create webhook resume: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Resume "+resumeToken)
	request.Header.Set("Idempotency-Key", idempotencyKey)
	request.Header.Set("X-Supaflow-Timestamp", strconv.FormatInt(timestamp.UTC().Unix(), 10))
	response, err := c.http.Do(request)
	if err != nil {
		return Run{}, fmt.Errorf("supaflow: send webhook resume: %w", err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBody+1))
	if err != nil {
		return Run{}, fmt.Errorf("supaflow: read webhook resume: %w", err)
	}
	if len(payload) > maxResponseBody {
		return Run{}, errors.New("supaflow: response exceeds size limit")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		problem := &APIError{Status: response.StatusCode, Code: "request_failed", Title: response.Status}
		_ = json.Unmarshal(payload, problem)
		return Run{}, problem
	}
	var result Run
	if err = json.Unmarshal(payload, &result); err != nil {
		return Run{}, fmt.Errorf("supaflow: decode webhook resume: %w", err)
	}
	return result, nil
}
