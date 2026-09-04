package supaflow

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// StreamEvents consumes ordered SSE events after cursor until the server closes
// the terminal stream, callback fails, or ctx is cancelled.
func (c *Client) StreamEvents(ctx context.Context, runID string, afterCursor uint64, callback func(Event) error) error {
	if callback == nil {
		return errors.New("supaflow: event callback is required")
	}
	path := "/v1/runs/" + url.PathEscape(runID) + "/events?afterCursor=" + strconv.FormatUint(afterCursor, 10)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL.String()+path, nil)
	if err != nil {
		return fmt.Errorf("supaflow: create event request: %w", err)
	}
	request.Header.Set("Accept", "text/event-stream")
	request.Header.Set("X-API-Key", c.apiKey)
	response, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("supaflow: open event stream: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return decodeStreamProblem(response)
	}
	scanner := bufio.NewScanner(response.Body)
	scanner.Buffer(make([]byte, 4096), maxResponseBody)
	var data []string
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if err := deliverEvent(data, afterCursor, callback); err != nil {
				return err
			}
			data = data[:0]
			continue
		}
		if strings.HasPrefix(line, "data:") {
			data = append(data, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("supaflow: read event stream: %w", err)
	}
	return deliverEvent(data, afterCursor, callback)
}

func deliverEvent(lines []string, afterCursor uint64, callback func(Event) error) error {
	if len(lines) == 0 {
		return nil
	}
	var event Event
	if err := json.Unmarshal([]byte(strings.Join(lines, "\n")), &event); err != nil {
		return fmt.Errorf("supaflow: decode event: %w", err)
	}
	if event.Cursor <= afterCursor || event.Type == "" {
		return nil
	}
	return callback(event)
}

func decodeStreamProblem(response *http.Response) error {
	payload, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBody))
	if err != nil {
		return fmt.Errorf("supaflow: read event error: %w", err)
	}
	problem := &APIError{Status: response.StatusCode, Code: "request_failed", Title: response.Status}
	_ = json.Unmarshal(payload, problem)
	return problem
}
