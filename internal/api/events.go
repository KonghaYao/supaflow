package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/pazhou/supaflow/internal/runtime"
)

// EventStreamConfig bounds concurrent streams, database polling, and socket writes.
type EventStreamConfig struct {
	GlobalLimit  int
	TenantLimit  int
	PollMin      time.Duration
	PollMax      time.Duration
	WriteTimeout time.Duration
}

func defaultEventStreamConfig() EventStreamConfig {
	return EventStreamConfig{GlobalLimit: 512, TenantLimit: 64, PollMin: 250 * time.Millisecond, PollMax: 2 * time.Second, WriteTimeout: 5 * time.Second}
}

type eventStreamLimiter struct {
	mu      sync.Mutex
	config  EventStreamConfig
	global  int
	tenants map[string]int
}

func newEventStreamLimiter(config EventStreamConfig) (*eventStreamLimiter, error) {
	if config.GlobalLimit < 1 || config.GlobalLimit > 10000 || config.TenantLimit < 1 || config.TenantLimit > config.GlobalLimit ||
		config.PollMin < 10*time.Millisecond || config.PollMax < config.PollMin || config.PollMax > 30*time.Second ||
		config.WriteTimeout < 100*time.Millisecond || config.WriteTimeout > 30*time.Second {
		return nil, errors.New("event stream limits are invalid")
	}
	return &eventStreamLimiter{config: config, tenants: make(map[string]int)}, nil
}

func (l *eventStreamLimiter) acquire(tenantID string) (func(), bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.global >= l.config.GlobalLimit || l.tenants[tenantID] >= l.config.TenantLimit {
		return nil, false
	}
	l.global++
	l.tenants[tenantID]++
	return func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.global--
		l.tenants[tenantID]--
		if l.tenants[tenantID] == 0 {
			delete(l.tenants, tenantID)
		}
	}, true
}

// SetEventStreamConfig configures production SSE resource limits before Routes is served.
func (h *Handler) SetEventStreamConfig(config EventStreamConfig) error {
	limiter, err := newEventStreamLimiter(config)
	if err != nil {
		return err
	}
	h.eventStreams = limiter
	return nil
}

func (h *Handler) events(w http.ResponseWriter, r *http.Request) {
	principal, _ := PrincipalFromContext(r.Context())
	cursor, err := eventCursor(r)
	if err != nil {
		writeProblem(w, r, http.StatusBadRequest, "invalid_cursor", "event cursor must be a non-negative integer")
		return
	}
	release, allowed := h.eventStreams.acquire(principal.TenantID)
	if !allowed {
		w.Header().Set("Retry-After", "1")
		writeProblem(w, r, http.StatusTooManyRequests, "stream_capacity_exceeded", "event stream capacity is exhausted")
		return
	}
	defer release()
	if _, err = h.runtime.GetRun(r.Context(), runtime.TenantID(principal.TenantID), r.PathValue("runID")); err != nil {
		h.writeError(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "private, no-cache, no-store, no-transform")
	w.Header().Set("X-Accel-Buffering", "no")
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeProblem(w, r, http.StatusInternalServerError, "stream_unsupported", "event streaming is unavailable")
		return
	}
	config := h.eventStreams.config
	pollDelay := config.PollMin
	poll := time.NewTimer(0)
	heartbeat := time.NewTicker(15 * time.Second)
	defer poll.Stop()
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			if !writeEventStream(w, config.WriteTimeout, func() error {
				_, writeErr := io.WriteString(w, ": keepalive\n\n")
				return writeErr
			}) {
				return
			}
			flusher.Flush()
		case <-poll.C:
			events, listErr := h.runtime.ListEvents(r.Context(), runtime.TenantID(principal.TenantID), r.PathValue("runID"), cursor, 256)
			if listErr != nil {
				return
			}
			terminal := false
			for _, event := range events {
				data, marshalErr := json.Marshal(eventDTO(event))
				if marshalErr != nil || !writeEventStream(w, config.WriteTimeout, func() error {
					_, writeErr := fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", event.Cursor, event.Type, data)
					return writeErr
				}) {
					return
				}
				cursor = event.Cursor
				terminal = terminal || isTerminalEvent(event.Type)
			}
			if len(events) > 0 {
				flusher.Flush()
				pollDelay = config.PollMin
			} else {
				pollDelay = min(pollDelay*2, config.PollMax)
			}
			if terminal {
				return
			}
			poll.Reset(pollDelay)
		}
	}
}

func writeEventStream(w http.ResponseWriter, timeout time.Duration, write func() error) bool {
	controller := http.NewResponseController(w)
	if err := controller.SetWriteDeadline(time.Now().Add(timeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return false
	}
	return write() == nil
}
