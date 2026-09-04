package runtime

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"sort"
	"time"
)

const (
	maxWebhookResumeBytes = 256 << 10
	maxStreamChunkBytes   = 64 << 10
	maxStreamBytes        = 1 << 20
	maxStreamChunks       = 4096
)

// ExpireRuns atomically terminalizes a bounded deterministic batch.
func (m *MemoryStore) ExpireRuns(_ context.Context, tenantID TenantID, now time.Time, limit int) (int, error) {
	if limit < 1 || limit > 1000 {
		return 0, errorf(CodeInvalidArgument, "expiry limit must be between 1 and 1000")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := make([]string, 0)
	for _, run := range m.runs {
		if run.TenantID == tenantID && !IsTerminalRun(run.Status) && run.DeadlineAt != nil && !now.Before(*run.DeadlineAt) {
			ids = append(ids, run.ID)
		}
	}
	sort.Strings(ids)
	if len(ids) > limit {
		ids = ids[:limit]
	}
	for _, runID := range ids {
		m.expireRunLocked(tenantID, runID, now)
	}
	remaining := limit - len(ids)
	if remaining <= 0 {
		return len(ids), nil
	}
	type dueApproval struct {
		key       string
		interrupt Interrupt
	}
	due := make([]dueApproval, 0)
	for key, interrupt := range m.interrupts {
		run := m.runs[scoped(tenantID, interrupt.RunID)]
		if interrupt.TenantID == tenantID && interrupt.Kind == "approval" && interrupt.Status == InterruptPending &&
			!now.Before(interrupt.ExpiresAt) && !IsTerminalRun(run.Status) {
			due = append(due, dueApproval{key: key, interrupt: interrupt})
		}
	}
	sort.Slice(due, func(i, j int) bool {
		if due[i].interrupt.ExpiresAt.Equal(due[j].interrupt.ExpiresAt) {
			return due[i].interrupt.ID < due[j].interrupt.ID
		}
		return due[i].interrupt.ExpiresAt.Before(due[j].interrupt.ExpiresAt)
	})
	if len(due) > remaining {
		due = due[:remaining]
	}
	expired := 0
	for _, item := range due {
		runKey := scoped(tenantID, item.interrupt.RunID)
		run := m.runs[runKey]
		if IsTerminalRun(run.Status) {
			continue
		}
		m.finalizeExpiredApprovalLocked(run, item.key, item.interrupt, now)
		expired++
	}
	return len(ids) + expired, nil
}

// ExpireRun terminalizes exactly one due run and invalidates its leases.
func (m *MemoryStore) ExpireRun(_ context.Context, tenantID TenantID, runID string, now time.Time) (bool, error) {
	if err := validateTenantAndID(tenantID, runID); err != nil {
		return false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	run, exists := m.runs[scoped(tenantID, runID)]
	if !exists {
		return false, errorf(CodeNotFound, "run not found")
	}
	if IsTerminalRun(run.Status) || run.DeadlineAt == nil || now.Before(*run.DeadlineAt) {
		return false, nil
	}
	m.expireRunLocked(tenantID, runID, now)
	return true, nil
}

func (m *MemoryStore) expireRunLocked(tenantID TenantID, runID string, now time.Time) {
	key := scoped(tenantID, runID)
	run := m.runs[key]
	if IsTerminalRun(run.Status) {
		return
	}
	run.Status, run.UpdatedAt, run.Version = RunError, now, run.Version+1
	m.runs[key] = run
	for attemptKey, attempt := range m.attempts {
		if attempt.TenantID != tenantID || attempt.RunID != runID || isTerminalAttempt(attempt.Status) {
			continue
		}
		attempt.Status, attempt.ErrorCode, attempt.FinishedAt, attempt.UpdatedAt = AttemptError, "run_timeout", timePointer(now), now
		m.attempts[attemptKey] = attempt
		delete(m.leases, attemptKey)
		nodeKey := nodeScoped(tenantID, runID, attempt.NodeID)
		node := m.nodes[nodeKey]
		node.Status, node.UpdatedAt = NodeError, now
		m.nodes[nodeKey] = node
	}
	for interruptKey, interrupt := range m.interrupts {
		if interrupt.TenantID == tenantID && interrupt.RunID == runID && interrupt.Status == InterruptPending {
			interrupt.Status, interrupt.ResolvedAt = InterruptCancelled, timePointer(now)
			m.interrupts[interruptKey] = interrupt
		}
	}
	payload := json.RawMessage(`{"code":"run_timeout"}`)
	m.appendEventLocked(tenantID, runID, Event{Type: "run.timed_out", Data: payload, At: now})
	m.appendEventLocked(tenantID, runID, Event{Type: "run.error", Data: payload, At: now})
	for _, link := range m.subworkflowLinks {
		if link.TenantID == tenantID && link.ParentRunID == runID {
			m.expireRunLocked(tenantID, link.ChildRunID, now)
		}
	}
}

func (m *MemoryStore) finalizeExpiredApprovalLocked(run Run, interruptKey string, interrupt Interrupt, now time.Time) {
	for key, attempt := range m.attempts {
		if attempt.TenantID != run.TenantID || attempt.RunID != run.ID || isTerminalAttempt(attempt.Status) {
			continue
		}
		attempt.Status, attempt.FinishedAt, attempt.UpdatedAt = AttemptCancelled, timePointer(now), now
		if attempt.ID == interrupt.AttemptID {
			attempt.Status, attempt.ErrorCode = AttemptError, "node_timeout"
		}
		m.attempts[key] = attempt
		delete(m.leases, key)
		nodeKey := nodeScoped(run.TenantID, run.ID, attempt.NodeID)
		node := m.nodes[nodeKey]
		node.Status, node.UpdatedAt = NodeCancelled, now
		if attempt.ID == interrupt.AttemptID {
			node.Status = NodeError
		}
		m.nodes[nodeKey] = node
	}
	for key, candidate := range m.interrupts {
		if candidate.TenantID != run.TenantID || candidate.RunID != run.ID || candidate.Status != InterruptPending {
			continue
		}
		candidate.Status, candidate.ResolvedAt = InterruptCancelled, timePointer(now)
		if key == interruptKey {
			candidate.Status = InterruptExpired
		}
		m.interrupts[key] = candidate
	}
	run.Status, run.UpdatedAt, run.Version = RunError, now, run.Version+1
	m.runs[scoped(run.TenantID, run.ID)] = run
	payload, _ := json.Marshal(map[string]string{"code": "node_timeout", "nodeId": interrupt.NodeID})
	m.appendEventLocked(run.TenantID, run.ID, Event{Type: "node.timed_out", Data: payload, At: now})
	m.appendEventLocked(run.TenantID, run.ID, Event{Type: "run.error", Data: payload, At: now})
}

func isTerminalAttempt(status AttemptStatus) bool {
	return status == AttemptSuccess || status == AttemptFailed || status == AttemptError || status == AttemptCancelled
}

// AppendStreamChunk persists a bounded stream delta behind the current lease.
func (m *MemoryStore) AppendStreamChunk(_ context.Context, req AppendStreamChunkRequest) (Event, error) {
	if req.Sequence < 1 || req.Sequence > maxStreamChunks || len(req.Chunk) == 0 || len(req.Chunk) > maxStreamChunkBytes || !json.Valid(req.Chunk) {
		return Event{}, errorf(CodeInvalidArgument, "stream chunk must be valid JSON and no larger than 64 KiB")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, attempt, _, err := m.requireLeaseLocked(req.TenantID, req.RunID, req.AttemptID, req.WorkerID, req.FencingToken, req.Now); err != nil {
		return Event{}, err
	} else {
		key := scoped(req.TenantID, req.AttemptID)
		digest := sha256.Sum256(req.Chunk)
		if record, ok := m.streamRecords[key][req.Sequence]; ok {
			if subtle.ConstantTimeCompare(record.digest[:], digest[:]) != 1 {
				return Event{}, errorf(CodeConflict, "stream sequence was used for different content")
			}
			return cloneEvent(record.event), nil
		}
		if req.Sequence != m.streamChunks[key]+1 {
			return Event{}, errorf(CodeConflict, "stream sequence is not contiguous")
		}
		if m.streamChunks[key]+1 > maxStreamChunks || m.streamBytes[key]+len(req.Chunk) > maxStreamBytes {
			return Event{}, errorf(CodeConflict, "attempt stream limit exceeded")
		}
		m.streamChunks[key]++
		m.streamBytes[key] += len(req.Chunk)
		payload, _ := json.Marshal(map[string]any{
			"attemptId": req.AttemptID, "nodeId": attempt.NodeID,
			"sequence": req.Sequence, "chunk": json.RawMessage(req.Chunk),
		})
		event := Event{Type: "stream.chunk", Data: payload, At: req.Now}
		m.appendEventLocked(req.TenantID, req.RunID, event)
		stored := m.events[scoped(req.TenantID, req.RunID)]
		storedEvent := cloneEvent(stored[len(stored)-1])
		if m.streamRecords[key] == nil {
			m.streamRecords[key] = make(map[int]streamChunkRecord)
		}
		m.streamRecords[key][req.Sequence] = streamChunkRecord{digest: digest, event: storedEvent}
		return cloneEvent(storedEvent), nil
	}
}

// ResumeWebhook consumes only webhook interrupts and stores arbitrary JSON as resume data.
func (m *MemoryStore) ResumeWebhook(_ context.Context, req ResumeWebhookRequest) (ResumeWebhookResult, error) {
	if len(req.IdempotencyKey) < 8 || len(req.IdempotencyKey) > 128 || len(req.Data) > maxWebhookResumeBytes || !json.Valid(req.Data) {
		return ResumeWebhookResult{}, errorf(CodeInvalidArgument, "webhook resume request is invalid")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	idemKey := scoped(req.TenantID, req.IdempotencyKey)
	if existing, ok := m.webhookIdempotency[idemKey]; ok {
		if subtle.ConstantTimeCompare(existing.requestHash[:], req.RequestHash[:]) != 1 {
			return ResumeWebhookResult{}, errorf(CodeIdempotencyConflict, "idempotency key was used for a different request")
		}
		return cloneWebhookResult(existing.result), nil
	}
	key := scoped(req.TenantID, req.InterruptID)
	interrupt, ok := m.interrupts[key]
	if !ok || interrupt.Kind != "webhook" || interrupt.RunID != req.RunID || interrupt.NodeID != req.NodeID || interrupt.AttemptID != req.AttemptID || subtle.ConstantTimeCompare(interrupt.TokenDigest[:], req.TokenDigest[:]) != 1 {
		return ResumeWebhookResult{}, errorf(CodeApprovalInvalid, "webhook resume token is invalid")
	}
	if interrupt.Status != InterruptPending || interrupt.ResolvedAt != nil {
		return ResumeWebhookResult{}, errorf(CodeApprovalConsumed, "webhook resume token was already consumed")
	}
	if !req.Now.Before(interrupt.ExpiresAt) {
		return ResumeWebhookResult{}, errorf(CodeApprovalExpired, "webhook resume token has expired")
	}
	attemptKey := scoped(req.TenantID, interrupt.AttemptID)
	attempt := m.attempts[attemptKey]
	runKey := scoped(req.TenantID, interrupt.RunID)
	run := m.runs[runKey]
	if attempt.Status != AttemptSuspended || (run.Status != RunSuspended && run.Status != RunRunning) {
		return ResumeWebhookResult{}, errorf(CodeInvalidTransition, "run cannot resume webhook interrupt")
	}
	interrupt.Status, interrupt.ResolvedAt, interrupt.Decision = InterruptResolved, timePointer(req.Now), cloneJSON(req.Data)
	m.interrupts[key] = interrupt
	attempt.Status, attempt.ResumeData, attempt.NextAttemptAt, attempt.UpdatedAt = AttemptPending, cloneJSON(req.Data), req.Now, req.Now
	m.attempts[attemptKey] = attempt
	nodeKey := nodeScoped(req.TenantID, run.ID, attempt.NodeID)
	node := m.nodes[nodeKey]
	node.Status, node.UpdatedAt = NodePending, req.Now
	m.nodes[nodeKey] = node
	if run.Status == RunSuspended {
		run.Status = RunPending
	}
	run.UpdatedAt, run.Version = req.Now, run.Version+1
	m.runs[runKey] = run
	m.appendEventLocked(req.TenantID, run.ID, Event{Type: "webhook.resumed", Data: cloneJSON(req.Data), At: req.Now})
	result := ResumeWebhookResult{Interrupt: cloneInterrupt(interrupt), Run: cloneRun(run)}
	m.webhookIdempotency[idemKey] = webhookIdempotencyRecord{requestHash: req.RequestHash, result: result}
	return cloneWebhookResult(result), nil
}

func cloneWebhookResult(result ResumeWebhookResult) ResumeWebhookResult {
	return ResumeWebhookResult{Interrupt: cloneInterrupt(result.Interrupt), Run: cloneRun(result.Run)}
}
