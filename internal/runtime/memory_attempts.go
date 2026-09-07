package runtime

import (
	"context"
	"encoding/json"
	"strconv"
	"time"
)

// ClaimAttempt atomically claims pending work or takes over an expired lease.
func (m *MemoryStore) ClaimAttempt(ctx context.Context, req ClaimAttemptRequest) (*WorkItem, error) {
	if _, err := m.ExpireRuns(ctx, req.OrganizationID, req.Now, 100); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := validateOrganization(req.OrganizationID); err != nil {
		return nil, err
	}
	if req.WorkerID == "" || req.LeaseDuration <= 0 {
		return nil, errorf(CodeInvalidArgument, "worker id and positive lease duration are required")
	}
	m.resumeDueTimersLocked(req.OrganizationID, req.Now)
	for _, attemptKey := range m.attemptOrder {
		attempt := m.attempts[attemptKey]
		if attempt.OrganizationID != req.OrganizationID {
			continue
		}
		runKey := scoped(req.OrganizationID, attempt.RunID)
		run := m.runs[runKey]
		if run.Status != RunPending && run.Status != RunRunning && run.Status != RunCancelling {
			continue
		}
		claimable := attempt.Status == AttemptPending && !attempt.NextAttemptAt.After(req.Now)
		lease := m.leases[attemptKey]
		if attempt.Status == AttemptRunning && !req.Now.Before(lease.ExpiresAt) {
			claimable = true
		}
		if !claimable {
			continue
		}
		tookOver := attempt.Status == AttemptRunning
		lease = Lease{
			OrganizationID: req.OrganizationID, RunID: run.ID, AttemptID: attempt.ID,
			WorkerID: req.WorkerID, FencingToken: lease.FencingToken + 1,
			ExpiresAt: req.Now.Add(req.LeaseDuration), UpdatedAt: req.Now,
		}
		m.leases[attemptKey] = lease
		attempt.Status = AttemptRunning
		if attempt.StartedAt == nil {
			attempt.StartedAt = timePointer(req.Now)
		}
		attempt.UpdatedAt = req.Now
		m.attempts[attemptKey] = attempt
		nodeKey := nodeScoped(req.OrganizationID, run.ID, attempt.NodeID)
		node := m.nodes[nodeKey]
		node.Status = NodeRunning
		node.UpdatedAt = req.Now
		m.nodes[nodeKey] = node
		if run.Status == RunPending {
			firstStart := true
			for _, event := range m.events[runKey] {
				if event.Type == "run.started" {
					firstStart = false
					break
				}
			}
			run.Status = RunRunning
			run.Version++
			run.UpdatedAt = req.Now
			m.runs[runKey] = run
			if firstStart {
				m.appendEventLocked(req.OrganizationID, run.ID, Event{Type: "run.started", At: req.Now})
			}
		}
		m.appendEventLocked(req.OrganizationID, run.ID, Event{Type: "attempt.claimed", At: req.Now})
		if tookOver {
			m.appendEventLocked(req.OrganizationID, run.ID, Event{Type: "attempt.lease_taken_over", At: req.Now})
		}
		return &WorkItem{
			Run: cloneRun(run), Node: node, Attempt: cloneAttempt(attempt), Lease: lease,
			Snapshot: m.latestSnapshotLocked(req.OrganizationID, run.ID, attempt.ID),
			Takeover: tookOver,
		}, nil
	}
	return nil, nil
}

func (m *MemoryStore) resumeDueTimersLocked(organizationID OrganizationID, now time.Time) {
	for key, interrupt := range m.interrupts {
		if interrupt.OrganizationID != organizationID || interrupt.Status != InterruptPending || !interrupt.AutoResume || now.Before(interrupt.ExpiresAt) {
			continue
		}
		attemptKey := scoped(organizationID, interrupt.AttemptID)
		attempt, exists := m.attempts[attemptKey]
		if !exists || attempt.Status != AttemptSuspended {
			continue
		}
		interrupt.Status, interrupt.ResolvedAt = InterruptResolved, timePointer(now)
		interrupt.Decision = json.RawMessage(`{"expired":true}`)
		m.interrupts[key] = interrupt
		attempt.Status, attempt.ResumeData, attempt.NextAttemptAt, attempt.UpdatedAt = AttemptPending, cloneJSON(interrupt.Decision), now, now
		m.attempts[attemptKey] = attempt
		nodeKey := nodeScoped(organizationID, attempt.RunID, attempt.NodeID)
		node := m.nodes[nodeKey]
		node.Status, node.UpdatedAt = NodePending, now
		m.nodes[nodeKey] = node
		runKey := scoped(organizationID, attempt.RunID)
		run := m.runs[runKey]
		run.Status, run.UpdatedAt, run.Version = RunRunning, now, run.Version+1
		m.runs[runKey] = run
		m.appendEventLocked(organizationID, attempt.RunID, Event{Type: "wait.resumed", At: now})
	}
}

// RenewLease extends a currently valid lease with matching ownership.
func (m *MemoryStore) RenewLease(_ context.Context, req RenewLeaseRequest) (Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if req.LeaseDuration <= 0 {
		return Lease{}, errorf(CodeInvalidArgument, "lease duration must be positive")
	}
	lease, _, _, err := m.requireLeaseLocked(req.OrganizationID, req.RunID, req.AttemptID, req.WorkerID, req.FencingToken, req.Now)
	if err != nil {
		return Lease{}, err
	}
	lease.ExpiresAt = req.Now.Add(req.LeaseDuration)
	lease.UpdatedAt = req.Now
	m.leases[scoped(req.OrganizationID, req.AttemptID)] = lease
	return lease, nil
}

// CompleteAttempt commits all success-side durable records atomically.
func (m *MemoryStore) CompleteAttempt(_ context.Context, req CompleteAttemptRequest) (Run, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, attempt, run, err := m.requireLeaseLocked(req.OrganizationID, req.RunID, req.AttemptID, req.WorkerID, req.FencingToken, req.Now)
	if err != nil {
		return Run{}, err
	}
	if run.Status == RunCancelling || run.CancelRequestedAt != nil {
		// Cancellation wins the completion race. Results produced after intent
		// was persisted are not observable and must not schedule more work.
		req.RunStatus = RunCancelled
		req.Output = nil
		req.Snapshot = nil
		req.Next = nil
		req.Events = nil
	}
	if req.RunStatus != RunRunning && req.RunStatus != RunSuccess && req.RunStatus != RunCancelled {
		return Run{}, errorf(CodeInvalidArgument, "completion run status is invalid")
	}
	if run.Status == RunCancelling && req.RunStatus != RunCancelled {
		return Run{}, errorf(CodeInvalidTransition, "cancelling run only accepts cancelled completion")
	}
	effectiveStatus := req.RunStatus
	for _, next := range req.Next {
		if next.NodeID == "" || next.Executor == "" {
			return Run{}, errorf(CodeInvalidArgument, "successor node id and executor are required")
		}
	}
	if run.Status != effectiveStatus {
		if err := requireRunTransition(run.Status, effectiveStatus); err != nil {
			return Run{}, err
		}
	}
	attemptKey := scoped(req.OrganizationID, attempt.ID)
	nodeKey := nodeScoped(req.OrganizationID, run.ID, attempt.NodeID)
	node := m.nodes[nodeKey]
	if req.RunStatus == RunCancelled {
		attempt.Status = AttemptCancelled
		node.Status = NodeCancelled
	} else {
		attempt.Status = AttemptSuccess
		node.Status = NodeSuccess
	}
	attempt.FinishedAt = timePointer(req.Now)
	attempt.UpdatedAt = req.Now
	node.UpdatedAt = req.Now
	m.attempts[attemptKey] = attempt
	m.nodes[nodeKey] = node
	delete(m.leases, attemptKey)
	var completionOutput *Output
	if req.Output != nil {
		output := cloneOutput(*req.Output)
		m.nextID++
		if output.ID == "" {
			output.ID = memoryID("out", m.nextID)
		}
		output.OrganizationID, output.RunID = req.OrganizationID, run.ID
		output.NodeID, output.AttemptID, output.CreatedAt = attempt.NodeID, attempt.ID, req.Now
		m.outputs[scoped(req.OrganizationID, output.ID)] = output
		completionOutput = &output
	}
	if req.Snapshot != nil {
		m.storeSnapshotLocked(req.OrganizationID, run.ID, attempt.ID, req.Now, *req.Snapshot)
	}
	for _, next := range req.Next {
		if err := m.enqueueSuccessorLocked(run, next, req.Now); err != nil {
			return Run{}, err
		}
	}
	if effectiveStatus != RunCancelled {
		effectiveStatus = m.remainingRunStatusLocked(req.OrganizationID, run.ID)
	}
	if effectiveStatus == RunCancelled {
		m.cancelIdleAttemptsLocked(req.OrganizationID, run.ID, req.Now)
	}
	if run.Status != effectiveStatus {
		run.Status = effectiveStatus
	}
	if effectiveStatus == RunSuccess && completionOutput != nil {
		run.Output = projectRunOutput(*completionOutput)
	}
	run.Version++
	run.UpdatedAt = req.Now
	m.runs[scoped(req.OrganizationID, run.ID)] = run
	m.appendEventLocked(req.OrganizationID, run.ID, Event{Type: completionEventType(effectiveStatus), At: req.Now})
	for _, event := range req.Events {
		m.appendEventLocked(req.OrganizationID, run.ID, event)
	}
	return cloneRun(run), nil
}

func projectRunOutput(output Output) *RunOutput {
	return &RunOutput{
		Inline: cloneJSON(output.Inline), ContentType: output.ContentType,
		Size: output.Size, SHA256: output.SHA256, Sensitive: output.Sensitive,
		External: output.ObjectRef != "", RetentionStatus: "active",
	}
}

// SuspendAttempt atomically persists the interrupt, snapshot, and suspended state.
func (m *MemoryStore) SuspendAttempt(_ context.Context, req SuspendAttemptRequest) (Interrupt, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, attempt, run, err := m.requireLeaseLocked(req.OrganizationID, req.RunID, req.AttemptID, req.WorkerID, req.FencingToken, req.Now)
	if err != nil {
		return Interrupt{}, err
	}
	if run.Status != RunRunning {
		return Interrupt{}, errorf(CodeInvalidTransition, "only a running run can suspend")
	}
	interrupt := req.Interrupt
	if interrupt.ID == "" || !req.Now.Before(interrupt.ExpiresAt) {
		return Interrupt{}, errorf(CodeInvalidArgument, "interrupt id and future expiry are required")
	}
	if interrupt.TokenDigest == ([32]byte{}) {
		return Interrupt{}, errorf(CodeInvalidArgument, "interrupt token digest is required")
	}
	if _, exists := m.interrupts[scoped(req.OrganizationID, interrupt.ID)]; exists {
		return Interrupt{}, errorf(CodeConflict, "interrupt id already exists")
	}
	interrupt.OrganizationID, interrupt.RunID = req.OrganizationID, run.ID
	interrupt.NodeID, interrupt.AttemptID = attempt.NodeID, attempt.ID
	interrupt.Status = InterruptPending
	interrupt.CreatedAt = req.Now
	attempt.Status = AttemptSuspended
	attempt.UpdatedAt = req.Now
	m.attempts[scoped(req.OrganizationID, attempt.ID)] = attempt
	leaseKey := scoped(req.OrganizationID, attempt.ID)
	lease := m.leases[leaseKey]
	lease.WorkerID = ""
	lease.ExpiresAt = req.Now
	lease.UpdatedAt = req.Now
	m.leases[leaseKey] = lease
	nodeKey := nodeScoped(req.OrganizationID, run.ID, attempt.NodeID)
	node := m.nodes[nodeKey]
	node.Status = NodeSuspended
	node.UpdatedAt = req.Now
	m.nodes[nodeKey] = node
	if m.hasRunnableWorkLocked(req.OrganizationID, run.ID, attempt.ID) {
		run.Status = RunRunning
	} else {
		run.Status = RunSuspended
	}
	run.Version++
	run.UpdatedAt = req.Now
	m.runs[scoped(req.OrganizationID, run.ID)] = run
	m.interrupts[scoped(req.OrganizationID, interrupt.ID)] = cloneInterrupt(interrupt)
	if req.Snapshot != nil {
		m.storeSnapshotLocked(req.OrganizationID, run.ID, attempt.ID, req.Now, *req.Snapshot)
	}
	if run.Status == RunSuspended {
		m.appendEventLocked(req.OrganizationID, run.ID, Event{Type: "run.suspended", At: req.Now})
	}
	for _, event := range req.Events {
		m.appendEventLocked(req.OrganizationID, run.ID, event)
	}
	return cloneInterrupt(interrupt), nil
}

// FailAttempt atomically records terminal failure or enqueues one bounded retry.
func (m *MemoryStore) FailAttempt(_ context.Context, req FailAttemptRequest) (Run, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, attempt, run, err := m.requireLeaseLocked(req.OrganizationID, req.RunID, req.AttemptID, req.WorkerID, req.FencingToken, req.Now)
	if err != nil {
		return Run{}, err
	}
	if req.RetryAt != nil {
		if run.Status != RunRunning {
			return Run{}, errorf(CodeInvalidTransition, "only a running run can retry")
		}
		if req.RetryAt.Before(req.Now) {
			return Run{}, errorf(CodeInvalidArgument, "retry time cannot be in the past")
		}
	} else {
		if req.TerminalStatus != RunFailed && req.TerminalStatus != RunError && req.TerminalStatus != RunCancelled {
			return Run{}, errorf(CodeInvalidArgument, "terminal failure status is invalid")
		}
		if err := requireRunTransition(run.Status, req.TerminalStatus); err != nil {
			return Run{}, err
		}
	}
	if req.RetryAt == nil && req.TerminalStatus == RunError {
		attempt.Status = AttemptError
	} else if req.RetryAt == nil && req.TerminalStatus == RunCancelled {
		attempt.Status = AttemptCancelled
	} else {
		attempt.Status = AttemptFailed
	}
	attempt.ErrorCode = req.ErrorCode
	attempt.FinishedAt = timePointer(req.Now)
	attempt.UpdatedAt = req.Now
	m.attempts[scoped(req.OrganizationID, attempt.ID)] = attempt
	delete(m.leases, scoped(req.OrganizationID, attempt.ID))
	if req.RetryAt != nil {
		retry := m.createAttemptLocked(run, attempt.NodeID, attempt.Executor, attempt.Input, *req.RetryAt, attempt.Number+1, attempt.ExecutionID)
		retry.ResumeData = cloneJSON(attempt.ResumeData)
		m.attempts[scoped(req.OrganizationID, retry.ID)] = retry
		m.appendEventLocked(req.OrganizationID, run.ID, Event{Type: "attempt.retry_scheduled", At: req.Now})
	} else {
		run.Status = req.TerminalStatus
		nodeKey := nodeScoped(req.OrganizationID, run.ID, attempt.NodeID)
		node := m.nodes[nodeKey]
		if req.TerminalStatus == RunError {
			node.Status = NodeError
		} else if req.TerminalStatus == RunCancelled {
			node.Status = NodeCancelled
		} else {
			node.Status = NodeFailed
		}
		node.UpdatedAt = req.Now
		m.nodes[nodeKey] = node
		eventType := "run.failed"
		if req.TerminalStatus == RunError {
			eventType = "run.error"
		} else if req.TerminalStatus == RunCancelled {
			eventType = "run.cancelled"
		}
		m.appendEventLocked(req.OrganizationID, run.ID, Event{Type: eventType, At: req.Now})
	}
	run.Version++
	run.UpdatedAt = req.Now
	m.runs[scoped(req.OrganizationID, run.ID)] = run
	for _, event := range req.Events {
		m.appendEventLocked(req.OrganizationID, run.ID, event)
	}
	return cloneRun(run), nil
}

func (m *MemoryStore) requireLeaseLocked(organizationID OrganizationID, runID, attemptID, workerID string, token uint64, now time.Time) (Lease, Attempt, Run, error) {
	attemptKey := scoped(organizationID, attemptID)
	attempt, ok := m.attempts[attemptKey]
	if !ok || attempt.RunID != runID {
		return Lease{}, Attempt{}, Run{}, errorf(CodeNotFound, "attempt not found")
	}
	lease, ok := m.leases[attemptKey]
	if !ok {
		return Lease{}, Attempt{}, Run{}, errorf(CodeLeaseLost, "attempt lease is not owned by worker")
	}
	if lease.FencingToken != token {
		return Lease{}, Attempt{}, Run{}, errorf(CodeStaleFencingToken, "attempt fencing token is stale")
	}
	if lease.WorkerID != workerID {
		return Lease{}, Attempt{}, Run{}, errorf(CodeLeaseLost, "attempt lease is not owned by worker")
	}
	if !now.Before(lease.ExpiresAt) {
		return Lease{}, Attempt{}, Run{}, errorf(CodeLeaseLost, "attempt lease has expired")
	}
	if attempt.Status != AttemptRunning {
		return Lease{}, Attempt{}, Run{}, errorf(CodeConflict, "attempt is not running")
	}
	run, ok := m.runs[scoped(organizationID, runID)]
	if !ok {
		return Lease{}, Attempt{}, Run{}, errorf(CodeNotFound, "run not found")
	}
	return lease, attempt, run, nil
}

func (m *MemoryStore) storeSnapshotLocked(organizationID OrganizationID, runID, attemptID string, now time.Time, snapshot Snapshot) {
	m.nextID++
	if snapshot.ID == "" {
		snapshot.ID = memoryID("snap", m.nextID)
	}
	snapshot.OrganizationID, snapshot.RunID, snapshot.AttemptID = organizationID, runID, attemptID
	snapshot.Sequence = uint64(len(m.snapshots[scoped(organizationID, runID)]) + 1)
	snapshot.Data = cloneJSON(snapshot.Data)
	snapshot.CreatedAt = now
	m.snapshots[scoped(organizationID, runID)] = append(m.snapshots[scoped(organizationID, runID)], snapshot)
}

func completionEventType(status RunStatus) string {
	if status == RunCancelled {
		return "run.cancelled"
	}
	if status == RunCancelling {
		return "attempt.cancelled"
	}
	if status == RunSuccess {
		return "run.succeeded"
	}
	return "attempt.completed"
}

func memoryID(prefix string, id uint64) string { return prefix + "_" + strconv.FormatUint(id, 10) }

func cloneAttempt(attempt Attempt) Attempt {
	attempt.Input = cloneJSON(attempt.Input)
	attempt.ResumeData = cloneJSON(attempt.ResumeData)
	if attempt.StartedAt != nil {
		attempt.StartedAt = timePointer(*attempt.StartedAt)
	}
	if attempt.FinishedAt != nil {
		attempt.FinishedAt = timePointer(*attempt.FinishedAt)
	}
	return attempt
}

func (m *MemoryStore) latestSnapshotLocked(organizationID OrganizationID, runID, attemptID string) *Snapshot {
	stored := m.snapshots[scoped(organizationID, runID)]
	for i := len(stored) - 1; i >= 0; i-- {
		if stored[i].AttemptID != attemptID {
			continue
		}
		snapshot := stored[i]
		snapshot.Data = cloneJSON(snapshot.Data)
		return &snapshot
	}
	return nil
}

func cloneOutput(output Output) Output { output.Inline = cloneJSON(output.Inline); return output }

func marshalDecision(decision ApprovalDecision) ([]byte, error) {
	data, err := json.Marshal(decision)
	if err != nil {
		return nil, &Error{Code: CodeInvalidArgument, Message: "approval decision is invalid", Cause: err}
	}
	return data, nil
}
