package runtime

import (
	"context"
	"crypto/subtle"
	"fmt"
	"sort"
	"sync"
	"time"
)

type idempotencyRecord struct {
	runID       string
	fingerprint string
}

type approvalIdempotencyRecord struct {
	requestHash [32]byte
	result      ResolveInterruptResult
}

type webhookIdempotencyRecord struct {
	requestHash [32]byte
	result      ResumeWebhookResult
}

type streamChunkRecord struct {
	digest [32]byte
	event  Event
}

// MemoryStore is a thread-safe executable specification of Store semantics.
// It is intended for tests and local development, not durable production use.
type MemoryStore struct {
	mu sync.Mutex

	runs                map[string]Run
	idempotency         map[string]idempotencyRecord
	approvalIdempotency map[string]approvalIdempotencyRecord
	webhookIdempotency  map[string]webhookIdempotencyRecord
	nodes               map[string]NodeState
	attempts            map[string]Attempt
	attemptOrder        []string
	leases              map[string]Lease
	events              map[string][]Event
	interrupts          map[string]Interrupt
	outputs             map[string]Output
	snapshots           map[string][]Snapshot
	joinContributions   map[string]map[string]NewAttempt
	subworkflowLinks    map[string]SubworkflowLink
	rerunLineage        map[string]RerunLineage
	streamBytes         map[string]int
	streamChunks        map[string]int
	streamRecords       map[string]map[int]streamChunkRecord
	nextID              uint64
}

// NewMemoryStore constructs an empty in-memory runtime store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		runs: make(map[string]Run), idempotency: make(map[string]idempotencyRecord),
		approvalIdempotency: make(map[string]approvalIdempotencyRecord),
		webhookIdempotency:  make(map[string]webhookIdempotencyRecord),
		nodes:               make(map[string]NodeState), attempts: make(map[string]Attempt),
		leases: make(map[string]Lease), events: make(map[string][]Event),
		interrupts: make(map[string]Interrupt), outputs: make(map[string]Output),
		snapshots: make(map[string][]Snapshot), joinContributions: make(map[string]map[string]NewAttempt), subworkflowLinks: make(map[string]SubworkflowLink),
		rerunLineage: make(map[string]RerunLineage),
		streamBytes:  make(map[string]int), streamChunks: make(map[string]int),
		streamRecords: make(map[string]map[int]streamChunkRecord),
	}
}

// RerunRun atomically validates historical state, creates lineage, and seeds work.
func (m *MemoryStore) RerunRun(_ context.Context, req RerunRunRequest) (CreateRunResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	run := req.Run
	idemKey := scoped(run.TenantID, run.IdempotencyKey)
	if existing, ok := m.idempotency[idemKey]; ok {
		if existing.fingerprint != run.RequestFingerprint {
			return CreateRunResult{}, errorf(CodeIdempotencyConflict, "idempotency key was used for a different request")
		}
		return CreateRunResult{Run: cloneRun(m.runs[scoped(run.TenantID, existing.runID)])}, nil
	}
	source, ok := m.runs[scoped(run.TenantID, req.SourceRunID)]
	if !ok {
		return CreateRunResult{}, errorf(CodeNotFound, "source run not found")
	}
	if !IsTerminalRun(source.Status) {
		return CreateRunResult{}, errorf(CodeInvalidTransition, "only terminal runs can be rerun")
	}
	input := source.Input
	if req.FromNodeID != "" {
		var latest *Attempt
		for _, attempt := range m.attempts {
			candidate := attempt
			if candidate.TenantID == run.TenantID && candidate.RunID == source.ID && candidate.NodeID == req.FromNodeID && (latest == nil || candidate.Number > latest.Number) {
				latest = &candidate
			}
		}
		if latest == nil {
			return CreateRunResult{}, errorf(CodeNotFound, "source node attempt not found")
		}
		input = latest.Input
	}
	run.Plan.ID, run.Plan.Hash, run.Input = source.Plan.ID, source.Plan.Hash, cloneJSON(input)
	lineage := RerunLineage{SourceRunID: source.ID, FromNodeID: req.FromNodeID, CreatedAt: run.CreatedAt}
	run.Rerun = &lineage
	m.runs[scoped(run.TenantID, run.ID)] = run
	m.idempotency[idemKey] = idempotencyRecord{runID: run.ID, fingerprint: run.RequestFingerprint}
	m.rerunLineage[scoped(run.TenantID, run.ID)] = lineage
	m.appendEventLocked(run.TenantID, run.ID, Event{Type: "run.created", At: run.CreatedAt})
	m.createAttemptLocked(run, run.Plan.EntryNodeID, run.Plan.Executor, run.Input, run.CreatedAt, 1, "")
	return CreateRunResult{Run: cloneRun(run), Created: true}, nil
}

// CreateSubworkflow atomically creates a child and immutable parent lineage.
func (m *MemoryStore) CreateSubworkflow(_ context.Context, req CreateSubworkflowRunRequest) (CreateRunResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	link := req.Link
	key := scoped(link.TenantID, link.ParentExecutionID)
	if existing, ok := m.subworkflowLinks[key]; ok {
		record, found := m.idempotency[scoped(req.Run.TenantID, req.Run.IdempotencyKey)]
		if !found || existing.ChildRunID != record.runID || record.fingerprint != req.Run.RequestFingerprint {
			return CreateRunResult{}, errorf(CodeConflict, "parent execution already links another child")
		}
		return CreateRunResult{Run: cloneRun(m.runs[scoped(req.Run.TenantID, record.runID)])}, nil
	}
	parent, ok := m.runs[scoped(link.TenantID, link.ParentRunID)]
	if !ok {
		return CreateRunResult{}, errorf(CodeNotFound, "parent run not found")
	}
	if IsTerminalRun(parent.Status) || parent.CancelRequestedAt != nil {
		return CreateRunResult{}, errorf(CodeInvalidTransition, "parent run cannot start a subworkflow")
	}
	parentAttempt, ok := m.attempts[scoped(link.TenantID, link.ParentAttemptID)]
	if !ok || parentAttempt.RunID != link.ParentRunID || parentAttempt.NodeID != link.ParentNodeID || parentAttempt.ExecutionID != link.ParentExecutionID {
		return CreateRunResult{}, errorf(CodeNotFound, "parent attempt not found")
	}
	result, err := m.createRunLocked(req.Run)
	if err != nil {
		return CreateRunResult{}, err
	}
	link.ChildRunID = result.Run.ID
	m.subworkflowLinks[key] = link
	return result, nil
}

// CreateRun atomically enforces tenant-scoped idempotency and seeds entry work.
func (m *MemoryStore) CreateRun(_ context.Context, req CreateRunRequest) (CreateRunResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.createRunLocked(req.Run)
}

func (m *MemoryStore) createRunLocked(run Run) (CreateRunResult, error) {
	if err := validateTenantAndID(run.TenantID, run.ID); err != nil {
		return CreateRunResult{}, err
	}
	idemKey := scoped(run.TenantID, run.IdempotencyKey)
	if existing, ok := m.idempotency[idemKey]; ok {
		if existing.fingerprint != run.RequestFingerprint {
			return CreateRunResult{}, errorf(CodeIdempotencyConflict, "idempotency key was used for a different request")
		}
		found := cloneRun(m.runs[scoped(run.TenantID, existing.runID)])
		return CreateRunResult{Run: found, Created: false}, nil
	}
	if _, exists := m.runs[scoped(run.TenantID, run.ID)]; exists {
		return CreateRunResult{}, errorf(CodeConflict, "run id already exists")
	}
	if run.Status != RunPending || run.Plan.ID == "" || run.Plan.Hash == "" {
		return CreateRunResult{}, errorf(CodeInvalidArgument, "new run must be pending and bind an immutable plan")
	}
	if run.Plan.EntryNodeID != "" && run.Plan.Executor == "" {
		return CreateRunResult{}, errorf(CodeInvalidArgument, "entry node executor is required")
	}
	run.Input = cloneJSON(run.Input)
	m.runs[scoped(run.TenantID, run.ID)] = run
	m.idempotency[idemKey] = idempotencyRecord{runID: run.ID, fingerprint: run.RequestFingerprint}
	m.appendEventLocked(run.TenantID, run.ID, Event{Type: "run.created", At: run.CreatedAt})
	if run.Plan.EntryNodeID != "" {
		m.createAttemptLocked(run, run.Plan.EntryNodeID, run.Plan.Executor, run.Input, run.CreatedAt, 1, "")
	}
	return CreateRunResult{Run: cloneRun(run), Created: true}, nil
}

// GetRun returns a tenant-scoped run.
func (m *MemoryStore) GetRun(_ context.Context, tenantID TenantID, runID string) (Run, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	run, ok := m.runs[scoped(tenantID, runID)]
	if !ok {
		return Run{}, errorf(CodeNotFound, "run not found")
	}
	return cloneRun(run), nil
}

// RequestCancel persists intent atomically and immediately finalizes idle work.
func (m *MemoryStore) RequestCancel(_ context.Context, tenantID TenantID, runID string, now time.Time) (CancelRunResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	result, err := m.requestCancelLocked(tenantID, runID, now)
	if err != nil {
		return CancelRunResult{}, err
	}
	m.cancelDescendantsLocked(tenantID, runID, now, map[string]bool{runID: true})
	return result, nil
}

func (m *MemoryStore) requestCancelLocked(tenantID TenantID, runID string, now time.Time) (CancelRunResult, error) {
	key := scoped(tenantID, runID)
	run, ok := m.runs[key]
	if !ok {
		return CancelRunResult{}, errorf(CodeNotFound, "run not found")
	}
	if run.CancelRequestedAt != nil || run.Status == RunCancelled {
		return CancelRunResult{Run: cloneRun(run), AlreadyRequested: true}, nil
	}
	if IsTerminalRun(run.Status) {
		return CancelRunResult{}, errorf(CodeInvalidTransition, "terminal run cannot be cancelled")
	}
	run.CancelRequestedAt = timePointer(now)
	active := m.hasActiveAttemptLocked(tenantID, runID)
	if active {
		if err := requireRunTransition(run.Status, RunCancelling); err != nil {
			return CancelRunResult{}, err
		}
		run.Status = RunCancelling
	} else {
		if err := requireRunTransition(run.Status, RunCancelled); err != nil {
			return CancelRunResult{}, err
		}
		run.Status = RunCancelled
		m.cancelIdleAttemptsLocked(tenantID, runID, now)
	}
	run.Version++
	run.UpdatedAt = now
	m.runs[key] = run
	m.appendEventLocked(tenantID, runID, Event{Type: "run.cancel_requested", At: now})
	if run.Status == RunCancelled {
		m.appendEventLocked(tenantID, runID, Event{Type: "run.cancelled", At: now})
	}
	return CancelRunResult{Run: cloneRun(run)}, nil
}

func (m *MemoryStore) cancelDescendantsLocked(tenantID TenantID, parentRunID string, now time.Time, visited map[string]bool) {
	for _, link := range m.subworkflowLinks {
		if link.TenantID != tenantID || link.ParentRunID != parentRunID || visited[link.ChildRunID] {
			continue
		}
		visited[link.ChildRunID] = true
		_, _ = m.requestCancelLocked(tenantID, link.ChildRunID, now)
		m.cancelDescendantsLocked(tenantID, link.ChildRunID, now, visited)
	}
}

// ListEvents returns ordered events strictly after afterCursor.
func (m *MemoryStore) ListEvents(_ context.Context, tenantID TenantID, runID string, afterCursor uint64, limit int) ([]Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.runs[scoped(tenantID, runID)]; !ok {
		return nil, errorf(CodeNotFound, "run not found")
	}
	stored := m.events[scoped(tenantID, runID)]
	result := make([]Event, 0, min(limit, len(stored)))
	for _, event := range stored {
		if event.Cursor <= afterCursor {
			continue
		}
		result = append(result, cloneEvent(event))
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

// ListInterrupts returns only unresolved interrupts in deterministic creation order.
func (m *MemoryStore) ListInterrupts(_ context.Context, tenantID TenantID, runID string) ([]Interrupt, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.runs[scoped(tenantID, runID)]; !ok {
		return nil, errorf(CodeNotFound, "run not found")
	}
	result := make([]Interrupt, 0)
	for _, interrupt := range m.interrupts {
		if interrupt.TenantID == tenantID && interrupt.RunID == runID && interrupt.Status == InterruptPending {
			result = append(result, cloneInterrupt(interrupt))
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].ID < result[j].ID
		}
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})
	return result, nil
}

// ResolveInterrupt consumes one approval and resumes its attempt atomically.
func (m *MemoryStore) ResolveInterrupt(_ context.Context, req ResolveInterruptRequest) (ResolveInterruptResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(req.IdempotencyKey) < 8 || len(req.IdempotencyKey) > 128 {
		return ResolveInterruptResult{}, errorf(CodeInvalidArgument, "idempotency key must contain 8 to 128 characters")
	}
	idempotencyKey := scoped(req.TenantID, req.IdempotencyKey)
	if existing, found := m.approvalIdempotency[idempotencyKey]; found {
		if subtle.ConstantTimeCompare(existing.requestHash[:], req.RequestHash[:]) != 1 {
			return ResolveInterruptResult{}, errorf(CodeIdempotencyConflict, "idempotency key was used for a different request")
		}
		return cloneResolveInterruptResult(existing.result), nil
	}
	key := scoped(req.TenantID, req.InterruptID)
	interrupt, ok := m.interrupts[key]
	if !ok || subtle.ConstantTimeCompare(interrupt.TokenDigest[:], req.TokenDigest[:]) != 1 {
		return ResolveInterruptResult{}, errorf(CodeApprovalInvalid, "approval token is invalid")
	}
	if interrupt.Kind != "approval" && interrupt.Kind != "human_approval" {
		return ResolveInterruptResult{}, errorf(CodeApprovalInvalid, "approval token is invalid")
	}
	if interrupt.Status != InterruptPending || interrupt.ResolvedAt != nil {
		return ResolveInterruptResult{}, errorf(CodeApprovalConsumed, "approval was already resolved")
	}
	if !req.Now.Before(interrupt.ExpiresAt) {
		return ResolveInterruptResult{}, errorf(CodeApprovalExpired, "approval token has expired")
	}
	if req.DecidedBy == "" || req.Decision.DecidedBy != req.DecidedBy {
		return ResolveInterruptResult{}, errorf(CodeInvalidArgument, "approval actor is required and must match the decision")
	}
	runKey := scoped(req.TenantID, interrupt.RunID)
	run := m.runs[runKey]
	if run.Status != RunSuspended && run.Status != RunRunning {
		return ResolveInterruptResult{}, errorf(CodeInvalidTransition, "run cannot resume an interrupt")
	}
	attemptKey := scoped(req.TenantID, interrupt.AttemptID)
	attempt := m.attempts[attemptKey]
	if attempt.Status != AttemptSuspended {
		return ResolveInterruptResult{}, errorf(CodeConflict, "attempt is not suspended")
	}
	decision, err := marshalDecision(req.Decision)
	if err != nil {
		return ResolveInterruptResult{}, err
	}
	interrupt.ResolvedAt = timePointer(req.Now)
	interrupt.Status = InterruptResolved
	interrupt.Decision = decision
	m.interrupts[key] = interrupt
	attempt.Status = AttemptPending
	attempt.ResumeData = cloneJSON(decision)
	attempt.NextAttemptAt = req.Now
	attempt.UpdatedAt = req.Now
	m.attempts[attemptKey] = attempt
	nodeKey := nodeScoped(req.TenantID, attempt.RunID, attempt.NodeID)
	node := m.nodes[nodeKey]
	node.Status = NodePending
	node.UpdatedAt = req.Now
	m.nodes[nodeKey] = node
	if run.Status == RunSuspended {
		run.Status = RunPending
	}
	run.UpdatedAt = req.Now
	run.Version++
	m.runs[runKey] = run
	m.appendEventLocked(req.TenantID, run.ID, Event{Type: "approval.resolved", At: req.Now})
	result := ResolveInterruptResult{Interrupt: cloneInterrupt(interrupt), Run: cloneRun(run)}
	m.approvalIdempotency[idempotencyKey] = approvalIdempotencyRecord{requestHash: req.RequestHash, result: result}
	return cloneResolveInterruptResult(result), nil
}

func cloneResolveInterruptResult(result ResolveInterruptResult) ResolveInterruptResult {
	return ResolveInterruptResult{Interrupt: cloneInterrupt(result.Interrupt), Run: cloneRun(result.Run)}
}

func (m *MemoryStore) appendEventLocked(tenantID TenantID, runID string, event Event) {
	key := scoped(tenantID, runID)
	event.TenantID = tenantID
	event.RunID = runID
	event.Cursor = uint64(len(m.events[key]) + 1)
	event.Data = cloneJSON(event.Data)
	m.events[key] = append(m.events[key], event)
}

func (m *MemoryStore) createAttemptLocked(run Run, nodeID, executor string, input []byte, at time.Time, number int, executionID string) Attempt {
	if existing, ok := m.nodes[nodeScoped(run.TenantID, run.ID, nodeID)]; ok && number <= existing.AttemptCount {
		number = existing.AttemptCount + 1
	}
	m.nextID++
	attemptID := fmt.Sprintf("att_%d", m.nextID)
	if executionID == "" {
		executionID = attemptID
	}
	attempt := Attempt{
		TenantID: run.TenantID, ID: attemptID, ExecutionID: executionID, RunID: run.ID,
		NodeID: nodeID, Executor: executor, Number: number, Status: AttemptPending,
		Input: cloneJSON(input), NextAttemptAt: at, CreatedAt: at, UpdatedAt: at,
	}
	key := scoped(run.TenantID, attempt.ID)
	m.attempts[key] = attempt
	m.attemptOrder = append(m.attemptOrder, key)
	m.nodes[nodeScoped(run.TenantID, run.ID, nodeID)] = NodeState{
		TenantID: run.TenantID, RunID: run.ID, NodeID: nodeID, Status: NodePending,
		AttemptCount: number, LatestAttemptID: attempt.ID, UpdatedAt: at,
	}
	return attempt
}

func scoped(tenantID TenantID, id string) string { return string(tenantID) + "\x00" + id }

func nodeScoped(tenantID TenantID, runID, nodeID string) string {
	return string(tenantID) + "\x00" + runID + "\x00" + nodeID
}

func (m *MemoryStore) hasActiveAttemptLocked(tenantID TenantID, runID string) bool {
	for _, attempt := range m.attempts {
		if attempt.TenantID == tenantID && attempt.RunID == runID && attempt.Status == AttemptRunning {
			return true
		}
	}
	return false
}

func (m *MemoryStore) cancelIdleAttemptsLocked(tenantID TenantID, runID string, now time.Time) {
	for key, attempt := range m.attempts {
		if attempt.TenantID != tenantID || attempt.RunID != runID {
			continue
		}
		if attempt.Status == AttemptPending || attempt.Status == AttemptSuspended {
			attempt.Status = AttemptCancelled
			attempt.FinishedAt = timePointer(now)
			attempt.UpdatedAt = now
			m.attempts[key] = attempt
			nodeKey := nodeScoped(tenantID, runID, attempt.NodeID)
			node := m.nodes[nodeKey]
			node.Status = NodeCancelled
			node.UpdatedAt = now
			m.nodes[nodeKey] = node
		}
	}
	for key, interrupt := range m.interrupts {
		if interrupt.TenantID == tenantID && interrupt.RunID == runID && interrupt.Status == InterruptPending {
			interrupt.Status = InterruptCancelled
			interrupt.ResolvedAt = timePointer(now)
			m.interrupts[key] = interrupt
		}
	}
}

func cloneRun(run Run) Run {
	run.Input = cloneJSON(run.Input)
	if run.Rerun != nil {
		lineage := *run.Rerun
		run.Rerun = &lineage
	}
	if run.Output != nil {
		output := *run.Output
		output.Inline = cloneJSON(output.Inline)
		run.Output = &output
	}
	if run.CancelRequestedAt != nil {
		run.CancelRequestedAt = timePointer(*run.CancelRequestedAt)
	}
	if run.DeadlineAt != nil {
		run.DeadlineAt = timePointer(*run.DeadlineAt)
	}
	return run
}

func cloneEvent(event Event) Event { event.Data = cloneJSON(event.Data); return event }

func cloneInterrupt(interrupt Interrupt) Interrupt {
	interrupt.Decision = cloneJSON(interrupt.Decision)
	if interrupt.ResolvedAt != nil {
		interrupt.ResolvedAt = timePointer(*interrupt.ResolvedAt)
	}
	return interrupt
}

func cloneJSON(value []byte) []byte { return append([]byte(nil), value...) }

func timePointer(value time.Time) *time.Time { copy := value; return &copy }
