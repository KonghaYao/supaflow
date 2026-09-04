package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

// Clock supplies deterministic time to services and workers.
type Clock interface {
	Now() time.Time
}

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now().UTC() }

// IDGenerator creates opaque, non-sequential externally visible identifiers.
type IDGenerator interface {
	NewID(prefix string) (string, error)
}

// Service provides tenant-scoped runtime application operations.
type Service struct {
	store        Store
	clock        Clock
	ids          IDGenerator
	resumeTokens *ResumeTokenIssuer
	telemetry    Telemetry
}

// SetResumeTokenIssuer enables verification for the public webhook-resume
// operation. Configure it during process assembly before serving requests.
func (s *Service) SetResumeTokenIssuer(issuer *ResumeTokenIssuer) { s.resumeTokens = issuer }

// SetTelemetry enables tenant-hashed runtime operation spans.
func (s *Service) SetTelemetry(telemetry Telemetry) { s.telemetry = telemetry }

// NewService constructs a runtime application service.
func NewService(store Store, clock Clock, ids IDGenerator) *Service {
	if clock == nil {
		clock = wallClock{}
	}
	if ids == nil {
		ids = CryptoIDGenerator{}
	}
	return &Service{store: store, clock: clock, ids: ids}
}

// StartRunRequest binds a run to an immutable plan and idempotency key.
type StartRunRequest struct {
	Plan           PlanRef
	Input          json.RawMessage
	IdempotencyKey string
	Priority       int
	Timeout        time.Duration
}

// RerunRequest creates a fresh run from a terminal run, optionally at one plan node.
type RerunRequest struct {
	SourceRunID    string
	FromNodeID     string
	EntryNodeID    string
	EntryExecutor  string
	IdempotencyKey string
	Priority       int
	Timeout        time.Duration
}

const (
	minimumRunTimeout = time.Second
	maximumRunTimeout = 30 * 24 * time.Hour
)

// Rerun creates a new run bound to the source immutable plan. The store owns
// terminal-state validation and historical input selection atomically.
func (s *Service) Rerun(ctx context.Context, tenantID TenantID, req RerunRequest) (CreateRunResult, error) {
	if err := validateTenantAndID(tenantID, req.SourceRunID); err != nil {
		return CreateRunResult{}, err
	}
	if strings.TrimSpace(req.EntryNodeID) == "" || strings.TrimSpace(req.EntryExecutor) == "" {
		return CreateRunResult{}, errorf(CodeInvalidArgument, "rerun entry node and executor are required")
	}
	if len(req.IdempotencyKey) < 8 || len(req.IdempotencyKey) > 128 {
		return CreateRunResult{}, errorf(CodeInvalidArgument, "idempotency key must contain 8 to 128 characters")
	}
	if err := validatePriority(req.Priority); err != nil {
		return CreateRunResult{}, err
	}
	if err := validateRunTimeout(req.Timeout); err != nil {
		return CreateRunResult{}, err
	}
	fingerprint := rerunFingerprint(req.SourceRunID, req.FromNodeID, req.Priority, req.Timeout)
	runID, err := s.ids.NewID("run")
	if err != nil {
		return CreateRunResult{}, &Error{Code: CodeConflict, Message: "could not allocate run id", Cause: err}
	}
	now := s.clock.Now()
	deadline := deadlineFromTimeout(now, req.Timeout)
	return s.store.RerunRun(ctx, RerunRunRequest{
		Run: Run{TenantID: tenantID, ID: runID, Plan: PlanRef{EntryNodeID: req.EntryNodeID, Executor: req.EntryExecutor},
			Status: RunPending, Priority: req.Priority, IdempotencyKey: req.IdempotencyKey, RequestFingerprint: fingerprint,
			DeadlineAt: deadline, CreatedAt: now, UpdatedAt: now, Version: 1},
		SourceRunID: req.SourceRunID, FromNodeID: req.FromNodeID,
	})
}

// StartSubworkflowRequest starts one child and records immutable lineage.
type StartSubworkflowRequest struct {
	ParentRunID       string
	ParentNodeID      string
	ParentAttemptID   string
	ParentExecutionID string
	Plan              PlanRef
	Input             json.RawMessage
	IdempotencyKey    string
}

// StartSubworkflow idempotently creates a child run and links it to its parent attempt.
func (s *Service) StartSubworkflow(ctx context.Context, tenantID TenantID, req StartSubworkflowRequest) (CreateRunResult, error) {
	if req.ParentRunID == "" || req.ParentNodeID == "" || req.ParentAttemptID == "" || req.ParentExecutionID == "" {
		return CreateRunResult{}, errorf(CodeInvalidArgument, "subworkflow parent scope is required")
	}
	parent, err := s.store.GetRun(ctx, tenantID, req.ParentRunID)
	if err != nil {
		return CreateRunResult{}, err
	}
	child, err := s.prepareRun(tenantID, StartRunRequest{Plan: req.Plan, Input: req.Input, IdempotencyKey: req.IdempotencyKey})
	if err != nil {
		return CreateRunResult{}, err
	}
	// A child may choose an earlier deadline in the future, but can never
	// outlive its parent. Today subworkflow nodes inherit the exact parent bound.
	if parent.DeadlineAt != nil {
		deadline := *parent.DeadlineAt
		child.Run.DeadlineAt = &deadline
	}
	return s.store.CreateSubworkflow(ctx, CreateSubworkflowRunRequest{Run: child.Run, Link: SubworkflowLink{
		TenantID: tenantID, ParentRunID: req.ParentRunID, ParentNodeID: req.ParentNodeID,
		ParentAttemptID: req.ParentAttemptID, ParentExecutionID: req.ParentExecutionID, ChildRunID: child.Run.ID, CreatedAt: child.Run.CreatedAt,
	}})
}

// StartRun creates a run exactly once per tenant and idempotency key. Reusing a
// key with a different plan hash or input returns CodeIdempotencyConflict.
func (s *Service) StartRun(ctx context.Context, tenantID TenantID, req StartRunRequest) (CreateRunResult, error) {
	if s.telemetry != nil {
		traced, complete := s.telemetry.Start(ctx, "runtime.start", string(tenantID))
		result, err := s.startRun(traced, tenantID, req)
		complete(err)
		return result, err
	}
	return s.startRun(ctx, tenantID, req)
}

func (s *Service) startRun(ctx context.Context, tenantID TenantID, req StartRunRequest) (CreateRunResult, error) {
	prepared, err := s.prepareRun(tenantID, req)
	if err != nil {
		return CreateRunResult{}, err
	}
	return s.store.CreateRun(ctx, prepared)
}

func (s *Service) prepareRun(tenantID TenantID, req StartRunRequest) (CreateRunRequest, error) {
	if err := validateTenant(tenantID); err != nil {
		return CreateRunRequest{}, err
	}
	if strings.TrimSpace(req.Plan.ID) == "" || strings.TrimSpace(req.Plan.Hash) == "" {
		return CreateRunRequest{}, errorf(CodeInvalidArgument, "plan id and hash are required")
	}
	if req.Plan.EntryNodeID != "" && strings.TrimSpace(req.Plan.Executor) == "" {
		return CreateRunRequest{}, errorf(CodeInvalidArgument, "entry node executor is required")
	}
	if strings.TrimSpace(req.IdempotencyKey) == "" || len(req.IdempotencyKey) > 255 {
		return CreateRunRequest{}, errorf(CodeInvalidArgument, "idempotency key must contain 1 to 255 characters")
	}
	if err := validatePriority(req.Priority); err != nil {
		return CreateRunRequest{}, err
	}
	if err := validateRunTimeout(req.Timeout); err != nil {
		return CreateRunRequest{}, err
	}
	if len(req.Input) == 0 {
		req.Input = json.RawMessage(`null`)
	}
	if !json.Valid(req.Input) {
		return CreateRunRequest{}, errorf(CodeInvalidArgument, "input must be valid JSON")
	}
	input := make(json.RawMessage, 0, len(req.Input))
	input = append(input, req.Input...)
	fingerprint, err := requestFingerprint(req.Plan, input, req.Priority, req.Timeout)
	if err != nil {
		return CreateRunRequest{}, err
	}
	runID, err := s.ids.NewID("run")
	if err != nil {
		return CreateRunRequest{}, &Error{Code: CodeConflict, Message: "could not allocate run id", Cause: err}
	}
	now := s.clock.Now()
	deadline := deadlineFromTimeout(now, req.Timeout)
	return CreateRunRequest{Run: Run{
		TenantID: tenantID, ID: runID, Plan: req.Plan, Status: RunPending, Priority: req.Priority,
		Input: input, IdempotencyKey: req.IdempotencyKey,
		RequestFingerprint: fingerprint, DeadlineAt: deadline, CreatedAt: now, UpdatedAt: now, Version: 1,
	}}, nil
}

// GetRun returns a run only inside tenantID.
func (s *Service) GetRun(ctx context.Context, tenantID TenantID, runID string) (Run, error) {
	if err := validateTenantAndID(tenantID, runID); err != nil {
		return Run{}, err
	}
	return s.store.GetRun(ctx, tenantID, runID)
}

// CancelRun persists cancellation intent before executor cancellation is attempted.
func (s *Service) CancelRun(ctx context.Context, tenantID TenantID, runID string) (CancelRunResult, error) {
	if err := validateTenantAndID(tenantID, runID); err != nil {
		return CancelRunResult{}, err
	}
	return s.store.RequestCancel(ctx, tenantID, runID, s.clock.Now())
}

// ListEvents returns events strictly after cursor, ordered by cursor.
func (s *Service) ListEvents(ctx context.Context, tenantID TenantID, runID string, afterCursor uint64, limit int) ([]Event, error) {
	if err := validateTenantAndID(tenantID, runID); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 1000 {
		return nil, errorf(CodeInvalidArgument, "event limit must be between 1 and 1000")
	}
	return s.store.ListEvents(ctx, tenantID, runID, afterCursor, limit)
}

// ListInterrupts returns unresolved, unexpired or expired pending interrupts for
// a run. The token digest is cleared because it is not part of the API model.
func (s *Service) ListInterrupts(ctx context.Context, tenantID TenantID, runID string) ([]Interrupt, error) {
	if err := validateTenantAndID(tenantID, runID); err != nil {
		return nil, err
	}
	interrupts, err := s.store.ListInterrupts(ctx, tenantID, runID)
	if err != nil {
		return nil, err
	}
	for i := range interrupts {
		interrupts[i].TokenDigest = [32]byte{}
	}
	return interrupts, nil
}

// ResolveApprovalCommand is the authenticated, idempotent approval input.
type ResolveApprovalCommand struct {
	ActorID        string
	InterruptID    string
	ResumeToken    string
	IdempotencyKey string
	Decision       ApprovalDecision
}

// ResolveApproval consumes a raw resume token once. Only its digest and a
// request fingerprint reach the store, which resolves or replays atomically.
func (s *Service) ResolveApproval(ctx context.Context, tenantID TenantID, command ResolveApprovalCommand) (ResolveInterruptResult, error) {
	if err := validateTenantAndID(tenantID, command.InterruptID); err != nil {
		return ResolveInterruptResult{}, err
	}
	if command.ResumeToken == "" {
		return ResolveInterruptResult{}, errorf(CodeApprovalInvalid, "approval token is required")
	}
	if strings.TrimSpace(command.ActorID) == "" {
		return ResolveInterruptResult{}, errorf(CodeInvalidArgument, "approval actor id is required")
	}
	if len(command.IdempotencyKey) < 8 || len(command.IdempotencyKey) > 128 {
		return ResolveInterruptResult{}, errorf(CodeInvalidArgument, "idempotency key must contain 8 to 128 characters")
	}
	digest := ResumeTokenDigest(command.ResumeToken)
	command.Decision.DecidedBy = command.ActorID
	requestHash, err := approvalRequestHash(command.InterruptID, digest, command.Decision)
	if err != nil {
		return ResolveInterruptResult{}, err
	}
	return s.store.ResolveInterrupt(ctx, ResolveInterruptRequest{
		TenantID: tenantID, InterruptID: command.InterruptID, TokenDigest: digest,
		IdempotencyKey: command.IdempotencyKey, RequestHash: requestHash,
		DecidedBy: command.ActorID, Decision: command.Decision, Now: s.clock.Now(),
	})
}

func approvalRequestHash(interruptID string, tokenDigest [32]byte, decision ApprovalDecision) ([32]byte, error) {
	payload, err := json.Marshal(struct {
		InterruptID string           `json:"interruptId"`
		TokenDigest [32]byte         `json:"tokenDigest"`
		Decision    ApprovalDecision `json:"decision"`
	}{InterruptID: interruptID, TokenDigest: tokenDigest, Decision: decision})
	if err != nil {
		return [32]byte{}, errorf(CodeInvalidArgument, "approval decision is invalid")
	}
	return sha256.Sum256(payload), nil
}

func requestFingerprint(plan PlanRef, input json.RawMessage, priority int, timeout time.Duration) (string, error) {
	compact := make([]byte, 0, len(input))
	compact, err := json.Marshal(json.RawMessage(input))
	if err != nil {
		return "", errorf(CodeInvalidArgument, "input must be valid JSON")
	}
	h := sha256.New()
	h.Write([]byte(plan.ID))
	h.Write([]byte{0})
	h.Write([]byte(plan.Hash))
	h.Write([]byte{0})
	h.Write([]byte(strconv.Itoa(priority)))
	h.Write([]byte{0})
	h.Write([]byte(strconv.FormatInt(timeout.Milliseconds(), 10)))
	h.Write([]byte{0})
	h.Write(compact)
	// Existing normal-run fingerprints remain byte-for-byte stable. Shadow mode
	// changes execution semantics, so it receives a disjoint idempotency scope.
	if strings.HasPrefix(plan.Executor, "shadow-v1:") {
		h.Write([]byte("\x00shadow-v1"))
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func rerunFingerprint(sourceRunID, fromNodeID string, priority int, timeout time.Duration) string {
	h := sha256.New()
	h.Write([]byte("rerun\x00"))
	h.Write([]byte(sourceRunID))
	h.Write([]byte{0})
	h.Write([]byte(fromNodeID))
	h.Write([]byte{0})
	h.Write([]byte(strconv.Itoa(priority)))
	h.Write([]byte{0})
	h.Write([]byte(strconv.FormatInt(timeout.Milliseconds(), 10)))
	return hex.EncodeToString(h.Sum(nil))
}

func validateRunTimeout(timeout time.Duration) error {
	if timeout == 0 {
		return nil
	}
	if timeout < minimumRunTimeout || timeout > maximumRunTimeout {
		return errorf(CodeInvalidArgument, "run timeout must be between 1 second and 30 days")
	}
	return nil
}

func deadlineFromTimeout(now time.Time, timeout time.Duration) *time.Time {
	if timeout == 0 {
		return nil
	}
	deadline := now.Add(timeout)
	return &deadline
}

func validatePriority(priority int) error {
	if priority < -1000 || priority > 1000 {
		return errorf(CodeInvalidArgument, "priority must be between -1000 and 1000")
	}
	return nil
}

func validateTenant(tenantID TenantID) error {
	if strings.TrimSpace(string(tenantID)) == "" {
		return errorf(CodeInvalidArgument, "tenant id is required")
	}
	return nil
}

func validateTenantAndID(tenantID TenantID, id string) error {
	if err := validateTenant(tenantID); err != nil {
		return err
	}
	if strings.TrimSpace(id) == "" {
		return errorf(CodeInvalidArgument, "resource id is required")
	}
	return nil
}
