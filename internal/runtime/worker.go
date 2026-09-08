package runtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"math/big"
	"time"
)

const detachedWorkerCleanupTimeout = 5 * time.Second

// CryptoIDGenerator generates opaque IDs using cryptographic randomness.
type CryptoIDGenerator struct{}

// NewID returns a prefixed 128-bit random identifier.
func (CryptoIDGenerator) NewID(prefix string) (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return prefix + "_" + hex.EncodeToString(buf), nil
}

// ExecutionRequest is the executor-facing attempt contract.
type ExecutionRequest struct {
	OrganizationID OrganizationID
	RunID          string
	Plan           PlanRef
	NodeID         string
	AttemptID      string
	ExecutionID    string
	Attempt        int
	Executor       string
	Input          json.RawMessage
	ResumeData     json.RawMessage
	Snapshot       json.RawMessage
	FencingToken   uint64
	Takeover       bool
}

// Suspension asks the orchestrator to persist an approval interrupt.
type Suspension struct {
	Kind       string
	ExpiresIn  time.Duration
	Snapshot   json.RawMessage
	AutoResume bool
}

// ExecutionResult is one successful executor call. Success may either complete
// the run, enqueue explicit successors, or suspend for approval.
type ExecutionResult struct {
	Output   *Output
	Snapshot *Snapshot
	Next     []NewAttempt
	Suspend  *Suspension
	Final    bool
}

// Executor runs and best-effort cancels node attempts. Implementations must use
// ExecutionRequest.FencingToken or a derived idempotency key for side effects.
type Executor interface {
	Execute(context.Context, ExecutionRequest) (ExecutionResult, error)
	Cancel(context.Context, ExecutionRequest) error
}

// StreamingExecutor optionally emits durable JSON deltas while producing the
// same final ExecutionResult as Execute. Emit returning an error must stop the
// provider stream so stale fencing can never publish further chunks.
type StreamingExecutor interface {
	ExecuteStream(context.Context, ExecutionRequest, func(json.RawMessage) error) (ExecutionResult, error)
}

// ExecutionError classifies executor failures for bounded retry.
type ExecutionError struct {
	Code           string
	Retryable      bool
	TerminalStatus RunStatus
	Cause          error
}

// Error implements error without exposing the underlying diagnostic cause.
func (e *ExecutionError) Error() string {
	if e == nil || e.Code == "" {
		return "executor failed"
	}
	return "executor failed: " + e.Code
}

// Unwrap returns the diagnostic cause for trusted logging.
func (e *ExecutionError) Unwrap() error { return e.Cause }

// RetryPolicy bounds retries and computes exponential backoff with jitter.
type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Jitter      float64
}

// Validate checks that the retry policy is finite and well formed.
func (p RetryPolicy) Validate() error {
	if p.MaxAttempts < 1 {
		return errorf(CodeInvalidArgument, "max attempts must be at least one")
	}
	if p.BaseDelay <= 0 || p.MaxDelay < p.BaseDelay {
		return errorf(CodeInvalidArgument, "retry delays are invalid")
	}
	if p.Jitter < 0 || p.Jitter > 1 {
		return errorf(CodeInvalidArgument, "retry jitter must be between zero and one")
	}
	return nil
}

// Backoff returns a capped exponential delay for the failed attempt number.
func (p RetryPolicy) Backoff(attemptNumber int) time.Duration {
	exponent := max(attemptNumber-1, 0)
	factor := math.Pow(2, float64(exponent))
	delay := time.Duration(float64(p.BaseDelay) * factor)
	if delay > p.MaxDelay || delay < 0 {
		delay = p.MaxDelay
	}
	if p.Jitter == 0 {
		return delay
	}
	span := int64(float64(delay) * p.Jitter)
	if span <= 0 {
		return delay
	}
	n, err := rand.Int(rand.Reader, big.NewInt(span*2+1))
	if err != nil {
		return delay
	}
	return delay - time.Duration(span) + time.Duration(n.Int64())
}

// Orchestrator atomically claims and advances at most one attempt per call.
// Scheduling loops belong to process adapters so each iteration remains bounded.
type Orchestrator struct {
	store         Store
	executor      Executor
	clock         Clock
	ids           IDGenerator
	workerID      string
	leaseDuration time.Duration
	cancelTimeout time.Duration
	retry         RetryPolicy
	resumeTokens  *ResumeTokenIssuer
	telemetry     Telemetry
}

// OrchestratorConfig configures bounded worker execution.
type OrchestratorConfig struct {
	WorkerID      string
	LeaseDuration time.Duration
	CancelTimeout time.Duration
	RetryPolicy   RetryPolicy
	ResumeTokens  *ResumeTokenIssuer
	Telemetry     Telemetry
}

var errRunCancellationRequested = errors.New("run cancellation requested")
var errRunDeadlineElapsed = errors.New("run deadline elapsed")

// NewOrchestrator constructs an orchestrator after validating finite policies.
func NewOrchestrator(store Store, executor Executor, clock Clock, ids IDGenerator, cfg OrchestratorConfig) (*Orchestrator, error) {
	if store == nil || executor == nil || cfg.ResumeTokens == nil {
		return nil, errorf(CodeInvalidArgument, "store, executor, and resume token issuer are required")
	}
	if cfg.WorkerID == "" || cfg.LeaseDuration <= 0 {
		return nil, errorf(CodeInvalidArgument, "worker id and positive lease duration are required")
	}
	if cfg.CancelTimeout == 0 {
		cfg.CancelTimeout = detachedWorkerCleanupTimeout
	}
	if cfg.CancelTimeout < 0 {
		return nil, errorf(CodeInvalidArgument, "cancel timeout must be positive")
	}
	if err := cfg.RetryPolicy.Validate(); err != nil {
		return nil, err
	}
	if clock == nil {
		clock = wallClock{}
	}
	if ids == nil {
		ids = CryptoIDGenerator{}
	}
	return &Orchestrator{
		store: store, executor: executor, clock: clock, ids: ids,
		workerID: cfg.WorkerID, leaseDuration: cfg.LeaseDuration, cancelTimeout: cfg.CancelTimeout, retry: cfg.RetryPolicy,
		resumeTokens: cfg.ResumeTokens,
		telemetry:    cfg.Telemetry,
	}, nil
}

// AdvanceResult reports one bounded claim iteration. Raw approval tokens never
// leave the shared issuer through the worker result.
type AdvanceResult struct {
	Worked    bool
	Run       *Run
	Interrupt *Interrupt
}

// AdvanceNext dynamically discovers, claims, and processes at most one attempt.
func (o *Orchestrator) AdvanceNext(ctx context.Context) (AdvanceResult, error) {
	store, ok := o.store.(DynamicStore)
	if !ok {
		return AdvanceResult{}, errorf(CodeConflict, "dynamic work claiming is unavailable")
	}
	now := o.clock.Now()
	work, err := store.ClaimNext(ctx, ClaimNextAttemptRequest{WorkerID: o.workerID, Now: now, LeaseDuration: o.leaseDuration})
	if err != nil || work == nil {
		return AdvanceResult{}, err
	}
	return o.advanceWork(ctx, work)
}

// Advance claims and processes at most one attempt for organizationID.
func (o *Orchestrator) Advance(ctx context.Context, organizationID OrganizationID) (AdvanceResult, error) {
	if o.telemetry != nil {
		traced, complete := o.telemetry.Start(ctx, "runtime.attempt", string(organizationID))
		result, err := o.advance(traced, organizationID)
		complete(err)
		return result, err
	}
	return o.advance(ctx, organizationID)
}

func (o *Orchestrator) advance(ctx context.Context, organizationID OrganizationID) (AdvanceResult, error) {
	if err := validateOrganization(organizationID); err != nil {
		return AdvanceResult{}, err
	}
	now := o.clock.Now()
	if _, err := o.store.ExpireRuns(ctx, organizationID, now, 100); err != nil {
		return AdvanceResult{}, err
	}
	claimCtx := ctx
	completeClaim := func(error) {}
	if o.telemetry != nil {
		claimCtx, completeClaim = o.telemetry.Start(ctx, "runtime.claim", string(organizationID))
	}
	work, err := o.store.ClaimAttempt(claimCtx, ClaimAttemptRequest{
		OrganizationID: organizationID, WorkerID: o.workerID, Now: now, LeaseDuration: o.leaseDuration,
	})
	completeClaim(err)
	if err != nil || work == nil {
		return AdvanceResult{}, err
	}
	return o.advanceWork(ctx, work)
}

func (o *Orchestrator) advanceWork(ctx context.Context, work *WorkItem) (AdvanceResult, error) {
	organizationID := work.Attempt.OrganizationID
	req := ExecutionRequest{
		OrganizationID: organizationID, RunID: work.Run.ID, Plan: work.Run.Plan, NodeID: work.Attempt.NodeID,
		AttemptID: work.Attempt.ID, ExecutionID: work.Attempt.ExecutionID, Attempt: work.Attempt.Number,
		Executor: work.Attempt.Executor, Input: cloneJSON(work.Attempt.Input),
		ResumeData:   cloneJSON(work.Attempt.ResumeData),
		FencingToken: work.Lease.FencingToken,
		Takeover:     work.Takeover,
	}
	if work.Snapshot != nil {
		req.Snapshot = cloneJSON(work.Snapshot.Data)
	}
	if work.Run.Status == RunCancelling {
		// Cancel is a best-effort remote-runner signal. The durable cancellation
		// transition must not be held hostage by an adapter that already lost the
		// process or returns an error while cleaning it up.
		run, completeErr := o.store.CompleteAttempt(ctx, CompleteAttemptRequest{
			OrganizationID: organizationID, RunID: work.Run.ID, AttemptID: work.Attempt.ID,
			WorkerID: o.workerID, FencingToken: work.Lease.FencingToken,
			Now: o.clock.Now(), RunStatus: RunCancelled,
		})
		o.cancelExecutorBounded(ctx, req)
		return AdvanceResult{Worked: true, Run: &run}, completeErr
	}
	result, executeErr, leaseErr := o.executeWithLease(ctx, work, req)
	if errors.Is(leaseErr, errRunCancellationRequested) {
		run, completeErr := o.store.CompleteAttempt(ctx, CompleteAttemptRequest{
			OrganizationID: organizationID, RunID: work.Run.ID, AttemptID: work.Attempt.ID,
			WorkerID: o.workerID, FencingToken: work.Lease.FencingToken,
			Now: o.clock.Now(), RunStatus: RunCancelled,
		})
		return AdvanceResult{Worked: true, Run: &run}, completeErr
	}
	if errors.Is(leaseErr, errRunDeadlineElapsed) {
		run, readErr := o.store.GetRun(ctx, organizationID, work.Run.ID)
		return AdvanceResult{Worked: true, Run: &run}, readErr
	}
	if leaseErr != nil {
		// The worker no longer owns the attempt. Never submit failure or output
		// with the stale token; another worker may already be executing it.
		return AdvanceResult{Worked: true}, leaseErr
	}
	if executeErr != nil {
		return o.fail(ctx, work, executeErr)
	}
	if result.Suspend != nil {
		return o.suspend(ctx, work, result)
	}
	// The store derives terminal success from global durable quiescence. A
	// single End must never finish a run while another fan-out path is active.
	status := RunRunning
	run, err := o.store.CompleteAttempt(ctx, CompleteAttemptRequest{
		OrganizationID: organizationID, RunID: work.Run.ID, AttemptID: work.Attempt.ID,
		WorkerID: o.workerID, FencingToken: work.Lease.FencingToken,
		Now: o.clock.Now(), RunStatus: status, Output: result.Output,
		Snapshot: result.Snapshot, Next: result.Next,
	})
	return AdvanceResult{Worked: true, Run: &run}, err
}

func (o *Orchestrator) executeWithLease(ctx context.Context, work *WorkItem, req ExecutionRequest) (ExecutionResult, error, error) {
	executionCtx, cancelExecution := context.WithCancel(ctx)
	if work.Run.DeadlineAt != nil {
		deadlineCtx, cancelDeadline := context.WithDeadline(executionCtx, *work.Run.DeadlineAt)
		defer cancelDeadline()
		executionCtx = deadlineCtx
	}
	leaseCtx, stopRenewal := context.WithCancel(ctx)
	renewalResult := make(chan error, 1)
	go func() {
		renewalResult <- o.renewLeaseUntilDone(leaseCtx, cancelExecution, work, req)
	}()

	var result ExecutionResult
	var executeErr error
	if streaming, ok := o.executor.(StreamingExecutor); ok {
		streamSequence := 0
		result, executeErr = streaming.ExecuteStream(executionCtx, req, func(chunk json.RawMessage) error {
			streamSequence++
			_, appendErr := o.store.AppendStreamChunk(executionCtx, AppendStreamChunkRequest{
				OrganizationID: req.OrganizationID, RunID: req.RunID, AttemptID: req.AttemptID,
				WorkerID: o.workerID, FencingToken: req.FencingToken, Sequence: streamSequence,
				Now: o.clock.Now(), Chunk: chunk,
			})
			return appendErr
		})
	} else {
		result, executeErr = o.executor.Execute(executionCtx, req)
	}
	stopRenewal()
	renewalErr := <-renewalResult
	if work.Run.DeadlineAt != nil && !time.Now().UTC().Before(*work.Run.DeadlineAt) {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), detachedWorkerCleanupTimeout)
		_, _ = o.store.ExpireRun(cleanupCtx, work.Run.OrganizationID, work.Run.ID, time.Now().UTC())
		o.cancelExecutorBounded(cleanupCtx, req)
		cancelCleanup()
		renewalErr = errRunDeadlineElapsed
	}
	cancelExecution()
	return result, executeErr, renewalErr
}

func (o *Orchestrator) renewLeaseUntilDone(ctx context.Context, cancelExecution context.CancelFunc, work *WorkItem, req ExecutionRequest) error {
	interval := o.leaseDuration / 3
	if interval <= 0 {
		interval = time.Nanosecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if work.Run.DeadlineAt != nil && !o.clock.Now().Before(*work.Run.DeadlineAt) {
				cancelExecution()
				cleanupCtx, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), detachedWorkerCleanupTimeout)
				_, _ = o.store.ExpireRun(cleanupCtx, work.Run.OrganizationID, work.Run.ID, o.clock.Now())
				o.cancelExecutorBounded(cleanupCtx, req)
				cancelCleanup()
				return errRunDeadlineElapsed
			}
			_, err := o.store.RenewLease(ctx, RenewLeaseRequest{
				OrganizationID: work.Run.OrganizationID, RunID: work.Run.ID, AttemptID: work.Attempt.ID,
				WorkerID: o.workerID, FencingToken: work.Lease.FencingToken,
				Now: o.clock.Now(), LeaseDuration: o.leaseDuration,
			})
			if err != nil {
				cancelExecution()
				return err
			}
			run, err := o.store.GetRun(ctx, work.Run.OrganizationID, work.Run.ID)
			if err != nil {
				cancelExecution()
				return err
			}
			if run.Status == RunCancelling || run.CancelRequestedAt != nil {
				// Keep the explicit cancellation SPI even for in-process executors:
				// remote runners need this signal in addition to context cancellation.
				cancelExecution()
				o.cancelExecutorBounded(ctx, req)
				return errRunCancellationRequested
			}
		}
	}
}

// cancelExecutorBounded prevents a broken adapter from blocking the durable
// worker loop. Implementations are still required to honor context; the
// goroutine boundary contains a non-compliant adapter to this one signal.
func (o *Orchestrator) cancelExecutorBounded(ctx context.Context, req ExecutionRequest) {
	cancelCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), o.cancelTimeout)
	defer cancel()
	done := make(chan struct{}, 1)
	go func() {
		_ = o.executor.Cancel(cancelCtx, req)
		done <- struct{}{}
	}()
	select {
	case <-done:
	case <-cancelCtx.Done():
	}
}

func (o *Orchestrator) fail(ctx context.Context, work *WorkItem, executeErr error) (AdvanceResult, error) {
	executionCode := "executor_failed"
	retryable := false
	terminal := RunFailed
	var classified *ExecutionError
	if errors.As(executeErr, &classified) {
		if classified.Code != "" {
			executionCode = classified.Code
		}
		retryable = classified.Retryable
		if classified.TerminalStatus == RunError {
			terminal = RunError
		}
	}
	now := o.clock.Now()
	var retryAt *time.Time
	if retryable && work.Attempt.Number < o.retry.MaxAttempts && work.Run.Status != RunCancelling {
		next := now.Add(o.retry.Backoff(work.Attempt.Number))
		retryAt = &next
		terminal = ""
	}
	run, err := o.store.FailAttempt(ctx, FailAttemptRequest{
		OrganizationID: work.Run.OrganizationID, RunID: work.Run.ID, AttemptID: work.Attempt.ID,
		WorkerID: o.workerID, FencingToken: work.Lease.FencingToken,
		Now: now, ErrorCode: executionCode, RetryAt: retryAt, TerminalStatus: terminal,
	})
	if err != nil {
		return AdvanceResult{Worked: true}, err
	}
	return AdvanceResult{Worked: true, Run: &run}, nil
}

func (o *Orchestrator) suspend(ctx context.Context, work *WorkItem, result ExecutionResult) (AdvanceResult, error) {
	now := o.clock.Now()
	if result.Suspend.ExpiresIn <= 0 {
		return o.fail(ctx, work, &ExecutionError{Code: "invalid_suspension", Retryable: false})
	}
	interruptID, err := o.ids.NewID("int")
	if err != nil {
		return AdvanceResult{Worked: true}, &Error{Code: CodeConflict, Message: "could not allocate interrupt id", Cause: err}
	}
	interrupt := Interrupt{
		OrganizationID: work.Run.OrganizationID, ID: interruptID, RunID: work.Run.ID,
		NodeID: work.Attempt.NodeID, AttemptID: work.Attempt.ID,
		Kind: result.Suspend.Kind, Status: InterruptPending,
		ExpiresAt: now.Add(result.Suspend.ExpiresIn), AutoResume: result.Suspend.AutoResume, CreatedAt: now,
	}
	token, err := o.resumeTokens.Issue(interrupt)
	if err != nil {
		return AdvanceResult{Worked: true}, &Error{Code: CodeConflict, Message: "could not issue approval token", Cause: err}
	}
	interrupt.TokenDigest = ResumeTokenDigest(token)
	var snapshot *Snapshot
	if len(result.Suspend.Snapshot) > 0 {
		snapshot = &Snapshot{Data: cloneJSON(result.Suspend.Snapshot)}
	}
	stored, err := o.store.SuspendAttempt(ctx, SuspendAttemptRequest{
		OrganizationID: work.Run.OrganizationID, RunID: work.Run.ID, AttemptID: work.Attempt.ID,
		WorkerID: o.workerID, FencingToken: work.Lease.FencingToken,
		Now: now, Interrupt: interrupt, Snapshot: snapshot,
	})
	if err != nil {
		return AdvanceResult{Worked: true}, err
	}
	return AdvanceResult{Worked: true, Interrupt: &stored}, nil
}
