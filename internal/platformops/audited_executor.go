package platformops

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

// AuditRecorder is the append-only evidence seam required by high-risk execution.
type AuditRecorder interface {
	Record(context.Context, AuditRecord) (AuditRecord, error)
}

// AuditedExecutor fails closed when execution authorization evidence cannot be persisted.
type AuditedExecutor struct {
	next  workflowruntime.Executor
	audit AuditRecorder
}

// NewAuditedExecutor wraps an executor with allow, deny, fail, and cancel evidence.
func NewAuditedExecutor(next workflowruntime.Executor, audit AuditRecorder) (*AuditedExecutor, error) {
	if next == nil || audit == nil {
		return nil, ErrInvalidArgument
	}
	return &AuditedExecutor{next: next, audit: audit}, nil
}

// Execute records an allow before the side effect and its eventual outcome afterward.
func (e *AuditedExecutor) Execute(ctx context.Context, request workflowruntime.ExecutionRequest) (workflowruntime.ExecutionResult, error) {
	return e.execute(ctx, request, nil)
}

// ExecuteStream preserves audit evidence while forwarding optional streaming.
func (e *AuditedExecutor) ExecuteStream(ctx context.Context, request workflowruntime.ExecutionRequest, emit func(json.RawMessage) error) (workflowruntime.ExecutionResult, error) {
	return e.execute(ctx, request, emit)
}

func (e *AuditedExecutor) execute(ctx context.Context, request workflowruntime.ExecutionRequest, emit func(json.RawMessage) error) (workflowruntime.ExecutionResult, error) {
	metadata, _ := json.Marshal(map[string]any{"runId": request.RunID, "nodeId": request.NodeID, "executor": request.Executor, "fencingToken": request.FencingToken})
	base := AuditRecord{TenantID: string(request.TenantID), ActorID: request.AttemptID, Action: "execution.execute", ResourceType: "attempt", ResourceID: request.AttemptID, Metadata: metadata}
	base.Outcome = AuditAllowed
	if _, err := e.audit.Record(ctx, base); err != nil {
		return workflowruntime.ExecutionResult{}, &workflowruntime.ExecutionError{Code: "audit_unavailable", Retryable: true, Cause: err}
	}
	var result workflowruntime.ExecutionResult
	var err error
	if stream, ok := e.next.(workflowruntime.StreamingExecutor); ok && emit != nil {
		result, err = stream.ExecuteStream(ctx, request, emit)
	} else {
		result, err = e.next.Execute(ctx, request)
	}
	if err == nil {
		return result, nil
	}
	base.Outcome = executionFailureOutcome(err)
	_, auditErr := e.audit.Record(context.WithoutCancel(ctx), base)
	return result, errors.Join(err, auditErr)
}

// Cancel delegates cancellation and records whether the high-risk action failed.
func (e *AuditedExecutor) Cancel(ctx context.Context, request workflowruntime.ExecutionRequest) error {
	err := e.next.Cancel(ctx, request)
	outcome := AuditAllowed
	if err != nil {
		outcome = AuditFailed
	}
	_, auditErr := e.audit.Record(context.WithoutCancel(ctx), AuditRecord{
		TenantID: string(request.TenantID), ActorID: request.AttemptID, Action: "execution.cancel",
		ResourceType: "attempt", ResourceID: request.AttemptID, Outcome: outcome,
	})
	return errors.Join(err, auditErr)
}

func executionFailureOutcome(err error) AuditOutcome {
	var executionErr *workflowruntime.ExecutionError
	if errors.As(err, &executionErr) {
		code := strings.ToLower(executionErr.Code)
		if strings.Contains(code, "denied") || strings.Contains(code, "forbidden") || strings.Contains(code, "policy") {
			return AuditDenied
		}
	}
	return AuditFailed
}
