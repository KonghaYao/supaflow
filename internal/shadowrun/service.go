// Package shadowrun starts durable comparison runs whose external effects are
// replaced by explicitly reviewed stubs.
package shadowrun

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/pazhou/supaflow/internal/platform"
	"github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

// ErrorCode is a stable application error identifier exposed by adapters.
type ErrorCode string

const (
	// CodeShadowStubRequired means at least one side-effect node lacks the reviewed stub.
	CodeShadowStubRequired ErrorCode = "shadow_stub_required"
	// CodeInvalidShadowStubBinding means a binding targets the wrong node or stub kind.
	CodeInvalidShadowStubBinding ErrorCode = "invalid_shadow_stub_binding"
	// CodeInvalidShadowPlan means the immutable plan cannot be started.
	CodeInvalidShadowPlan ErrorCode = "invalid_shadow_plan"
)

// Error is a safe shadow-run application error. Cause is retained only for
// trusted diagnostics and is never used as an HTTP response title.
type Error struct {
	Code  ErrorCode
	Cause error
}

// Error implements error.
func (e *Error) Error() string {
	if e == nil || e.Code == "" {
		return "shadow run failed"
	}
	return "shadow run failed: " + string(e.Code)
}

// Unwrap returns the diagnostic cause.
func (e *Error) Unwrap() error { return e.Cause }

// StartRequest is the validated application input for one idempotent shadow run.
type StartRequest struct {
	PlanID         string
	PlanHash       string
	Input          json.RawMessage
	StubBindings   map[string]string
	IdempotencyKey string
	Priority       int
	Timeout        time.Duration
}

// Service coordinates immutable-plan validation with durable runtime admission.
type Service struct {
	catalog platform.PlanCatalog
	runtime *runtime.Service
}

// NewService constructs the shadow-run application service.
func NewService(catalog platform.PlanCatalog, runtimeService *runtime.Service) (*Service, error) {
	if catalog == nil || runtimeService == nil {
		return nil, errors.New("shadowrun: catalog and runtime service are required")
	}
	return &Service{catalog: catalog, runtime: runtimeService}, nil
}

// Start verifies the complete stub binding set before creating any run state.
func (s *Service) Start(ctx context.Context, organizationID string, request StartRequest) (runtime.CreateRunResult, error) {
	published, err := s.catalog.Resolve(ctx, organizationID, request.PlanID, request.PlanHash)
	if err != nil {
		return runtime.CreateRunResult{}, err
	}
	if published.Plan == nil {
		return runtime.CreateRunResult{}, &Error{Code: CodeInvalidShadowPlan}
	}
	if err = workflow.ValidateShadowStubBindings(published.Plan, request.StubBindings); err != nil {
		var bindingErr *workflow.ShadowBindingError
		if errors.As(err, &bindingErr) && bindingErr.Kind == workflow.ShadowBindingMissing {
			return runtime.CreateRunResult{}, &Error{Code: CodeShadowStubRequired, Cause: err}
		}
		return runtime.CreateRunResult{}, &Error{Code: CodeInvalidShadowStubBinding, Cause: err}
	}
	order := published.Plan.TopologicalOrder()
	if len(order) == 0 {
		return runtime.CreateRunResult{}, &Error{Code: CodeInvalidShadowPlan}
	}
	entry, found := published.Plan.Node(order[0])
	if !found {
		return runtime.CreateRunResult{}, &Error{Code: CodeInvalidShadowPlan}
	}
	return s.runtime.StartRun(ctx, runtime.OrganizationID(organizationID), runtime.StartRunRequest{
		Plan: runtime.PlanRef{
			ID: published.ID, Hash: published.Hash, WorkflowID: published.WorkflowID,
			EntryNodeID: entry.ID, Executor: workflow.ShadowAttemptExecutor(entry),
		},
		Input: request.Input, IdempotencyKey: request.IdempotencyKey,
		Priority: request.Priority, Timeout: request.Timeout,
	})
}
