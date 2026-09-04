// Package executor provides production adapters for executing immutable workflow plans.
package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/pazhou/supaflow/internal/platform"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

const (
	defaultRequestLimit  = int64(1 << 20)
	defaultResponseLimit = int64(4 << 20)
	defaultDialTimeout   = 10 * time.Second
)

// CredentialResolver resolves an opaque tenant-scoped credential reference to
// HTTP headers. Implementations must never return secrets in error messages.
type CredentialResolver interface {
	ResolveHTTPHeaders(context.Context, AttemptScope, string) (http.Header, error)
}

// CredentialAuthorizer persists least-privilege grants for references extracted
// from the already verified immutable plan node before any adapter can resolve them.
type CredentialAuthorizer interface {
	AuthorizeCredentials(context.Context, workflowruntime.ExecutionRequest, workflow.PlanNode) error
}

// DNSResolver resolves every address for an HTTP destination before dialing it.
type DNSResolver interface {
	LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
}

// DialContextFunc opens a policy-approved network address. The executor invokes
// it only after resolving and validating every destination IP, including after
// redirects. It is primarily an adapter seam for controlled network runtimes.
type DialContextFunc func(context.Context, string, string) (net.Conn, error)

// Config defines finite HTTP resource limits and outbound network policy.
// AllowedHosts is an exact, case-insensitive allowlist. A private literal IP is
// permitted only when that exact IP is present; DNS names may never resolve to
// a private or otherwise non-public address.
type Config struct {
	AllowedHosts         []string
	CredentialResolver   CredentialResolver
	CredentialAuthorizer CredentialAuthorizer
	DNSResolver          DNSResolver
	RequestBodyLimit     int64
	ResponseBodyLimit    int64
	DialTimeout          time.Duration
	DialContext          DialContextFunc
	Models               ModelRegistry
	Agents               AgentRegistry
	Tools                ToolRegistry
	Subworkflows         SubworkflowRunner
	Isolation            IsolationRunner
	Outputs              OutputManager
}

// PlanExecutor dispatches nodes from a tenant-owned immutable execution plan.
// It is safe for concurrent use.
type PlanExecutor struct {
	catalog     platform.PlanCatalog
	http        *httpExecutor
	credentials CredentialAuthorizer
	models      ModelRegistry
	agents      AgentRegistry
	tools       ToolRegistry
	subflow     SubworkflowRunner
	isolate     IsolationRunner
	outputs     OutputManager
	cleanupWait time.Duration

	mu     sync.Mutex
	active map[string]*activeAttempt
}

type activeAttempt struct {
	cancel       context.CancelFunc
	fencingToken uint64
}

var _ workflowruntime.Executor = (*PlanExecutor)(nil)
var _ workflowruntime.StreamingExecutor = (*PlanExecutor)(nil)

// NewPlanExecutor validates its dependencies and builds a fail-closed executor.
func NewPlanExecutor(catalog platform.PlanCatalog, cfg Config) (*PlanExecutor, error) {
	if catalog == nil {
		return nil, errors.New("executor: plan catalog is required")
	}
	if cfg.RequestBodyLimit == 0 {
		cfg.RequestBodyLimit = defaultRequestLimit
	}
	if cfg.ResponseBodyLimit == 0 {
		cfg.ResponseBodyLimit = defaultResponseLimit
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = defaultDialTimeout
	}
	if cfg.RequestBodyLimit < 1 || cfg.ResponseBodyLimit < 1 || cfg.DialTimeout < 1 {
		return nil, errors.New("executor: HTTP limits and dial timeout must be positive")
	}
	httpExecutor, err := newHTTPExecutor(cfg)
	if err != nil {
		return nil, err
	}
	return &PlanExecutor{
		catalog: catalog, http: httpExecutor, credentials: cfg.CredentialAuthorizer, models: cfg.Models, agents: cfg.Agents,
		tools: cfg.Tools, subflow: cfg.Subworkflows, isolate: cfg.Isolation, outputs: cfg.Outputs,
		cleanupWait: defaultOutputCleanupTimeout, active: make(map[string]*activeAttempt),
	}, nil
}

// Execute resolves the pinned plan, validates its executor binding, and runs
// exactly one node attempt.
func (e *PlanExecutor) Execute(ctx context.Context, req workflowruntime.ExecutionRequest) (workflowruntime.ExecutionResult, error) {
	return e.execute(ctx, req, nil)
}

// ExecuteStream emits deltas only for plan nodes whose immutable capability
// declares streaming. Other node types deliberately degrade to Execute.
func (e *PlanExecutor) ExecuteStream(ctx context.Context, req workflowruntime.ExecutionRequest, emit func(json.RawMessage) error) (workflowruntime.ExecutionResult, error) {
	return e.execute(ctx, req, emit)
}

func (e *PlanExecutor) execute(ctx context.Context, req workflowruntime.ExecutionRequest, emit func(json.RawMessage) error) (workflowruntime.ExecutionResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, release, err := e.register(ctx, req)
	if err != nil {
		return workflowruntime.ExecutionResult{}, err
	}
	defer release()

	published, err := e.catalog.Resolve(ctx, string(req.TenantID), req.Plan.ID, req.Plan.Hash)
	if err != nil {
		retryable := !errors.Is(err, platform.ErrPlanNotFound)
		return workflowruntime.ExecutionResult{}, executionError("plan_unavailable", retryable, errors.New("immutable plan could not be resolved"))
	}
	if published.Plan == nil || published.ID != req.Plan.ID || published.Hash != req.Plan.Hash || published.Plan.Hash() != req.Plan.Hash {
		return workflowruntime.ExecutionResult{}, executionError("plan_mismatch", false, errors.New("resolved plan did not match its immutable reference"))
	}
	if err := validateExecutablePlan(published.Plan); err != nil {
		return workflowruntime.ExecutionResult{}, err
	}
	node, ok := published.Plan.Node(req.NodeID)
	if !ok {
		return workflowruntime.ExecutionResult{}, executionError("unknown_node", false, errors.New("attempt node is absent from the immutable plan"))
	}
	shadowMode := workflow.IsShadowAttemptExecutor(req.Executor)
	expectedExecutor := node.Executor.Kind
	if shadowMode {
		expectedExecutor = workflow.ShadowAttemptExecutor(node)
	}
	if req.Executor == "" || req.Executor != expectedExecutor {
		return workflowruntime.ExecutionResult{}, executionError("executor_binding_mismatch", false, errors.New("attempt executor differs from the immutable plan"))
	}
	stubbed := shadowMode && workflow.ShadowStubRequired(node)
	if req.Takeover && !stubbed && !node.Capabilities.RecoverySafe {
		return workflowruntime.ExecutionResult{}, executionError("unsafe_lease_takeover", false, errors.New("node cannot be re-executed after an ambiguous lease loss"))
	}
	if stubbed {
		return e.executeShadowStub(ctx, req, node, published.Plan)
	}
	// Sensitive nodes never emit ordinary stream events. Their immutable plan
	// classification forces the final value into protected object storage, and
	// disabling deltas prevents plaintext from leaking through SSE/outbox first.
	if node.SensitiveOutput {
		emit = nil
	}
	if e.credentials != nil && !shadowMode {
		if err := e.credentials.AuthorizeCredentials(ctx, req, node); err != nil {
			return workflowruntime.ExecutionResult{}, executionError("credential_grant_failed", false, errors.New("attempt credential grant was rejected"))
		}
	}
	if len(req.Input) == 0 {
		req.Input = json.RawMessage(`null`)
	}
	if !json.Valid(req.Input) {
		return workflowruntime.ExecutionResult{}, executionError("invalid_input", false, errors.New("attempt input is not JSON"))
	}
	executionCtx, cancelNode := context.WithTimeout(ctx, time.Duration(node.TimeoutMS)*time.Millisecond)
	defer cancelNode()

	var output json.RawMessage
	var result workflowruntime.ExecutionResult
	resolvedResult := false
	switch node.Executor.Kind {
	case "builtin.start":
		output = cloneJSON(req.Input)
	case "builtin.end":
		output = cloneJSON(req.Input)
		result.Final = true
	case "builtin.human-approval":
		result, err = e.executeApproval(executionCtx, req, node, published.Plan)
		resolvedResult = true
	case "http.request":
		output, err = e.http.execute(executionCtx, req, node)
	case "eino.llm":
		if node.Capabilities.Streaming && emit != nil {
			output, err = executeModelStream(executionCtx, e.models, req, node, emit)
		} else {
			output, err = executeModel(executionCtx, e.models, req, node)
		}
	case "eino.agent":
		if node.Capabilities.Streaming && emit != nil {
			output, err = executeAgentStream(executionCtx, e.agents, req, node, emit)
		} else {
			output, err = executeAgent(executionCtx, e.agents, req, node)
		}
	case "eino.tool":
		output, err = executeTool(executionCtx, e.tools, req, node)
	case "builtin.condition", "builtin.switch", "builtin.loop", "builtin.transform":
		result, err = e.executeControl(executionCtx, req, node, published.Plan)
		resolvedResult = true
	case "builtin.delay", "builtin.wait", "builtin.webhook-resume":
		result, err = e.executeWait(executionCtx, req, node, published.Plan)
		resolvedResult = true
	case "builtin.subworkflow":
		result, err = e.executeSubworkflowNode(executionCtx, req, node, published.Plan)
		resolvedResult = true
	case "isolated.shell", "isolated.python":
		output, err = executeIsolated(executionCtx, e.isolate, req, node)
	default:
		return workflowruntime.ExecutionResult{}, executionError("unknown_executor", false, errors.New("plan requested an unsupported executor"))
	}
	if errors.Is(executionCtx.Err(), context.DeadlineExceeded) {
		return workflowruntime.ExecutionResult{}, executionError("node_timeout", false, context.DeadlineExceeded)
	}
	if err != nil {
		return workflowruntime.ExecutionResult{}, applyRetrySafety(node, err)
	}
	if resolvedResult {
		return withShadowSuccessors(published.Plan, result, shadowMode), nil
	}

	if !result.Final {
		result.Next, err = successors(published.Plan, node.ID, output)
		if err != nil {
			return workflowruntime.ExecutionResult{}, err
		}
	}
	result.Output, err = e.makeOutput(executionCtx, req, node, output)
	if err != nil {
		return workflowruntime.ExecutionResult{}, err
	}
	return withShadowSuccessors(published.Plan, result, shadowMode), nil
}

func applyRetrySafety(node workflow.PlanNode, err error) error {
	var classified *workflowruntime.ExecutionError
	if !errors.As(err, &classified) || !classified.Retryable || node.Capabilities.AllowsRetry(classified.Code) {
		return err
	}
	filtered := *classified
	filtered.Retryable = false
	return &filtered
}

// Cancel best-effort cancels an active attempt. It is idempotent and does not
// affect attempts belonging to another tenant or run.
func (e *PlanExecutor) Cancel(_ context.Context, req workflowruntime.ExecutionRequest) error {
	key := attemptKey(req)
	e.mu.Lock()
	active := e.active[key]
	e.mu.Unlock()
	if active != nil && active.fencingToken == req.FencingToken {
		active.cancel()
	}
	return nil
}

func (e *PlanExecutor) register(parent context.Context, req workflowruntime.ExecutionRequest) (context.Context, func(), error) {
	if strings.TrimSpace(string(req.TenantID)) == "" || req.RunID == "" || req.AttemptID == "" || req.FencingToken == 0 {
		return nil, nil, executionError("invalid_execution_request", false, errors.New("attempt scope is incomplete"))
	}
	key := attemptKey(req)
	ctx, cancel := context.WithCancel(parent)
	entry := &activeAttempt{cancel: cancel, fencingToken: req.FencingToken}
	e.mu.Lock()
	if current := e.active[key]; current != nil {
		if req.FencingToken <= current.fencingToken {
			e.mu.Unlock()
			cancel()
			return nil, nil, executionError("attempt_already_running", false, errors.New("attempt is already active in this worker"))
		}
		current.cancel()
	}
	e.active[key] = entry
	e.mu.Unlock()
	return ctx, func() {
		cancel()
		e.mu.Lock()
		if e.active[key] == entry {
			delete(e.active, key)
		}
		e.mu.Unlock()
	}, nil
}

func (e *PlanExecutor) executeApproval(ctx context.Context, req workflowruntime.ExecutionRequest, node workflow.PlanNode, plan *workflow.ExecutionPlan) (workflowruntime.ExecutionResult, error) {
	var cfg approvalConfig
	if err := decodeStrict(node.Config, &cfg); err != nil || strings.TrimSpace(cfg.Prompt) == "" {
		return workflowruntime.ExecutionResult{}, executionError("invalid_node_config", false, errors.New("approval configuration is invalid"))
	}
	if len(req.ResumeData) == 0 {
		expires := time.Duration(node.TimeoutMS) * time.Millisecond
		if cfg.TimeoutMS > 0 {
			expires = time.Duration(cfg.TimeoutMS) * time.Millisecond
		}
		snapshot, _ := json.Marshal(approvalSnapshot{Prompt: cfg.Prompt, Request: cloneJSON(req.Input)})
		if int64(len(snapshot)) > node.OutputLimit {
			return workflowruntime.ExecutionResult{}, executionError("output_limit_exceeded", false, errors.New("approval snapshot exceeded the immutable plan limit"))
		}
		return workflowruntime.ExecutionResult{Suspend: &workflowruntime.Suspension{
			Kind: "approval", ExpiresIn: expires, Snapshot: snapshot,
		}}, nil
	}
	var decision workflowruntime.ApprovalDecision
	if err := decodeStrict(req.ResumeData, &decision); err != nil || strings.TrimSpace(decision.DecidedBy) == "" {
		return workflowruntime.ExecutionResult{}, executionError("invalid_approval_decision", false, errors.New("approval decision is invalid"))
	}
	if !decision.Approved {
		return workflowruntime.ExecutionResult{}, executionError("approval_rejected", false, errors.New("approval was rejected"))
	}
	output, err := json.Marshal(decision)
	if err != nil {
		return workflowruntime.ExecutionResult{}, executionError("invalid_approval_decision", false, errors.New("approval decision could not be encoded"))
	}
	next, err := successors(plan, node.ID, output)
	if err != nil {
		return workflowruntime.ExecutionResult{}, err
	}
	stored, err := e.makeOutput(ctx, req, node, output)
	if err != nil {
		return workflowruntime.ExecutionResult{}, err
	}
	return workflowruntime.ExecutionResult{Output: stored, Next: next}, nil
}

type approvalConfig struct {
	Prompt    string `json:"prompt"`
	TimeoutMS int64  `json:"timeoutMs,omitempty"`
}

type approvalSnapshot struct {
	Prompt  string          `json:"prompt"`
	Request json.RawMessage `json:"request"`
}

// validateExecutablePlan keeps the Phase 1 execution contract honest. Durable
// fan-in needs persisted edge contributions and an atomic join-ready CAS in the
// runtime store; accepting those plans here would duplicate joins after restart.
func validateExecutablePlan(plan *workflow.ExecutionPlan) error {
	if plan == nil {
		return executionError("invalid_plan", false, errors.New("execution plan is absent"))
	}
	if len(plan.Nodes()) == 0 {
		return executionError("invalid_plan", false, errors.New("execution plan has no nodes"))
	}
	return nil
}

func executionError(code string, retryable bool, cause error) *workflowruntime.ExecutionError {
	return &workflowruntime.ExecutionError{Code: code, Retryable: retryable, Cause: cause}
}

func attemptKey(req workflowruntime.ExecutionRequest) string {
	return string(req.TenantID) + "\x00" + req.RunID + "\x00" + req.AttemptID
}

func cloneJSON(value json.RawMessage) json.RawMessage {
	if value == nil {
		return nil
	}
	return append(json.RawMessage(nil), value...)
}

func decodeStrict(value json.RawMessage, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("unexpected trailing JSON")
		}
		return err
	}
	return nil
}
