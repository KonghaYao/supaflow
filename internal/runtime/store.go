package runtime

import (
	"context"
	"encoding/json"
	"time"
)

// CreateRunRequest is the atomic input to tenant-scoped idempotent run creation.
type CreateRunRequest struct {
	Run Run
}

// CreateSubworkflowRunRequest atomically creates a child and its immutable
// parent-attempt lineage. Adapters must never expose a child without the link.
type CreateSubworkflowRunRequest struct {
	Run  Run
	Link SubworkflowLink
}

// CreateRunResult distinguishes a new run from an idempotent replay.
type CreateRunResult struct {
	Run     Run
	Created bool
}

// RerunRunRequest atomically creates a new run from terminal historical state.
type RerunRunRequest struct {
	Run         Run
	SourceRunID string
	FromNodeID  string
}

// CancelRunResult describes the durable cancellation intent and current state.
type CancelRunResult struct {
	Run              Run
	AlreadyRequested bool
}

// ResolveInterruptRequest atomically consumes one approval token and makes its
// suspended attempt runnable again.
type ResolveInterruptRequest struct {
	TenantID       TenantID
	InterruptID    string
	TokenDigest    [32]byte
	IdempotencyKey string
	RequestHash    [32]byte
	DecidedBy      string
	Decision       ApprovalDecision
	Now            time.Time
}

// ResolveInterruptResult contains the consumed interrupt and resumed run.
type ResolveInterruptResult struct {
	Interrupt Interrupt
	Run       Run
}

// ResumeWebhookRequest atomically consumes one webhook-only resume token.
// Claims are verified by the token issuer before this persistence boundary and
// rechecked against the locked interrupt row by durable adapters.
type ResumeWebhookRequest struct {
	TenantID       TenantID
	InterruptID    string
	RunID          string
	NodeID         string
	AttemptID      string
	TokenDigest    [32]byte
	IdempotencyKey string
	RequestHash    [32]byte
	Data           json.RawMessage
	Now            time.Time
}

// ResumeWebhookResult contains the consumed webhook interrupt and resumed run.
type ResumeWebhookResult struct {
	Interrupt Interrupt
	Run       Run
}

// AppendStreamChunkRequest persists one bounded executor delta behind the
// active attempt lease. Sequence makes provider replay idempotent after lease
// takeover; it starts at one for each attempt.
type AppendStreamChunkRequest struct {
	TenantID     TenantID
	RunID        string
	AttemptID    string
	WorkerID     string
	FencingToken uint64
	Sequence     int
	Now          time.Time
	Chunk        json.RawMessage
}

// ClaimAttemptRequest claims pending work or takes over an expired lease.
type ClaimAttemptRequest struct {
	TenantID      TenantID
	WorkerID      string
	Now           time.Time
	LeaseDuration time.Duration
}

// WorkItem is the immutable worker view produced by an atomic claim.
type WorkItem struct {
	Run      Run
	Node     NodeState
	Attempt  Attempt
	Lease    Lease
	Snapshot *Snapshot
	Takeover bool
}

// RenewLeaseRequest extends a lease only when worker identity and fencing token
// still match the current owner.
type RenewLeaseRequest struct {
	TenantID      TenantID
	RunID         string
	AttemptID     string
	WorkerID      string
	FencingToken  uint64
	Now           time.Time
	LeaseDuration time.Duration
}

// NewAttempt describes successor work committed atomically with completion.
type NewAttempt struct {
	NodeID        string
	Executor      string
	Input         json.RawMessage
	NextAttemptAt time.Time
	// EdgeID is the immutable control-flow edge delivering this contribution.
	// A store uses it as the join CAS key, making takeover/replay idempotent.
	EdgeID string
	// SourceNodeID and SourcePort identify the persisted upstream value.
	SourceNodeID string
	SourcePort   string
	// TargetPort names the input receiving Input at the successor.
	TargetPort string
	// RequiredEdges is the complete immutable barrier for an all-input join.
	RequiredEdges []string
	// StaticInputs are literal bindings pinned in the immutable plan.
	StaticInputs map[string]json.RawMessage
	// AggregateInput asks the store to construct an object keyed by target port.
	AggregateInput bool
}

// CompleteAttemptRequest atomically commits the output, snapshot, events,
// successor attempts, and resulting run state guarded by the fencing token.
type CompleteAttemptRequest struct {
	TenantID     TenantID
	RunID        string
	AttemptID    string
	WorkerID     string
	FencingToken uint64
	Now          time.Time
	RunStatus    RunStatus
	Output       *Output
	Snapshot     *Snapshot
	Next         []NewAttempt
	Events       []Event
}

// SuspendAttemptRequest atomically suspends an attempt and persists its
// single-use interrupt. Raw approval tokens never cross this store boundary.
type SuspendAttemptRequest struct {
	TenantID     TenantID
	RunID        string
	AttemptID    string
	WorkerID     string
	FencingToken uint64
	Now          time.Time
	Interrupt    Interrupt
	Snapshot     *Snapshot
	Events       []Event
}

// FailAttemptRequest atomically records failure and either creates a bounded
// retry attempt or finalizes the run.
type FailAttemptRequest struct {
	TenantID       TenantID
	RunID          string
	AttemptID      string
	WorkerID       string
	FencingToken   uint64
	Now            time.Time
	ErrorCode      string
	RetryAt        *time.Time
	TerminalStatus RunStatus
	Events         []Event
}

// Store is the durable runtime port. Every public/API operation is tenant
// scoped. Each mutation method represents one database transaction; adapters
// must not split the documented atomic writes across transactions. Event writes
// include their matching outbox delivery in that transaction. Fencing tokens
// are monotonic per attempt, including across suspend and resume; adapters must
// retain the fencing high-water mark when releasing a suspended attempt lease.
type Store interface {
	CreateRun(context.Context, CreateRunRequest) (CreateRunResult, error)
	CreateSubworkflow(context.Context, CreateSubworkflowRunRequest) (CreateRunResult, error)
	RerunRun(context.Context, RerunRunRequest) (CreateRunResult, error)
	GetRun(context.Context, TenantID, string) (Run, error)
	RequestCancel(context.Context, TenantID, string, time.Time) (CancelRunResult, error)
	ListEvents(context.Context, TenantID, string, uint64, int) ([]Event, error)
	ListInterrupts(context.Context, TenantID, string) ([]Interrupt, error)
	// ResolveInterrupt must atomically consume the token, record DecidedBy, copy
	// the decision into Attempt.ResumeData, and make the suspended attempt runnable.
	ResolveInterrupt(context.Context, ResolveInterruptRequest) (ResolveInterruptResult, error)
	ResumeWebhook(context.Context, ResumeWebhookRequest) (ResumeWebhookResult, error)
	// ExpireRuns atomically terminalizes a bounded batch whose durable deadline
	// elapsed. Deleting their leases makes running executors lose fencing.
	ExpireRuns(context.Context, TenantID, time.Time, int) (int, error)
	// ExpireRun terminalizes only runID when its durable deadline elapsed. Worker
	// deadline paths use this targeted mutation so one run cannot expire another.
	ExpireRun(context.Context, TenantID, string, time.Time) (bool, error)
	AppendStreamChunk(context.Context, AppendStreamChunkRequest) (Event, error)

	ClaimAttempt(context.Context, ClaimAttemptRequest) (*WorkItem, error)
	RenewLease(context.Context, RenewLeaseRequest) (Lease, error)
	CompleteAttempt(context.Context, CompleteAttemptRequest) (Run, error)
	SuspendAttempt(context.Context, SuspendAttemptRequest) (Interrupt, error)
	FailAttempt(context.Context, FailAttemptRequest) (Run, error)
}
