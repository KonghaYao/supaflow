package runtime

import (
	"encoding/json"
	"time"
)

const (
	// DefaultTenantMaxActiveRuns is the bootstrap run concurrency limit used
	// only until an administrator persists an explicit tenant quota.
	DefaultTenantMaxActiveRuns = 100
	// DefaultTenantMaxActiveWorkers is the bootstrap worker reservation limit.
	DefaultTenantMaxActiveWorkers = 50
	// DefaultTenantMaxQueueDepth is the bootstrap durable run backlog limit.
	DefaultTenantMaxQueueDepth = 1000
)

// TenantID is the authenticated tenant boundary for every runtime operation.
type TenantID string

// RunStatus is the durable lifecycle state of a workflow run.
type RunStatus string

const (
	RunPending    RunStatus = "PENDING"
	RunRunning    RunStatus = "RUNNING"
	RunSuspended  RunStatus = "SUSPENDED"
	RunCancelling RunStatus = "CANCELLING"
	RunSuccess    RunStatus = "SUCCESS"
	RunFailed     RunStatus = "FAILED"
	RunError      RunStatus = "ERROR"
	RunCancelled  RunStatus = "CANCELLED"
)

// NodeStatus is the logical state of a node across its physical attempts.
type NodeStatus string

const (
	NodePending   NodeStatus = "PENDING"
	NodeRunning   NodeStatus = "RUNNING"
	NodeSuspended NodeStatus = "SUSPENDED"
	NodeSuccess   NodeStatus = "SUCCESS"
	NodeFailed    NodeStatus = "FAILED"
	NodeError     NodeStatus = "ERROR"
	NodeCancelled NodeStatus = "CANCELLED"
)

// AttemptStatus is the state of one physical node execution.
type AttemptStatus string

const (
	AttemptPending   AttemptStatus = "PENDING"
	AttemptRunning   AttemptStatus = "RUNNING"
	AttemptSuspended AttemptStatus = "SUSPENDED"
	AttemptSuccess   AttemptStatus = "SUCCESS"
	AttemptFailed    AttemptStatus = "FAILED"
	AttemptError     AttemptStatus = "ERROR"
	AttemptCancelled AttemptStatus = "CANCELLED"
)

// InterruptStatus describes whether an external action is still actionable.
type InterruptStatus string

const (
	InterruptPending   InterruptStatus = "PENDING"
	InterruptResolved  InterruptStatus = "RESOLVED"
	InterruptExpired   InterruptStatus = "EXPIRED"
	InterruptCancelled InterruptStatus = "CANCELLED"
)

// PlanRef binds a run to one immutable execution plan and its content hash.
// Entry is optional so the run module does not depend on compiler types.
type PlanRef struct {
	ID          string
	Hash        string
	WorkflowID  string
	EntryNodeID string
	Executor    string
}

// Run is the tenant-scoped durable workflow execution aggregate.
type Run struct {
	TenantID           TenantID
	ID                 string
	Plan               PlanRef
	Status             RunStatus
	Priority           int
	Input              json.RawMessage
	IdempotencyKey     string
	RequestFingerprint string
	Output             *RunOutput
	Rerun              *RerunLineage
	DeadlineAt         *time.Time
	CancelRequestedAt  *time.Time
	CreatedAt          time.Time
	UpdatedAt          time.Time
	Version            uint64
}

// RerunLineage records the immutable source and optional restart node of a rerun.
type RerunLineage struct {
	SourceRunID string
	FromNodeID  string
	CreatedAt   time.Time
}

// RunOutput is the safe terminal output projection exposed by a run. Inline
// contains JSON only for database-inline values; externally stored values are
// represented by metadata without exposing an internal object-store locator.
type RunOutput struct {
	Inline          json.RawMessage
	ContentType     string
	Size            int64
	SHA256          string
	Sensitive       bool
	External        bool
	RetentionStatus string
}

// NodeState is a logical node projection across retries and lease takeovers.
type NodeState struct {
	TenantID        TenantID
	RunID           string
	NodeID          string
	Status          NodeStatus
	AttemptCount    int
	LatestAttemptID string
	UpdatedAt       time.Time
}

// Attempt is one physical execution of a logical node.
type Attempt struct {
	TenantID TenantID
	ID       string
	// ExecutionID is stable for one logical node activation across physical retries.
	// External deduplication must bind to this value rather than ID.
	ExecutionID   string
	RunID         string
	NodeID        string
	Executor      string
	Number        int
	Status        AttemptStatus
	Input         json.RawMessage
	ResumeData    json.RawMessage
	NextAttemptAt time.Time
	StartedAt     *time.Time
	FinishedAt    *time.Time
	ErrorCode     string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// Lease grants one worker temporary ownership with a monotonically increasing
// fencing token. A token is only meaningful for its attempt.
type Lease struct {
	TenantID     TenantID
	RunID        string
	AttemptID    string
	WorkerID     string
	FencingToken uint64
	ExpiresAt    time.Time
	UpdatedAt    time.Time
}

// Event is an immutable run event. Cursor is monotonically increasing per run.
type Event struct {
	TenantID TenantID
	RunID    string
	Cursor   uint64
	Type     string
	Data     json.RawMessage
	At       time.Time
}

// Interrupt is a durable, single-use suspension awaiting an external decision.
type Interrupt struct {
	TenantID    TenantID
	ID          string
	RunID       string
	NodeID      string
	AttemptID   string
	Kind        string
	Status      InterruptStatus
	TokenDigest [32]byte
	ExpiresAt   time.Time
	ResolvedAt  *time.Time
	Decision    json.RawMessage
	AutoResume  bool
	CreatedAt   time.Time
}

// Output describes a bounded result payload or an external object reference.
type Output struct {
	TenantID    TenantID
	ID          string
	RunID       string
	NodeID      string
	AttemptID   string
	ContentType string
	Size        int64
	SHA256      string
	Sensitive   bool
	Inline      json.RawMessage
	ObjectRef   string
	CreatedAt   time.Time
}

// Snapshot is the durable orchestration checkpoint committed with an attempt.
type Snapshot struct {
	TenantID  TenantID
	ID        string
	RunID     string
	AttemptID string
	Sequence  uint64
	Data      json.RawMessage
	CreatedAt time.Time
}

// SubworkflowLink is immutable parent-child execution lineage.
type SubworkflowLink struct {
	TenantID        TenantID
	ParentRunID     string
	ParentNodeID    string
	ParentAttemptID string
	// ParentExecutionID is stable across physical attempt retries and owns the
	// child-run idempotency relation.
	ParentExecutionID string
	ChildRunID        string
	CreatedAt         time.Time
}

// ApprovalDecision is the external decision stored for an interrupt.
type ApprovalDecision struct {
	Approved  bool            `json:"approved"`
	Comment   string          `json:"comment,omitempty"`
	Data      json.RawMessage `json:"data,omitempty"`
	DecidedBy string          `json:"decidedBy"`
}
