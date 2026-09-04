package executor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

const defaultOutputCleanupTimeout = 5 * time.Second

// ExternalOutput is the safe result of persisting bytes outside the runtime database.
// ObjectRef is an opaque tenant-scoped ID and must never contain a storage key.
type ExternalOutput struct {
	ObjectRef string
	Size      int64
	SHA256    string
}

// RunOutputReader resolves a run's output through the tenant-owned runtime
// relation rather than accepting an untrusted object reference directly.
type RunOutputReader interface {
	// ReadRunOutput resolves bytes only through a tenant-owned run relation and
	// verifies them against the runtime's immutable output metadata.
	ReadRunOutput(context.Context, workflowruntime.TenantID, string, workflowruntime.RunOutput) (json.RawMessage, error)
}

// OutputManager persists, reads, and compensates executor outputs outside the runtime database.
type OutputManager interface {
	RunOutputReader
	StoreOutput(context.Context, workflowruntime.TenantID, string, string, []byte) (ExternalOutput, error)
	DiscardOutput(context.Context, workflowruntime.TenantID, string) error
}

func (e *PlanExecutor) makeOutput(ctx context.Context, req workflowruntime.ExecutionRequest, node workflow.PlanNode, value json.RawMessage) (*workflowruntime.Output, error) {
	if !json.Valid(value) {
		return nil, executionError("invalid_output", false, errors.New("executor output is not JSON"))
	}
	digest := sha256.Sum256(value)
	encodedDigest := hex.EncodeToString(digest[:])
	output := &workflowruntime.Output{
		TenantID: req.TenantID, RunID: req.RunID, NodeID: req.NodeID, AttemptID: req.AttemptID,
		ContentType: "application/json", Size: int64(len(value)), SHA256: encodedDigest, Sensitive: node.SensitiveOutput,
	}
	if int64(len(value)) <= node.OutputLimit && !node.SensitiveOutput {
		output.Inline = cloneJSON(value)
		return output, nil
	}
	if e.outputs == nil {
		code := "output_limit_exceeded"
		if node.SensitiveOutput {
			code = "sensitive_output_unavailable"
		}
		return nil, executionError(code, false, errors.New("executor output requires unavailable external storage"))
	}
	external, err := e.outputs.StoreOutput(ctx, req.TenantID, req.RunID, output.ContentType, value)
	if err != nil {
		return nil, executionError("output_storage_failed", true, errors.New("executor output could not be stored"))
	}
	if external.ObjectRef == "" || external.Size != output.Size || external.SHA256 != output.SHA256 {
		if external.ObjectRef != "" {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), e.cleanupWait)
			_ = e.outputs.DiscardOutput(cleanupCtx, req.TenantID, external.ObjectRef)
			cancel()
		}
		return nil, executionError("output_storage_invalid", false, errors.New("stored output metadata did not match executor output"))
	}
	output.ObjectRef = external.ObjectRef
	return output, nil
}

// ManagedOutputStore compensates pre-written objects when the fenced runtime
// completion rejects them or atomically resolves a cancellation race.
type ManagedOutputStore struct {
	workflowruntime.Store
	outputs     OutputManager
	cleanupWait time.Duration
}

// NewManagedOutputStore decorates the durable runtime store with object cleanup.
func NewManagedOutputStore(store workflowruntime.Store, outputs OutputManager) (*ManagedOutputStore, error) {
	if store == nil || outputs == nil {
		return nil, errors.New("executor: runtime store and output manager are required")
	}
	return &ManagedOutputStore{Store: store, outputs: outputs, cleanupWait: defaultOutputCleanupTimeout}, nil
}

// CompleteAttempt delegates the atomic runtime commit and removes any object
// which the commit rejected or discarded because cancellation won the race.
func (s *ManagedOutputStore) CompleteAttempt(ctx context.Context, req workflowruntime.CompleteAttemptRequest) (workflowruntime.Run, error) {
	run, err := s.Store.CompleteAttempt(ctx, req)
	if req.Output == nil || req.Output.ObjectRef == "" || (err == nil && run.Status != workflowruntime.RunCancelled) {
		return run, err
	}
	// A lost commit acknowledgement may hide a successful transaction that now
	// references this object. Retention can reclaim an actual orphan later;
	// eager compensation here could destroy a committed successful output.
	if workflowruntime.IsCommitOutcomeUnknown(err) {
		return run, err
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.cleanupWait)
	defer cancel()
	cleanupErr := s.outputs.DiscardOutput(cleanupCtx, req.TenantID, req.Output.ObjectRef)
	if cleanupErr == nil {
		return run, err
	}
	cleanupErr = fmt.Errorf("discard uncommitted output: %w", cleanupErr)
	if err != nil {
		return run, errors.Join(err, cleanupErr)
	}
	return run, cleanupErr
}

var _ workflowruntime.Store = (*ManagedOutputStore)(nil)
