package runtime

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
)

// RunFilter is a bounded tenant-scoped query for recent runs.
type RunFilter struct {
	WorkflowID string
	Status     RunStatus
	Limit      int
}

// RunSummary is the safe list projection used by the Studio monitor.
type RunSummary struct {
	Run        Run
	WorkflowID string
}

// AttemptDiagnostic is a safe attempt projection. Input is recursively
// redacted and failure details are limited to a stable code.
type AttemptDiagnostic struct {
	Attempt Attempt
}

// NodeDiagnostic is a logical node with its ordered physical attempts and
// safe output metadata/value.
type NodeDiagnostic struct {
	Node     NodeState
	Attempts []AttemptDiagnostic
	Output   *RunOutput
}

// RunDetails is the complete tenant-scoped monitor projection.
type RunDetails struct {
	Run             Run
	Nodes           []NodeDiagnostic
	LastEventCursor uint64
}

// MonitorStore is an optional read model implemented by production and local
// stores without widening the transactional runtime command port.
type MonitorStore interface {
	ListRuns(context.Context, TenantID, RunFilter) ([]RunSummary, error)
	GetRunDetails(context.Context, TenantID, string) (RunDetails, error)
}

// ListRuns returns recent runs using bounded filters inside one tenant.
func (s *Service) ListRuns(ctx context.Context, tenantID TenantID, filter RunFilter) ([]RunSummary, error) {
	if err := validateTenant(tenantID); err != nil {
		return nil, err
	}
	if filter.Limit <= 0 || filter.Limit > 200 || (filter.Status != "" && !validRunStatus(filter.Status)) {
		return nil, errorf(CodeInvalidArgument, "run filter is invalid")
	}
	store, ok := s.store.(MonitorStore)
	if !ok {
		return nil, errorf(CodeConflict, "run monitor is unavailable")
	}
	return store.ListRuns(ctx, tenantID, filter)
}

// GetRunDetails returns node attempts, safe inputs/outputs, and failure codes.
func (s *Service) GetRunDetails(ctx context.Context, tenantID TenantID, runID string) (RunDetails, error) {
	if err := validateTenantAndID(tenantID, runID); err != nil {
		return RunDetails{}, err
	}
	store, ok := s.store.(MonitorStore)
	if !ok {
		return RunDetails{}, errorf(CodeConflict, "run monitor is unavailable")
	}
	details, err := store.GetRunDetails(ctx, tenantID, runID)
	if err != nil {
		return RunDetails{}, err
	}
	redactRunDetails(&details)
	return details, nil
}

func validRunStatus(status RunStatus) bool {
	switch status {
	case RunPending, RunRunning, RunSuspended, RunCancelling, RunSuccess, RunFailed, RunError, RunCancelled:
		return true
	default:
		return false
	}
}

func redactRunDetails(details *RunDetails) {
	details.Run.Input = redactJSON(details.Run.Input)
	if details.Run.Output != nil && details.Run.Output.Sensitive {
		details.Run.Output.Inline = nil
	}
	for nodeIndex := range details.Nodes {
		node := &details.Nodes[nodeIndex]
		if node.Output != nil && node.Output.Sensitive {
			node.Output.Inline = nil
		}
		for attemptIndex := range node.Attempts {
			attempt := &node.Attempts[attemptIndex].Attempt
			attempt.Input = redactJSON(attempt.Input)
			attempt.ResumeData = nil
		}
	}
}

func redactJSON(input json.RawMessage) json.RawMessage {
	if len(input) == 0 {
		return nil
	}
	var value any
	if json.Unmarshal(input, &value) != nil {
		return json.RawMessage(`null`)
	}
	redactValue(value)
	encoded, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`null`)
	}
	return encoded
}

func redactValue(value any) {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			normalized := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(key, "_", ""), "-", ""))
			if strings.Contains(normalized, "password") || strings.Contains(normalized, "secret") ||
				strings.Contains(normalized, "token") || strings.Contains(normalized, "apikey") || normalized == "authorization" {
				typed[key] = "[REDACTED]"
				continue
			}
			redactValue(child)
		}
	case []any:
		for _, child := range typed {
			redactValue(child)
		}
	}
}

func sortRunSummaries(items []RunSummary) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].Run.CreatedAt.Equal(items[j].Run.CreatedAt) {
			return items[i].Run.ID > items[j].Run.ID
		}
		return items[i].Run.CreatedAt.After(items[j].Run.CreatedAt)
	})
}

func sortNodeDiagnostics(items []NodeDiagnostic) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].Node.UpdatedAt.Equal(items[j].Node.UpdatedAt) {
			return items[i].Node.NodeID < items[j].Node.NodeID
		}
		return items[i].Node.UpdatedAt.Before(items[j].Node.UpdatedAt)
	})
	for index := range items {
		sort.Slice(items[index].Attempts, func(i, j int) bool {
			return items[index].Attempts[i].Attempt.Number < items[index].Attempts[j].Attempt.Number
		})
	}
}
