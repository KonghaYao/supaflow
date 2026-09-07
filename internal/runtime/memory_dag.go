package runtime

import (
	"encoding/json"
	"strings"
	"time"
)

func (m *MemoryStore) enqueueSuccessorLocked(run Run, next NewAttempt, now time.Time) error {
	if (next.EdgeID == "" || len(next.RequiredEdges) <= 1) && !next.AggregateInput {
		at := next.NextAttemptAt
		if at.IsZero() {
			at = now
		}
		m.createAttemptLocked(run, next.NodeID, next.Executor, next.Input, at, 1, "")
		return nil
	}
	if len(next.RequiredEdges) <= 1 {
		input, err := aggregateSingleInput(next)
		if err != nil {
			return err
		}
		at := next.NextAttemptAt
		if at.IsZero() {
			at = now
		}
		m.createAttemptLocked(run, next.NodeID, next.Executor, input, at, 1, "")
		return nil
	}
	joinKey := nodeScoped(run.OrganizationID, run.ID, next.NodeID)
	contributions := m.joinContributions[joinKey]
	if contributions == nil {
		contributions = make(map[string]NewAttempt)
		m.joinContributions[joinKey] = contributions
	}
	if _, exists := contributions[next.EdgeID]; !exists {
		contributions[next.EdgeID] = next
	}
	required := make(map[string]struct{}, len(next.RequiredEdges))
	for _, edgeID := range next.RequiredEdges {
		if edgeID != "" {
			required[edgeID] = struct{}{}
		}
	}
	for edgeID := range required {
		if _, exists := contributions[edgeID]; !exists {
			return nil
		}
	}
	input, err := mergeJoinInput(contributions, required)
	if err != nil {
		return err
	}
	delete(m.joinContributions, joinKey)
	at := next.NextAttemptAt
	if at.IsZero() {
		at = now
	}
	m.createAttemptLocked(run, next.NodeID, next.Executor, input, at, 1, "")
	return nil
}

func mergeJoinInput(contributions map[string]NewAttempt, required map[string]struct{}) (json.RawMessage, error) {
	values := make(map[string]json.RawMessage, len(required))
	for edgeID := range required {
		item := contributions[edgeID]
		for port, literal := range item.StaticInputs {
			if existing, duplicate := values[port]; duplicate && string(existing) != string(literal) {
				return nil, errorf(CodeConflict, "join literal binding mismatch")
			}
			values[port] = cloneJSON(literal)
		}
		port := item.TargetPort
		if port == "" {
			port = item.SourceNodeID
		}
		if port == "" {
			port = edgeID
		}
		if _, duplicate := values[port]; duplicate {
			return nil, errorf(CodeConflict, "join has duplicate target port")
		}
		values[port] = cloneJSON(item.Input)
	}
	raw, err := json.Marshal(values)
	if err != nil {
		return nil, errorf(CodeConflict, "could not encode joined input")
	}
	return raw, nil
}

func aggregateSingleInput(next NewAttempt) (json.RawMessage, error) {
	values := make(map[string]json.RawMessage, len(next.StaticInputs)+1)
	for port, literal := range next.StaticInputs {
		values[port] = cloneJSON(literal)
	}
	port := next.TargetPort
	if port == "" {
		port = next.SourceNodeID
	}
	if port == "" {
		port = "input"
	}
	if _, duplicate := values[port]; duplicate {
		return nil, errorf(CodeConflict, "input has duplicate target port")
	}
	values[port] = cloneJSON(next.Input)
	raw, err := json.Marshal(values)
	if err != nil {
		return nil, errorf(CodeConflict, "could not encode input bindings")
	}
	return raw, nil
}

func (m *MemoryStore) hasUnfinishedWorkLocked(organizationID OrganizationID, runID string) bool {
	for _, attempt := range m.attempts {
		if attempt.OrganizationID == organizationID && attempt.RunID == runID &&
			(attempt.Status == AttemptPending || attempt.Status == AttemptRunning || attempt.Status == AttemptSuspended) {
			return true
		}
	}
	prefix := string(organizationID) + "\x00" + runID + "\x00"
	for key := range m.joinContributions {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

func (m *MemoryStore) remainingRunStatusLocked(organizationID OrganizationID, runID string) RunStatus {
	if m.hasRunnableWorkLocked(organizationID, runID, "") {
		return RunRunning
	}
	for _, attempt := range m.attempts {
		if attempt.OrganizationID == organizationID && attempt.RunID == runID && attempt.Status == AttemptSuspended {
			return RunSuspended
		}
	}
	if m.hasUnfinishedWorkLocked(organizationID, runID) {
		return RunRunning
	}
	return RunSuccess
}

func (m *MemoryStore) hasRunnableWorkLocked(organizationID OrganizationID, runID, excludingAttemptID string) bool {
	for _, attempt := range m.attempts {
		if attempt.OrganizationID == organizationID && attempt.RunID == runID && attempt.ID != excludingAttemptID &&
			(attempt.Status == AttemptPending || attempt.Status == AttemptRunning) {
			return true
		}
	}
	return false
}
