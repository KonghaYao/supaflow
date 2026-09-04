package runtime

import "context"

// ListRuns returns the in-memory monitor projection for tests and local use.
func (m *MemoryStore) ListRuns(_ context.Context, tenantID TenantID, filter RunFilter) ([]RunSummary, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	items := make([]RunSummary, 0, filter.Limit)
	for _, run := range m.runs {
		if run.TenantID != tenantID || (filter.Status != "" && run.Status != filter.Status) ||
			(filter.WorkflowID != "" && run.Plan.WorkflowID != filter.WorkflowID) {
			continue
		}
		items = append(items, RunSummary{Run: cloneRun(run)})
	}
	sortRunSummaries(items)
	if len(items) > filter.Limit {
		items = items[:filter.Limit]
	}
	return items, nil
}

// GetRunDetails returns all logical nodes and attempts in one tenant.
func (m *MemoryStore) GetRunDetails(_ context.Context, tenantID TenantID, runID string) (RunDetails, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	run, found := m.runs[scoped(tenantID, runID)]
	if !found {
		return RunDetails{}, errorf(CodeNotFound, "run not found")
	}
	byNode := make(map[string]*NodeDiagnostic)
	for _, node := range m.nodes {
		if node.TenantID != tenantID || node.RunID != runID {
			continue
		}
		copy := NodeDiagnostic{Node: node, Attempts: []AttemptDiagnostic{}}
		byNode[node.NodeID] = &copy
	}
	for _, attempt := range m.attempts {
		if attempt.TenantID != tenantID || attempt.RunID != runID {
			continue
		}
		node := byNode[attempt.NodeID]
		if node == nil {
			continue
		}
		node.Attempts = append(node.Attempts, AttemptDiagnostic{Attempt: cloneAttempt(attempt)})
	}
	for _, output := range m.outputs {
		if output.TenantID != tenantID || output.RunID != runID {
			continue
		}
		node := byNode[output.NodeID]
		if node == nil {
			continue
		}
		projection := RunOutput{Inline: cloneJSON(output.Inline), ContentType: output.ContentType,
			Size: output.Size, SHA256: output.SHA256, Sensitive: output.Sensitive, External: output.ObjectRef != ""}
		node.Output = &projection
	}
	nodes := make([]NodeDiagnostic, 0, len(byNode))
	for _, node := range byNode {
		nodes = append(nodes, *node)
	}
	sortNodeDiagnostics(nodes)
	return RunDetails{Run: cloneRun(run), Nodes: nodes, LastEventCursor: uint64(len(m.events[scoped(tenantID, runID)]))}, nil
}

var _ MonitorStore = (*MemoryStore)(nil)
