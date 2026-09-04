package workflow

import (
	"fmt"
	"sort"
	"strings"
)

const (
	// ShadowStubExecutorKind is the only reviewed shadow stub binding. Runtime
	// attempts persist this value, so restarts cannot fall back to a real adapter.
	ShadowStubExecutorKind = "builtin.shadow-noop.v1"
	// ShadowAttemptExecutorPrefix makes the execution mode durable while retaining
	// the exact real-or-stub executor selected for each attempt.
	ShadowAttemptExecutorPrefix = "shadow-v1:"

	// ShadowBindingMissing reports side-effect nodes without an explicit stub.
	ShadowBindingMissing = "missing"
	// ShadowBindingInvalid reports unknown nodes, safe nodes, or unreviewed stubs.
	ShadowBindingInvalid = "invalid"
)

// ShadowBindingError describes a fail-closed shadow binding violation without
// exposing node configuration or credentials.
type ShadowBindingError struct {
	Kind    string
	NodeIDs []string
}

// Error implements error.
func (e *ShadowBindingError) Error() string {
	if e == nil {
		return "workflow: invalid shadow stub bindings"
	}
	return fmt.Sprintf("workflow: %s shadow stub bindings for nodes %v", e.Kind, e.NodeIDs)
}

// ShadowStubRequired reports whether a plan node may interact with anything
// outside deterministic in-process control flow. The closed allowlist is
// deliberate: newly registered executors remain unsafe until reviewed.
func ShadowStubRequired(node PlanNode) bool {
	permissions := node.Permissions
	if permissions.Network || permissions.Credentials || permissions.Filesystem || permissions.Compute {
		return true
	}
	switch node.Executor.Kind {
	case "builtin.start", "builtin.end", "builtin.condition", "builtin.switch",
		"builtin.loop", "builtin.transform", "builtin.human-approval",
		"builtin.delay", "builtin.wait", "builtin.webhook-resume":
		return false
	default:
		return true
	}
}

// ShadowAttemptExecutor returns the only valid durable executor binding for a
// node in shadow mode. Pure nodes retain their real binding; unsafe nodes bind
// the reviewed stub.
func ShadowAttemptExecutor(node PlanNode) string {
	kind := node.Executor.Kind
	if ShadowStubRequired(node) {
		kind = ShadowStubExecutorKind
	}
	return ShadowAttemptExecutorPrefix + kind
}

// IsShadowAttemptExecutor reports whether an attempt carries the durable
// shadow-mode marker.
func IsShadowAttemptExecutor(kind string) bool {
	return strings.HasPrefix(kind, ShadowAttemptExecutorPrefix)
}

// ValidateShadowStubBindings requires an exact node-to-stub mapping for every
// potentially side-effecting node. Extra bindings are rejected so typos cannot
// create a false impression that a real node was replaced.
func ValidateShadowStubBindings(plan *ExecutionPlan, bindings map[string]string) error {
	if plan == nil {
		return &ShadowBindingError{Kind: ShadowBindingInvalid}
	}
	required := make(map[string]struct{})
	missing := make([]string, 0)
	for _, node := range plan.Nodes() {
		if !ShadowStubRequired(node) {
			continue
		}
		required[node.ID] = struct{}{}
		if bindings[node.ID] == "" {
			missing = append(missing, node.ID)
		}
	}
	invalid := make([]string, 0)
	for nodeID, kind := range bindings {
		_, needed := required[nodeID]
		if !needed || kind != ShadowStubExecutorKind {
			invalid = append(invalid, nodeID)
		}
	}
	if len(invalid) > 0 {
		sort.Strings(invalid)
		return &ShadowBindingError{Kind: ShadowBindingInvalid, NodeIDs: invalid}
	}
	if len(missing) > 0 {
		return &ShadowBindingError{Kind: ShadowBindingMissing, NodeIDs: missing}
	}
	return nil
}
