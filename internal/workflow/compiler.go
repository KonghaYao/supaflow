package workflow

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

// PlanNode is one fully resolved node in an immutable execution plan.
type PlanNode struct {
	ID              string                  `json:"id"`
	Type            string                  `json:"type"`
	Version         string                  `json:"version"`
	Config          json.RawMessage         `json:"config"`
	Inputs          map[string]InputBinding `json:"inputs,omitempty"`
	Capabilities    CapabilitySet           `json:"capabilities"`
	TimeoutMS       int64                   `json:"timeoutMs"`
	OutputLimit     int64                   `json:"outputLimitBytes"`
	SensitiveOutput bool                    `json:"sensitiveOutput,omitempty"`
	Permissions     PermissionSet           `json:"permissions"`
	Executor        ExecutorBinding         `json:"executor"`
}

// ExecutionPlan is a canonical, content-addressed plan. Its state is private;
// accessors return defensive copies so published plans cannot drift in memory.
type ExecutionPlan struct {
	schemaVersion    string
	workflowID       string
	nodes            []PlanNode
	edges            []EdgeDefinition
	topologicalOrder []string
	hash             string
}

type planSemantics struct {
	SchemaVersion    string           `json:"schemaVersion"`
	WorkflowID       string           `json:"workflowId"`
	Nodes            []PlanNode       `json:"nodes"`
	Edges            []EdgeDefinition `json:"edges"`
	TopologicalOrder []string         `json:"topologicalOrder"`
}

type planEnvelope struct {
	planSemantics
	Hash string `json:"hash"`
}

// ParseExecutionPlan decodes an immutable execution-plan envelope and verifies
// its content hash before exposing it to executors. It does not consult the
// current node registry, because publication permanently pins resolved node
// versions, capabilities, permissions, and executor bindings.
func ParseExecutionPlan(input json.RawMessage) (*ExecutionPlan, error) {
	var envelope planEnvelope
	if err := decodeStrict(input, &envelope); err != nil {
		return nil, fmt.Errorf("workflow: decode execution plan: %w", err)
	}
	if envelope.SchemaVersion != DefinitionSchemaV1 || envelope.WorkflowID == "" {
		return nil, fmt.Errorf("workflow: execution plan identity is invalid")
	}
	recordedHash, err := hex.DecodeString(envelope.Hash)
	if err != nil || len(recordedHash) != sha256.Size || envelope.Hash != hex.EncodeToString(recordedHash) {
		return nil, fmt.Errorf("workflow: execution plan hash is invalid")
	}
	canonical, err := json.Marshal(envelope.planSemantics)
	if err != nil {
		return nil, fmt.Errorf("workflow: encode execution plan semantics: %w", err)
	}
	digest := sha256.Sum256(canonical)
	if subtle.ConstantTimeCompare(recordedHash, digest[:]) != 1 {
		return nil, fmt.Errorf("workflow: execution plan integrity check failed")
	}
	if err := validateParsedPlan(envelope.planSemantics); err != nil {
		return nil, err
	}
	return &ExecutionPlan{
		schemaVersion:    envelope.SchemaVersion,
		workflowID:       envelope.WorkflowID,
		nodes:            clonePlanNodes(envelope.Nodes),
		edges:            append([]EdgeDefinition(nil), envelope.Edges...),
		topologicalOrder: append([]string(nil), envelope.TopologicalOrder...),
		hash:             envelope.Hash,
	}, nil
}

func validateParsedPlan(semantics planSemantics) error {
	nodes := make(map[string]struct{}, len(semantics.Nodes))
	for index, node := range semantics.Nodes {
		if node.ID == "" || node.Type == "" || node.Version == "" || node.Executor.Kind == "" {
			return fmt.Errorf("workflow: execution plan node %d is invalid", index)
		}
		if _, exists := nodes[node.ID]; exists {
			return fmt.Errorf("workflow: execution plan contains duplicate node %q", node.ID)
		}
		nodes[node.ID] = struct{}{}
	}
	if len(nodes) == 0 || len(semantics.TopologicalOrder) != len(nodes) {
		return fmt.Errorf("workflow: execution plan topology is incomplete")
	}
	seenOrder := make(map[string]struct{}, len(nodes))
	orderIndex := make(map[string]int, len(nodes))
	for index, nodeID := range semantics.TopologicalOrder {
		if _, exists := nodes[nodeID]; !exists {
			return fmt.Errorf("workflow: execution plan topology references unknown node %q", nodeID)
		}
		if _, exists := seenOrder[nodeID]; exists {
			return fmt.Errorf("workflow: execution plan topology repeats node %q", nodeID)
		}
		seenOrder[nodeID] = struct{}{}
		orderIndex[nodeID] = index
	}
	for _, edge := range semantics.Edges {
		if _, exists := nodes[edge.FromNode]; !exists {
			return fmt.Errorf("workflow: execution plan edge references unknown source %q", edge.FromNode)
		}
		if _, exists := nodes[edge.ToNode]; !exists {
			return fmt.Errorf("workflow: execution plan edge references unknown target %q", edge.ToNode)
		}
		toNode := nodeByID(envelopeNodes(semantics.Nodes), edge.ToNode)
		if orderIndex[edge.FromNode] >= orderIndex[edge.ToNode] && toNode.Type != NodeTypeLoop {
			return fmt.Errorf("workflow: execution plan topology violates edge %q", edge.ID)
		}
	}
	return nil
}

// Compile validates a definition, resolves every registry entry, and creates a
// deterministic immutable execution plan.
func Compile(definition WorkflowDefinition, registry *Registry, policy CompilePolicy) (*ExecutionPlan, error) {
	if err := ValidateDefinition(definition, registry, policy); err != nil {
		return nil, err
	}
	nodes := make([]PlanNode, 0, len(definition.Nodes))
	nodeIndex := make(map[string]NodeDefinition, len(definition.Nodes))
	outgoing := make(map[string][]string)
	incoming := make(map[string][]string)
	for _, node := range definition.Nodes {
		nodeIndex[node.ID] = node
		spec, _ := registry.Resolve(node.Type, node.Version)
		config, err := canonicalizeJSON(node.Config)
		if err != nil {
			return nil, fmt.Errorf("workflow: canonicalize config for %s: %w", node.ID, err)
		}
		sensitive, err := sensitiveOutput(node.Metadata)
		if err != nil {
			return nil, fmt.Errorf("workflow: sensitive output policy for %s: %w", node.ID, err)
		}
		capabilities := resolvedNodeCapabilities(node.Type, config, spec.Capabilities)
		nodes = append(nodes, PlanNode{
			ID: node.ID, Type: node.Type, Version: node.Version,
			Config: config, Inputs: canonicalBindings(node.Inputs),
			Capabilities: capabilities, TimeoutMS: spec.DefaultTimeoutMS,
			OutputLimit: spec.MaximumOutputBytes, Permissions: spec.Permissions,
			SensitiveOutput: sensitive,
			Executor:        spec.Executor,
		})
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	edges := append([]EdgeDefinition(nil), definition.Edges...)
	sort.Slice(edges, func(i, j int) bool {
		left, right := edges[i], edges[j]
		if left.FromNode != right.FromNode {
			return left.FromNode < right.FromNode
		}
		if left.FromPort != right.FromPort {
			return left.FromPort < right.FromPort
		}
		if left.ToNode != right.ToNode {
			return left.ToNode < right.ToNode
		}
		if left.ToPort != right.ToPort {
			return left.ToPort < right.ToPort
		}
		return left.ID < right.ID
	})
	for _, edge := range edges {
		outgoing[edge.FromNode] = append(outgoing[edge.FromNode], edge.ToNode)
		incoming[edge.ToNode] = append(incoming[edge.ToNode], edge.FromNode)
	}
	order, _ := loopAwareTopologicalOrder(nodeIndex, outgoing, incoming)
	semantics := planSemantics{
		SchemaVersion: DefinitionSchemaV1, WorkflowID: definition.ID,
		Nodes: nodes, Edges: edges, TopologicalOrder: order,
	}
	canonical, err := json.Marshal(semantics)
	if err != nil {
		return nil, fmt.Errorf("workflow: marshal execution plan: %w", err)
	}
	digest := sha256.Sum256(canonical)
	return &ExecutionPlan{
		schemaVersion: semantics.SchemaVersion, workflowID: semantics.WorkflowID,
		nodes: clonePlanNodes(nodes), edges: append([]EdgeDefinition(nil), edges...),
		topologicalOrder: append([]string(nil), order...), hash: hex.EncodeToString(digest[:]),
	}, nil
}

func resolvedNodeCapabilities(nodeType string, config json.RawMessage, base CapabilitySet) CapabilitySet {
	resolved := base
	resolved.RetrySafeErrors = append([]string(nil), base.RetrySafeErrors...)
	if nodeType != NodeTypeHTTP {
		return resolved
	}
	var policy struct {
		Method    string `json:"method"`
		RetrySafe bool   `json:"retrySafe,omitempty"`
	}
	if json.Unmarshal(config, &policy) != nil {
		resolved.RetrySafeErrors = nil
		resolved.RecoverySafe = false
		return resolved
	}
	switch policy.Method {
	case "GET", "HEAD", "PUT", "DELETE":
		policy.RetrySafe = true
	}
	if policy.RetrySafe {
		resolved.RetrySafeErrors = []string{"http_response_failed", "http_timeout", "http_transport_failed"}
		resolved.RecoverySafe = true
	} else {
		resolved.RetrySafeErrors = nil
		resolved.RecoverySafe = false
	}
	return resolved
}

func sensitiveOutput(metadata map[string]json.RawMessage) (bool, error) {
	raw, exists := metadata["sensitiveOutput"]
	if !exists {
		return false, nil
	}
	var sensitive bool
	if err := json.Unmarshal(raw, &sensitive); err != nil {
		return false, errors.New("sensitiveOutput must be a boolean")
	}
	return sensitive, nil
}

func envelopeNodes(nodes []PlanNode) map[string]NodeDefinition {
	result := make(map[string]NodeDefinition, len(nodes))
	for _, node := range nodes {
		result[node.ID] = NodeDefinition{ID: node.ID, Type: node.Type}
	}
	return result
}

func nodeByID(nodes map[string]NodeDefinition, id string) NodeDefinition { return nodes[id] }

// SchemaVersion returns the canonical plan schema version.
func (p *ExecutionPlan) SchemaVersion() string {
	if p == nil {
		return ""
	}
	return p.schemaVersion
}

// WorkflowID returns the canonical workflow identifier.
func (p *ExecutionPlan) WorkflowID() string {
	if p == nil {
		return ""
	}
	return p.workflowID
}

// Hash returns the lowercase SHA-256 digest of the canonical plan semantics.
func (p *ExecutionPlan) Hash() string {
	if p == nil {
		return ""
	}
	return p.hash
}

// Nodes returns resolved nodes in deterministic node-ID order.
func (p *ExecutionPlan) Nodes() []PlanNode {
	if p == nil {
		return nil
	}
	return clonePlanNodes(p.nodes)
}

// Node returns a defensive copy of the resolved node with the given ID.
func (p *ExecutionPlan) Node(id string) (PlanNode, bool) {
	if p == nil {
		return PlanNode{}, false
	}
	index := sort.Search(len(p.nodes), func(index int) bool { return p.nodes[index].ID >= id })
	if index == len(p.nodes) || p.nodes[index].ID != id {
		return PlanNode{}, false
	}
	return clonePlanNode(p.nodes[index]), true
}

// Edges returns control-flow edges in deterministic endpoint order.
func (p *ExecutionPlan) Edges() []EdgeDefinition {
	if p == nil {
		return nil
	}
	return append([]EdgeDefinition(nil), p.edges...)
}

// TopologicalOrder returns the stable runnable order used for deterministic scheduling.
func (p *ExecutionPlan) TopologicalOrder() []string {
	if p == nil {
		return nil
	}
	return append([]string(nil), p.topologicalOrder...)
}

// MarshalJSON serializes the immutable plan and its verified content hash.
func (p *ExecutionPlan) MarshalJSON() ([]byte, error) {
	if p == nil {
		return []byte("null"), nil
	}
	return json.Marshal(planEnvelope{
		planSemantics: planSemantics{
			SchemaVersion: p.schemaVersion, WorkflowID: p.workflowID,
			Nodes: p.nodes, Edges: p.edges, TopologicalOrder: p.topologicalOrder,
		},
		Hash: p.hash,
	})
}

func canonicalBindings(bindings map[string]InputBinding) map[string]InputBinding {
	if bindings == nil {
		return nil
	}
	result := make(map[string]InputBinding, len(bindings))
	for name, binding := range bindings {
		if len(binding.Literal) != 0 {
			binding.Literal, _ = canonicalizeJSON(binding.Literal)
		}
		if binding.Source != nil {
			source := *binding.Source
			binding.Source = &source
		}
		result[name] = binding
	}
	return result
}

func clonePlanNodes(nodes []PlanNode) []PlanNode {
	if nodes == nil {
		return nil
	}
	result := make([]PlanNode, len(nodes))
	for index, node := range nodes {
		result[index] = clonePlanNode(node)
	}
	return result
}

func clonePlanNode(node PlanNode) PlanNode {
	node.Config = cloneRaw(node.Config)
	node.Inputs = canonicalBindings(node.Inputs)
	node.Capabilities.RetrySafeErrors = append([]string(nil), node.Capabilities.RetrySafeErrors...)
	return node
}
