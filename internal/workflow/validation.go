package workflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

var (
	// Stable resource IDs may be imported UUIDs whose first character is a digit.
	// Separators remain deliberately limited so IDs are safe in paths and cursors.
	idPattern       = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)
	nodeTypePattern = regexp.MustCompile(`^[a-z][a-z0-9.-]{0,127}$`)
	versionPattern  = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)
)

// CompilePolicy is the maximum resource authority granted at publication time.
// Node permissions are always enforced; requested capabilities are opt-in.
type CompilePolicy struct {
	AllowedCapabilities CapabilitySet
	AllowedPermissions  PermissionSet
}

// ValidationIssue is one stable, path-addressable definition error.
type ValidationIssue struct {
	Code    string `json:"code"`
	Path    string `json:"path"`
	Message string `json:"message"`
}

// ValidationError contains every independently detectable definition error.
type ValidationError struct {
	Issues []ValidationIssue `json:"issues"`
}

// ValidWorkflowID reports whether id may name a stored workflow. The rule lives here rather than at
// each write boundary because a definition stored under any other shape can only ever fail
// compilation: the draft write boundary and the compiler must agree on exactly the same set, or a
// draft that can never compile still gets stored.
func ValidWorkflowID(id string) bool { return idPattern.MatchString(id) }

// InvalidWorkflowIDIssue is the one description of that rule. The compiler and the draft write
// boundary both report it, so a caller sees the same code, path, and wording whichever of them
// rejects the id — two spellings of the same rejection would read as two different problems.
func InvalidWorkflowIDIssue() ValidationIssue {
	return ValidationIssue{
		Code: "invalid_id", Path: "id",
		Message: "workflow id must start with a letter or digit and contain only letters, digits, underscores, or hyphens",
	}
}

// Error implements error.
func (e *ValidationError) Error() string {
	if e == nil || len(e.Issues) == 0 {
		return "workflow definition is invalid"
	}
	return fmt.Sprintf("workflow definition is invalid: %s at %s", e.Issues[0].Code, e.Issues[0].Path)
}

type validationState struct {
	definition WorkflowDefinition
	registry   *Registry
	policy     CompilePolicy
	issues     []ValidationIssue
	nodes      map[string]NodeDefinition
	specs      map[string]NodeTypeVersion
	incoming   map[string][]string
	outgoing   map[string][]string
}

// ValidateDefinition performs structural, registry, type, security, and DAG validation.
func ValidateDefinition(definition WorkflowDefinition, registry *Registry, policy CompilePolicy) error {
	state := newValidationState(definition, registry, policy)
	state.validateIdentity()
	if state.validateCollectionLimits() {
		return state.result()
	}
	state.validateNodes()
	state.validateEdges()
	state.validateFanoutLimits()
	state.validateGraph()
	return state.result()
}

func (s *validationState) result() error {
	if len(s.issues) == 0 {
		return nil
	}
	sort.SliceStable(s.issues, func(i, j int) bool {
		if s.issues[i].Path != s.issues[j].Path {
			return s.issues[i].Path < s.issues[j].Path
		}
		return s.issues[i].Code < s.issues[j].Code
	})
	return &ValidationError{Issues: s.issues}
}

func newValidationState(definition WorkflowDefinition, registry *Registry, policy CompilePolicy) *validationState {
	return &validationState{
		definition: definition, registry: registry, policy: policy,
		nodes: make(map[string]NodeDefinition), specs: make(map[string]NodeTypeVersion),
		incoming: make(map[string][]string), outgoing: make(map[string][]string),
	}
}

func (s *validationState) add(code, path, message string) {
	s.issues = append(s.issues, ValidationIssue{Code: code, Path: path, Message: message})
}

func (s *validationState) validateIdentity() {
	if s.definition.SchemaVersion != DefinitionSchemaV1 {
		s.add("unsupported_schema_version", "schemaVersion", "schemaVersion must be v1")
	}
	if !ValidWorkflowID(s.definition.ID) {
		s.issues = append(s.issues, InvalidWorkflowIDIssue())
	}
	if s.registry == nil {
		s.add("registry_required", "nodes", "a node registry is required")
	}
}

func (s *validationState) validateNodes() {
	startCount, endCount := 0, 0
	for index, node := range s.definition.Nodes {
		path := fmt.Sprintf("nodes[%d]", index)
		if !idPattern.MatchString(node.ID) {
			s.add("invalid_id", path+".id", "node id is invalid")
		}
		if !nodeTypePattern.MatchString(node.Type) {
			s.add("invalid_node_type", path+".type", "node type identifier is invalid")
		}
		if !versionPattern.MatchString(node.Version) {
			s.add("invalid_node_version", path+".version", "node version must be a semantic version")
		}
		if _, exists := s.nodes[node.ID]; exists {
			s.add("duplicate_node", path+".id", "node id must be unique")
			continue
		}
		s.nodes[node.ID] = node
		if node.Type == NodeTypeStart {
			startCount++
		}
		if node.Type == NodeTypeEnd {
			endCount++
		}
		spec, found := s.registry.Resolve(node.Type, node.Version)
		if !found {
			if s.registry.hasType(node.Type) {
				s.add("unknown_node_version", path+".version", fmt.Sprintf("node type version %s@%s is not registered", node.Type, node.Version))
			} else {
				s.add("unknown_node_type", path+".type", fmt.Sprintf("node type %s is not registered", node.Type))
			}
			continue
		}
		s.specs[node.ID] = spec
		s.validateNode(path, node, spec)
	}
	if startCount != 1 {
		s.add("invalid_start_count", "nodes", "workflow must contain exactly one Start node")
	}
	if endCount == 0 {
		s.add("missing_end", "nodes", "workflow must contain at least one End node")
	}
}

func (s *validationState) validateNode(path string, node NodeDefinition, spec NodeTypeVersion) {
	canonicalConfig, err := canonicalizeJSON(node.Config)
	if err != nil {
		s.add("invalid_config_json", path+".config", err.Error())
	} else {
		if secretPath := findPlaintextSecret(canonicalConfig); secretPath != "" {
			s.add("plaintext_secret", path+".config"+secretPath, "secret values must use a credential reference")
		}
		if spec.ValidateConfig != nil {
			if err := spec.ValidateConfig(canonicalConfig); err != nil {
				s.add("invalid_config", path+".config", err.Error())
			}
		}
	}
	if !s.policy.AllowedPermissions.Allows(spec.Permissions) {
		s.add("permission_denied", path, "node permissions exceed the compile policy")
	}
	if !s.policy.AllowedCapabilities.Allows(spec.Capabilities) {
		s.add("capability_denied", path, "node capabilities exceed the compile policy")
	}
	for name, binding := range node.Inputs {
		port, exists := spec.Inputs[name]
		bindingPath := path + ".inputs." + name
		if !idPattern.MatchString(name) {
			s.add("invalid_id", bindingPath, "input port id is invalid")
		}
		if !exists {
			s.add("unknown_input_port", bindingPath, "input port is not declared by the node type")
			continue
		}
		if !validValueType(binding.Type) {
			s.add("invalid_value_type", bindingPath+".type", "binding type is not supported")
		} else if !typeAssignable(binding.Type, port.Type) {
			s.add("input_type_mismatch", bindingPath+".type", "binding type is incompatible with the input port")
		}
		hasLiteral := len(binding.Literal) != 0
		hasSource := binding.Source != nil
		if hasLiteral == hasSource {
			s.add("invalid_binding", bindingPath, "exactly one literal or source is required")
		} else if hasLiteral {
			if err := validateLiteral(binding.Literal, binding.Type); err != nil {
				s.add("invalid_literal", bindingPath+".literal", err.Error())
			} else if secretPath := findPlaintextSecret(binding.Literal); secretPath != "" {
				s.add("plaintext_secret", bindingPath+".literal"+secretPath, "secret values must use a credential reference")
			}
		}
		if hasSource {
			if !idPattern.MatchString(binding.Source.NodeID) {
				s.add("invalid_id", bindingPath+".source.nodeId", "binding source node id is invalid")
			}
			if !idPattern.MatchString(binding.Source.Port) {
				s.add("invalid_id", bindingPath+".source.port", "binding source port id is invalid")
			}
		}
	}
	for name, port := range spec.Inputs {
		if port.Required {
			if _, exists := node.Inputs[name]; !exists {
				s.add("missing_input", path+".inputs."+name, "required input is missing")
			}
		}
	}
}

func (s *validationState) validateEdges() {
	edgeIDs := make(map[string]bool)
	edgeKeys := make(map[string]bool)
	for index, edge := range s.definition.Edges {
		path := fmt.Sprintf("edges[%d]", index)
		if !idPattern.MatchString(edge.ID) {
			s.add("invalid_id", path+".id", "edge id is invalid")
		} else if edgeIDs[edge.ID] {
			s.add("duplicate_edge_id", path+".id", "edge id must be unique")
		}
		edgeIDs[edge.ID] = true
		for field, value := range map[string]string{
			"fromNode": edge.FromNode, "fromPort": edge.FromPort,
			"toNode": edge.ToNode, "toPort": edge.ToPort,
		} {
			if !idPattern.MatchString(value) {
				s.add("invalid_id", path+"."+field, "edge endpoint id is invalid")
			}
		}
		key := edge.FromNode + "\x00" + edge.FromPort + "\x00" + edge.ToNode + "\x00" + edge.ToPort
		if edgeKeys[key] {
			s.add("duplicate_edge", path, "edge endpoints must be unique")
		}
		edgeKeys[key] = true
		from, fromExists := s.nodes[edge.FromNode]
		to, toExists := s.nodes[edge.ToNode]
		if !fromExists {
			s.add("unknown_edge_source", path+".fromNode", "edge source node does not exist")
		}
		if !toExists {
			s.add("unknown_edge_target", path+".toNode", "edge target node does not exist")
		}
		if !fromExists || !toExists {
			continue
		}
		fromSpec, fromResolved := s.specs[from.ID]
		toSpec, toResolved := s.specs[to.ID]
		if fromResolved {
			if _, exists := fromSpec.Outputs[edge.FromPort]; !exists && from.Type != NodeTypeSwitch {
				s.add("unknown_output_port", path+".fromPort", "source output port does not exist")
			}
		}
		if toResolved {
			if _, exists := toSpec.Inputs[edge.ToPort]; !exists && to.Type != NodeTypeEnd {
				s.add("unknown_input_port", path+".toPort", "target input port does not exist")
			}
		}
		if fromResolved && toResolved {
			fromPort, fromOK := fromSpec.Outputs[edge.FromPort]
			if !fromOK && from.Type == NodeTypeSwitch {
				fromPort, fromOK = PortDefinition{Type: ValueTypeAny}, true
			}
			toPort, toOK := toSpec.Inputs[edge.ToPort]
			if !toOK && to.Type == NodeTypeEnd {
				toPort, toOK = PortDefinition{Type: ValueTypeAny}, true
			}
			if fromOK && toOK && !typeAssignable(fromPort.Type, toPort.Type) {
				s.add("edge_type_mismatch", path, "source output type is incompatible with target input type")
			}
		}
		s.outgoing[edge.FromNode] = append(s.outgoing[edge.FromNode], edge.ToNode)
		s.incoming[edge.ToNode] = append(s.incoming[edge.ToNode], edge.FromNode)
	}
	for nodeID, node := range s.nodes {
		if node.Type == NodeTypeStart && len(s.incoming[nodeID]) != 0 {
			s.add("start_has_incoming", "nodes."+nodeID, "Start node cannot have incoming edges")
		}
		if node.Type == NodeTypeEnd && len(s.outgoing[nodeID]) != 0 {
			s.add("end_has_outgoing", "nodes."+nodeID, "End node cannot have outgoing edges")
		}
	}
	s.validateSources()
}

func (s *validationState) validateSources() {
	for nodeIndex, node := range s.definition.Nodes {
		for inputName, binding := range node.Inputs {
			if binding.Source == nil {
				continue
			}
			path := fmt.Sprintf("nodes[%d].inputs.%s.source", nodeIndex, inputName)
			sourceSpec, exists := s.specs[binding.Source.NodeID]
			if !exists {
				s.add("unknown_binding_source", path+".nodeId", "binding source node does not exist or has an unknown version")
				continue
			}
			output, exists := sourceSpec.Outputs[binding.Source.Port]
			if !exists {
				s.add("unknown_output_port", path+".port", "binding source output port does not exist")
				continue
			}
			if !typeAssignable(output.Type, binding.Type) {
				s.add("binding_type_mismatch", path, "source output type is incompatible with the binding type")
			}
			if !contains(s.incoming[node.ID], binding.Source.NodeID) {
				s.add("binding_without_edge", path, "binding source must have a direct control-flow edge to the target node")
			}
		}
	}
}

func (s *validationState) validateGraph() {
	if len(s.nodes) == 0 {
		return
	}
	if _, cycle := loopAwareTopologicalOrder(s.nodes, s.outgoing, s.incoming); cycle {
		s.add("cycle_detected", "edges", "cycles must close through a bounded Loop node")
		return
	}
	startID := ""
	for id, node := range s.nodes {
		if node.Type == NodeTypeStart {
			startID = id
			break
		}
	}
	if startID == "" {
		return
	}
	reachable := walk(startID, s.outgoing)
	for id := range s.nodes {
		if !reachable[id] {
			s.add("unreachable_node", "nodes."+id, "node is not reachable from Start")
		}
	}
	canReachEnd := make(map[string]bool)
	for id, node := range s.nodes {
		if node.Type == NodeTypeEnd {
			for reachableID := range walk(id, s.incoming) {
				canReachEnd[reachableID] = true
			}
		}
	}
	for id := range s.nodes {
		if reachable[id] && !canReachEnd[id] {
			s.add("no_path_to_end", "nodes."+id, "node is not on a path to an End node")
		}
	}
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func validValueType(valueType ValueType) bool {
	switch valueType {
	case ValueTypeAny, ValueTypeString, ValueTypeNumber, ValueTypeInteger, ValueTypeBoolean, ValueTypeObject, ValueTypeArray:
		return true
	default:
		return false
	}
}

func typeAssignable(source, target ValueType) bool {
	return source == target || source == ValueTypeAny || target == ValueTypeAny || (source == ValueTypeInteger && target == ValueTypeNumber)
}

func validateLiteral(raw json.RawMessage, valueType ValueType) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return err
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return err
	}
	valid := false
	switch valueType {
	case ValueTypeAny:
		valid = true
	case ValueTypeString:
		_, valid = value.(string)
	case ValueTypeNumber:
		_, valid = value.(json.Number)
	case ValueTypeInteger:
		number, ok := value.(json.Number)
		valid = ok && !strings.ContainsAny(number.String(), ".eE")
	case ValueTypeBoolean:
		_, valid = value.(bool)
	case ValueTypeObject:
		_, valid = value.(map[string]any)
	case ValueTypeArray:
		_, valid = value.([]any)
	}
	if !valid {
		return fmt.Errorf("literal does not match declared type %q", valueType)
	}
	return nil
}

func findPlaintextSecret(config json.RawMessage) string {
	decoder := json.NewDecoder(bytes.NewReader(config))
	decoder.UseNumber()
	var value any
	if decoder.Decode(&value) != nil {
		return ""
	}
	return findSecretValue(value, "")
}

func findSecretValue(value any, path string) string {
	switch typed := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			normalized := strings.NewReplacer("-", "", "_", "", ".", "").Replace(strings.ToLower(key))
			isReference := strings.HasSuffix(normalized, "ref") || strings.HasSuffix(normalized, "reference")
			sensitive := strings.Contains(normalized, "password") || strings.Contains(normalized, "secret") ||
				strings.Contains(normalized, "token") || strings.Contains(normalized, "authorization") ||
				strings.Contains(normalized, "apikey") || strings.Contains(normalized, "privatekey") ||
				strings.Contains(normalized, "accesskey")
			if sensitive && !isReference && typed[key] != nil {
				return path + "." + key
			}
			if nested := findSecretValue(typed[key], path+"."+key); nested != "" {
				return nested
			}
		}
	case []any:
		for index, item := range typed {
			if nested := findSecretValue(item, fmt.Sprintf("%s[%d]", path, index)); nested != "" {
				return nested
			}
		}
	}
	return ""
}
