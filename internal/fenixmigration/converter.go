// Package fenixmigration converts frozen Fenix workflow data into Supaflow's
// canonical, editor-independent contracts without keeping a compatibility runtime.
package fenixmigration

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/pazhou/supaflow/internal/workflow"
	"gopkg.in/yaml.v3"
)

var safeID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)

// PlanReference resolves an already imported child workflow publication.
type PlanReference struct {
	WorkflowID string
	PlanID     string
	PlanHash   string
}

// Options supplies explicit references that cannot safely be inferred from YAML.
type Options struct {
	Subworkflows       map[string]PlanReference
	HTTPNodeCredential map[string]string
}

// Blocker identifies one legacy construct that needs an explicit operator decision.
type Blocker struct {
	NodeID  string `json:"nodeId,omitempty"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// BlockedError prevents a lossy or unsafe migration.
type BlockedError struct{ Blockers []Blocker }

// Error summarizes deterministic blockers without embedding source payloads.
func (e *BlockedError) Error() string {
	return fmt.Sprintf("fenix workflow migration blocked by %d incompatible construct(s)", len(e.Blockers))
}

// Result is a canonical definition plus non-sensitive migration evidence.
type Result struct {
	Definition workflow.WorkflowDefinition
	SourceHash string
}

type legacyWorkflow struct {
	SchemaVersion string                    `yaml:"schema_version"`
	Name          string                    `yaml:"name"`
	Description   string                    `yaml:"description,omitempty"`
	Params        map[string]map[string]any `yaml:"params,omitempty"`
	Secrets       []string                  `yaml:"secrets,omitempty"`
	Timeout       *int                      `yaml:"timeout,omitempty"`
	Nodes         []legacyNode              `yaml:"nodes"`
}

type legacyRetry struct {
	Count   int    `yaml:"count"`
	Delay   int    `yaml:"delay,omitempty"`
	Backoff string `yaml:"backoff,omitempty"`
}

type legacyNode struct {
	ID              string                    `yaml:"id"`
	Type            string                    `yaml:"type"`
	Description     string                    `yaml:"description,omitempty"`
	DependsOn       []string                  `yaml:"depends_on,omitempty"`
	Condition       string                    `yaml:"condition,omitempty"`
	Timeout         *int                      `yaml:"timeout,omitempty"`
	Retry           *legacyRetry              `yaml:"retry,omitempty"`
	Env             map[string]string         `yaml:"env,omitempty"`
	Outputs         map[string]map[string]any `yaml:"outputs,omitempty"`
	Command         any                       `yaml:"command,omitempty"`
	CWD             string                    `yaml:"cwd,omitempty"`
	Inputs          map[string]string         `yaml:"inputs,omitempty"`
	Code            string                    `yaml:"code,omitempty"`
	Requirements    []string                  `yaml:"requirements,omitempty"`
	Agent           string                    `yaml:"agent,omitempty"`
	Prompt          string                    `yaml:"prompt,omitempty"`
	OutputMessages  *int                      `yaml:"output_messages,omitempty"`
	URL             string                    `yaml:"url,omitempty"`
	Method          string                    `yaml:"method,omitempty"`
	Headers         map[string]string         `yaml:"headers,omitempty"`
	Body            any                       `yaml:"body,omitempty"`
	DisplayData     any                       `yaml:"display_data,omitempty"`
	ExpiresIn       *int                      `yaml:"expires_in,omitempty"`
	Ref             string                    `yaml:"ref,omitempty"`
	Params          map[string]any            `yaml:"params,omitempty"`
	IgnoreErrors    bool                      `yaml:"ignore_errors,omitempty"`
	MaxIterations   int                       `yaml:"max_iterations,omitempty"`
	Output          map[string]string         `yaml:"output,omitempty"`
	Tool            string                    `yaml:"tool,omitempty"`
	Slurm           map[string]any            `yaml:"slurm,omitempty"`
	Script          map[string]any            `yaml:"script,omitempty"`
	Foreach         string                    `yaml:"foreach,omitempty"`
	MaxConcurrent   int                       `yaml:"maxConcurrent,omitempty"`
	ContinueOnError bool                      `yaml:"continueOnError,omitempty"`
}

// ConvertYAML strictly converts one frozen Fenix YAML version. Constructs whose
// semantics differ are reported as blockers instead of being approximated.
func ConvertYAML(workflowID string, source []byte, options Options) (Result, error) {
	if !safeID.MatchString(workflowID) {
		return Result{}, errors.New("workflow id is invalid")
	}
	var legacy legacyWorkflow
	decoder := yaml.NewDecoder(bytes.NewReader(source))
	decoder.KnownFields(true)
	if err := decoder.Decode(&legacy); err != nil {
		return Result{}, fmt.Errorf("decode Fenix YAML: %w", err)
	}
	if legacy.SchemaVersion != "1" || strings.TrimSpace(legacy.Name) == "" || len(legacy.Nodes) == 0 {
		return Result{}, errors.New("Fenix YAML requires schema_version 1, name, and nodes")
	}
	digest := sha256.Sum256(source)
	definition := workflow.WorkflowDefinition{SchemaVersion: workflow.DefinitionSchemaV1, ID: workflowID}
	definition.Metadata = legacyMetadata(legacy, hex.EncodeToString(digest[:]))
	used := make(map[string]bool, len(legacy.Nodes)+2)
	idMap := make(map[string]string, len(legacy.Nodes))
	blockers := make([]Blocker, 0)
	for _, node := range legacy.Nodes {
		if strings.TrimSpace(node.ID) == "" || idMap[node.ID] != "" {
			blockers = append(blockers, Blocker{NodeID: node.ID, Code: "invalid_node_id", Message: "node ids must be non-empty and unique"})
			continue
		}
		idMap[node.ID] = migrationID(node.ID, used)
	}
	startID := migrationID("migration_start", used)
	definition.Nodes = append(definition.Nodes, nodeDefinition(startID, workflow.NodeTypeStart, map[string]any{}))
	for _, legacyNode := range legacy.Nodes {
		for _, dependency := range legacyNode.DependsOn {
			if idMap[dependency] == "" {
				blockers = append(blockers, Blocker{NodeID: legacyNode.ID, Code: "unknown_dependency", Message: "depends_on references an unknown node"})
			}
		}
		mapped, nodeBlockers := convertNode(legacyNode, idMap[legacyNode.ID], options)
		blockers = append(blockers, nodeBlockers...)
		if len(nodeBlockers) == 0 {
			definition.Nodes = append(definition.Nodes, mapped)
		}
	}
	if len(blockers) > 0 {
		sort.Slice(blockers, func(i, j int) bool {
			if blockers[i].NodeID != blockers[j].NodeID {
				return blockers[i].NodeID < blockers[j].NodeID
			}
			return blockers[i].Code < blockers[j].Code
		})
		return Result{}, &BlockedError{Blockers: blockers}
	}
	edges, syntheticEnd := buildEdges(legacy.Nodes, idMap, startID, definition.Nodes, used)
	if syntheticEnd != nil {
		definition.Nodes = append(definition.Nodes, *syntheticEnd)
	}
	definition.Edges = edges
	return Result{Definition: definition, SourceHash: hex.EncodeToString(digest[:])}, nil
}

func convertNode(node legacyNode, id string, options Options) (workflow.NodeDefinition, []Blocker) {
	blockers := commonBlockers(node)
	config := map[string]any{}
	inputs := map[string]workflow.InputBinding{}
	typeName := ""
	switch node.Type {
	case "shell":
		typeName = workflow.NodeTypeShell
		source, ok := shellSource(node.Command)
		if !ok || strings.TrimSpace(source) == "" {
			blockers = append(blockers, block(node, "invalid_shell", "shell command is required"))
		}
		config["source"] = source
	case "python":
		typeName = workflow.NodeTypePython
		if node.Code == "" {
			blockers = append(blockers, block(node, "invalid_python", "python code is required"))
		}
		if len(node.Requirements) > 0 {
			blockers = append(blockers, block(node, "python_requirements", "preinstall requirements in the immutable runner image"))
		}
		config["source"] = node.Code
	case "agent":
		typeName = workflow.NodeTypeAgent
		if node.Agent == "" || node.Prompt == "" {
			blockers = append(blockers, block(node, "invalid_agent", "agent and prompt are required"))
		}
		if dynamic(node.Prompt) {
			blockers = append(blockers, block(node, "dynamic_expression", "agent prompt expressions require manual canonical binding"))
		}
		config["name"] = node.Agent
		inputs["input"] = literalBinding(node.Prompt)
	case "api":
		typeName = workflow.NodeTypeHTTP
		if node.URL == "" || dynamic(node.URL) {
			blockers = append(blockers, block(node, "dynamic_expression", "HTTP URL must be static"))
		}
		method := strings.ToUpper(node.Method)
		if method == "" {
			method = "GET"
		}
		config["method"], config["url"] = method, node.URL
		credential := options.HTTPNodeCredential[node.ID]
		headers := make(map[string]string)
		for name, value := range node.Headers {
			if dynamic(value) {
				blockers = append(blockers, block(node, "dynamic_expression", "HTTP header expressions require a credential reference"))
				continue
			}
			if sensitiveHeader(name) {
				if credential == "" {
					blockers = append(blockers, block(node, "plaintext_credential", "move sensitive HTTP headers into a pre-created credential reference"))
				}
				continue
			}
			headers[name] = value
		}
		if len(headers) > 0 {
			config["headers"] = headers
		}
		if credential != "" {
			config["credentialRef"] = credential
		}
		if node.Timeout != nil {
			config["timeoutMs"] = int64(*node.Timeout) * 1000
		}
		body, bodyOK := node.Body.(string)
		if node.Body != nil && !bodyOK {
			blockers = append(blockers, block(node, "http_body", "HTTP body must be a string containing static JSON"))
		}
		if body != "" {
			if dynamic(body) || !json.Valid([]byte(body)) {
				blockers = append(blockers, block(node, "http_body", "HTTP body must be static JSON"))
			} else {
				inputs["body"] = workflow.InputBinding{Type: workflow.ValueTypeAny, Literal: json.RawMessage(body)}
			}
		}
	case "audit":
		typeName = workflow.NodeTypeHumanApproval
		prompt := "Approval required"
		if node.DisplayData != nil {
			encoded, _ := json.Marshal(node.DisplayData)
			prompt = strings.Trim(string(encoded), `"`)
		}
		config["prompt"] = prompt
		if node.ExpiresIn != nil {
			config["timeoutMs"] = int64(*node.ExpiresIn) * 1000
		}
	case "workflow":
		typeName = workflow.NodeTypeSubworkflow
		ref, ok := options.Subworkflows[node.Ref]
		if !ok || ref.WorkflowID == "" || ref.PlanID == "" || ref.PlanHash == "" {
			blockers = append(blockers, block(node, "subworkflow_reference", "supply the imported child plan reference"))
		} else {
			config["workflowId"], config["planId"], config["planHash"] = ref.WorkflowID, ref.PlanID, ref.PlanHash
		}
		if len(node.Params) > 0 || node.IgnoreErrors {
			blockers = append(blockers, block(node, "subworkflow_policy", "params and ignore_errors require an explicit canonical redesign"))
		}
	case "transform":
		typeName = workflow.NodeTypeTransform
		selects := make(map[string]string, len(node.Output))
		for key, value := range node.Output {
			if !strings.HasPrefix(value, "$") || dynamic(value) {
				blockers = append(blockers, block(node, "transform_expression", "transform outputs must use canonical JSON paths"))
			} else {
				selects[key] = value
			}
		}
		if len(selects) == 0 {
			blockers = append(blockers, block(node, "invalid_transform", "transform output is required"))
		}
		config["select"] = selects
	case "end":
		typeName = workflow.NodeTypeEnd
		if len(node.Inputs) > 0 {
			blockers = append(blockers, block(node, "end_expression", "end expressions require explicit canonical bindings"))
		}
	case "loop":
		typeName = workflow.NodeTypeLoop
		blockers = append(blockers, block(node, "nested_loop", "flatten the legacy nested loop into explicit canonical control flow"))
	case "custom":
		typeName = workflow.NodeTypeTool
		blockers = append(blockers, block(node, "custom_tool", "register and map the custom tool explicitly"))
	default:
		blockers = append(blockers, block(node, "unknown_node_type", "node type is not supported"))
	}
	mapped := nodeDefinition(id, typeName, config)
	if len(inputs) > 0 {
		mapped.Inputs = inputs
	}
	if id != node.ID {
		mapped.Metadata = map[string]json.RawMessage{"fenixNodeId": mustJSON(node.ID)}
	}
	return mapped, blockers
}

func commonBlockers(node legacyNode) []Blocker {
	result := make([]Blocker, 0)
	if node.Condition != "" {
		result = append(result, block(node, "node_condition", "convert the legacy condition into an explicit Condition node"))
	}
	if node.Timeout != nil && node.Type != "api" && node.Type != "audit" {
		result = append(result, block(node, "node_timeout", "node timeout differs from the pinned node version"))
	}
	if node.Retry != nil {
		result = append(result, block(node, "node_retry", "explicit retry policy requires a new pinned node version"))
	}
	if len(node.Env) > 0 || node.CWD != "" || len(node.Inputs) > 0 && node.Type != "end" {
		result = append(result, block(node, "host_context", "host cwd/env/input expressions cannot cross the sandbox boundary"))
	}
	if len(node.Outputs) > 0 {
		result = append(result, block(node, "file_output", "migrate file outputs to object-storage references"))
	}
	return result
}

func buildEdges(nodes []legacyNode, ids map[string]string, startID string, mapped []workflow.NodeDefinition, used map[string]bool) ([]workflow.EdgeDefinition, *workflow.NodeDefinition) {
	types := make(map[string]string, len(mapped))
	outgoing := make(map[string]bool)
	for _, node := range mapped {
		types[node.ID] = node.Type
	}
	edges := make([]workflow.EdgeDefinition, 0, len(nodes)+2)
	for _, node := range nodes {
		to := ids[node.ID]
		dependencies := node.DependsOn
		if len(dependencies) == 0 {
			dependencies = []string{""}
		}
		for _, dependency := range dependencies {
			from := startID
			if dependency != "" {
				from = ids[dependency]
			}
			edgeID := migrationID("edge_"+from+"_"+to, used)
			edges = append(edges, workflow.EdgeDefinition{ID: edgeID, FromNode: from, FromPort: outputPort(types[from]), ToNode: to, ToPort: inputPort(types[to])})
			outgoing[from] = true
		}
	}
	hasEnd := false
	for _, node := range mapped {
		if node.Type == workflow.NodeTypeEnd {
			hasEnd = true
		}
	}
	if !hasEnd {
		endID := migrationID("migration_end", used)
		mappedEnd := nodeDefinition(endID, workflow.NodeTypeEnd, map[string]any{})
		for _, node := range mapped {
			if node.Type == workflow.NodeTypeEnd || outgoing[node.ID] {
				continue
			}
			edgeID := migrationID("edge_"+node.ID+"_"+endID, used)
			edges = append(edges, workflow.EdgeDefinition{ID: edgeID, FromNode: node.ID, FromPort: outputPort(node.Type), ToNode: endID, ToPort: "result"})
		}
		return edges, &mappedEnd
	}
	return edges, nil
}

func nodeDefinition(id, nodeType string, config map[string]any) workflow.NodeDefinition {
	return workflow.NodeDefinition{ID: id, Type: nodeType, Version: workflow.BuiltinNodeVersion, Config: mustJSON(config)}
}
func literalBinding(value string) workflow.InputBinding {
	return workflow.InputBinding{Type: workflow.ValueTypeString, Literal: mustJSON(value)}
}
func mustJSON(value any) json.RawMessage { encoded, _ := json.Marshal(value); return encoded }
func block(node legacyNode, code, message string) Blocker {
	return Blocker{NodeID: node.ID, Code: code, Message: message}
}
func dynamic(value string) bool { return strings.Contains(value, "${{") }
func sensitiveHeader(value string) bool {
	normalized := strings.ToLower(strings.ReplaceAll(value, "-", ""))
	return normalized == "authorization" || normalized == "xapikey" || strings.Contains(normalized, "token") || strings.Contains(normalized, "secret")
}

func shellSource(command any) (string, bool) {
	switch value := command.(type) {
	case string:
		return value, value != ""
	case []any:
		parts := make([]string, len(value))
		for index, item := range value {
			text, ok := item.(string)
			if !ok {
				return "", false
			}
			parts[index] = shellQuote(text)
		}
		return "exec " + strings.Join(parts, " "), len(parts) > 0
	default:
		return "", false
	}
}

func shellQuote(value string) string { return "'" + strings.ReplaceAll(value, "'", `'"'"'`) + "'" }

func migrationID(source string, used map[string]bool) string {
	candidate := source
	if !safeID.MatchString(candidate) {
		digest := sha256.Sum256([]byte(source))
		candidate = "m_" + hex.EncodeToString(digest[:8])
	}
	base := candidate
	for suffix := 2; used[candidate]; suffix++ {
		candidate = fmt.Sprintf("%s_%d", base, suffix)
	}
	used[candidate] = true
	return candidate
}

func inputPort(nodeType string) string {
	switch nodeType {
	case workflow.NodeTypeHTTP:
		return "body"
	case workflow.NodeTypeHumanApproval:
		return "request"
	case workflow.NodeTypeEnd:
		return "result"
	default:
		return "input"
	}
}
func outputPort(nodeType string) string {
	switch nodeType {
	case workflow.NodeTypeStart:
		return "input"
	case workflow.NodeTypeHTTP:
		return "response"
	case workflow.NodeTypeHumanApproval:
		return "decision"
	default:
		return "output"
	}
}

func legacyMetadata(value legacyWorkflow, sourceHash string) map[string]json.RawMessage {
	metadata := map[string]json.RawMessage{"name": mustJSON(value.Name), "fenixSourceHash": mustJSON(sourceHash)}
	if value.Description != "" {
		metadata["description"] = mustJSON(value.Description)
	}
	if len(value.Params) > 0 {
		metadata["params"] = mustJSON(value.Params)
	}
	if len(value.Secrets) > 0 {
		metadata["credentialNames"] = mustJSON(value.Secrets)
	}
	if value.Timeout != nil {
		metadata["timeoutSeconds"] = mustJSON(*value.Timeout)
	}
	return metadata
}
