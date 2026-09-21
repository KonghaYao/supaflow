// Package flowgram contains the only backend model coupled to FlowGram's document shape.
package flowgram

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/pazhou/supaflow/internal/workflow"
)

const schemaVersion = "flowgram-v1"

// Adapter converts the UI-owned FlowGram document into an editor-independent definition.
type Adapter struct{}

type document struct {
	SchemaVersion string                     `json:"schemaVersion"`
	WorkflowID    string                     `json:"workflowId"`
	Nodes         []node                     `json:"nodes"`
	Edges         []edge                     `json:"edges"`
	Groups        []group                    `json:"groups,omitempty"`
	Viewport      *viewport                  `json:"viewport,omitempty"`
	Metadata      map[string]json.RawMessage `json:"metadata,omitempty"`
}

type node struct {
	ID      string `json:"id"`
	Type    string `json:"type"`
	Version string `json:"version"`
	// Label is the canvas caption the editor always writes. Like position and ui it is
	// presentation metadata with no execution meaning, so the converter drops it. Decoding
	// is strict, so an unlisted field here is not tolerated: omitting Label made every
	// document the editor saved undecodable and every compile, dry-run, and publish-draft
	// of it fail.
	Label    string                     `json:"label,omitempty"`
	Config   json.RawMessage            `json:"config"`
	Inputs   map[string]inputBinding    `json:"inputs,omitempty"`
	Position position                   `json:"position"`
	GroupID  string                     `json:"groupId,omitempty"`
	UI       map[string]json.RawMessage `json:"ui,omitempty"`
}

type inputBinding struct {
	Type    workflow.ValueType        `json:"type"`
	Literal json.RawMessage           `json:"literal,omitempty"`
	Source  *workflow.OutputReference `json:"source,omitempty"`
}

type edge struct {
	ID           string `json:"id"`
	SourceNodeID string `json:"sourceNodeId"`
	SourcePortID string `json:"sourcePortId"`
	TargetNodeID string `json:"targetNodeId"`
	TargetPortID string `json:"targetPortId"`
}

type group struct {
	ID      string   `json:"id"`
	Title   string   `json:"title"`
	NodeIDs []string `json:"nodeIds"`
}

type position struct{ X, Y float64 }
type viewport struct{ X, Y, Zoom float64 }

// ToDefinition strictly validates and converts one FlowGram editor document.
func (Adapter) ToDefinition(raw json.RawMessage) (workflow.WorkflowDefinition, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var source document
	if err := decoder.Decode(&source); err != nil {
		// 形状错误上报给调用方，所以交给 workflow.JSONTypeError 去掉 Go 结构体名与类型名。
		return workflow.WorkflowDefinition{}, fmt.Errorf("decode FlowGram document: %w", workflow.JSONTypeError(err))
	}
	if err := ensureEOF(decoder); err != nil {
		return workflow.WorkflowDefinition{}, err
	}
	if source.SchemaVersion != schemaVersion || source.WorkflowID == "" {
		return workflow.WorkflowDefinition{}, errors.New("FlowGram schemaVersion and workflowId are required")
	}
	result := workflow.WorkflowDefinition{SchemaVersion: workflow.DefinitionSchemaV1, ID: source.WorkflowID, Metadata: source.Metadata}
	result.Nodes = make([]workflow.NodeDefinition, 0, len(source.Nodes))
	for _, item := range source.Nodes {
		inputs := make(map[string]workflow.InputBinding, len(item.Inputs))
		for name, input := range item.Inputs {
			inputs[name] = workflow.InputBinding{Type: input.Type, Literal: input.Literal, Source: input.Source}
		}
		result.Nodes = append(result.Nodes, workflow.NodeDefinition{ID: item.ID, Type: item.Type, Version: item.Version, Config: item.Config, Inputs: inputs})
	}
	result.Edges = make([]workflow.EdgeDefinition, 0, len(source.Edges))
	for _, item := range source.Edges {
		result.Edges = append(result.Edges, workflow.EdgeDefinition{ID: item.ID, FromNode: item.SourceNodeID, FromPort: item.SourcePortID, ToNode: item.TargetNodeID, ToPort: item.TargetPortID})
	}
	return result, nil
}

func ensureEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("FlowGram document contains multiple JSON values")
		}
		return fmt.Errorf("decode FlowGram trailing data: %w", err)
	}
	return nil
}
