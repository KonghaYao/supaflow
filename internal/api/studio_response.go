package api

import (
	"encoding/json"
	"time"

	"github.com/pazhou/supaflow/internal/definition"
	"github.com/pazhou/supaflow/internal/workflow"
)

type draftResponse struct {
	ID         string          `json:"id"`
	WorkflowID string          `json:"workflowId"`
	Document   json.RawMessage `json:"document"`
	Revision   uint64          `json:"revision"`
	UpdatedBy  string          `json:"updatedBy"`
	CreatedAt  time.Time       `json:"createdAt"`
	UpdatedAt  time.Time       `json:"updatedAt"`
}

func draftDTO(value definition.Draft) draftResponse {
	return draftResponse{ID: value.ID, WorkflowID: value.WorkflowID, Document: value.Document,
		Revision: value.Revision, UpdatedBy: value.UpdatedBy, CreatedAt: value.CreatedAt, UpdatedAt: value.UpdatedAt}
}

type versionSummaryResponse struct {
	WorkflowID  string    `json:"workflowId"`
	Version     uint64    `json:"version"`
	VersionID   string    `json:"versionId"`
	PlanID      string    `json:"planId"`
	PlanHash    string    `json:"planHash"`
	PublishedAt time.Time `json:"publishedAt"`
}

func versionSummaryDTO(value definition.Version) versionSummaryResponse {
	return versionSummaryResponse{WorkflowID: value.WorkflowID, Version: value.Version, VersionID: value.VersionID,
		PlanID: value.PlanID, PlanHash: value.PlanHash, PublishedAt: value.PublishedAt}
}

type versionResponse struct {
	versionSummaryResponse
	Definition workflow.WorkflowDefinition `json:"definition"`
}

func versionDTO(value definition.Version) versionResponse {
	return versionResponse{versionSummaryResponse: versionSummaryDTO(value), Definition: value.Definition}
}

type dryRunResponse struct {
	Mode     string                `json:"mode"`
	PlanHash string                `json:"planHash"`
	Steps    []previewStepResponse `json:"steps"`
	Output   json.RawMessage       `json:"output"`
}

type previewStepResponse struct {
	NodeID       string          `json:"nodeId"`
	Type         string          `json:"type"`
	Status       string          `json:"status"`
	SelectedPort string          `json:"selectedPort,omitempty"`
	Output       json.RawMessage `json:"output,omitempty"`
}

func dryRunDTO(value definition.DryRunResult) dryRunResponse {
	steps := make([]previewStepResponse, len(value.Steps))
	for index, step := range value.Steps {
		steps[index] = previewStepResponse{NodeID: step.NodeID, Type: step.Type, Status: step.Status, SelectedPort: step.SelectedPort, Output: step.Output}
	}
	return dryRunResponse{Mode: "simulated", PlanHash: value.PlanHash, Steps: steps, Output: value.Output}
}

type nodeTestResponse struct {
	Mode         string                   `json:"mode"`
	NodeID       string                   `json:"nodeId"`
	NodeType     string                   `json:"nodeType"`
	Executor     workflow.ExecutorBinding `json:"executor"`
	Status       string                   `json:"status"`
	SelectedPort string                   `json:"selectedPort,omitempty"`
	Output       json.RawMessage          `json:"output,omitempty"`
}

func nodeTestDTO(value definition.NodeTestResult) nodeTestResponse {
	return nodeTestResponse{Mode: "simulated", NodeID: value.NodeID, NodeType: value.NodeType,
		Executor: value.Executor, Status: value.Status, SelectedPort: value.SelectedPort, Output: value.Output}
}
