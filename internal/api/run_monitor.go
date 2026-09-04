package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pazhou/supaflow/internal/runtime"
)

func (h *Handler) registerRunMonitorRoutes(mux *http.ServeMux) {
	mux.Handle("GET /v1/runs", h.require(RoleViewer, http.HandlerFunc(h.listRuns)))
	mux.Handle("GET /v1/runs/{runID}/details", h.require(RoleViewer, http.HandlerFunc(h.runDetails)))
}

func (h *Handler) listRuns(w http.ResponseWriter, r *http.Request) {
	principal, _ := PrincipalFromContext(r.Context())
	limit := 50
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			writeProblem(w, r, http.StatusBadRequest, "invalid_argument", "limit must be an integer")
			return
		}
		limit = parsed
	}
	filter := runtime.RunFilter{WorkflowID: strings.TrimSpace(r.URL.Query().Get("workflowId")),
		Status: runtime.RunStatus(strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("status")))), Limit: limit}
	items, err := h.runtime.ListRuns(r.Context(), runtime.TenantID(principal.TenantID), filter)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	response := make([]runSummaryResponse, len(items))
	for index, item := range items {
		response[index] = runSummaryResponse{runResponse: runDTO(item.Run), WorkflowID: item.WorkflowID}
	}
	writeJSON(w, http.StatusOK, response)
}

func (h *Handler) runDetails(w http.ResponseWriter, r *http.Request) {
	principal, _ := PrincipalFromContext(r.Context())
	details, err := h.runtime.GetRunDetails(r.Context(), runtime.TenantID(principal.TenantID), r.PathValue("runID"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, runDetailsDTO(details))
}

type runSummaryResponse struct {
	runResponse
	WorkflowID string `json:"workflowId"`
}

type runDetailsResponse struct {
	Run             runResponse              `json:"run"`
	Input           json.RawMessage          `json:"input"`
	Nodes           []nodeDiagnosticResponse `json:"nodes"`
	LastEventCursor uint64                   `json:"lastEventCursor"`
}

type nodeDiagnosticResponse struct {
	NodeID          string                      `json:"nodeId"`
	Status          runtime.NodeStatus          `json:"status"`
	AttemptCount    int                         `json:"attemptCount"`
	LatestAttemptID string                      `json:"latestAttemptId"`
	UpdatedAt       time.Time                   `json:"updatedAt"`
	Attempts        []attemptDiagnosticResponse `json:"attempts"`
	Output          json.RawMessage             `json:"output,omitempty"`
	OutputMetadata  *outputMetadata             `json:"outputMetadata,omitempty"`
}

type attemptDiagnosticResponse struct {
	ID            string                `json:"id"`
	Number        int                   `json:"number"`
	Executor      string                `json:"executor"`
	Status        runtime.AttemptStatus `json:"status"`
	Input         json.RawMessage       `json:"input"`
	ErrorCode     string                `json:"errorCode,omitempty"`
	NextAttemptAt time.Time             `json:"nextAttemptAt"`
	StartedAt     *time.Time            `json:"startedAt,omitempty"`
	FinishedAt    *time.Time            `json:"finishedAt,omitempty"`
	CreatedAt     time.Time             `json:"createdAt"`
	UpdatedAt     time.Time             `json:"updatedAt"`
}

func runDetailsDTO(details runtime.RunDetails) runDetailsResponse {
	response := runDetailsResponse{Run: runDTO(details.Run), Input: bytes.Clone(details.Run.Input),
		Nodes: make([]nodeDiagnosticResponse, len(details.Nodes)), LastEventCursor: details.LastEventCursor}
	for index, diagnostic := range details.Nodes {
		node := nodeDiagnosticResponse{NodeID: diagnostic.Node.NodeID, Status: diagnostic.Node.Status,
			AttemptCount: diagnostic.Node.AttemptCount, LatestAttemptID: diagnostic.Node.LatestAttemptID,
			UpdatedAt: diagnostic.Node.UpdatedAt, Attempts: make([]attemptDiagnosticResponse, len(diagnostic.Attempts))}
		for attemptIndex, item := range diagnostic.Attempts {
			attempt := item.Attempt
			node.Attempts[attemptIndex] = attemptDiagnosticResponse{ID: attempt.ID, Number: attempt.Number,
				Executor: attempt.Executor, Status: attempt.Status, Input: bytes.Clone(attempt.Input), ErrorCode: attempt.ErrorCode,
				NextAttemptAt: attempt.NextAttemptAt, StartedAt: attempt.StartedAt, FinishedAt: attempt.FinishedAt,
				CreatedAt: attempt.CreatedAt, UpdatedAt: attempt.UpdatedAt}
		}
		if diagnostic.Output != nil {
			storage := "inline"
			if diagnostic.Output.External {
				storage = "object"
			} else if !diagnostic.Output.Sensitive {
				node.Output = bytes.Clone(diagnostic.Output.Inline)
			}
			node.OutputMetadata = &outputMetadata{ContentType: diagnostic.Output.ContentType, SizeBytes: diagnostic.Output.Size,
				SHA256: diagnostic.Output.SHA256, Sensitive: diagnostic.Output.Sensitive, Storage: storage}
		}
		response.Nodes[index] = node
	}
	return response
}
