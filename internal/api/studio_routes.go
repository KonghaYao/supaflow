package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/pazhou/supaflow/internal/definition"
)

func (h *Handler) registerStudioRoutes(mux *http.ServeMux) {
	if h.definitions == nil || h.nodeTypeCatalog == nil {
		return
	}
	mux.Handle("POST /v1/workflows", h.require(RoleEditor, http.HandlerFunc(h.createWorkflow)))
	mux.Handle("GET /v1/workflows", h.require(RoleViewer, http.HandlerFunc(h.listWorkflows)))
	mux.Handle("GET /v1/workflows/{workflowID}", h.require(RoleViewer, http.HandlerFunc(h.getWorkflow)))
	mux.Handle("PUT /v1/workflows/{workflowID}", h.require(RoleEditor, http.HandlerFunc(h.updateWorkflow)))
	mux.Handle("DELETE /v1/workflows/{workflowID}", h.require(RoleEditor, http.HandlerFunc(h.deleteWorkflow)))
	mux.Handle("POST /v1/workflows/{workflowID}/compile", h.require(RoleEditor, http.HandlerFunc(h.compileDraft)))
	mux.Handle("POST /v1/workflows/{workflowID}/publish-draft", h.require(RolePublisher, http.HandlerFunc(h.publishDraft)))
	mux.Handle("GET /v1/workflows/{workflowID}/versions", h.require(RoleViewer, http.HandlerFunc(h.listVersions)))
	mux.Handle("GET /v1/workflows/{workflowID}/versions/latest", h.require(RoleViewer, http.HandlerFunc(h.latestVersion)))
	mux.Handle("GET /v1/workflows/{workflowID}/versions/diff", h.require(RoleViewer, http.HandlerFunc(h.diffVersions)))
	mux.Handle("GET /v1/workflows/{workflowID}/versions/{version}", h.require(RoleViewer, http.HandlerFunc(h.getVersion)))
	mux.Handle("POST /v1/workflows/{workflowID}/versions/{version}/republish", h.require(RolePublisher, http.HandlerFunc(h.republishVersion)))
	mux.Handle("POST /v1/workflows/{workflowID}/dry-run", h.require(RoleEditor, http.HandlerFunc(h.dryRun)))
	mux.Handle("POST /v1/workflows/{workflowID}/node-test", h.require(RoleEditor, http.HandlerFunc(h.nodeTest)))
	mux.Handle("GET /v1/node-types", h.require(RoleViewer, http.HandlerFunc(h.nodeTypes)))
}

func (h *Handler) createWorkflow(w http.ResponseWriter, r *http.Request) {
	principal, _ := PrincipalFromContext(r.Context())
	key, ok := requireIdempotencyKey(w, r)
	if !ok {
		return
	}
	var body struct {
		WorkflowID string          `json:"workflowId"`
		Document   json.RawMessage `json:"document"`
	}
	if decodeJSON(w, r, &body) != nil {
		return
	}
	result, err := h.definitions.CreateDraft(r.Context(), principal.TenantID, definition.CreateDraftCommand{
		WorkflowID: body.WorkflowID, Document: body.Document, ActorID: principal.ActorID, IdempotencyKey: key,
	})
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, draftDTO(result))
}

func (h *Handler) listWorkflows(w http.ResponseWriter, r *http.Request) {
	principal, _ := PrincipalFromContext(r.Context())
	items, err := h.definitions.ListDrafts(r.Context(), principal.TenantID)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	result := make([]draftResponse, len(items))
	for index, item := range items {
		result[index] = draftDTO(item)
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *Handler) getWorkflow(w http.ResponseWriter, r *http.Request) {
	principal, _ := PrincipalFromContext(r.Context())
	result, err := h.definitions.GetDraft(r.Context(), principal.TenantID, r.PathValue("workflowID"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, draftDTO(result))
}

func (h *Handler) updateWorkflow(w http.ResponseWriter, r *http.Request) {
	principal, _ := PrincipalFromContext(r.Context())
	key, ok := requireIdempotencyKey(w, r)
	if !ok {
		return
	}
	var body struct {
		Document         json.RawMessage `json:"document"`
		ExpectedRevision uint64          `json:"expectedRevision"`
	}
	if decodeJSON(w, r, &body) != nil {
		return
	}
	result, err := h.definitions.UpdateDraft(r.Context(), principal.TenantID, definition.UpdateDraftCommand{
		WorkflowID: r.PathValue("workflowID"), Document: body.Document, ExpectedRevision: body.ExpectedRevision,
		ActorID: principal.ActorID, IdempotencyKey: key,
	})
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, draftDTO(result))
}

func (h *Handler) deleteWorkflow(w http.ResponseWriter, r *http.Request) {
	principal, _ := PrincipalFromContext(r.Context())
	key, ok := requireIdempotencyKey(w, r)
	if !ok {
		return
	}
	var body struct {
		ExpectedRevision uint64 `json:"expectedRevision"`
	}
	if decodeJSON(w, r, &body) != nil {
		return
	}
	err := h.definitions.DeleteDraft(r.Context(), principal.TenantID, definition.DeleteDraftCommand{
		WorkflowID: r.PathValue("workflowID"), ExpectedRevision: body.ExpectedRevision,
		ActorID: principal.ActorID, IdempotencyKey: key,
	})
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) compileDraft(w http.ResponseWriter, r *http.Request) {
	principal, _ := PrincipalFromContext(r.Context())
	plan, err := h.definitions.CompileDraft(r.Context(), principal.TenantID, r.PathValue("workflowID"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, plan)
}

func (h *Handler) publishDraft(w http.ResponseWriter, r *http.Request) {
	principal, _ := PrincipalFromContext(r.Context())
	key, ok := requireIdempotencyKey(w, r)
	if !ok {
		return
	}
	published, created, err := h.definitions.PublishDraft(r.Context(), principal.TenantID, r.PathValue("workflowID"), key)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, published)
}

func (h *Handler) listVersions(w http.ResponseWriter, r *http.Request) {
	principal, _ := PrincipalFromContext(r.Context())
	items, err := h.definitions.ListVersions(r.Context(), principal.TenantID, r.PathValue("workflowID"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	result := make([]versionSummaryResponse, len(items))
	for index, item := range items {
		result[index] = versionSummaryDTO(item)
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *Handler) latestVersion(w http.ResponseWriter, r *http.Request) {
	principal, _ := PrincipalFromContext(r.Context())
	item, err := h.definitions.LatestVersion(r.Context(), principal.TenantID, r.PathValue("workflowID"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, versionSummaryDTO(item))
}

func (h *Handler) getVersion(w http.ResponseWriter, r *http.Request) {
	principal, _ := PrincipalFromContext(r.Context())
	version, ok := parseVersion(w, r, r.PathValue("version"))
	if !ok {
		return
	}
	item, err := h.definitions.GetVersion(r.Context(), principal.TenantID, r.PathValue("workflowID"), version)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, versionDTO(item))
}

func (h *Handler) diffVersions(w http.ResponseWriter, r *http.Request) {
	principal, _ := PrincipalFromContext(r.Context())
	from, ok := parseVersion(w, r, r.URL.Query().Get("from"))
	if !ok {
		return
	}
	to, ok := parseVersion(w, r, r.URL.Query().Get("to"))
	if !ok {
		return
	}
	result, err := h.definitions.DiffVersions(r.Context(), principal.TenantID, r.PathValue("workflowID"), from, to)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *Handler) republishVersion(w http.ResponseWriter, r *http.Request) {
	principal, _ := PrincipalFromContext(r.Context())
	key, ok := requireIdempotencyKey(w, r)
	if !ok {
		return
	}
	version, ok := parseVersion(w, r, r.PathValue("version"))
	if !ok {
		return
	}
	result, created, err := h.definitions.Republish(r.Context(), principal.TenantID, r.PathValue("workflowID"), version, key)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, result)
}

func (h *Handler) dryRun(w http.ResponseWriter, r *http.Request) {
	principal, _ := PrincipalFromContext(r.Context())
	var body struct {
		SampleInput json.RawMessage `json:"sampleInput"`
	}
	if decodeJSON(w, r, &body) != nil {
		return
	}
	result, err := h.definitions.DryRun(r.Context(), principal.TenantID, r.PathValue("workflowID"), body.SampleInput)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, dryRunDTO(result))
}

func (h *Handler) nodeTest(w http.ResponseWriter, r *http.Request) {
	principal, _ := PrincipalFromContext(r.Context())
	var body struct {
		NodeID      string          `json:"nodeId"`
		SampleInput json.RawMessage `json:"sampleInput"`
	}
	if decodeJSON(w, r, &body) != nil {
		return
	}
	result, err := h.definitions.TestNode(r.Context(), principal.TenantID, r.PathValue("workflowID"), body.NodeID, body.SampleInput)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, nodeTestDTO(result))
}

func (h *Handler) nodeTypes(w http.ResponseWriter, r *http.Request) {
	entries, err := h.nodeTypeCatalog.ListNodeTypes(r.Context())
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, entries)
}

func parseVersion(w http.ResponseWriter, r *http.Request, value string) (uint64, bool) {
	version, err := strconv.ParseUint(value, 10, 64)
	if err != nil || version == 0 {
		writeProblem(w, r, http.StatusBadRequest, "invalid_version", "version must be a positive integer")
		return 0, false
	}
	return version, true
}
