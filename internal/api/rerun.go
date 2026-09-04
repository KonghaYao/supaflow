package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	"github.com/pazhou/supaflow/internal/runtime"
)

type rerunRequestBody struct {
	FromNodeID json.RawMessage `json:"fromNodeId"`
	Priority   int             `json:"priority"`
	TimeoutMS  int64           `json:"timeoutMs"`
}

func (h *Handler) rerunRun(w http.ResponseWriter, r *http.Request) {
	principal, _ := PrincipalFromContext(r.Context())
	idempotencyKey, ok := requireIdempotencyKey(w, r)
	if !ok {
		return
	}
	var body *rerunRequestBody
	if err := decodeJSON(w, r, &body); err != nil {
		return
	}
	if body == nil {
		writeProblem(w, r, http.StatusBadRequest, "invalid_json", "request body must be an object")
		return
	}
	fromNodeID := ""
	if len(body.FromNodeID) > 0 {
		if bytes.Equal(bytes.TrimSpace(body.FromNodeID), []byte("null")) || json.Unmarshal(body.FromNodeID, &fromNodeID) != nil || fromNodeID == "" {
			writeProblem(w, r, http.StatusBadRequest, "invalid_argument", "fromNodeId must be a non-empty string")
			return
		}
	}
	source, err := h.runtime.GetRun(r.Context(), runtime.TenantID(principal.TenantID), r.PathValue("runID"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	published, err := h.catalog.Resolve(r.Context(), principal.TenantID, source.Plan.ID, source.Plan.Hash)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	entryNodeID := fromNodeID
	if entryNodeID == "" {
		order := published.Plan.TopologicalOrder()
		if len(order) == 0 {
			writeProblem(w, r, http.StatusConflict, "invalid_plan", "published plan has no entry node")
			return
		}
		entryNodeID = order[0]
	}
	entry, found := published.Plan.Node(entryNodeID)
	if !found {
		writeProblem(w, r, http.StatusBadRequest, "invalid_argument", "fromNodeId does not exist in the source plan")
		return
	}
	result, err := h.runtime.Rerun(r.Context(), runtime.TenantID(principal.TenantID), runtime.RerunRequest{
		SourceRunID: source.ID, FromNodeID: fromNodeID, EntryNodeID: entry.ID,
		EntryExecutor: entry.Executor.Kind, IdempotencyKey: idempotencyKey, Priority: body.Priority,
		Timeout: time.Duration(body.TimeoutMS) * time.Millisecond,
	})
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	status := http.StatusOK
	if result.Created {
		status = http.StatusAccepted
	}
	writeJSON(w, status, runDTO(result.Run))
}
