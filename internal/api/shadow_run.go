package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/pazhou/supaflow/internal/shadowrun"
)

type shadowRunRequest struct {
	PlanID       string            `json:"planId"`
	PlanHash     string            `json:"planHash"`
	Input        json.RawMessage   `json:"input"`
	StubBindings map[string]string `json:"stubBindings"`
	Priority     int               `json:"priority"`
	TimeoutMS    int64             `json:"timeoutMs"`
}

func (h *Handler) startShadowRun(w http.ResponseWriter, r *http.Request) {
	principal, _ := PrincipalFromContext(r.Context())
	idempotencyKey, ok := requireIdempotencyKey(w, r)
	if !ok {
		return
	}
	var body shadowRunRequest
	if err := decodeJSON(w, r, &body); err != nil {
		return
	}
	if body.StubBindings == nil {
		writeProblem(w, r, http.StatusBadRequest, string(shadowrun.CodeInvalidShadowStubBinding), "stubBindings must be an object")
		return
	}
	result, err := h.shadowRuns.Start(r.Context(), principal.TenantID, shadowrun.StartRequest{
		PlanID: body.PlanID, PlanHash: body.PlanHash, Input: body.Input,
		StubBindings: body.StubBindings, IdempotencyKey: idempotencyKey,
		Priority: body.Priority, Timeout: time.Duration(body.TimeoutMS) * time.Millisecond,
	})
	if err != nil {
		var shadowErr *shadowrun.Error
		if errors.As(err, &shadowErr) {
			switch shadowErr.Code {
			case shadowrun.CodeShadowStubRequired:
				writeProblem(w, r, http.StatusConflict, string(shadowErr.Code), "every side-effect node requires the reviewed shadow stub")
			case shadowrun.CodeInvalidShadowStubBinding:
				writeProblem(w, r, http.StatusBadRequest, string(shadowErr.Code), "shadow stub bindings are invalid")
			default:
				writeProblem(w, r, http.StatusConflict, string(shadowErr.Code), "shadow plan cannot be started")
			}
			return
		}
		h.writeError(w, r, err)
		return
	}
	status := http.StatusOK
	if result.Created {
		status = http.StatusAccepted
	}
	writeJSON(w, status, runDTO(result.Run))
}
