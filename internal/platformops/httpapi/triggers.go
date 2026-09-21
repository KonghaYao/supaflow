package httpapi

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pazhou/supaflow/internal/platformops"
)

func (h *Handler) createTrigger(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	var body triggerBody
	if !decodeJSON(w, r, &body) {
		return
	}
	key, ok := mutationKey(w, r)
	if !ok {
		return
	}
	trigger, err := h.services.Triggers.Create(r.Context(), platformops.CreateTriggerRequest{
		OrganizationID: p.OrganizationID, ActorID: p.ActorID, WorkflowID: body.WorkflowID, Kind: body.Kind,
		Name: body.Name, Config: body.Config, ScheduleInterval: time.Duration(body.ScheduleIntervalSeconds) * time.Second,
		MisfirePolicy: body.MisfirePolicy, FirstFireAt: body.FirstFireAt, SigningCredentialID: body.SigningCredentialID,
		EventType: body.EventType, IdempotencyKey: key})
	respond(w, r, trigger, err, http.StatusCreated)
}

func (h *Handler) listTriggers(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	var enabled *bool
	if raw := strings.TrimSpace(r.URL.Query().Get("enabled")); raw != "" {
		value, err := strconv.ParseBool(raw)
		if err != nil {
			writeProblem(w, r, http.StatusBadRequest, "invalid_request")
			return
		}
		enabled = &value
	}
	items, err := h.services.Triggers.List(r.Context(), platformops.ListTriggersRequest{OrganizationID: p.OrganizationID,
		WorkflowID: strings.TrimSpace(r.URL.Query().Get("workflowId")), Kind: platformops.TriggerKind(strings.TrimSpace(r.URL.Query().Get("kind"))),
		Enabled: enabled, Limit: queryLimit(r, 100)})
	respond(w, r, items, err, http.StatusOK)
}

func (h *Handler) getTrigger(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	trigger, err := h.services.Triggers.Get(r.Context(), p.OrganizationID, r.PathValue("id"))
	respond(w, r, trigger, err, http.StatusOK)
}

func (h *Handler) updateTrigger(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	key, ok := mutationKey(w, r)
	if !ok {
		return
	}
	var body updateTriggerBody
	if !decodeJSON(w, r, &body) {
		return
	}
	trigger, err := h.services.Triggers.Update(r.Context(), platformops.UpdateTriggerRequest{OrganizationID: p.OrganizationID,
		ActorID: p.ActorID, TriggerID: r.PathValue("id"), Name: body.Name, Config: body.Config,
		ScheduleInterval: time.Duration(body.ScheduleIntervalSeconds) * time.Second, MisfirePolicy: body.MisfirePolicy,
		FirstFireAt: body.FirstFireAt, SigningCredentialID: body.SigningCredentialID, EventType: body.EventType,
		ExpectedUpdatedAt: body.ExpectedUpdatedAt, IdempotencyKey: key})
	respond(w, r, trigger, err, http.StatusOK)
}

func (h *Handler) changeTriggerState(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	key, ok := mutationKey(w, r)
	if !ok {
		return
	}
	var body struct {
		Enabled           bool      `json:"enabled"`
		ExpectedUpdatedAt time.Time `json:"expectedUpdatedAt"`
	}
	if !decodeJSON(w, r, &body) {
		return
	}
	trigger, err := h.services.Triggers.ChangeState(r.Context(), platformops.ChangeTriggerStateRequest{OrganizationID: p.OrganizationID,
		ActorID: p.ActorID, TriggerID: r.PathValue("id"), Enabled: body.Enabled,
		ExpectedUpdatedAt: body.ExpectedUpdatedAt, IdempotencyKey: key, Now: time.Now().UTC()})
	respond(w, r, trigger, err, http.StatusOK)
}

func (h *Handler) deleteTrigger(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	key, ok := mutationKey(w, r)
	if !ok {
		return
	}
	expected, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(r.URL.Query().Get("expectedUpdatedAt")))
	if err != nil {
		writeProblem(w, r, http.StatusBadRequest, "invalid_request")
		return
	}
	err = h.services.Triggers.Delete(r.Context(), platformops.DeleteTriggerRequest{OrganizationID: p.OrganizationID, ActorID: p.ActorID,
		TriggerID: r.PathValue("id"), ExpectedUpdatedAt: expected, IdempotencyKey: key})
	if err != nil {
		respond(w, r, nil, err, http.StatusNoContent)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type triggerBody struct {
	WorkflowID              string                    `json:"workflowId"`
	Kind                    platformops.TriggerKind   `json:"kind"`
	Name                    string                    `json:"name"`
	Config                  json.RawMessage           `json:"config"`
	ScheduleIntervalSeconds int64                     `json:"scheduleIntervalSeconds"`
	MisfirePolicy           platformops.MisfirePolicy `json:"misfirePolicy"`
	FirstFireAt             *time.Time                `json:"firstFireAt"`
	SigningCredentialID     string                    `json:"signingCredentialId"`
	EventType               string                    `json:"eventType"`
}

type updateTriggerBody struct {
	Name                    string                    `json:"name"`
	Config                  json.RawMessage           `json:"config"`
	ScheduleIntervalSeconds int64                     `json:"scheduleIntervalSeconds"`
	MisfirePolicy           platformops.MisfirePolicy `json:"misfirePolicy"`
	FirstFireAt             *time.Time                `json:"firstFireAt"`
	SigningCredentialID     string                    `json:"signingCredentialId"`
	EventType               string                    `json:"eventType"`
	ExpectedUpdatedAt       time.Time                 `json:"expectedUpdatedAt"`
}

func (h *Handler) fireTrigger(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	var body struct {
		Kind           platformops.TriggerKind `json:"kind"`
		IdempotencyKey string                  `json:"idempotencyKey"`
		Payload        json.RawMessage         `json:"payload"`
	}
	if !decodeJSON(w, r, &body) {
		return
	}
	firing, created, err := h.services.Triggers.Fire(r.Context(), platformops.FireRequest{OrganizationID: p.OrganizationID,
		TriggerID: r.PathValue("id"), ExpectedKind: body.Kind, DeduplicationKey: body.IdempotencyKey, Payload: body.Payload})
	respond(w, r, map[string]any{"firing": firing, "created": created}, err, http.StatusOK)
}

func (h *Handler) receiveEvent(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	var body struct {
		EventID string          `json:"eventId"`
		Payload json.RawMessage `json:"payload"`
	}
	if !decodeJSON(w, r, &body) {
		return
	}
	firings, err := h.services.Triggers.ReceiveEvent(r.Context(), p.OrganizationID, r.PathValue("eventType"), body.EventID, body.Payload, time.Now().UTC())
	respond(w, r, map[string]any{"firings": firings}, err, http.StatusAccepted)
}
