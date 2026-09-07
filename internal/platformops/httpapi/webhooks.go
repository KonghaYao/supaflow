package httpapi

import (
	"net/http"
	"time"

	"github.com/pazhou/supaflow/internal/platformops"
)

func (h *Handler) listWebhookSubscriptions(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	items, err := h.services.Webhooks.List(r.Context(), p.OrganizationID, queryLimit(r, 100))
	respond(w, items, err, http.StatusOK)
}

func (h *Handler) createWebhookSubscription(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	var body struct {
		Name                string   `json:"name"`
		EndpointURL         string   `json:"endpointUrl"`
		EventTypes          []string `json:"eventTypes"`
		SigningCredentialID string   `json:"signingCredentialId"`
		TimeoutMS           int      `json:"timeoutMs"`
	}
	if !decodeJSON(w, r, &body) {
		return
	}
	item, err := h.services.Webhooks.Create(r.Context(), platformops.CreateOutboundWebhookRequest{
		OrganizationID: p.OrganizationID, ActorID: p.ActorID, Name: body.Name, EndpointURL: body.EndpointURL,
		EventTypes: body.EventTypes, SigningCredentialID: body.SigningCredentialID,
		Timeout: time.Duration(body.TimeoutMS) * time.Millisecond,
	})
	respond(w, item, err, http.StatusCreated)
}

func (h *Handler) enableWebhookSubscription(w http.ResponseWriter, r *http.Request) {
	h.setWebhookSubscriptionEnabled(w, r, true)
}

func (h *Handler) disableWebhookSubscription(w http.ResponseWriter, r *http.Request) {
	h.setWebhookSubscriptionEnabled(w, r, false)
}

func (h *Handler) setWebhookSubscriptionEnabled(w http.ResponseWriter, r *http.Request, enabled bool) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	item, err := h.services.Webhooks.SetEnabled(r.Context(), p.OrganizationID, r.PathValue("id"), p.ActorID, enabled, time.Now().UTC())
	respond(w, item, err, http.StatusOK)
}

func (h *Handler) deleteWebhookSubscription(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	err := h.services.Webhooks.Delete(r.Context(), p.OrganizationID, r.PathValue("id"), p.ActorID, time.Now().UTC())
	respond(w, map[string]bool{"deleted": err == nil}, err, http.StatusOK)
}
