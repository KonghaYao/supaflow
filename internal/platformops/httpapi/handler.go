// Package httpapi exposes authenticated platform-operations APIs for the Studio console.
package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pazhou/supaflow/internal/api"
	"github.com/pazhou/supaflow/internal/platformops"
)

// Services contains the application seams consumed by the operations transport.
type Services struct {
	Triggers    *platformops.TriggerService
	Credentials *platformops.CredentialService
	Admission   *platformops.Admission
	Audit       *platformops.AuditService
	Retention   *platformops.Retention
	Delivery    *platformops.PostgresDeliveryStore
	Capacity    *platformops.CapacityMonitor
	Workers     *platformops.WorkerRegistry
	Readiness   *platformops.Readiness
	Webhooks    OutboundWebhookManager
	Mutations   platformops.MutationLedger
}

// OutboundWebhookManager is the tenant-scoped subscription lifecycle seam.
type OutboundWebhookManager interface {
	Create(context.Context, platformops.CreateOutboundWebhookRequest) (platformops.OutboundWebhookSubscription, error)
	List(context.Context, string, int) ([]platformops.OutboundWebhookSubscription, error)
	SetEnabled(context.Context, string, string, string, bool, time.Time) (platformops.OutboundWebhookSubscription, error)
	Delete(context.Context, string, string, string, time.Time) error
}

// Handler exposes operations endpoints without owning authentication middleware.
type Handler struct{ services Services }

// NewHandler validates mandatory platform operations services.
func NewHandler(services Services) (*Handler, error) {
	if services.Triggers == nil || services.Credentials == nil || services.Admission == nil ||
		services.Audit == nil || services.Retention == nil || services.Delivery == nil ||
		services.Capacity == nil || services.Workers == nil || services.Readiness == nil || services.Webhooks == nil || services.Mutations == nil {
		return nil, platformops.ErrInvalidArgument
	}
	return &Handler{services: services}, nil
}

// Routes returns a mux intended to be mounted behind api.Authenticator.Middleware.
func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ops/health", h.health)
	mux.HandleFunc("GET /ops/ready", h.ready)
	mux.HandleFunc("GET /ops/metrics", h.capacity)
	mux.HandleFunc("GET /ops/capacity", h.capacity)
	mux.HandleFunc("GET /ops/audit", h.audit)
	mux.HandleFunc("GET /ops/dlq", h.deadLetters)
	mux.HandleFunc("POST /ops/dlq/{id}/requeue", h.requeueDeadLetter)
	mux.HandleFunc("POST /ops/workers/{id}/drain", h.drainWorker)
	mux.HandleFunc("POST /ops/quotas", h.configureQuota)
	mux.HandleFunc("POST /ops/triggers", h.createTrigger)
	mux.HandleFunc("GET /ops/triggers", h.listTriggers)
	mux.HandleFunc("GET /ops/triggers/{id}", h.getTrigger)
	mux.HandleFunc("PUT /ops/triggers/{id}", h.updateTrigger)
	mux.HandleFunc("POST /ops/triggers/{id}/state", h.changeTriggerState)
	mux.HandleFunc("DELETE /ops/triggers/{id}", h.deleteTrigger)
	mux.HandleFunc("POST /ops/triggers/{id}/fire", h.fireTrigger)
	mux.HandleFunc("POST /ops/events/{eventType}", h.receiveEvent)
	mux.HandleFunc("POST /ops/credentials", h.createCredential)
	mux.HandleFunc("POST /ops/credentials/{id}/rotate", h.rotateCredential)
	mux.HandleFunc("POST /ops/credentials/{id}/revoke", h.revokeCredential)
	mux.HandleFunc("POST /ops/retention", h.setRetention)
	mux.HandleFunc("POST /ops/exports", h.exportTenant)
	mux.HandleFunc("GET /ops/webhook-subscriptions", h.listWebhookSubscriptions)
	mux.HandleFunc("POST /ops/webhook-subscriptions", h.createWebhookSubscription)
	mux.HandleFunc("POST /ops/webhook-subscriptions/{id}/enable", h.enableWebhookSubscription)
	mux.HandleFunc("POST /ops/webhook-subscriptions/{id}/disable", h.disableWebhookSubscription)
	mux.HandleFunc("DELETE /ops/webhook-subscriptions/{id}", h.deleteWebhookSubscription)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		h.idempotentMutations(mux).ServeHTTP(w, r)
	})
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	if _, ok := principal(w, r); !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// WebhookRoutes returns the signature-authenticated public hook surface.
// It must be mounted outside API-key middleware at POST /hooks/{publicHash}.
func (h *Handler) WebhookRoutes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /hooks/{publicHash}", h.receiveWebhook)
	return mux
}

func (h *Handler) receiveWebhook(w http.ResponseWriter, r *http.Request) {
	timestampSeconds, err := strconv.ParseInt(strings.TrimSpace(r.Header.Get("X-Supaflow-Timestamp")), 10, 64)
	if err != nil {
		writeProblem(w, http.StatusUnauthorized, "invalid_signature")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
	if err != nil || len(body) > 1<<20 {
		writeProblem(w, http.StatusBadRequest, "invalid_request")
		return
	}
	trigger, err := h.services.Triggers.ResolvePublicWebhook(r.Context(), r.PathValue("publicHash"))
	if err != nil {
		respond(w, nil, err, http.StatusAccepted)
		return
	}
	platformops.AttributeTelemetryTenant(r.Context(), trigger.TenantID)
	firing, created, err := h.services.Triggers.ReceiveWebhook(r.Context(), platformops.WebhookRequest{
		TenantID: trigger.TenantID, TriggerID: trigger.ID, RequestID: strings.TrimSpace(r.Header.Get("X-Supaflow-Request-ID")),
		Timestamp: time.Unix(timestampSeconds, 0).UTC(), Signature: r.Header.Get("X-Supaflow-Signature"),
		Body: body, Now: time.Now().UTC(),
	})
	respond(w, map[string]any{"firingId": firing.ID, "created": created}, err, http.StatusAccepted)
}

func (h *Handler) ready(w http.ResponseWriter, r *http.Request) {
	if _, ok := principal(w, r); !ok {
		return
	}
	failures, err := h.services.Readiness.Check(r.Context())
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ready": false, "failures": failures})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ready": true})
}

func (h *Handler) capacity(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok || !requireRole(w, p, api.RoleViewer, api.RoleOperator, api.RoleAdmin) {
		return
	}
	snapshot, err := h.services.Capacity.Snapshot(r.Context(), p.TenantID, time.Now().UTC())
	respond(w, snapshot, err, http.StatusOK)
}

func (h *Handler) audit(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok || !requireRole(w, p, api.RoleOperator, api.RoleAdmin) {
		return
	}
	limit := queryLimit(r, 100)
	records, err := h.services.Audit.Query(r.Context(), platformops.AuditQuery{
		TenantID: p.TenantID, CanViewAudit: true, ResourceType: r.URL.Query().Get("resourceType"),
		ResourceID: r.URL.Query().Get("resourceId"), Limit: limit,
	})
	respond(w, records, err, http.StatusOK)
}

func (h *Handler) deadLetters(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok || !requireRole(w, p, api.RoleOperator, api.RoleAdmin) {
		return
	}
	letters, err := h.services.Delivery.ListDeadLetters(r.Context(), p.TenantID, queryLimit(r, 100))
	respond(w, letters, err, http.StatusOK)
}

func (h *Handler) requeueDeadLetter(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok || !requireRole(w, p, api.RoleOperator, api.RoleAdmin) {
		return
	}
	err := h.services.Delivery.RequeueDeadLetter(r.Context(), p.TenantID, r.PathValue("id"), time.Now().UTC())
	respond(w, map[string]bool{"requeued": err == nil}, err, http.StatusOK)
}

func (h *Handler) drainWorker(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok || !requireRole(w, p, api.RoleOperator, api.RoleAdmin) {
		return
	}
	err := h.services.Workers.BeginDrain(r.Context(), p.TenantID, r.PathValue("id"), time.Now().UTC())
	respond(w, map[string]bool{"draining": err == nil}, err, http.StatusOK)
}

func (h *Handler) configureQuota(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok || !requireRole(w, p, api.RoleAdmin) {
		return
	}
	var body struct {
		MaxActiveRuns    int `json:"maxActiveRuns"`
		MaxActiveWorkers int `json:"maxActiveWorkers"`
		MaxQueueDepth    int `json:"maxQueueDepth"`
	}
	if !decodeJSON(w, r, &body) {
		return
	}
	err := h.services.Admission.ConfigureQuota(r.Context(), platformops.Quota{
		TenantID: p.TenantID, MaxActiveRuns: body.MaxActiveRuns,
		MaxActiveWorkers: body.MaxActiveWorkers, MaxQueueDepth: body.MaxQueueDepth,
	})
	respond(w, map[string]bool{"configured": err == nil}, err, http.StatusOK)
}

func (h *Handler) createCredential(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok || !requireRole(w, p, api.RoleAdmin) {
		return
	}
	var body struct{ Name, Purpose, Secret string }
	if !decodeJSON(w, r, &body) {
		return
	}
	secret := []byte(body.Secret)
	body.Secret = ""
	credential, err := h.services.Credentials.Create(r.Context(), platformops.CreateCredentialRequest{
		TenantID: p.TenantID, ActorID: p.ActorID, Name: body.Name, Purpose: body.Purpose, Secret: secret,
	})
	respond(w, credential, err, http.StatusCreated)
}

func (h *Handler) rotateCredential(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok || !requireRole(w, p, api.RoleAdmin) {
		return
	}
	var body struct {
		Version int    `json:"version"`
		Secret  string `json:"secret"`
	}
	if !decodeJSON(w, r, &body) {
		return
	}
	secret := []byte(body.Secret)
	body.Secret = ""
	credential, err := h.services.Credentials.Rotate(r.Context(), p.TenantID, r.PathValue("id"), p.ActorID, body.Version, secret)
	respond(w, credential, err, http.StatusOK)
}

func (h *Handler) revokeCredential(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok || !requireRole(w, p, api.RoleAdmin) {
		return
	}
	credential, err := h.services.Credentials.Revoke(r.Context(), p.TenantID, r.PathValue("id"), p.ActorID, time.Now().UTC())
	respond(w, credential, err, http.StatusOK)
}

func (h *Handler) setRetention(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok || !requireRole(w, p, api.RoleAdmin) {
		return
	}
	var body struct {
		EventDays, OutputDays int
		ArchiveBeforeDelete   bool
	}
	if !decodeJSON(w, r, &body) {
		return
	}
	err := h.services.Retention.SetPolicy(r.Context(), platformops.RetentionPolicy{
		TenantID: p.TenantID, EventDays: body.EventDays, OutputDays: body.OutputDays,
		ArchiveBeforeDelete: body.ArchiveBeforeDelete,
	})
	respond(w, map[string]bool{"configured": err == nil}, err, http.StatusOK)
}

func (h *Handler) exportTenant(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok || !requireRole(w, p, api.RoleAdmin) {
		return
	}
	result, err := h.services.Retention.ExportTenant(r.Context(), p.TenantID, p.ActorID, time.Now().UTC().Add(24*time.Hour))
	respond(w, result, err, http.StatusAccepted)
}

func principal(w http.ResponseWriter, r *http.Request) (api.Principal, bool) {
	p, ok := api.PrincipalFromContext(r.Context())
	if !ok || p.TenantID == "" || p.ActorID == "" {
		writeProblem(w, http.StatusUnauthorized, "unauthorized")
		return api.Principal{}, false
	}
	return p, true
}

func requireRole(w http.ResponseWriter, principal api.Principal, roles ...api.Role) bool {
	for _, role := range roles {
		if principal.Role == role {
			return true
		}
	}
	writeProblem(w, http.StatusForbidden, "forbidden")
	return false
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	body, err := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
	if err != nil || len(body) > 1<<20 {
		writeProblem(w, http.StatusBadRequest, "invalid_request")
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(target); err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid_request")
		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeProblem(w, http.StatusBadRequest, "invalid_request")
		return false
	}
	return true
}

func respond(w http.ResponseWriter, value any, err error, success int) {
	if err == nil {
		writeJSON(w, success, value)
		return
	}
	status, code := http.StatusInternalServerError, "internal_error"
	switch {
	case errors.Is(err, platformops.ErrInvalidArgument):
		status, code = http.StatusBadRequest, "invalid_request"
	case errors.Is(err, platformops.ErrNotFound):
		status, code = http.StatusNotFound, "not_found"
	case errors.Is(err, platformops.ErrPermissionDenied):
		status, code = http.StatusForbidden, "forbidden"
	case errors.Is(err, platformops.ErrSignature):
		status, code = http.StatusUnauthorized, "invalid_signature"
	case errors.Is(err, platformops.ErrCredentialForbidden), errors.Is(err, platformops.ErrCredentialRevoked):
		status, code = http.StatusForbidden, "credential_forbidden"
	case errors.Is(err, platformops.ErrConflict), errors.Is(err, platformops.ErrReplay):
		status, code = http.StatusConflict, "conflict"
	case errors.Is(err, platformops.ErrRateLimited):
		status, code = http.StatusTooManyRequests, "rate_limited"
		var limited *platformops.RateLimitError
		if errors.As(err, &limited) {
			w.Header().Set("Retry-After", strconv.Itoa(max(1, int(limited.RetryAfter.Seconds()))))
		}
	}
	writeProblem(w, status, code)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeProblem(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, status, map[string]any{"status": status, "code": code})
}

func queryLimit(r *http.Request, fallback int) int {
	value := strings.TrimSpace(r.URL.Query().Get("limit"))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}
	return parsed
}

func mutationKey(w http.ResponseWriter, r *http.Request) (string, bool) {
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if len(key) < 8 || len(key) > 128 {
		writeProblem(w, http.StatusBadRequest, "invalid_idempotency_key")
		return "", false
	}
	return key, true
}
