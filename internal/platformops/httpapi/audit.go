package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/pazhou/supaflow/internal/api"
	"github.com/pazhou/supaflow/internal/platformops"
)

type auditResponseWriter struct {
	header   http.Header
	body     bytes.Buffer
	status   int
	tooLarge bool
}

const maximumMutationResponseBytes = 1 << 20

func (w *auditResponseWriter) Header() http.Header { return w.header }
func (w *auditResponseWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}
func (w *auditResponseWriter) Write(value []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	remaining := maximumMutationResponseBytes - w.body.Len()
	if remaining > 0 {
		chunk := value
		if len(chunk) > remaining {
			chunk = chunk[:remaining]
		}
		_, _ = w.body.Write(chunk)
	}
	if len(value) > remaining {
		w.tooLarge = true
	}
	// The downstream handler must not retry or allocate another copy. The
	// middleware replaces this response with a bounded durable failure.
	return len(value), nil
}

func mutationAuditRecord(r *http.Request, status int) *platformops.AuditRecord {
	action, resourceType, resourceID := mutationAuditTarget(r)
	principal, ok := api.PrincipalFromContext(r.Context())
	if action == "" || !ok {
		return nil
	}
	outcome := platformops.AuditAllowed
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		outcome = platformops.AuditDenied
	} else if status >= 400 {
		outcome = platformops.AuditFailed
	}
	if outcome == platformops.AuditAllowed && (platformops.MutationCommitted(r.Context()) || atomicallyAudited(action)) {
		return nil
	}
	metadata, _ := json.Marshal(map[string]any{
		"method": r.Method, "status": status,
		"requestIdHash": platformops.AuditIdentifier(r.Header.Get("X-Request-ID")),
	})
	return &platformops.AuditRecord{
		OrganizationID: principal.OrganizationID, ActorID: principal.ActorID, Action: action,
		ResourceType: resourceType, ResourceID: resourceID, Outcome: outcome, Metadata: metadata,
	}
}

func (h *Handler) auditMutation(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		action, _, _ := mutationAuditTarget(r)
		if action == "" {
			next.ServeHTTP(w, r)
			return
		}
		captured := &auditResponseWriter{header: make(http.Header)}
		next.ServeHTTP(captured, r)
		status := captured.status
		if status == 0 {
			status = http.StatusOK
		}
		if record := mutationAuditRecord(r, status); record != nil && h.services.Audit != nil {
			if _, err := h.services.Audit.Record(r.Context(), *record); err != nil {
				writeProblem(w, http.StatusServiceUnavailable, "audit_unavailable")
				return
			}
		}
		for key, values := range captured.header {
			w.Header()[key] = append([]string(nil), values...)
		}
		w.WriteHeader(status)
		_, _ = w.Write(captured.body.Bytes())
	})
}

func atomicallyAudited(action string) bool {
	return strings.HasPrefix(action, "credential.") || strings.HasPrefix(action, "webhook_subscription.")
}

func mutationAuditTarget(r *http.Request) (string, string, string) {
	if r.Method == http.MethodGet {
		return "", "", ""
	}
	path := strings.TrimPrefix(r.URL.Path, "/ops/")
	segments := strings.Split(path, "/")
	resourceID := "organization"
	if len(segments) > 1 && segments[1] != "" {
		resourceID = segments[1]
	}
	switch {
	case path == "quotas":
		return "quota.configure", "organization_quota", resourceID
	case path == "retention":
		return "retention.configure", "retention_policy", resourceID
	case path == "exports":
		return "retention.export", "organization", resourceID
	case strings.HasPrefix(path, "workers/"):
		return "worker.drain", "worker", resourceID
	case strings.HasPrefix(path, "dlq/"):
		return "delivery.requeue", "dead_letter", resourceID
	case strings.HasPrefix(path, "triggers/") && strings.HasSuffix(path, "/fire"):
		return "trigger.fire", "trigger", resourceID
	case path == "triggers":
		return "trigger.create", "trigger", resourceID
	case strings.HasPrefix(path, "triggers/"):
		return "trigger.mutate", "trigger", resourceID
	case strings.HasPrefix(path, "events/"):
		return "trigger.event.receive", "event", resourceID
	case strings.HasPrefix(path, "credentials/"):
		return "credential.mutate", "credential", resourceID
	case path == "credentials":
		return "credential.create", "credential", resourceID
	case strings.HasPrefix(path, "webhook-subscriptions/"):
		return "webhook_subscription.mutate", "webhook_subscription", resourceID
	case path == "webhook-subscriptions":
		return "webhook_subscription.create", "webhook_subscription", resourceID
	default:
		return "", "", ""
	}
}
