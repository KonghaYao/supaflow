package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/pazhou/supaflow/internal/apimutation"
	"github.com/pazhou/supaflow/internal/platformops"
)

// AuditRecorder appends immutable mutation evidence without coupling API handlers to storage.
type AuditRecorder interface {
	Record(context.Context, platformops.AuditRecord) (platformops.AuditRecord, error)
}

type auditStatusWriter struct {
	header http.Header
	body   bytes.Buffer
	status int
}

func (w *auditStatusWriter) Header() http.Header { return w.header }

func (w *auditStatusWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

func (w *auditStatusWriter) Write(value []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.Write(value)
}

func (h *Handler) auditMutations(next http.Handler) http.Handler {
	if h.audit == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		action, resourceType, resourceID := apiMutationAuditTarget(r)
		if action == "" {
			next.ServeHTTP(w, r)
			return
		}
		var operation *apimutation.Operation
		principal, authenticated := PrincipalFromContext(r.Context())
		key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
		if authenticated && key != "" {
			body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBody+1))
			if err != nil || len(body) > maxRequestBody {
				writeProblem(w, r, http.StatusRequestEntityTooLarge, "request_too_large", "request body exceeds limit")
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			operation = apimutation.New(principal.OrganizationID, principal.ActorID, action, resourceType, resourceID,
				key, r.Method, r.URL.EscapedPath(), body, platformops.AuditIdentifier(r.Header.Get("X-Request-ID")))
			r = r.WithContext(apimutation.WithContext(r.Context(), operation))
		}
		tracked := &auditStatusWriter{header: make(http.Header)}
		next.ServeHTTP(tracked, r)
		status := tracked.status
		if status == 0 {
			status = http.StatusOK
		}
		outcome := platformops.AuditAllowed
		if status == http.StatusUnauthorized || status == http.StatusForbidden {
			outcome = platformops.AuditDenied
		} else if status >= 400 {
			outcome = platformops.AuditFailed
		}
		principal, ok := PrincipalFromContext(r.Context())
		if !ok {
			copyAuditResponse(w, tracked, status)
			return
		}
		metadata, _ := json.Marshal(map[string]any{"method": r.Method, "status": status, "requestIdHash": platformops.AuditIdentifier(r.Header.Get("X-Request-ID"))})
		if outcome == platformops.AuditAllowed && operation != nil && operation.Committed() {
			copyAuditResponse(w, tracked, status)
			return
		}
		if outcome == platformops.AuditAllowed && operation != nil {
			slog.Error("API mutation returned without durable completion", "action", action, "operation_id", operation.ID)
			writeProblem(w, r, http.StatusInternalServerError, "mutation_not_durable", "mutation completion could not be verified")
			return
		}
		if _, err := h.audit.Record(r.Context(), platformops.AuditRecord{
			OrganizationID: principal.OrganizationID, ActorID: principal.ActorID, Action: action,
			ResourceType: resourceType, ResourceID: resourceID, Outcome: outcome, Metadata: metadata,
		}); err != nil {
			slog.Error("persist API audit failed", "action", action, "error", platformops.RedactDiagnostic(err.Error()))
			writeProblem(w, r, http.StatusInternalServerError, "audit_unavailable", "mutation evidence could not be persisted")
			return
		}
		copyAuditResponse(w, tracked, status)
	})
}

func copyAuditResponse(w http.ResponseWriter, tracked *auditStatusWriter, status int) {
	for key, values := range tracked.header {
		w.Header()[key] = append([]string(nil), values...)
	}
	w.WriteHeader(status)
	_, _ = w.Write(tracked.body.Bytes())
}

func apiMutationAuditTarget(r *http.Request) (string, string, string) {
	if r.Method == http.MethodGet {
		return "", "", ""
	}
	path := strings.TrimPrefix(r.URL.Path, "/v1/")
	parts := strings.Split(path, "/")
	resourceID := "new"
	if len(parts) > 1 && parts[1] != "" {
		resourceID = parts[1]
	}
	switch {
	case path == "runs":
		return "run.start", "run", resourceID
	case path == "workflows":
		return "workflow.create", "workflow", resourceID
	case len(parts) == 2 && parts[0] == "workflows" && r.Method == http.MethodPut:
		return "workflow.update", "workflow", resourceID
	case len(parts) == 2 && parts[0] == "workflows" && r.Method == http.MethodDelete:
		return "workflow.delete", "workflow", resourceID
	case strings.HasPrefix(path, "runs/") && strings.HasSuffix(path, "/cancel"):
		return "run.cancel", "run", resourceID
	case strings.HasPrefix(path, "runs/") && strings.HasSuffix(path, "/rerun"):
		return "run.rerun", "run", resourceID
	case strings.HasPrefix(path, "interrupts/") && strings.HasSuffix(path, "/resolve"):
		return "approval.resolve", "interrupt", resourceID
	case strings.HasPrefix(path, "workflows/") && strings.Contains(path, "/publish"):
		return "workflow.publish", "workflow", resourceID
	case strings.HasPrefix(path, "workflows/") && strings.Contains(path, "/republish"):
		return "workflow.republish", "workflow", resourceID
	default:
		return "", "", ""
	}
}
