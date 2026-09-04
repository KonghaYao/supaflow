package httpapi

import (
	"bytes"
	"crypto/sha256"
	"io"
	"net/http"
	"strings"

	"github.com/pazhou/supaflow/internal/api"
	"github.com/pazhou/supaflow/internal/platformops"
)

const maximumMutationRequestBytes = int64(2 << 20)

func (h *Handler) idempotentMutations(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		operation, _, _ := mutationAuditTarget(r)
		if operation == "" {
			next.ServeHTTP(w, r)
			return
		}
		key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
		if len(key) < 8 || len(key) > 128 {
			writeProblem(w, http.StatusBadRequest, "invalid_idempotency_key")
			return
		}
		principal, ok := api.PrincipalFromContext(r.Context())
		if !ok {
			writeProblem(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		// Authorization must precede ledger lookup on every request. A completed
		// replay is still a current request and must not preserve permissions an
		// actor had before a role downgrade.
		if !mutationRoleAllowed(operation, principal.Role) {
			if record := mutationAuditRecord(r, http.StatusForbidden); record != nil && h.services.Audit != nil {
				if _, err := h.services.Audit.Record(r.Context(), *record); err != nil {
					writeProblem(w, http.StatusServiceUnavailable, "audit_unavailable")
					return
				}
			}
			writeProblem(w, http.StatusForbidden, "forbidden")
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, maximumMutationRequestBytes+1))
		if err != nil || int64(len(body)) > maximumMutationRequestBytes {
			writeProblem(w, http.StatusBadRequest, "invalid_request")
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		fingerprint := sha256.Sum256(append([]byte(r.Method+"\n"+r.URL.RequestURI()+"\n"), body...))
		record, err := h.services.Mutations.Claim(r.Context(), principal.TenantID, operation, key, fingerprint[:])
		if err != nil {
			respond(w, nil, err, http.StatusOK)
			return
		}
		if record.Replay {
			if record.ContentType != "" {
				w.Header().Set("Content-Type", record.ContentType)
			}
			w.Header().Set("Idempotency-Replayed", "true")
			w.WriteHeader(record.StatusCode)
			_, _ = w.Write(record.Body)
			return
		}
		r = r.WithContext(platformops.ContextWithMutationOperation(r.Context(), record.OperationID))
		captured := &auditResponseWriter{header: make(http.Header)}
		next.ServeHTTP(captured, r)
		status := captured.status
		if status == 0 {
			status = http.StatusOK
		}
		contentType := captured.header.Get("Content-Type")
		completion := platformops.MutationCompletion{
			Owner: record.Owner, StatusCode: status, ContentType: contentType,
			Body: captured.body.Bytes(), Audit: mutationAuditRecord(r, status),
		}
		if err = h.services.Mutations.Complete(r.Context(), principal.TenantID, operation, key, completion); err != nil {
			respond(w, nil, err, http.StatusOK)
			return
		}
		for name, values := range captured.header {
			w.Header()[name] = append([]string(nil), values...)
		}
		w.WriteHeader(status)
		_, _ = w.Write(captured.body.Bytes())
	})
}

func mutationRoleAllowed(operation string, role api.Role) bool {
	switch operation {
	case "quota.configure", "retention.configure", "retention.export",
		"credential.create", "credential.mutate",
		"webhook_subscription.create", "webhook_subscription.mutate":
		return role == api.RoleAdmin
	case "worker.drain", "delivery.requeue", "trigger.create", "trigger.mutate",
		"trigger.fire", "trigger.event.receive":
		return role == api.RoleOperator || role == api.RoleAdmin
	default:
		return false
	}
}
