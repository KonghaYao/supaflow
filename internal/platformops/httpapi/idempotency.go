package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"net/http"
	"strings"
	"time"

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
		requestContext := r.Context()
		handlerContext, cancelHandler := context.WithCancel(requestContext)
		defer cancelHandler()
		_, resourceType, resourceID := mutationAuditTarget(r)
		r = r.WithContext(platformops.ContextWithMutation(handlerContext, platformops.MutationOperation{
			ID: record.OperationID, TenantID: principal.TenantID, ActorID: principal.ActorID,
			Action: operation, ResourceType: resourceType, ResourceID: resourceID,
		}))
		var renewalDone chan error
		if record.RenewAfter > 0 {
			renewalDone = make(chan error, 1)
			go h.renewMutationLease(r.Context(), cancelHandler, principal.TenantID, operation, key, record, renewalDone)
		}
		captured := &auditResponseWriter{header: make(http.Header)}
		next.ServeHTTP(captured, r)
		status := captured.status
		if status == 0 {
			status = http.StatusOK
		}
		if captured.tooLarge {
			status = http.StatusInsufficientStorage
			captured.header = make(http.Header)
			captured.header.Set("Content-Type", "application/problem+json")
			captured.body.Reset()
			_, _ = captured.body.WriteString(`{"error":"mutation_response_too_large"}`)
		}
		contentType := captured.header.Get("Content-Type")
		completion := platformops.MutationCompletion{
			Owner: record.Owner, StatusCode: status, ContentType: contentType,
			Body: captured.body.Bytes(), Audit: mutationAuditRecord(r, status),
		}
		cancelHandler()
		if renewalDone != nil {
			if renewalErr := <-renewalDone; renewalErr != nil {
				respond(w, nil, renewalErr, http.StatusOK)
				return
			}
			if err = h.services.Mutations.Renew(requestContext, principal.TenantID, operation, key, record.Owner); err != nil {
				respond(w, nil, err, http.StatusOK)
				return
			}
		}
		if operation == "trigger.event.receive" && status == http.StatusTooManyRequests {
			if err = h.services.Mutations.Retry(requestContext, principal.TenantID, operation, key, record.Owner); err != nil {
				respond(w, nil, err, http.StatusOK)
				return
			}
			for name, values := range captured.header {
				w.Header()[name] = append([]string(nil), values...)
			}
			w.WriteHeader(status)
			_, _ = w.Write(captured.body.Bytes())
			return
		}
		if err = h.services.Mutations.Complete(requestContext, principal.TenantID, operation, key, completion); err != nil {
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

func (h *Handler) renewMutationLease(ctx context.Context, cancel context.CancelFunc, tenantID, operation, key string, record platformops.MutationRecord, done chan<- error) {
	interval := record.RenewAfter
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			done <- nil
			return
		case <-ticker.C:
			if err := h.services.Mutations.Renew(ctx, tenantID, operation, key, record.Owner); err != nil {
				if ctx.Err() != nil {
					done <- nil
					return
				}
				cancel()
				done <- err
				return
			}
		}
	}
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
