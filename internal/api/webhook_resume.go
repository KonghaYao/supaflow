package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

const webhookResumeWindow = 5 * time.Minute

func (h *Handler) resumeWebhook(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	idempotencyKey, ok := requireIdempotencyKey(w, r)
	if !ok {
		return
	}
	if !validWebhookTimestamp(r.Header.Get("X-Supaflow-Timestamp"), time.Now().UTC()) {
		writeProblem(w, r, http.StatusUnauthorized, "invalid_webhook_timestamp", "webhook timestamp is outside the accepted window")
		return
	}
	const tokenPrefix = "Resume "
	authorization := r.Header.Get("Authorization")
	if !strings.HasPrefix(authorization, tokenPrefix) {
		writeProblem(w, r, http.StatusUnauthorized, "invalid_resume_token", "webhook resume token is required")
		return
	}
	data, err := readWebhookJSON(w, r)
	if err != nil {
		writeProblem(w, r, http.StatusBadRequest, "invalid_json", "webhook body must contain one JSON value no larger than 256 KiB")
		return
	}
	result, err := h.runtime.ResumeWebhook(r.Context(), workflowruntime.ResumeWebhookCommand{
		InterruptID: r.PathValue("interruptID"), ResumeToken: strings.TrimSpace(strings.TrimPrefix(authorization, tokenPrefix)),
		IdempotencyKey: idempotencyKey, Data: data,
	})
	if err != nil {
		if workflowruntime.IsCode(err, workflowruntime.CodeApprovalInvalid) || workflowruntime.IsCode(err, workflowruntime.CodeApprovalExpired) {
			writeProblem(w, r, http.StatusUnauthorized, "invalid_resume_token", "webhook resume token is invalid or expired")
			return
		}
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, runDTO(result.Run))
}

func validWebhookTimestamp(value string, now time.Time) bool {
	seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return false
	}
	delta := now.Sub(time.Unix(seconds, 0).UTC())
	return delta >= -webhookResumeWindow && delta <= webhookResumeWindow
}

func readWebhookJSON(w http.ResponseWriter, r *http.Request) (json.RawMessage, error) {
	r.Body = http.MaxBytesReader(w, r.Body, maxWebhookResumeBody)
	contents, err := io.ReadAll(r.Body)
	if err != nil || len(contents) == 0 || !json.Valid(contents) {
		return nil, errors.New("invalid webhook JSON")
	}
	return json.RawMessage(contents), nil
}

const maxWebhookResumeBody = 256 << 10
