package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/pazhou/supaflow/internal/platformops"
	"github.com/pazhou/supaflow/internal/runtime"
)

const maximumOutputDownloadBytes = int64(1 << 30)

// getRunOutput streams one opaque, organization-owned external output. Internal
// object keys and filesystem paths never cross this boundary.
func (h *Handler) getRunOutput(w http.ResponseWriter, r *http.Request) {
	principal, _ := PrincipalFromContext(r.Context())
	run, err := h.runtime.GetRun(r.Context(), runtime.OrganizationID(principal.OrganizationID), r.PathValue("runID"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	if run.Output == nil {
		writeProblem(w, r, http.StatusNotFound, "output_not_found", "run output is unavailable")
		return
	}
	if run.Output.RetentionStatus == "archived" || run.Output.RetentionStatus == "deleted" {
		writeProblem(w, r, http.StatusGone, "output_expired", "run output is no longer retained")
		return
	}
	if !run.Output.External {
		writeProblem(w, r, http.StatusConflict, "output_inline", "run output is available in the run projection")
		return
	}
	if run.Output.Sensitive && false {
		h.recordOutputAudit(w, r, principal, run.ID, platformops.AuditDenied)
		return
	}
	if h.outputs == nil || run.Output.Size < 0 || run.Output.Size > maximumOutputDownloadBytes {
		writeProblem(w, r, http.StatusServiceUnavailable, "output_unavailable", "run output cannot be streamed")
		return
	}
	reader, metadata, err := h.outputs.OpenRunOutput(r.Context(), principal.OrganizationID, run.ID)
	if errors.Is(err, platformops.ErrNotFound) {
		writeProblem(w, r, http.StatusGone, "output_expired", "run output is no longer retained")
		return
	}
	if err != nil {
		writeProblem(w, r, http.StatusServiceUnavailable, "output_unavailable", "run output cannot be streamed")
		return
	}
	defer reader.Close()
	if metadata.RunID != run.ID || metadata.Size != run.Output.Size || metadata.SHA256 != run.Output.SHA256 || metadata.ContentType != run.Output.ContentType {
		writeProblem(w, r, http.StatusConflict, "output_integrity_mismatch", "run output metadata failed verification")
		return
	}
	if !h.recordOutputAudit(w, r, principal, run.ID, platformops.AuditAllowed) {
		return
	}
	w.Header().Set("Content-Type", metadata.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(metadata.Size, 10))
	w.Header().Set("X-Content-SHA256", metadata.SHA256)
	w.Header().Set("ETag", fmt.Sprintf("\"sha256:%s\"", metadata.SHA256))
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	written, copyErr := io.Copy(w, io.LimitReader(reader, metadata.Size+1))
	if copyErr != nil || written != metadata.Size {
		slog.Error("stream run output failed", "run_id", run.ID, "written", written, "error", platformops.RedactDiagnostic(fmt.Sprint(copyErr)))
	}
}

func (h *Handler) recordOutputAudit(w http.ResponseWriter, r *http.Request, principal Principal, runID string, outcome platformops.AuditOutcome) bool {
	if h.audit == nil {
		writeProblem(w, r, http.StatusServiceUnavailable, "audit_unavailable", "output access cannot be audited")
		return false
	}
	metadata, _ := json.Marshal(map[string]string{"requestIdHash": platformops.AuditIdentifier(r.Header.Get("X-Request-ID"))})
	if _, err := h.audit.Record(r.Context(), platformops.AuditRecord{OrganizationID: principal.OrganizationID, ActorID: principal.ActorID,
		Action: "run.output.read", ResourceType: "run", ResourceID: runID, Outcome: outcome, Metadata: metadata}); err != nil {
		writeProblem(w, r, http.StatusServiceUnavailable, "audit_unavailable", "output access cannot be audited")
		return false
	}
	if outcome == platformops.AuditDenied {
		writeProblem(w, r, http.StatusForbidden, "forbidden", "permission denied")
		return false
	}
	return true
}
