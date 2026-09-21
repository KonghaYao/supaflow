package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/pazhou/supaflow/internal/platformops"
	"github.com/pazhou/supaflow/internal/workflow"
)

type problem struct {
	Type      string         `json:"type"`
	Title     string         `json:"title"`
	Status    int            `json:"status"`
	Code      string         `json:"code"`
	RequestID string         `json:"requestId"`
	Issues    []problemIssue `json:"issues,omitempty"`
}

// problemIssue is the wire form of one workflow.ValidationIssue. It is a separate DTO so
// the published contract does not change shape whenever the domain type does.
type problemIssue struct {
	Code    string `json:"code"`
	Path    string `json:"path"`
	Message string `json:"message"`
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeProblem(w http.ResponseWriter, r *http.Request, status int, code, title string) {
	writeProblemValue(w, r, problem{
		Type:   "https://supaflow.dev/problems/" + code,
		Title:  title,
		Status: status,
		Code:   code,
	})
}

// writeValidationProblem reports every independently detected definition issue. The
// validation error already carries a code, a JSON path, and a message per issue, but the
// response used to expose only the first of them formatted into the title: a definition
// with two problems had to be fixed one publish attempt at a time, and the field-level
// message never reached the caller at all. Title keeps the previous wording so existing
// clients keep working; issues is added alongside it.
func writeValidationProblem(w http.ResponseWriter, r *http.Request, status int, code, title string, issues []workflow.ValidationIssue) {
	wire := make([]problemIssue, 0, len(issues))
	for _, issue := range issues {
		wire = append(wire, problemIssue{Code: issue.Code, Path: issue.Path, Message: issue.Message})
	}
	writeProblemValue(w, r, problem{
		Type:   "https://supaflow.dev/problems/" + code,
		Title:  title,
		Status: status,
		Code:   code,
		Issues: wire,
	})
}

// writeInternalError answers an unexpected failure and records it server-side. The response
// stays deliberately opaque, but without a log line the request id it carries named nothing.
func writeInternalError(w http.ResponseWriter, r *http.Request, err error) {
	writeLoggedProblem(w, r, http.StatusInternalServerError, "internal_error", "request could not be completed", err)
}

// writeLoggedProblem answers a classified server-side failure and records it under the same
// request id the response carries, so an operator can find the request the caller reported from
// the id alone. Callers that classify their own failure keep their own code and title; the
// response never repeats the diagnostic, because the diagnostic is for the server.
func writeLoggedProblem(w http.ResponseWriter, r *http.Request, status int, code, title string, err error) {
	id := requestID(r)
	// Pin the id on the request so the response repeats the value this line records:
	// requestID mints a fresh random id whenever the caller did not supply one.
	r.Header.Set("X-Request-ID", id)
	diagnostic := "unknown"
	if err != nil {
		diagnostic = platformops.RedactDiagnostic(err.Error())
	}
	slog.Error("request failed with an unexpected error",
		"requestId", id, "method", r.Method, "path", r.URL.Path, "code", code, "error", diagnostic)
	writeProblem(w, r, status, code, title)
}

func writeProblemValue(w http.ResponseWriter, r *http.Request, value problem) {
	w.Header().Set("Content-Type", "application/problem+json")
	if w.Header().Get("Cache-Control") == "" {
		w.Header().Set("Cache-Control", "no-store")
	}
	value.RequestID = requestID(r)
	writeJSON(w, value.Status, value)
}

func requestID(r *http.Request) string {
	if value := r.Header.Get("X-Request-ID"); value != "" && len(value) <= 128 {
		return value
	}
	var value [12]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "unavailable"
	}
	return hex.EncodeToString(value[:])
}
