// Package definition owns editable workflow drafts and immutable publication history.
package definition

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/pazhou/supaflow/internal/workflow"
)

var (
	// ErrNotFound is returned when a organization cannot see the requested definition.
	ErrNotFound = errors.New("definition not found")
	// ErrRevisionConflict protects a newer draft from stale autosave writes.
	ErrRevisionConflict = errors.New("draft revision conflict")
	// ErrIdempotencyConflict rejects reuse of a mutation key for another request.
	ErrIdempotencyConflict = errors.New("definition idempotency conflict")
	// ErrVersionSequenceConflict rejects migration that would renumber source history.
	ErrVersionSequenceConflict = errors.New("workflow version sequence conflict")
	// ErrOrganizationDraftLimit rejects creation after a organization reaches its durable draft quota.
	ErrOrganizationDraftLimit = errors.New("organization workflow draft limit exceeded")
)

// RequestError is a caller-correctable rejection: the request is wrong, not the server. Code
// selects the wire problem code, Reason is expressed in terms of the public document schema
// and identifiers so it is safe to return to the caller, and Cause keeps the internal detail
// for the server log. The definition service used to return bare errors for these, which
// landed in the API's generic branch and answered 500: a client mistake that a retry cannot
// fix looked like an outage, and the caller had no field to correct.
type RequestError struct {
	Code   string
	Reason string
	Cause  error
}

func (e *RequestError) Error() string { return e.Reason }

// Unwrap exposes the underlying failure to errors.Is and errors.As.
func (e *RequestError) Unwrap() error { return e.Cause }

// invalidRequest reports a request whose shape is wrong: missing revision, no identifier.
func invalidRequest(reason string) error {
	return &RequestError{Code: "invalid_argument", Reason: reason}
}

// invalidDocument reports a document that cannot be read as a workflow definition.
func invalidDocument(reason string, cause error) error {
	return &RequestError{Code: "invalid_workflow", Reason: reason, Cause: cause}
}

const (
	// DefaultPageLimit bounds Studio list responses when callers omit a limit.
	DefaultPageLimit = 50
	// MaxPageLimit is the hard upper bound for one Studio list response.
	MaxPageLimit = 100
	// MaxDraftsPerOrganization bounds durable editable definitions owned by one organization.
	MaxDraftsPerOrganization = 1000
)

// Draft is the organization-scoped editable document and optimistic revision.
type Draft struct {
	ID             string
	OrganizationID string
	WorkflowID     string
	Document       json.RawMessage
	Revision       uint64
	UpdatedBy      string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// CreateDraftResult reports whether an idempotent create inserted a new draft
// or atomically replayed the response recorded by the original mutation.
type CreateDraftResult struct {
	Draft   Draft
	Created bool
}

// DraftSummary is the bounded list projection; it deliberately excludes Document.
type DraftSummary struct {
	ID             string
	OrganizationID string
	WorkflowID     string
	Revision       uint64
	UpdatedBy      string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// DraftPage is one cursor-bounded page of draft summaries.
type DraftPage struct {
	Items      []DraftSummary
	NextCursor string
}

// Version is one immutable published workflow definition.
type Version struct {
	OrganizationID string
	WorkflowID     string
	VersionID      string
	PlanID         string
	Version        uint64
	PlanHash       string
	Definition     workflow.WorkflowDefinition
	PublishedAt    time.Time
}

// VersionSummary is the immutable list projection; it excludes Definition.
type VersionSummary struct {
	OrganizationID string
	WorkflowID     string
	VersionID      string
	PlanID         string
	Version        uint64
	PlanHash       string
	PublishedAt    time.Time
}

// VersionPage is one cursor-bounded page of immutable version summaries.
type VersionPage struct {
	Items      []VersionSummary
	NextCursor uint64
}

// VersionChange is one deterministic JSON-path change between definitions.
type VersionChange struct {
	Path   string          `json:"path"`
	Before json.RawMessage `json:"before,omitempty"`
	After  json.RawMessage `json:"after,omitempty"`
}

// VersionDiff compares two immutable versions.
type VersionDiff struct {
	WorkflowID string          `json:"workflowId"`
	From       uint64          `json:"from"`
	To         uint64          `json:"to"`
	Changes    []VersionChange `json:"changes"`
}

// CreateDraftCommand creates a draft through a organization-scoped idempotent mutation.
type CreateDraftCommand struct {
	WorkflowID     string
	Document       json.RawMessage
	ActorID        string
	IdempotencyKey string
}

// UpdateDraftCommand replaces a draft only at ExpectedRevision.
type UpdateDraftCommand struct {
	WorkflowID       string
	Document         json.RawMessage
	ExpectedRevision uint64
	ActorID          string
	IdempotencyKey   string
}

// DeleteDraftCommand deletes a draft only at ExpectedRevision.
type DeleteDraftCommand struct {
	WorkflowID       string
	ExpectedRevision uint64
	ActorID          string
	IdempotencyKey   string
}

// ImportVersionCommand preserves a source version while replaying history in order.
// Its caller must enforce administrator authorization before entering the domain.
type ImportVersionCommand struct {
	Definition     workflow.WorkflowDefinition
	SourceVersion  uint64
	IdempotencyKey string
}

// DraftAdapter converts editor-owned JSON into the canonical domain definition.
type DraftAdapter interface {
	ToDefinition(json.RawMessage) (workflow.WorkflowDefinition, error)
}
