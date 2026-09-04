// Package definition owns editable workflow drafts and immutable publication history.
package definition

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/pazhou/supaflow/internal/workflow"
)

var (
	// ErrNotFound is returned when a tenant cannot see the requested definition.
	ErrNotFound = errors.New("definition not found")
	// ErrRevisionConflict protects a newer draft from stale autosave writes.
	ErrRevisionConflict = errors.New("draft revision conflict")
	// ErrIdempotencyConflict rejects reuse of a mutation key for another request.
	ErrIdempotencyConflict = errors.New("definition idempotency conflict")
	// ErrVersionSequenceConflict rejects migration that would renumber source history.
	ErrVersionSequenceConflict = errors.New("workflow version sequence conflict")
)

// Draft is the tenant-scoped editable document and optimistic revision.
type Draft struct {
	ID         string
	TenantID   string
	WorkflowID string
	Document   json.RawMessage
	Revision   uint64
	UpdatedBy  string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// Version is one immutable published workflow definition.
type Version struct {
	TenantID    string
	WorkflowID  string
	VersionID   string
	PlanID      string
	Version     uint64
	PlanHash    string
	Definition  workflow.WorkflowDefinition
	PublishedAt time.Time
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

// CreateDraftCommand creates a draft through a tenant-scoped idempotent mutation.
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
