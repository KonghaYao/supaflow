package definition

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/pazhou/supaflow/internal/platform"
	"github.com/pazhou/supaflow/internal/workflow"
)

// Publisher is the immutable publication boundary used by Definition.
type Publisher interface {
	Publish(context.Context, string, string, workflow.WorkflowDefinition, string) (platform.PublishedPlan, bool, error)
}

type publicationReplayer interface {
	ReplayPublication(context.Context, string, string, string) (platform.PublishedPlan, bool, error)
}

// Service coordinates draft revisions, compilation, and publication history.
type Service struct {
	store     Store
	adapter   DraftAdapter
	publisher Publisher
	registry  *workflow.Registry
	policy    workflow.CompilePolicy
	previewMu sync.RWMutex
	preview   PreviewAdapter
}

// NewService validates and constructs the definition application service.
func NewService(store Store, adapter DraftAdapter, publisher Publisher, registry *workflow.Registry, policy workflow.CompilePolicy) (*Service, error) {
	if store == nil || adapter == nil || publisher == nil || registry == nil {
		return nil, errors.New("definition store, adapter, publisher, and registry are required")
	}
	return &Service{store: store, adapter: adapter, publisher: publisher, registry: registry, policy: policy}, nil
}

// CanonicalAdapter accepts a canonical WorkflowDefinition as a draft document.
type CanonicalAdapter struct{}

// ToDefinition strictly decodes a canonical draft.
func (CanonicalAdapter) ToDefinition(document json.RawMessage) (workflow.WorkflowDefinition, error) {
	var definition workflow.WorkflowDefinition
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&definition); err != nil {
		return workflow.WorkflowDefinition{}, fmt.Errorf("decode canonical draft: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return workflow.WorkflowDefinition{}, errors.New("canonical draft must contain one JSON value")
	}
	return definition, nil
}

// toDefinition converts a draft document and classifies every failure as a caller-correctable
// request error. The document id must name the resource that stores it: a mismatch is stored
// happily and then breaks compilation, dry-run, and publication of that draft, so it is
// rejected at the write boundary instead of being discovered later.
func (s *Service) toDefinition(document json.RawMessage, workflowID string) (workflow.WorkflowDefinition, error) {
	definition, err := s.adapter.ToDefinition(document)
	if err != nil {
		return workflow.WorkflowDefinition{}, invalidDocument(err.Error(), err)
	}
	if definition.ID != workflowID {
		return workflow.WorkflowDefinition{}, invalidDocument(
			fmt.Sprintf("document workflowId %q does not match %q", definition.ID, workflowID), nil)
	}
	return definition, nil
}

// CreateDraft creates or idempotently replays a draft.
func (s *Service) CreateDraft(ctx context.Context, organizationID string, command CreateDraftCommand) (Draft, error) {
	result, err := s.CreateDraftWithResult(ctx, organizationID, command)
	return result.Draft, err
}

// CreateDraftWithResult creates or replays a draft and preserves the atomic
// persistence outcome for administrative import and observability callers.
func (s *Service) CreateDraftWithResult(ctx context.Context, organizationID string, command CreateDraftCommand) (CreateDraftResult, error) {
	if err := validateMutation(organizationID, command.WorkflowID, command.ActorID, command.IdempotencyKey, command.Document); err != nil {
		return CreateDraftResult{}, err
	}
	if _, err := s.toDefinition(command.Document, command.WorkflowID); err != nil {
		return CreateDraftResult{}, err
	}
	id, err := randomID("draft")
	if err != nil {
		return CreateDraftResult{}, err
	}
	now := time.Now().UTC()
	request := MutationRequest{Draft: Draft{ID: id, OrganizationID: organizationID, WorkflowID: command.WorkflowID, Document: cloneJSON(command.Document), Revision: 1, UpdatedBy: command.ActorID, CreatedAt: now, UpdatedAt: now}, IdempotencyKey: command.IdempotencyKey}
	request.RequestHash = mutationHash("create", command.WorkflowID, command.ActorID, 0, command.Document)
	result, err := s.store.CreateDraft(ctx, request)
	if err == nil && len(result.Draft.Document) == 0 {
		result.Draft.Document = cloneJSON(command.Document)
	}
	return result, err
}

// GetDraft returns a organization-scoped draft.
func (s *Service) GetDraft(ctx context.Context, organizationID, workflowID string) (Draft, error) {
	if organizationID == "" || workflowID == "" {
		return Draft{}, errors.New("organization and workflow are required")
	}
	return s.store.GetDraft(ctx, organizationID, workflowID)
}

// ListDrafts returns one bounded page in stable workflow order.
func (s *Service) ListDrafts(ctx context.Context, organizationID, cursor string, limit int) (DraftPage, error) {
	if organizationID == "" || limit < 1 || limit > MaxPageLimit || len(cursor) > 128 {
		return DraftPage{}, errors.New("organization, valid cursor, and page limit are required")
	}
	return s.store.ListDrafts(ctx, organizationID, cursor, limit)
}

// UpdateDraft performs an optimistic autosave mutation.
func (s *Service) UpdateDraft(ctx context.Context, organizationID string, command UpdateDraftCommand) (Draft, error) {
	if err := validateMutation(organizationID, command.WorkflowID, command.ActorID, command.IdempotencyKey, command.Document); err != nil {
		return Draft{}, err
	}
	if command.ExpectedRevision == 0 {
		return Draft{}, invalidRequest("expected revision is required")
	}
	if _, err := s.toDefinition(command.Document, command.WorkflowID); err != nil {
		return Draft{}, err
	}
	request := MutationRequest{Draft: Draft{OrganizationID: organizationID, WorkflowID: command.WorkflowID, Document: cloneJSON(command.Document), UpdatedBy: command.ActorID, UpdatedAt: time.Now().UTC()}, ExpectedRevision: command.ExpectedRevision, IdempotencyKey: command.IdempotencyKey}
	request.RequestHash = mutationHash("update", command.WorkflowID, command.ActorID, command.ExpectedRevision, command.Document)
	result, err := s.store.UpdateDraft(ctx, request)
	if err == nil && len(result.Document) == 0 {
		result.Document = cloneJSON(command.Document)
	}
	return result, err
}

// DeleteDraft performs an optimistic idempotent deletion.
func (s *Service) DeleteDraft(ctx context.Context, organizationID string, command DeleteDraftCommand) error {
	if organizationID == "" || command.WorkflowID == "" || command.ActorID == "" || len(command.IdempotencyKey) < 8 || command.ExpectedRevision == 0 {
		return invalidRequest("valid delete command is required")
	}
	request := MutationRequest{Draft: Draft{OrganizationID: organizationID, WorkflowID: command.WorkflowID, UpdatedBy: command.ActorID, UpdatedAt: time.Now().UTC()}, ExpectedRevision: command.ExpectedRevision, IdempotencyKey: command.IdempotencyKey}
	request.RequestHash = mutationHash("delete", command.WorkflowID, command.ActorID, command.ExpectedRevision, nil)
	return s.store.DeleteDraft(ctx, request)
}

// CompileDraft converts the editor document and validates the canonical plan.
func (s *Service) CompileDraft(ctx context.Context, organizationID, workflowID string) (*workflow.ExecutionPlan, error) {
	draft, err := s.GetDraft(ctx, organizationID, workflowID)
	if err != nil {
		return nil, err
	}
	definition, err := s.toDefinition(draft.Document, workflowID)
	if err != nil {
		return nil, err
	}
	return workflow.Compile(definition, s.registry, s.policy)
}

// Publish stores an immutable version after publication succeeds.
func (s *Service) Publish(ctx context.Context, organizationID, workflowID string, definition workflow.WorkflowDefinition, key string) (platform.PublishedPlan, bool, error) {
	published, created, err := s.publisher.Publish(ctx, organizationID, workflowID, definition, key)
	if err != nil {
		return platform.PublishedPlan{}, false, err
	}
	if err := s.store.RecordVersion(ctx, Version{OrganizationID: organizationID, WorkflowID: workflowID, PlanID: published.ID, Version: published.Version, PlanHash: published.Hash, Definition: definition, PublishedAt: published.PublishedAt}); err != nil {
		return platform.PublishedPlan{}, false, err
	}
	return published, created, nil
}

// PublishDraft publishes the current editor document.
func (s *Service) PublishDraft(ctx context.Context, organizationID, workflowID, key string) (platform.PublishedPlan, bool, error) {
	if organizationID == "" || workflowID == "" || len(key) < 8 || len(key) > 128 {
		return platform.PublishedPlan{}, false, invalidRequest("organization, workflow, and valid idempotency key are required")
	}
	if replayer, ok := s.publisher.(publicationReplayer); ok {
		published, found, err := replayer.ReplayPublication(ctx, organizationID, workflowID, key)
		if err != nil || found {
			return published, false, err
		}
	}
	draft, err := s.GetDraft(ctx, organizationID, workflowID)
	if err != nil {
		return platform.PublishedPlan{}, false, err
	}
	definition, err := s.toDefinition(draft.Document, workflowID)
	if err != nil {
		return platform.PublishedPlan{}, false, err
	}
	return s.Publish(ctx, organizationID, workflowID, definition, key)
}

// ListVersions returns one bounded page of newest immutable summaries first.
func (s *Service) ListVersions(ctx context.Context, organizationID, workflowID string, cursor uint64, limit int) (VersionPage, error) {
	if organizationID == "" || workflowID == "" || limit < 1 || limit > MaxPageLimit {
		return VersionPage{}, errors.New("organization, workflow, and page limit are required")
	}
	return s.store.ListVersions(ctx, organizationID, workflowID, cursor, limit)
}

// GetVersion returns one immutable published definition.
func (s *Service) GetVersion(ctx context.Context, organizationID, workflowID string, version uint64) (Version, error) {
	if organizationID == "" || workflowID == "" || version == 0 {
		return Version{}, errors.New("organization, workflow, and version are required")
	}
	return s.store.GetVersion(ctx, organizationID, workflowID, version)
}

// LatestVersion resolves the immutable plan identity consumed by run clients.
func (s *Service) LatestVersion(ctx context.Context, organizationID, workflowID string) (Version, error) {
	if organizationID == "" || workflowID == "" {
		return Version{}, errors.New("organization and workflow are required")
	}
	return s.store.GetLatestVersion(ctx, organizationID, workflowID)
}

// Republish creates a new immutable version from an older definition.
func (s *Service) Republish(ctx context.Context, organizationID, workflowID string, version uint64, key string) (platform.PublishedPlan, bool, error) {
	stored, err := s.store.GetVersion(ctx, organizationID, workflowID, version)
	if err != nil {
		return platform.PublishedPlan{}, false, err
	}
	return s.Publish(ctx, organizationID, workflowID, stored.Definition, key)
}

// ImportVersion publishes one migration record only when it is the next source
// version. The application entry point must restrict this seam to administrators.
func (s *Service) ImportVersion(ctx context.Context, organizationID, workflowID string, command ImportVersionCommand) (platform.PublishedPlan, bool, error) {
	if organizationID == "" || workflowID == "" || command.SourceVersion == 0 || len(command.IdempotencyKey) < 8 || command.Definition.ID != workflowID {
		return platform.PublishedPlan{}, false, errors.New("valid version import command is required")
	}
	latest, err := s.store.GetLatestVersion(ctx, organizationID, workflowID)
	expected := uint64(1)
	if err == nil {
		expected = latest.Version + 1
	} else if !errors.Is(err, ErrNotFound) {
		return platform.PublishedPlan{}, false, err
	}
	if command.SourceVersion > expected {
		return platform.PublishedPlan{}, false, ErrVersionSequenceConflict
	}
	if command.SourceVersion < expected {
		stored, readErr := s.store.GetVersion(ctx, organizationID, workflowID, command.SourceVersion)
		if readErr != nil || definitionDigest(stored.Definition) != definitionDigest(command.Definition) {
			return platform.PublishedPlan{}, false, ErrVersionSequenceConflict
		}
		plan, compileErr := workflow.Compile(stored.Definition, s.registry, s.policy)
		if compileErr != nil || plan.Hash() != stored.PlanHash {
			return platform.PublishedPlan{}, false, ErrVersionSequenceConflict
		}
		return platform.PublishedPlan{ID: stored.PlanID, WorkflowID: stored.WorkflowID, Version: stored.Version,
			Hash: stored.PlanHash, Plan: plan, PublishedAt: stored.PublishedAt}, false, nil
	}
	published, created, err := s.Publish(ctx, organizationID, workflowID, command.Definition, command.IdempotencyKey)
	if err != nil {
		return platform.PublishedPlan{}, false, err
	}
	if published.Version != command.SourceVersion {
		return platform.PublishedPlan{}, false, ErrVersionSequenceConflict
	}
	return published, created, nil
}

func definitionDigest(value workflow.WorkflowDefinition) [32]byte {
	encoded, _ := json.Marshal(value)
	return sha256.Sum256(encoded)
}

// DiffVersions returns stable top-level canonical JSON changes.
func (s *Service) DiffVersions(ctx context.Context, organizationID, workflowID string, from, to uint64) (VersionDiff, error) {
	left, err := s.store.GetVersion(ctx, organizationID, workflowID, from)
	if err != nil {
		return VersionDiff{}, err
	}
	right, err := s.store.GetVersion(ctx, organizationID, workflowID, to)
	if err != nil {
		return VersionDiff{}, err
	}
	changes, err := diffDefinitions(left.Definition, right.Definition)
	if err != nil {
		return VersionDiff{}, err
	}
	return VersionDiff{WorkflowID: workflowID, From: from, To: to, Changes: changes}, nil
}

func diffDefinitions(left, right workflow.WorkflowDefinition) ([]VersionChange, error) {
	var before, after map[string]json.RawMessage
	l, _ := json.Marshal(left)
	r, _ := json.Marshal(right)
	if err := json.Unmarshal(l, &before); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(r, &after); err != nil {
		return nil, err
	}
	keys := make(map[string]bool)
	for key := range before {
		keys[key] = true
	}
	for key := range after {
		keys[key] = true
	}
	names := make([]string, 0, len(keys))
	for key := range keys {
		names = append(names, key)
	}
	sort.Strings(names)
	changes := make([]VersionChange, 0)
	for _, key := range names {
		if string(before[key]) != string(after[key]) {
			changes = append(changes, VersionChange{Path: "/" + key, Before: before[key], After: after[key]})
		}
	}
	return changes, nil
}

func validateMutation(organization, workflowID, actor, key string, document json.RawMessage) error {
	if organization == "" || workflowID == "" || actor == "" || len(key) < 8 || len(key) > 128 || !json.Valid(document) {
		return invalidRequest("valid organization, workflow, actor, idempotency key, and JSON document are required")
	}
	return nil
}
func mutationHash(action, workflowID, actor string, revision uint64, document json.RawMessage) [32]byte {
	payload, _ := json.Marshal([]any{action, workflowID, actor, revision, document})
	return sha256.Sum256(payload)
}
func randomID(prefix string) (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return prefix + "_" + hex.EncodeToString(value[:]), nil
}
func cloneJSON(value json.RawMessage) json.RawMessage { return append(json.RawMessage(nil), value...) }
