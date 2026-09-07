package definition

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/pazhou/supaflow/internal/workflow"
)

type mutationRecord struct {
	hash    [32]byte
	draft   *Draft
	deleted bool
}

// MemoryStore is the concurrent executable specification of definition.Store.
type MemoryStore struct {
	mu          sync.Mutex
	drafts      map[string]Draft
	idempotency map[string]mutationRecord
	versions    map[string][]Version
}

// NewMemoryStore constructs an empty definition store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{drafts: map[string]Draft{}, idempotency: map[string]mutationRecord{}, versions: map[string][]Version{}}
}

func scope(organization, value string) string { return organization + "\x00" + value }

// CreateDraft creates or replays one organization mutation.
func (m *MemoryStore) CreateDraft(_ context.Context, req MutationRequest) (CreateDraftResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if found, replay, err := m.replay(req.Draft.OrganizationID, req.IdempotencyKey, req.RequestHash); found || err != nil {
		if replay == nil {
			return CreateDraftResult{}, err
		}
		return CreateDraftResult{Draft: cloneDraft(*replay)}, err
	}
	key := scope(req.Draft.OrganizationID, req.Draft.WorkflowID)
	if _, exists := m.drafts[key]; exists {
		return CreateDraftResult{}, ErrRevisionConflict
	}
	count := 0
	for _, item := range m.drafts {
		if item.OrganizationID == req.Draft.OrganizationID {
			count++
		}
	}
	if count >= MaxDraftsPerOrganization {
		return CreateDraftResult{}, ErrOrganizationDraftLimit
	}
	draft := cloneDraft(req.Draft)
	m.drafts[key] = draft
	m.record(req, &draft, false)
	return CreateDraftResult{Draft: cloneDraft(draft), Created: true}, nil
}

// GetDraft reads one organization workflow draft.
func (m *MemoryStore) GetDraft(_ context.Context, organization, workflowID string) (Draft, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	draft, ok := m.drafts[scope(organization, workflowID)]
	if !ok {
		return Draft{}, ErrNotFound
	}
	return cloneDraft(draft), nil
}

// ListDrafts lists one bounded page of organization-owned summaries.
func (m *MemoryStore) ListDrafts(_ context.Context, organization, cursor string, limit int) (DraftPage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := []DraftSummary{}
	for _, item := range m.drafts {
		if item.OrganizationID == organization && item.WorkflowID > cursor {
			result = append(result, summarizeDraft(item))
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].WorkflowID < result[j].WorkflowID })
	page := DraftPage{Items: result}
	if len(page.Items) > limit {
		page.NextCursor = page.Items[limit-1].WorkflowID
		page.Items = page.Items[:limit]
	}
	return page, nil
}

// UpdateDraft performs one optimistic mutation.
func (m *MemoryStore) UpdateDraft(_ context.Context, req MutationRequest) (Draft, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if found, replay, err := m.replay(req.Draft.OrganizationID, req.IdempotencyKey, req.RequestHash); found || err != nil {
		if replay == nil {
			return Draft{}, err
		}
		return cloneDraft(*replay), err
	}
	key := scope(req.Draft.OrganizationID, req.Draft.WorkflowID)
	draft, ok := m.drafts[key]
	if !ok {
		return Draft{}, ErrNotFound
	}
	if draft.Revision != req.ExpectedRevision {
		return Draft{}, ErrRevisionConflict
	}
	draft.Document, draft.UpdatedBy, draft.UpdatedAt, draft.Revision = cloneJSON(req.Draft.Document), req.Draft.UpdatedBy, req.Draft.UpdatedAt, draft.Revision+1
	m.drafts[key] = draft
	m.record(req, &draft, false)
	return cloneDraft(draft), nil
}

// DeleteDraft deletes one optimistic mutation.
func (m *MemoryStore) DeleteDraft(_ context.Context, req MutationRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if found, _, err := m.replay(req.Draft.OrganizationID, req.IdempotencyKey, req.RequestHash); found || err != nil {
		return err
	}
	key := scope(req.Draft.OrganizationID, req.Draft.WorkflowID)
	draft, ok := m.drafts[key]
	if !ok {
		return ErrNotFound
	}
	if draft.Revision != req.ExpectedRevision {
		return ErrRevisionConflict
	}
	delete(m.drafts, key)
	m.record(req, nil, true)
	return nil
}

// RecordVersion records an immutable publication projection.
func (m *MemoryStore) RecordVersion(_ context.Context, version Version) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := scope(version.OrganizationID, version.WorkflowID)
	for _, current := range m.versions[key] {
		if current.Version == version.Version {
			return nil
		}
	}
	if version.VersionID == "" {
		version.VersionID = fmt.Sprintf("version_%d", version.Version)
	}
	version.Definition = cloneDefinition(version.Definition)
	m.versions[key] = append(m.versions[key], version)
	return nil
}

// ListVersions returns one bounded page of newest version summaries first.
func (m *MemoryStore) ListVersions(_ context.Context, organization, workflowID string, cursor uint64, limit int) (VersionPage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	values := m.versions[scope(organization, workflowID)]
	result := make([]VersionSummary, 0, min(limit+1, len(values)))
	for index := len(values) - 1; index >= 0; index-- {
		item := values[index]
		if cursor != 0 && item.Version >= cursor {
			continue
		}
		result = append(result, summarizeVersion(item))
		if len(result) == limit+1 {
			break
		}
	}
	page := VersionPage{Items: result}
	if len(page.Items) > limit {
		page.NextCursor = page.Items[limit-1].Version
		page.Items = page.Items[:limit]
	}
	return page, nil
}

// GetVersion returns one immutable definition version.
func (m *MemoryStore) GetVersion(_ context.Context, organization, workflowID string, version uint64) (Version, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, item := range m.versions[scope(organization, workflowID)] {
		if item.Version == version {
			return cloneVersion(item), nil
		}
	}
	return Version{}, ErrNotFound
}

// GetLatestVersion returns the newest published projection for one organization workflow.
func (m *MemoryStore) GetLatestVersion(_ context.Context, organization, workflowID string) (Version, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	values := m.versions[scope(organization, workflowID)]
	if len(values) == 0 {
		return Version{}, ErrNotFound
	}
	return cloneVersion(values[len(values)-1]), nil
}

func (m *MemoryStore) replay(organization, key string, hash [32]byte) (bool, *Draft, error) {
	if item, ok := m.idempotency[scope(organization, key)]; ok {
		if subtle.ConstantTimeCompare(item.hash[:], hash[:]) != 1 {
			return true, nil, ErrIdempotencyConflict
		}
		if item.draft == nil {
			return true, nil, nil
		}
		draft := cloneDraft(*item.draft)
		return true, &draft, nil
	}
	return false, nil, nil
}
func (m *MemoryStore) record(req MutationRequest, draft *Draft, deleted bool) {
	var cloned *Draft
	if draft != nil {
		value := cloneDraft(*draft)
		cloned = &value
	}
	m.idempotency[scope(req.Draft.OrganizationID, req.IdempotencyKey)] = mutationRecord{hash: req.RequestHash, draft: cloned, deleted: deleted}
}
func cloneDraft(d Draft) Draft       { d.Document = cloneJSON(d.Document); return d }
func cloneVersion(v Version) Version { v.Definition = cloneDefinition(v.Definition); return v }
func summarizeDraft(d Draft) DraftSummary {
	return DraftSummary{ID: d.ID, OrganizationID: d.OrganizationID, WorkflowID: d.WorkflowID, Revision: d.Revision,
		UpdatedBy: d.UpdatedBy, CreatedAt: d.CreatedAt, UpdatedAt: d.UpdatedAt}
}
func summarizeVersion(v Version) VersionSummary {
	return VersionSummary{OrganizationID: v.OrganizationID, WorkflowID: v.WorkflowID, VersionID: v.VersionID,
		PlanID: v.PlanID, Version: v.Version, PlanHash: v.PlanHash, PublishedAt: v.PublishedAt}
}
func cloneDefinition(value workflow.WorkflowDefinition) workflow.WorkflowDefinition {
	encoded, _ := json.Marshal(value)
	var result workflow.WorkflowDefinition
	_ = json.Unmarshal(encoded, &result)
	return result
}
