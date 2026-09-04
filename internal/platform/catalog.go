// Package platform composes workflow publication and runtime boundaries without
// coupling either domain to HTTP or database representations.
package platform

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"github.com/pazhou/supaflow/internal/workflow"
)

var (
	// ErrPlanNotFound hides whether a plan belongs to another tenant.
	ErrPlanNotFound = errors.New("published execution plan not found")
	// ErrPublicationConflict reports idempotency-key reuse with different content.
	ErrPublicationConflict = errors.New("publication idempotency conflict")
)

// PublishedPlan is the immutable publication projection exposed to application adapters.
type PublishedPlan struct {
	ID          string                  `json:"id"`
	WorkflowID  string                  `json:"workflowId"`
	Version     uint64                  `json:"version"`
	Hash        string                  `json:"hash"`
	Plan        *workflow.ExecutionPlan `json:"plan"`
	PublishedAt time.Time               `json:"publishedAt"`
}

// PlanCatalog is the small publication seam required by APIs and executors.
type PlanCatalog interface {
	Compile(context.Context, string, workflow.WorkflowDefinition) (*workflow.ExecutionPlan, error)
	Publish(context.Context, string, string, workflow.WorkflowDefinition, string) (PublishedPlan, bool, error)
	Resolve(context.Context, string, string, string) (PublishedPlan, error)
}

// NodeTypeCatalog exposes the durable node registry projection required by
// Studio discovery. Compiler and executor adapter caches may implement this
// interface for tests, but production control planes use PostgreSQL.
type NodeTypeCatalog interface {
	ListNodeTypes(context.Context) ([]workflow.NodeTypeVersion, error)
}

type publicationRecord struct {
	planID string
	hash   string
}

// MemoryCatalog is a concurrency-safe executable specification for local runs
// and protocol tests. Production deployments use a PostgreSQL catalog adapter.
type MemoryCatalog struct {
	mu           sync.RWMutex
	registry     *workflow.Registry
	policy       workflow.CompilePolicy
	plans        map[string]PublishedPlan
	versions     map[string]uint64
	publications map[string]publicationRecord
	now          func() time.Time
}

// NewMemoryCatalog creates an isolated tenant-scoped catalog.
func NewMemoryCatalog(registry *workflow.Registry, policy workflow.CompilePolicy) *MemoryCatalog {
	return &MemoryCatalog{
		registry: registry, policy: policy, plans: make(map[string]PublishedPlan),
		versions: make(map[string]uint64), publications: make(map[string]publicationRecord),
		now: func() time.Time { return time.Now().UTC() },
	}
}

// Compile validates a definition without mutating publication state.
func (c *MemoryCatalog) Compile(_ context.Context, tenantID string, definition workflow.WorkflowDefinition) (*workflow.ExecutionPlan, error) {
	if tenantID == "" {
		return nil, errors.New("tenant is required")
	}
	return workflow.Compile(definition, c.registry, c.policy)
}

// Publish atomically assigns a workflow-local version and enforces tenant-scoped idempotency.
func (c *MemoryCatalog) Publish(ctx context.Context, tenantID, workflowID string, definition workflow.WorkflowDefinition, idempotencyKey string) (PublishedPlan, bool, error) {
	if tenantID == "" || workflowID == "" || idempotencyKey == "" || definition.ID != workflowID {
		return PublishedPlan{}, false, errors.New("tenant, matching workflow id, and idempotency key are required")
	}
	plan, err := c.Compile(ctx, tenantID, definition)
	if err != nil {
		return PublishedPlan{}, false, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	idempotencyScope := tenantID + "\x00" + idempotencyKey
	if recorded, ok := c.publications[idempotencyScope]; ok {
		if recorded.hash != plan.Hash() {
			return PublishedPlan{}, false, ErrPublicationConflict
		}
		return c.plans[tenantID+"\x00"+recorded.planID], false, nil
	}
	planID, err := randomID("plan")
	if err != nil {
		return PublishedPlan{}, false, err
	}
	workflowScope := tenantID + "\x00" + workflowID
	version := c.versions[workflowScope] + 1
	published := PublishedPlan{
		ID: planID, WorkflowID: workflowID, Version: version, Hash: plan.Hash(),
		Plan: plan, PublishedAt: c.now(),
	}
	c.versions[workflowScope] = version
	c.plans[tenantID+"\x00"+planID] = published
	c.publications[idempotencyScope] = publicationRecord{planID: planID, hash: plan.Hash()}
	return published, true, nil
}

// Resolve verifies both tenant ownership and the caller-provided immutable hash.
func (c *MemoryCatalog) Resolve(_ context.Context, tenantID, planID, hash string) (PublishedPlan, error) {
	c.mu.RLock()
	published, ok := c.plans[tenantID+"\x00"+planID]
	c.mu.RUnlock()
	if !ok || published.Hash != hash {
		return PublishedPlan{}, ErrPlanNotFound
	}
	return published, nil
}

func randomID(prefix string) (string, error) {
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		return "", err
	}
	return prefix + "_" + hex.EncodeToString(data[:]), nil
}
