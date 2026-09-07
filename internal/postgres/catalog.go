package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/apimutation"
	"github.com/pazhou/supaflow/internal/platform"
	"github.com/pazhou/supaflow/internal/workflow"
)

// PlanCatalog is the PostgreSQL implementation of platform.PlanCatalog. It
// verifies and parses the immutable plan stored at publication time on reads.
type PlanCatalog struct {
	pool     *pgxpool.Pool
	registry *workflow.Registry
	policy   workflow.CompilePolicy
}

// NewPlanCatalog constructs a production publication catalog.
func NewPlanCatalog(pool *pgxpool.Pool, registry *workflow.Registry, policy workflow.CompilePolicy) *PlanCatalog {
	return &PlanCatalog{pool: pool, registry: registry, policy: policy}
}

var _ platform.PlanCatalog = (*PlanCatalog)(nil)

// Compile validates and compiles a definition without mutating publication state.
func (c *PlanCatalog) Compile(_ context.Context, organizationID string, definition workflow.WorkflowDefinition) (*workflow.ExecutionPlan, error) {
	if organizationID == "" {
		return nil, errors.New("organization is required")
	}
	return workflow.Compile(definition, c.registry, c.policy)
}

// Publish atomically allocates a workflow-local version and stores immutable
// definition and plan records under organization-scoped idempotency.
func (c *PlanCatalog) Publish(ctx context.Context, organizationID, workflowID string, definition workflow.WorkflowDefinition, idempotencyKey string) (platform.PublishedPlan, bool, error) {
	if c == nil || c.pool == nil || organizationID == "" || workflowID == "" ||
		idempotencyKey == "" || definition.ID != workflowID {
		return platform.PublishedPlan{}, false, errors.New("organization, matching workflow id, and idempotency key are required")
	}
	plan, err := c.Compile(ctx, organizationID, definition)
	if err != nil {
		return platform.PublishedPlan{}, false, err
	}
	definitionJSON, err := json.Marshal(definition)
	if err != nil {
		return platform.PublishedPlan{}, false, fmt.Errorf("encode workflow definition: %w", err)
	}
	planJSON, err := json.Marshal(plan)
	if err != nil {
		return platform.PublishedPlan{}, false, fmt.Errorf("encode execution plan: %w", err)
	}
	definitionHash := sha256.Sum256(definitionJSON)
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return platform.PublishedPlan{}, false, fmt.Errorf("begin workflow publication: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err = LockAPIMutationTx(ctx, tx); err != nil {
		if errors.Is(err, ErrIdempotencyConflict) {
			return platform.PublishedPlan{}, false, platform.ErrPublicationConflict
		}
		return platform.PublishedPlan{}, false, err
	}

	existing, found, err := resolvePublicationKey(ctx, tx, organizationID, idempotencyKey)
	if err != nil {
		return platform.PublishedPlan{}, false, err
	}
	if found {
		if existing.Hash != plan.Hash() {
			return platform.PublishedPlan{}, false, platform.ErrPublicationConflict
		}
		resolved, err := c.resolveTx(ctx, tx, organizationID, existing.ID, existing.Hash)
		if err != nil {
			return platform.PublishedPlan{}, false, err
		}
		if err = CompleteAPIMutationTx(ctx, tx, workflowID, resolved); err != nil {
			return platform.PublishedPlan{}, false, err
		}
		if err = tx.Commit(ctx); err != nil {
			return platform.PublishedPlan{}, false, fmt.Errorf("commit workflow publication replay: %w", err)
		}
		apimutation.MarkCommitted(ctx)
		return resolved, false, nil
	}
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "workflow-version-capacity:"+organizationID); err != nil {
		return platform.PublishedPlan{}, false, fmt.Errorf("lock workflow version capacity: %w", err)
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO workflows (id, organization_id, slug, name, created_by)
		VALUES ($1, $2, $1, $1, 'system') ON CONFLICT (organization_id, id) DO NOTHING`, workflowID, organizationID); err != nil {
		return platform.PublishedPlan{}, false, fmt.Errorf("ensure workflow record: %w", err)
	}
	var lockedWorkflowID string
	if err = tx.QueryRow(ctx, `
		SELECT id FROM workflows WHERE organization_id = $1 AND id = $2 FOR UPDATE`,
		organizationID, workflowID).Scan(&lockedWorkflowID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return platform.PublishedPlan{}, false, platform.ErrPublicationConflict
		}
		return platform.PublishedPlan{}, false, fmt.Errorf("lock workflow publication: %w", err)
	}
	// A concurrent request may have committed this key while this transaction
	// waited for the workflow lock.
	existing, found, err = resolvePublicationKey(ctx, tx, organizationID, idempotencyKey)
	if err != nil {
		return platform.PublishedPlan{}, false, err
	}
	if found {
		if existing.Hash != plan.Hash() {
			return platform.PublishedPlan{}, false, platform.ErrPublicationConflict
		}
		resolved, resolveErr := c.resolveTx(ctx, tx, organizationID, existing.ID, existing.Hash)
		if resolveErr != nil {
			return platform.PublishedPlan{}, false, resolveErr
		}
		if err = CompleteAPIMutationTx(ctx, tx, workflowID, resolved); err != nil {
			return platform.PublishedPlan{}, false, err
		}
		if err = tx.Commit(ctx); err != nil {
			return platform.PublishedPlan{}, false, fmt.Errorf("commit workflow publication replay: %w", err)
		}
		apimutation.MarkCommitted(ctx)
		return resolved, false, nil
	}
	var organizationVersionCount, workflowVersionCount uint64
	if err = tx.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE workflow_id = $2)
		FROM workflow_versions WHERE organization_id = $1`, organizationID, workflowID).Scan(&organizationVersionCount, &workflowVersionCount); err != nil {
		return platform.PublishedPlan{}, false, fmt.Errorf("count workflow versions: %w", err)
	}
	if organizationVersionCount >= platform.MaxVersionsPerOrganization {
		return platform.PublishedPlan{}, false, platform.ErrOrganizationVersionLimit
	}
	if workflowVersionCount >= platform.MaxVersionsPerWorkflow {
		return platform.PublishedPlan{}, false, platform.ErrWorkflowVersionLimit
	}
	var version uint64
	if err = tx.QueryRow(ctx, `
		SELECT COALESCE(MAX(version), 0) + 1 FROM workflow_versions
		WHERE organization_id = $1 AND workflow_id = $2`, organizationID, workflowID).Scan(&version); err != nil {
		return platform.PublishedPlan{}, false, fmt.Errorf("allocate workflow version: %w", err)
	}
	versionID, err := NewID("version")
	if err != nil {
		return platform.PublishedPlan{}, false, err
	}
	planID, err := NewID("plan")
	if err != nil {
		return platform.PublishedPlan{}, false, err
	}
	publishedAt := time.Now().UTC()
	if _, err = tx.Exec(ctx, `
		INSERT INTO workflow_versions (
		    id, organization_id, workflow_id, version, publication_key, definition,
		    definition_hash, published_by, published_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, 'system', $8)`,
		versionID, organizationID, workflowID, version, idempotencyKey, definitionJSON,
		definitionHash[:], publishedAt); err != nil {
		return platform.PublishedPlan{}, false, fmt.Errorf("persist workflow version: %w", err)
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO execution_plans (
		    id, organization_id, workflow_id, workflow_version_id, plan, plan_hash,
		    compiler_version, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		planID, organizationID, workflowID, versionID, planJSON, decodeHash(plan.Hash()),
		plan.SchemaVersion(), publishedAt); err != nil {
		return platform.PublishedPlan{}, false, fmt.Errorf("persist execution plan: %w", err)
	}
	published := platform.PublishedPlan{
		ID: planID, WorkflowID: workflowID, Version: version, Hash: plan.Hash(),
		Plan: plan, PublishedAt: publishedAt,
	}
	if err = CompleteAPIMutationTx(ctx, tx, workflowID, published); err != nil {
		return platform.PublishedPlan{}, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return platform.PublishedPlan{}, false, fmt.Errorf("commit workflow publication: %w", err)
	}
	apimutation.MarkCommitted(ctx)
	return published, true, nil
}

// ReplayPublication returns the immutable publication previously accepted for
// this key without reading or compiling the mutable current draft.
func (c *PlanCatalog) ReplayPublication(ctx context.Context, organizationID, workflowID, key string) (platform.PublishedPlan, bool, error) {
	if c == nil || c.pool == nil || organizationID == "" || workflowID == "" || key == "" {
		return platform.PublishedPlan{}, false, errors.New("organization, workflow, and idempotency key are required")
	}
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return platform.PublishedPlan{}, false, fmt.Errorf("begin publication replay: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err = LockAPIMutationTx(ctx, tx); err != nil {
		if errors.Is(err, ErrIdempotencyConflict) {
			return platform.PublishedPlan{}, false, platform.ErrPublicationConflict
		}
		return platform.PublishedPlan{}, false, err
	}
	existing, found, err := resolvePublicationKey(ctx, tx, organizationID, key)
	if err != nil || !found {
		return platform.PublishedPlan{}, found, err
	}
	published, err := c.resolveTx(ctx, tx, organizationID, existing.ID, existing.Hash)
	if err != nil {
		return platform.PublishedPlan{}, false, err
	}
	if published.WorkflowID != workflowID {
		return platform.PublishedPlan{}, false, platform.ErrPublicationConflict
	}
	if err = CompleteAPIMutationTx(ctx, tx, workflowID, published); err != nil {
		return platform.PublishedPlan{}, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return platform.PublishedPlan{}, false, fmt.Errorf("commit publication replay: %w", err)
	}
	apimutation.MarkCommitted(ctx)
	return published, true, nil
}

// Resolve reads an immutable plan with both organization and hash predicates.
func (c *PlanCatalog) Resolve(ctx context.Context, organizationID, planID, hash string) (platform.PublishedPlan, error) {
	if c == nil || c.pool == nil {
		return platform.PublishedPlan{}, errors.New("postgres catalog requires a pool")
	}
	return c.resolveTx(ctx, c.pool, organizationID, planID, hash)
}

type queryRower interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func (c *PlanCatalog) resolveTx(ctx context.Context, db queryRower, organizationID, planID, hash string) (platform.PublishedPlan, error) {
	var published platform.PublishedPlan
	var planJSON json.RawMessage
	err := db.QueryRow(ctx, `
		SELECT p.id, p.workflow_id, v.version, encode(p.plan_hash, 'hex'),
		       p.plan, v.published_at
		FROM execution_plans p
		JOIN workflow_versions v
		  ON v.organization_id = p.organization_id AND v.id = p.workflow_version_id
		WHERE p.organization_id = $1 AND p.id = $2 AND encode(p.plan_hash, 'hex') = $3`,
		organizationID, planID, hash).Scan(&published.ID, &published.WorkflowID,
		&published.Version, &published.Hash, &planJSON, &published.PublishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return platform.PublishedPlan{}, platform.ErrPlanNotFound
	}
	if err != nil {
		return platform.PublishedPlan{}, fmt.Errorf("resolve execution plan: %w", err)
	}
	published.Plan, err = workflow.ParseExecutionPlan(planJSON)
	if err != nil {
		return platform.PublishedPlan{}, fmt.Errorf("verify stored execution plan: %w", err)
	}
	if published.Plan.Hash() != published.Hash || published.Plan.WorkflowID() != published.WorkflowID {
		return platform.PublishedPlan{}, fmt.Errorf("stored execution plan failed integrity verification")
	}
	return published, nil
}

func resolvePublicationKey(ctx context.Context, tx pgx.Tx, organizationID, key string) (platform.PublishedPlan, bool, error) {
	var published platform.PublishedPlan
	err := tx.QueryRow(ctx, `
		SELECT p.id, encode(p.plan_hash, 'hex')
		FROM workflow_versions v
		JOIN execution_plans p ON p.organization_id = v.organization_id AND p.workflow_version_id = v.id
		WHERE v.organization_id = $1 AND v.publication_key = $2`, organizationID, key).Scan(&published.ID, &published.Hash)
	if errors.Is(err, pgx.ErrNoRows) {
		return platform.PublishedPlan{}, false, nil
	}
	if err != nil {
		return platform.PublishedPlan{}, false, fmt.Errorf("inspect publication idempotency: %w", err)
	}
	return published, true, nil
}
