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
func (c *PlanCatalog) Compile(_ context.Context, tenantID string, definition workflow.WorkflowDefinition) (*workflow.ExecutionPlan, error) {
	if tenantID == "" {
		return nil, errors.New("tenant is required")
	}
	return workflow.Compile(definition, c.registry, c.policy)
}

// Publish atomically allocates a workflow-local version and stores immutable
// definition and plan records under tenant-scoped idempotency.
func (c *PlanCatalog) Publish(ctx context.Context, tenantID, workflowID string, definition workflow.WorkflowDefinition, idempotencyKey string) (platform.PublishedPlan, bool, error) {
	if c == nil || c.pool == nil || tenantID == "" || workflowID == "" ||
		idempotencyKey == "" || definition.ID != workflowID {
		return platform.PublishedPlan{}, false, errors.New("tenant, matching workflow id, and idempotency key are required")
	}
	plan, err := c.Compile(ctx, tenantID, definition)
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

	existing, found, err := resolvePublicationKey(ctx, tx, tenantID, idempotencyKey)
	if err != nil {
		return platform.PublishedPlan{}, false, err
	}
	if found {
		if existing.Hash != plan.Hash() {
			return platform.PublishedPlan{}, false, platform.ErrPublicationConflict
		}
		resolved, err := c.resolveTx(ctx, tx, tenantID, existing.ID, existing.Hash)
		if err != nil {
			return platform.PublishedPlan{}, false, err
		}
		if err = CompleteAPIMutationTx(ctx, tx, workflowID, map[string]any{"planId": resolved.ID}); err != nil {
			return platform.PublishedPlan{}, false, err
		}
		if err = tx.Commit(ctx); err != nil {
			return platform.PublishedPlan{}, false, fmt.Errorf("commit workflow publication replay: %w", err)
		}
		apimutation.MarkCommitted(ctx)
		return resolved, false, nil
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO workflows (id, tenant_id, slug, name, created_by)
		VALUES ($1, $2, $1, $1, 'system') ON CONFLICT (tenant_id, id) DO NOTHING`, workflowID, tenantID); err != nil {
		return platform.PublishedPlan{}, false, fmt.Errorf("ensure workflow record: %w", err)
	}
	var lockedWorkflowID string
	if err = tx.QueryRow(ctx, `
		SELECT id FROM workflows WHERE tenant_id = $1 AND id = $2 FOR UPDATE`,
		tenantID, workflowID).Scan(&lockedWorkflowID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return platform.PublishedPlan{}, false, platform.ErrPublicationConflict
		}
		return platform.PublishedPlan{}, false, fmt.Errorf("lock workflow publication: %w", err)
	}
	// A concurrent request may have committed this key while this transaction
	// waited for the workflow lock.
	existing, found, err = resolvePublicationKey(ctx, tx, tenantID, idempotencyKey)
	if err != nil {
		return platform.PublishedPlan{}, false, err
	}
	if found {
		if existing.Hash != plan.Hash() {
			return platform.PublishedPlan{}, false, platform.ErrPublicationConflict
		}
		resolved, resolveErr := c.resolveTx(ctx, tx, tenantID, existing.ID, existing.Hash)
		if resolveErr != nil {
			return platform.PublishedPlan{}, false, resolveErr
		}
		if err = CompleteAPIMutationTx(ctx, tx, workflowID, map[string]any{"planId": resolved.ID}); err != nil {
			return platform.PublishedPlan{}, false, err
		}
		if err = tx.Commit(ctx); err != nil {
			return platform.PublishedPlan{}, false, fmt.Errorf("commit workflow publication replay: %w", err)
		}
		apimutation.MarkCommitted(ctx)
		return resolved, false, nil
	}
	var version uint64
	if err = tx.QueryRow(ctx, `
		SELECT COALESCE(MAX(version), 0) + 1 FROM workflow_versions
		WHERE tenant_id = $1 AND workflow_id = $2`, tenantID, workflowID).Scan(&version); err != nil {
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
		    id, tenant_id, workflow_id, version, publication_key, definition,
		    definition_hash, published_by, published_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, 'system', $8)`,
		versionID, tenantID, workflowID, version, idempotencyKey, definitionJSON,
		definitionHash[:], publishedAt); err != nil {
		return platform.PublishedPlan{}, false, fmt.Errorf("persist workflow version: %w", err)
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO execution_plans (
		    id, tenant_id, workflow_id, workflow_version_id, plan, plan_hash,
		    compiler_version, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		planID, tenantID, workflowID, versionID, planJSON, decodeHash(plan.Hash()),
		plan.SchemaVersion(), publishedAt); err != nil {
		return platform.PublishedPlan{}, false, fmt.Errorf("persist execution plan: %w", err)
	}
	if err = CompleteAPIMutationTx(ctx, tx, workflowID, map[string]any{"planId": planID}); err != nil {
		return platform.PublishedPlan{}, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return platform.PublishedPlan{}, false, fmt.Errorf("commit workflow publication: %w", err)
	}
	apimutation.MarkCommitted(ctx)
	return platform.PublishedPlan{
		ID: planID, WorkflowID: workflowID, Version: version, Hash: plan.Hash(),
		Plan: plan, PublishedAt: publishedAt,
	}, true, nil
}

// Resolve reads an immutable plan with both tenant and hash predicates.
func (c *PlanCatalog) Resolve(ctx context.Context, tenantID, planID, hash string) (platform.PublishedPlan, error) {
	if c == nil || c.pool == nil {
		return platform.PublishedPlan{}, errors.New("postgres catalog requires a pool")
	}
	return c.resolveTx(ctx, c.pool, tenantID, planID, hash)
}

type queryRower interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func (c *PlanCatalog) resolveTx(ctx context.Context, db queryRower, tenantID, planID, hash string) (platform.PublishedPlan, error) {
	var published platform.PublishedPlan
	var planJSON json.RawMessage
	err := db.QueryRow(ctx, `
		SELECT p.id, p.workflow_id, v.version, encode(p.plan_hash, 'hex'),
		       p.plan, v.published_at
		FROM execution_plans p
		JOIN workflow_versions v
		  ON v.tenant_id = p.tenant_id AND v.id = p.workflow_version_id
		WHERE p.tenant_id = $1 AND p.id = $2 AND encode(p.plan_hash, 'hex') = $3`,
		tenantID, planID, hash).Scan(&published.ID, &published.WorkflowID,
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

func resolvePublicationKey(ctx context.Context, tx pgx.Tx, tenantID, key string) (platform.PublishedPlan, bool, error) {
	var published platform.PublishedPlan
	err := tx.QueryRow(ctx, `
		SELECT p.id, encode(p.plan_hash, 'hex')
		FROM workflow_versions v
		JOIN execution_plans p ON p.tenant_id = v.tenant_id AND p.workflow_version_id = v.id
		WHERE v.tenant_id = $1 AND v.publication_key = $2`, tenantID, key).Scan(&published.ID, &published.Hash)
	if errors.Is(err, pgx.ErrNoRows) {
		return platform.PublishedPlan{}, false, nil
	}
	if err != nil {
		return platform.PublishedPlan{}, false, fmt.Errorf("inspect publication idempotency: %w", err)
	}
	return published, true, nil
}
