// Package postgres persists definition drafts and reads immutable publication history.
package postgres

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/apimutation"
	"github.com/pazhou/supaflow/internal/definition"
	basepostgres "github.com/pazhou/supaflow/internal/postgres"
)

// Store is the PostgreSQL implementation of definition.Store.
type Store struct{ pool *pgxpool.Pool }

// NewStore constructs a tenant-scoped definition store.
func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

var _ definition.Store = (*Store)(nil)

// CreateDraft atomically creates a draft and its idempotency result.
func (s *Store) CreateDraft(ctx context.Context, request definition.MutationRequest) (definition.Draft, error) {
	return s.mutateDraft(ctx, "create", request, func(tx pgx.Tx) (definition.Draft, error) {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "definition-draft-capacity:"+request.Draft.TenantID); err != nil {
			return definition.Draft{}, fmt.Errorf("lock workflow draft capacity: %w", err)
		}
		var draftCount int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM workflow_drafts WHERE tenant_id = $1`, request.Draft.TenantID).Scan(&draftCount); err != nil {
			return definition.Draft{}, fmt.Errorf("count workflow drafts: %w", err)
		}
		if draftCount >= definition.MaxDraftsPerTenant {
			return definition.Draft{}, definition.ErrTenantDraftLimit
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO workflows (id, tenant_id, slug, name, created_by)
			VALUES ($1, $2, $1, $1, $3) ON CONFLICT (tenant_id, id) DO NOTHING`,
			request.Draft.WorkflowID, request.Draft.TenantID, request.Draft.UpdatedBy); err != nil {
			return definition.Draft{}, fmt.Errorf("ensure workflow: %w", err)
		}
		var result definition.Draft
		err := tx.QueryRow(ctx, `
			INSERT INTO workflow_drafts
			    (id, tenant_id, workflow_id, revision, document, updated_by, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			RETURNING id, tenant_id, workflow_id, document, revision, updated_by, created_at, updated_at`,
			request.Draft.ID, request.Draft.TenantID, request.Draft.WorkflowID, request.Draft.Revision,
			request.Draft.Document, request.Draft.UpdatedBy, request.Draft.CreatedAt, request.Draft.UpdatedAt,
		).Scan(draftScan(&result)...)
		if err != nil {
			if isUniqueViolation(err) {
				return definition.Draft{}, definition.ErrRevisionConflict
			}
			return definition.Draft{}, fmt.Errorf("create workflow draft: %w", err)
		}
		return result, nil
	})
}

// GetDraft reads one tenant-owned draft.
func (s *Store) GetDraft(ctx context.Context, tenant, workflowID string) (definition.Draft, error) {
	return readDraft(ctx, s.pool, tenant, workflowID)
}

// ListDrafts lists a bounded page of tenant draft summaries in workflow order.
func (s *Store) ListDrafts(ctx context.Context, tenant, cursor string, limit int) (definition.DraftPage, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, tenant_id, workflow_id, revision, updated_by, created_at, updated_at
		FROM workflow_drafts WHERE tenant_id = $1 AND workflow_id > $2
		ORDER BY workflow_id LIMIT $3`, tenant, cursor, limit+1)
	if err != nil {
		return definition.DraftPage{}, fmt.Errorf("list workflow drafts: %w", err)
	}
	defer rows.Close()
	result := []definition.DraftSummary{}
	for rows.Next() {
		var item definition.DraftSummary
		if err := rows.Scan(&item.ID, &item.TenantID, &item.WorkflowID, &item.Revision, &item.UpdatedBy, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return definition.DraftPage{}, fmt.Errorf("scan workflow draft summary: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return definition.DraftPage{}, fmt.Errorf("iterate workflow drafts: %w", err)
	}
	page := definition.DraftPage{Items: result}
	if len(page.Items) > limit {
		page.NextCursor = page.Items[limit-1].WorkflowID
		page.Items = page.Items[:limit]
	}
	return page, nil
}

// UpdateDraft conditionally replaces a draft and records the immutable response.
func (s *Store) UpdateDraft(ctx context.Context, request definition.MutationRequest) (definition.Draft, error) {
	return s.mutateDraft(ctx, "update", request, func(tx pgx.Tx) (definition.Draft, error) {
		var result definition.Draft
		err := tx.QueryRow(ctx, `
			UPDATE workflow_drafts SET document = $1, updated_by = $2, updated_at = $3, revision = revision + 1
			WHERE tenant_id = $4 AND workflow_id = $5 AND revision = $6
			RETURNING id, tenant_id, workflow_id, document, revision, updated_by, created_at, updated_at`,
			request.Draft.Document, request.Draft.UpdatedBy, request.Draft.UpdatedAt,
			request.Draft.TenantID, request.Draft.WorkflowID, request.ExpectedRevision,
		).Scan(draftScan(&result)...)
		if errors.Is(err, pgx.ErrNoRows) {
			return definition.Draft{}, classifyMissingDraft(ctx, tx, request.Draft.TenantID, request.Draft.WorkflowID)
		}
		if err != nil {
			return definition.Draft{}, fmt.Errorf("update workflow draft: %w", err)
		}
		return result, nil
	})
}

// DeleteDraft conditionally removes a draft and records a replayable tombstone.
func (s *Store) DeleteDraft(ctx context.Context, request definition.MutationRequest) error {
	_, err := s.mutateDraft(ctx, "delete", request, func(tx pgx.Tx) (definition.Draft, error) {
		command, err := tx.Exec(ctx, `
			DELETE FROM workflow_drafts WHERE tenant_id = $1 AND workflow_id = $2 AND revision = $3`,
			request.Draft.TenantID, request.Draft.WorkflowID, request.ExpectedRevision)
		if err != nil {
			return definition.Draft{}, fmt.Errorf("delete workflow draft: %w", err)
		}
		if command.RowsAffected() == 0 {
			return definition.Draft{}, classifyMissingDraft(ctx, tx, request.Draft.TenantID, request.Draft.WorkflowID)
		}
		return definition.Draft{}, nil
	})
	return err
}

type mutationFunc func(pgx.Tx) (definition.Draft, error)

type mutationSnapshot struct {
	ID         string    `json:"id,omitempty"`
	TenantID   string    `json:"tenantId,omitempty"`
	WorkflowID string    `json:"workflowId,omitempty"`
	Revision   uint64    `json:"revision,omitempty"`
	UpdatedBy  string    `json:"updatedBy,omitempty"`
	CreatedAt  time.Time `json:"createdAt,omitempty"`
	UpdatedAt  time.Time `json:"updatedAt,omitempty"`
}

func (s *Store) mutateDraft(ctx context.Context, action string, request definition.MutationRequest, apply mutationFunc) (definition.Draft, error) {
	if s == nil || s.pool == nil {
		return definition.Draft{}, errors.New("postgres definition store requires a pool")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return definition.Draft{}, fmt.Errorf("begin definition mutation: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err = basepostgres.LockAPIMutationTx(ctx, tx); err != nil {
		if errors.Is(err, basepostgres.ErrIdempotencyConflict) {
			return definition.Draft{}, definition.ErrIdempotencyConflict
		}
		return definition.Draft{}, err
	}
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, hashtextextended($2, 0)))`, request.Draft.TenantID, request.IdempotencyKey); err != nil {
		return definition.Draft{}, fmt.Errorf("lock definition mutation: %w", err)
	}
	if replay, found, readErr := readMutation(ctx, tx, request); readErr != nil {
		return definition.Draft{}, readErr
	} else if found {
		if err = basepostgres.CompleteAPIMutationTx(ctx, tx, request.Draft.WorkflowID, map[string]any{"workflowId": request.Draft.WorkflowID}); err != nil {
			return definition.Draft{}, err
		}
		if err = tx.Commit(ctx); err != nil {
			return definition.Draft{}, fmt.Errorf("commit definition mutation replay: %w", err)
		}
		apimutation.MarkCommitted(ctx)
		return replay, nil
	}
	result, err := apply(tx)
	if err != nil {
		return definition.Draft{}, err
	}
	response, err := json.Marshal(snapshotDraft(result))
	if err != nil {
		return definition.Draft{}, fmt.Errorf("encode definition mutation response: %w", err)
	}
	mutationID, err := basepostgres.NewID("definition_mutation")
	if err != nil {
		return definition.Draft{}, err
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO definition_mutations
		    (id, tenant_id, idempotency_key, action, workflow_id, request_hash, response_snapshot)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`, mutationID, request.Draft.TenantID,
		request.IdempotencyKey, action, request.Draft.WorkflowID, request.RequestHash[:], response); err != nil {
		return definition.Draft{}, fmt.Errorf("record definition mutation: %w", err)
	}
	if err = basepostgres.CompleteAPIMutationTx(ctx, tx, request.Draft.WorkflowID, map[string]any{"workflowId": request.Draft.WorkflowID}); err != nil {
		return definition.Draft{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return definition.Draft{}, fmt.Errorf("commit definition mutation: %w", err)
	}
	apimutation.MarkCommitted(ctx)
	return result, nil
}

func readMutation(ctx context.Context, tx pgx.Tx, request definition.MutationRequest) (definition.Draft, bool, error) {
	var storedHash, response []byte
	err := tx.QueryRow(ctx, `
		SELECT request_hash, response_snapshot FROM definition_mutations
		WHERE tenant_id = $1 AND idempotency_key = $2`, request.Draft.TenantID, request.IdempotencyKey,
	).Scan(&storedHash, &response)
	if errors.Is(err, pgx.ErrNoRows) {
		return definition.Draft{}, false, nil
	}
	if err != nil {
		return definition.Draft{}, false, fmt.Errorf("inspect definition mutation: %w", err)
	}
	if len(storedHash) != len(request.RequestHash) || subtle.ConstantTimeCompare(storedHash, request.RequestHash[:]) != 1 {
		return definition.Draft{}, true, definition.ErrIdempotencyConflict
	}
	var snapshot mutationSnapshot
	if len(response) > 0 && string(response) != "null" && string(response) != "{}" {
		if err := json.Unmarshal(response, &snapshot); err != nil {
			return definition.Draft{}, true, fmt.Errorf("decode definition mutation response: %w", err)
		}
	}
	return restoreDraft(snapshot), true, nil
}

func snapshotDraft(value definition.Draft) mutationSnapshot {
	return mutationSnapshot{ID: value.ID, TenantID: value.TenantID, WorkflowID: value.WorkflowID, Revision: value.Revision,
		UpdatedBy: value.UpdatedBy, CreatedAt: value.CreatedAt, UpdatedAt: value.UpdatedAt}
}

func restoreDraft(value mutationSnapshot) definition.Draft {
	return definition.Draft{ID: value.ID, TenantID: value.TenantID, WorkflowID: value.WorkflowID, Revision: value.Revision,
		UpdatedBy: value.UpdatedBy, CreatedAt: value.CreatedAt, UpdatedAt: value.UpdatedAt}
}

func readDraft(ctx context.Context, db interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, tenant, workflowID string) (definition.Draft, error) {
	var result definition.Draft
	err := db.QueryRow(ctx, `
		SELECT id, tenant_id, workflow_id, document, revision, updated_by, created_at, updated_at
		FROM workflow_drafts WHERE tenant_id = $1 AND workflow_id = $2`, tenant, workflowID,
	).Scan(draftScan(&result)...)
	if errors.Is(err, pgx.ErrNoRows) {
		return definition.Draft{}, definition.ErrNotFound
	}
	if err != nil {
		return definition.Draft{}, fmt.Errorf("read workflow draft: %w", err)
	}
	return result, nil
}

func draftScan(target *definition.Draft) []any {
	return []any{&target.ID, &target.TenantID, &target.WorkflowID, &target.Document, &target.Revision, &target.UpdatedBy, &target.CreatedAt, &target.UpdatedAt}
}

func classifyMissingDraft(ctx context.Context, tx pgx.Tx, tenant, workflowID string) error {
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM workflow_drafts WHERE tenant_id = $1 AND workflow_id = $2)`, tenant, workflowID).Scan(&exists); err != nil {
		return fmt.Errorf("inspect workflow draft conflict: %w", err)
	}
	if exists {
		return definition.ErrRevisionConflict
	}
	return definition.ErrNotFound
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// RecordVersion verifies that the publication catalog persisted the matching immutable version.
func (s *Store) RecordVersion(ctx context.Context, expected definition.Version) error {
	actual, err := s.GetVersion(ctx, expected.TenantID, expected.WorkflowID, expected.Version)
	if err != nil {
		return err
	}
	if actual.PlanID != expected.PlanID || actual.PlanHash != expected.PlanHash {
		return errors.New("published definition projection does not match execution plan")
	}
	return nil
}

// ListVersions returns one bounded page of newest version summaries first.
func (s *Store) ListVersions(ctx context.Context, tenant, workflowID string, cursor uint64, limit int) (definition.VersionPage, error) {
	rows, err := s.pool.Query(ctx, versionSummarySelect+`
		WHERE v.tenant_id = $1 AND v.workflow_id = $2 AND ($3::bigint = 0 OR v.version < $3)
		ORDER BY v.version DESC LIMIT $4`, tenant, workflowID, cursor, limit+1)
	if err != nil {
		return definition.VersionPage{}, fmt.Errorf("list workflow versions: %w", err)
	}
	defer rows.Close()
	result := []definition.VersionSummary{}
	for rows.Next() {
		var item definition.VersionSummary
		if err := rows.Scan(&item.TenantID, &item.WorkflowID, &item.VersionID, &item.PlanID, &item.Version, &item.PlanHash, &item.PublishedAt); err != nil {
			return definition.VersionPage{}, fmt.Errorf("scan workflow version summary: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return definition.VersionPage{}, fmt.Errorf("iterate workflow versions: %w", err)
	}
	page := definition.VersionPage{Items: result}
	if len(page.Items) > limit {
		page.NextCursor = page.Items[limit-1].Version
		page.Items = page.Items[:limit]
	}
	return page, nil
}

// GetVersion returns one tenant workflow version.
func (s *Store) GetVersion(ctx context.Context, tenant, workflowID string, version uint64) (definition.Version, error) {
	return scanVersion(s.pool.QueryRow(ctx, versionSelect+` WHERE v.tenant_id = $1 AND v.workflow_id = $2 AND v.version = $3`, tenant, workflowID, version))
}

// GetLatestVersion returns the newest tenant workflow version.
func (s *Store) GetLatestVersion(ctx context.Context, tenant, workflowID string) (definition.Version, error) {
	return scanVersion(s.pool.QueryRow(ctx, versionSelect+` WHERE v.tenant_id = $1 AND v.workflow_id = $2 ORDER BY v.version DESC LIMIT 1`, tenant, workflowID))
}

const versionSelect = `
	SELECT v.tenant_id, v.workflow_id, v.id, p.id, v.version, encode(p.plan_hash, 'hex'), v.definition, v.published_at
	FROM workflow_versions v JOIN execution_plans p
	  ON p.tenant_id = v.tenant_id AND p.workflow_id = v.workflow_id AND p.workflow_version_id = v.id`

const versionSummarySelect = `
	SELECT v.tenant_id, v.workflow_id, v.id, p.id, v.version, encode(p.plan_hash, 'hex'), v.published_at
	FROM workflow_versions v JOIN execution_plans p
	  ON p.tenant_id = v.tenant_id AND p.workflow_id = v.workflow_id AND p.workflow_version_id = v.id`

type rowScanner interface{ Scan(...any) error }

func scanVersion(row rowScanner) (definition.Version, error) {
	var result definition.Version
	var raw json.RawMessage
	if err := row.Scan(&result.TenantID, &result.WorkflowID, &result.VersionID, &result.PlanID, &result.Version, &result.PlanHash, &raw, &result.PublishedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return definition.Version{}, definition.ErrNotFound
		}
		return definition.Version{}, fmt.Errorf("read workflow version: %w", err)
	}
	if err := json.Unmarshal(raw, &result.Definition); err != nil {
		return definition.Version{}, fmt.Errorf("decode workflow version: %w", err)
	}
	return result, nil
}
