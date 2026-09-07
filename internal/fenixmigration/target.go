package fenixmigration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/definition"
	definitionflowgram "github.com/pazhou/supaflow/internal/definition/flowgram"
	definitionpostgres "github.com/pazhou/supaflow/internal/definition/postgres"
	"github.com/pazhou/supaflow/internal/platformops"
	"github.com/pazhou/supaflow/internal/postgres"
	"github.com/pazhou/supaflow/internal/workflow"
)

// ImportedVersion identifies the exact immutable target projection.
type ImportedVersion struct {
	VersionID string
	PlanID    string
	PlanHash  string
	Version   uint64
}

// TargetAttestation binds a migration report to one concrete Supaflow
// installation and the canonical set of Fenix-imported records it contains.
type TargetAttestation struct {
	SchemaVersion   int
	InstallationID  string
	ContentHash     string
	WorkflowCount   int
	DefinitionCount int
	DraftCount      int
	TriggerCount    int
	RunCount        int
}

// Target is the privileged write boundary used by the one-time migration runner.
type Target interface {
	EnsureOrganization(context.Context, string, string) error
	ImportWorkflow(context.Context, platformops.ImportWorkflowRequest) (bool, error)
	ImportVersion(context.Context, string, string, uint64, workflow.WorkflowDefinition, string) (ImportedVersion, bool, error)
	ImportDraft(context.Context, string, string, string, workflow.WorkflowDefinition, string) (bool, error)
	ImportTrigger(context.Context, platformops.ImportTriggerRequest) (bool, error)
	ImportRun(context.Context, platformops.ImportRunProjectionRequest) (bool, error)
	Attest(context.Context) (TargetAttestation, error)
}

// ImportWorkflow preserves source-owned metadata before definitions create any implicit record.
func (t *PostgresTarget) ImportWorkflow(ctx context.Context, request platformops.ImportWorkflowRequest) (bool, error) {
	return t.admin.ImportWorkflow(ctx, request)
}

// PostgresTarget wires domain import seams to a migrated Supaflow database.
type PostgresTarget struct {
	pool        *pgxpool.Pool
	definitions *definition.Service
	admin       *platformops.AdminImporter
}

// NewPostgresTarget constructs the production target adapter.
func NewPostgresTarget(pool *pgxpool.Pool) (*PostgresTarget, error) {
	if pool == nil {
		return nil, errors.New("target database is required")
	}
	registry := workflow.BuiltinRegistry()
	policy := migrationCompilePolicy()
	catalog := postgres.NewPlanCatalog(pool, registry, policy)
	definitions, err := definition.NewService(definitionpostgres.NewStore(pool), definitionflowgram.Adapter{}, catalog, registry, policy)
	if err != nil {
		return nil, err
	}
	admin, err := platformops.NewAdminImporter(pool)
	if err != nil {
		return nil, err
	}
	return &PostgresTarget{pool: pool, definitions: definitions, admin: admin}, nil
}

// EnsureOrganization validates the opaque Fenix organization scope without persisting a local registry.
func (t *PostgresTarget) EnsureOrganization(_ context.Context, organizationID, name string) error {
	if organizationID == "" || name == "" {
		return errors.New("organization id and name are required")
	}
	return nil
}

// ImportVersion preserves source ordering through Definition's administrative seam.
func (t *PostgresTarget) ImportVersion(ctx context.Context, organizationID, workflowID string, sourceVersion uint64, value workflow.WorkflowDefinition, key string) (ImportedVersion, bool, error) {
	published, created, err := t.definitions.ImportVersion(ctx, organizationID, workflowID, definition.ImportVersionCommand{
		Definition: value, SourceVersion: sourceVersion, IdempotencyKey: key,
	})
	if err != nil {
		return ImportedVersion{}, false, err
	}
	version, err := t.definitions.GetVersion(ctx, organizationID, workflowID, sourceVersion)
	if err != nil {
		return ImportedVersion{}, false, err
	}
	if published.ID != version.PlanID || published.Hash != version.PlanHash {
		return ImportedVersion{}, false, errors.New("imported version projection is inconsistent")
	}
	return ImportedVersion{VersionID: version.VersionID, PlanID: version.PlanID, PlanHash: version.PlanHash, Version: version.Version}, created, nil
}

// ImportDraft stores a FlowGram adapter document derived from the converted canonical model.
func (t *PostgresTarget) ImportDraft(ctx context.Context, organizationID, workflowID, actorID string, value workflow.WorkflowDefinition, key string) (bool, error) {
	document, err := flowgramDocument(value)
	if err != nil {
		return false, err
	}
	result, err := t.definitions.CreateDraftWithResult(ctx, organizationID, definition.CreateDraftCommand{
		WorkflowID: workflowID, Document: document, ActorID: actorID, IdempotencyKey: key,
	})
	if errors.Is(err, definition.ErrRevisionConflict) {
		// A draft under another key is intentionally not overwritten during resume.
		return false, err
	}
	return result.Created, err
}

// ImportTrigger delegates to the secret-free administrative trigger importer.
func (t *PostgresTarget) ImportTrigger(ctx context.Context, request platformops.ImportTriggerRequest) (bool, error) {
	_, created, err := t.admin.ImportTrigger(ctx, request)
	return created, err
}

// ImportRun delegates to the terminal-history administrative importer.
func (t *PostgresTarget) ImportRun(ctx context.Context, request platformops.ImportRunProjectionRequest) (bool, error) {
	return t.admin.ImportRunProjection(ctx, request)
}

var (
	uuidPattern   = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	sha256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// Attest reads Supaflow's versioned, read-only database attestation. The
// function returns only identity, counts, and a digest; workflow data and
// secrets remain inside PostgreSQL.
func (t *PostgresTarget) Attest(ctx context.Context) (TargetAttestation, error) {
	var value TargetAttestation
	err := t.pool.QueryRow(ctx, `SELECT schema_version,installation_id,content_hash,
		workflow_count,definition_count,draft_count,trigger_count,run_count
		FROM fenix_target_attestation_v2()`).Scan(
		&value.SchemaVersion,
		&value.InstallationID,
		&value.ContentHash,
		&value.WorkflowCount,
		&value.DefinitionCount,
		&value.DraftCount,
		&value.TriggerCount,
		&value.RunCount,
	)
	if err != nil {
		return TargetAttestation{}, fmt.Errorf("read target attestation: %w", err)
	}
	if err := validateTargetAttestation(value); err != nil {
		return TargetAttestation{}, err
	}
	return value, nil
}

func validateTargetAttestation(value TargetAttestation) error {
	if value.SchemaVersion != 2 || !uuidPattern.MatchString(value.InstallationID) || !sha256Pattern.MatchString(value.ContentHash) {
		return errors.New("target attestation is malformed or unsupported")
	}
	if value.WorkflowCount < 0 || value.DefinitionCount < 0 || value.DraftCount < 0 || value.TriggerCount < 0 || value.RunCount < 0 {
		return errors.New("target attestation contains a negative count")
	}
	return nil
}

func flowgramDocument(value workflow.WorkflowDefinition) (json.RawMessage, error) {
	type position struct {
		X float64 `json:"x"`
		Y float64 `json:"y"`
	}
	type node struct {
		ID       string                           `json:"id"`
		Type     string                           `json:"type"`
		Version  string                           `json:"version"`
		Config   json.RawMessage                  `json:"config"`
		Inputs   map[string]workflow.InputBinding `json:"inputs,omitempty"`
		Position position                         `json:"position"`
	}
	nodes := make([]node, len(value.Nodes))
	for index, item := range value.Nodes {
		nodes[index] = node{ID: item.ID, Type: item.Type, Version: item.Version, Config: item.Config, Inputs: item.Inputs, Position: position{X: float64(index%5) * 280, Y: float64(index/5) * 180}}
	}
	edges := make([]map[string]string, len(value.Edges))
	for index, item := range value.Edges {
		edges[index] = map[string]string{"id": item.ID, "sourceNodeId": item.FromNode, "sourcePortId": item.FromPort, "targetNodeId": item.ToNode, "targetPortId": item.ToPort}
	}
	encoded, err := json.Marshal(map[string]any{"schemaVersion": "flowgram-v1", "workflowId": value.ID, "nodes": nodes, "edges": edges, "metadata": value.Metadata})
	if err != nil {
		return nil, fmt.Errorf("encode migrated FlowGram draft: %w", err)
	}
	return encoded, nil
}

func migrationCompilePolicy() workflow.CompilePolicy {
	return workflow.CompilePolicy{
		AllowedCapabilities: workflow.CapabilitySet{Streaming: true, Cancellable: true, Interruptible: true, Idempotent: true},
		AllowedPermissions:  workflow.PermissionSet{Network: true, Credentials: true, Filesystem: true, Compute: true},
	}
}

func migrationKey(kind string, parts ...string) string {
	digest := sha256.Sum256([]byte(kind + "\x00" + strings.Join(parts, "\x00")))
	return "fenix-" + kind + "-" + hex.EncodeToString(digest[:])
}

var _ Target = (*PostgresTarget)(nil)
