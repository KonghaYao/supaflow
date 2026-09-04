package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/platform"
	"github.com/pazhou/supaflow/internal/workflow"
)

const nodeRegistrySeedLock = "supaflow-node-registry-seed-v1"

// NodeRegistry is the PostgreSQL authority for immutable node definitions and
// versions. Executable validators remain in process and are verified against
// this projection during process startup.
type NodeRegistry struct {
	pool *pgxpool.Pool
}

// NewNodeRegistry creates a durable node registry adapter.
func NewNodeRegistry(pool *pgxpool.Pool) *NodeRegistry { return &NodeRegistry{pool: pool} }

var _ platform.NodeTypeCatalog = (*NodeRegistry)(nil)

// EnsureRegistry inserts previously unseen adapter definitions and rejects a
// reused NodeType@Version whose immutable contract differs from PostgreSQL.
func (s *NodeRegistry) EnsureRegistry(ctx context.Context, cache *workflow.Registry) error {
	if s == nil || s.pool == nil || cache == nil {
		return errors.New("postgres node registry requires a pool and adapter cache")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin node registry seed: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, nodeRegistrySeedLock); err != nil {
		return fmt.Errorf("lock node registry seed: %w", err)
	}
	for _, entry := range cache.Entries() {
		if err = ensureNodeTypeVersion(ctx, tx, entry); err != nil {
			return err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit node registry seed: %w", err)
	}
	return nil
}

func ensureNodeTypeVersion(ctx context.Context, tx pgx.Tx, entry workflow.NodeTypeVersion) error {
	displayName := strings.NewReplacer("-", " ", ".", " ").Replace(entry.Type)
	if _, err := tx.Exec(ctx, `
		INSERT INTO node_types (type,display_name,description)
		VALUES ($1,$2,'') ON CONFLICT (type) DO NOTHING`, entry.Type, displayName); err != nil {
		return fmt.Errorf("seed node type %s: %w", entry.Type, err)
	}
	var definitionMatches bool
	if err := tx.QueryRow(ctx, `
		SELECT display_name=$2 AND description='' FROM node_types WHERE type=$1`,
		entry.Type, displayName).Scan(&definitionMatches); err != nil {
		return fmt.Errorf("verify node type %s: %w", entry.Type, err)
	}
	if !definitionMatches {
		return fmt.Errorf("node registry definition drift for %s", entry.Type)
	}
	values := []struct {
		name  string
		value any
	}{
		{"capabilities", entry.Capabilities}, {"permissions", entry.Permissions},
		{"executor binding", entry.Executor}, {"input ports", nonNilPorts(entry.Inputs)},
		{"output ports", nonNilPorts(entry.Outputs)}, {"sensitive fields", nonNilStrings(entry.SensitiveFields)},
		{"migration metadata", nonNilMigrations(entry.Migrations)},
	}
	encoded := make([]string, len(values))
	for index, value := range values {
		contents, encodeErr := json.Marshal(value.value)
		if encodeErr != nil {
			return fmt.Errorf("encode node %s: %w", value.name, encodeErr)
		}
		encoded[index] = string(contents)
	}
	arguments := []any{
		entry.Type, entry.Version, string(entry.ConfigSchema), encoded[3], encoded[4],
		encoded[0], encoded[1], encoded[2], encoded[5], entry.DefaultTimeoutMS,
		entry.MaximumOutputBytes, encoded[6],
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO node_type_versions (
			node_type,version,config_schema,input_ports,output_ports,capabilities,
			permissions,executor_binding,sensitive_fields,default_timeout_ms,
			maximum_output_bytes,migration_metadata
		) VALUES ($1,$2,$3::jsonb,$4::jsonb,$5::jsonb,$6::jsonb,$7::jsonb,$8::jsonb,$9::jsonb,$10,$11,$12::jsonb)
		ON CONFLICT (node_type,version) DO NOTHING`, arguments...); err != nil {
		return fmt.Errorf("seed node version %s@%s: %w", entry.Type, entry.Version, err)
	}
	var matches bool
	err := tx.QueryRow(ctx, `
		SELECT config_schema=$3::jsonb AND input_ports=$4::jsonb AND output_ports=$5::jsonb
		   AND capabilities=$6::jsonb AND permissions=$7::jsonb AND executor_binding=$8::jsonb
		   AND sensitive_fields=$9::jsonb AND default_timeout_ms=$10
		   AND maximum_output_bytes=$11 AND migration_metadata=$12::jsonb
		FROM node_type_versions WHERE node_type=$1 AND version=$2`, arguments...).Scan(&matches)
	if err != nil {
		return fmt.Errorf("verify node version %s@%s: %w", entry.Type, entry.Version, err)
	}
	if !matches {
		return fmt.Errorf("node registry contract drift for %s@%s", entry.Type, entry.Version)
	}
	return nil
}

// ListNodeTypes returns the ordered PostgreSQL projection used by Studio and
// startup compatibility checks. It never attaches executable validators.
func (s *NodeRegistry) ListNodeTypes(ctx context.Context) ([]workflow.NodeTypeVersion, error) {
	if s == nil || s.pool == nil {
		return nil, errors.New("postgres node registry requires a pool")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT node_type,version,capabilities,default_timeout_ms,maximum_output_bytes,
		       permissions,executor_binding,input_ports,output_ports,config_schema,
		       sensitive_fields,migration_metadata
		FROM node_type_versions ORDER BY node_type,version`)
	if err != nil {
		return nil, fmt.Errorf("list node types: %w", err)
	}
	defer rows.Close()
	entries := make([]workflow.NodeTypeVersion, 0)
	for rows.Next() {
		entry, scanErr := scanNodeTypeVersion(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		entries = append(entries, entry)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate node types: %w", err)
	}
	return entries, nil
}

type nodeTypeScanner interface {
	Scan(...any) error
}

func scanNodeTypeVersion(row nodeTypeScanner) (workflow.NodeTypeVersion, error) {
	var entry workflow.NodeTypeVersion
	var capabilities, permissions, executorBinding, inputs, outputs, sensitiveFields, migrations []byte
	if err := row.Scan(&entry.Type, &entry.Version, &capabilities, &entry.DefaultTimeoutMS,
		&entry.MaximumOutputBytes, &permissions, &executorBinding, &inputs, &outputs,
		&entry.ConfigSchema, &sensitiveFields, &migrations); err != nil {
		return workflow.NodeTypeVersion{}, fmt.Errorf("scan node type version: %w", err)
	}
	for _, target := range []struct {
		name  string
		data  []byte
		value any
	}{
		{"capabilities", capabilities, &entry.Capabilities}, {"permissions", permissions, &entry.Permissions},
		{"executor binding", executorBinding, &entry.Executor}, {"input ports", inputs, &entry.Inputs},
		{"output ports", outputs, &entry.Outputs}, {"sensitive fields", sensitiveFields, &entry.SensitiveFields},
		{"migration metadata", migrations, &entry.Migrations},
	} {
		if err := json.Unmarshal(target.data, target.value); err != nil {
			return workflow.NodeTypeVersion{}, fmt.Errorf("decode %s for %s@%s: %w", target.name, entry.Type, entry.Version, err)
		}
	}
	entry.ValidateConfig = func(json.RawMessage) error { return nil }
	if _, err := workflow.NewRegistry(entry); err != nil {
		return workflow.NodeTypeVersion{}, fmt.Errorf("validate stored node version %s@%s: %w", entry.Type, entry.Version, err)
	}
	entry.ValidateConfig = nil
	return entry, nil
}

func nonNilPorts(value map[string]workflow.PortDefinition) map[string]workflow.PortDefinition {
	if value == nil {
		return map[string]workflow.PortDefinition{}
	}
	return value
}

func nonNilStrings(value []string) []string {
	if value == nil {
		return []string{}
	}
	return value
}

func nonNilMigrations(value []workflow.NodeMigrationMetadata) []workflow.NodeMigrationMetadata {
	if value == nil {
		return []workflow.NodeMigrationMetadata{}
	}
	return value
}
