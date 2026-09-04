package postgres

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// 验证迁移按版本排序并为内容生成稳定校验和。
func TestLoadMigrationsOrdersAndChecksums(t *testing.T) {
	t.Parallel()
	files := fstest.MapFS{
		"migrations/000002_second.sql": {Data: []byte("SELECT 2;")},
		"migrations/000001_first.sql":  {Data: []byte("SELECT 1;")},
	}
	migrations, err := LoadMigrations(files, "migrations")
	if err != nil {
		t.Fatalf("LoadMigrations() error = %v", err)
	}
	if len(migrations) != 2 || migrations[0].Version != 1 || migrations[1].Version != 2 {
		t.Fatalf("unexpected migration order: %#v", migrations)
	}
	expected := sha256.Sum256([]byte("SELECT 1;"))
	if migrations[0].Checksum != expected {
		t.Fatalf("checksum = %x, want %x", migrations[0].Checksum, expected)
	}
}

// 验证非法或重复版本在接触数据库前即被拒绝。
func TestLoadMigrationsRejectsInvalidHistory(t *testing.T) {
	t.Parallel()
	cases := map[string]fstest.MapFS{
		"invalid name": {
			"migrations/down.sql": {Data: []byte("DROP TABLE runs;")},
		},
		"duplicate version": {
			"migrations/000001_one.sql": {Data: []byte("SELECT 1;")},
			"migrations/000001_two.sql": {Data: []byte("SELECT 2;")},
		},
		"empty migration": {
			"migrations/000001_empty.sql": {Data: []byte(" \n")},
		},
		"version gap": {
			"migrations/000001_one.sql":   {Data: []byte("SELECT 1;")},
			"migrations/000003_three.sql": {Data: []byte("SELECT 3;")},
		},
	}
	for name, files := range cases {
		files := files
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := LoadMigrations(files, "migrations"); err == nil {
				t.Fatal("LoadMigrations() error = nil")
			}
		})
	}
}

// 验证数据库已应用的任一迁移文件从发布镜像消失时启动立即失败。
func TestMigratorRejectsDeletedAppliedMigration(t *testing.T) {
	databaseURL := os.Getenv("SUPAFLOW_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("SUPAFLOW_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	setup, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer setup.Close()
	schema := fmt.Sprintf("migrator_history_%d", time.Now().UnixNano())
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err = setup.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, cleanupErr := setup.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+identifier+" CASCADE"); cleanupErr != nil {
			t.Errorf("drop test schema: %v", cleanupErr)
		}
	}()
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema + ",public"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	migrations, err := LoadMigrations(os.DirFS("../.."), "migrations")
	if err != nil {
		t.Fatal(err)
	}
	migrator := NewMigrator(pool)
	if err = migrator.Up(ctx, migrations); err != nil {
		t.Fatal(err)
	}
	var schemaVersion int
	var installationID, contentHash string
	var definitionCount, draftCount, triggerCount, runCount int
	if err = pool.QueryRow(ctx, `SELECT schema_version,installation_id,content_hash,
		definition_count,draft_count,trigger_count,run_count
		FROM fenix_target_attestation_v1()`).Scan(
		&schemaVersion, &installationID, &contentHash,
		&definitionCount, &draftCount, &triggerCount, &runCount,
	); err != nil {
		t.Fatalf("query Fenix target attestation after full migration: %v", err)
	}
	if schemaVersion != 1 || installationID == "" || len(contentHash) != 64 ||
		definitionCount != 0 || draftCount != 0 || triggerCount != 0 || runCount != 0 {
		t.Fatalf("unexpected empty target attestation: %d %q %q %d %d %d %d",
			schemaVersion, installationID, contentHash,
			definitionCount, draftCount, triggerCount, runCount)
	}
	if len(migrations) < 2 {
		t.Fatal("test requires at least two migrations")
	}
	err = migrator.Up(ctx, migrations[:len(migrations)-1])
	if err == nil || !strings.Contains(err.Error(), "missing from source") {
		t.Fatalf("deleted applied migration error = %v", err)
	}
}

// 验证生产迁移覆盖持久运行所需表及关键数据库约束。
func TestProductionMigrationsContainDurabilityConstraints(t *testing.T) {
	t.Parallel()
	migrations, err := LoadMigrations(os.DirFS("../.."), "migrations")
	if err != nil {
		t.Fatalf("LoadMigrations() error = %v", err)
	}
	var combined strings.Builder
	for _, migration := range migrations {
		combined.WriteString(migration.SQL)
	}
	sql := combined.String()
	for _, table := range []string{
		"tenants", "api_keys", "workflows", "workflow_drafts", "workflow_versions",
		"node_types", "node_type_versions",
		"execution_plans", "runs", "node_states", "node_attempts", "worker_leases",
		"run_events", "run_snapshots", "node_outputs", "interrupts",
		"approval_decisions", "audit_records", "outbox_messages",
		"managed_objects", "object_upload_intents",
		"dag_joins", "dag_edge_inputs", "subworkflow_links",
		"definition_mutations", "run_rerun_lineage",
		"outbound_webhook_subscriptions", "outbound_webhook_receipts",
		"webhook_resumptions", "attempt_stream_state", "attempt_stream_chunks",
		"platform_installations",
		"workflow_imports", "fenix_cutover_seals",
	} {
		if !strings.Contains(sql, "CREATE TABLE "+table) {
			t.Errorf("missing table %s", table)
		}
	}
	for _, contract := range []string{
		"PRIMARY KEY (tenant_id, id)",
		"UNIQUE (tenant_id, idempotency_key)",
		"UNIQUE (tenant_id, run_id, sequence)",
		"response_snapshot jsonb NOT NULL",
		"CHECK ((inline_value IS NULL) <> (object_ref IS NULL))",
		"workflow_versions_immutable",
		"execution_plans_immutable",
		"audit_records_append_only",
		"ADD COLUMN output_id text",
		"ADD CONSTRAINT runs_output_fk",
		"FOREIGN KEY (tenant_id, output_id)",
		"ADD COLUMN deadline_at timestamptz", "runs_deadline_due_idx",
		"fenix_target_attestation_v1", "platform_installations_immutable",
		"JOIN node_states output_state", "'planHash', run.plan_hash",
		"fenix_target_attestation_v2", "'nodeAttempts'", "acquire_fenix_cutover_seal_v2",
	} {
		if !strings.Contains(sql, contract) {
			t.Errorf("missing SQL contract %q", contract)
		}
	}
}

// 验证逻辑执行身份迁移回填历史 attempt，并将子工作流幂等关系从物理 retry 提升到逻辑执行。
func TestLogicalExecutionIdentityMigrationBackfillsAndConstrainsLineage(t *testing.T) {
	t.Parallel()
	contents, err := os.ReadFile("../../migrations/000019_logical_execution_identity.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(contents)
	for _, contract := range []string{
		"UPDATE node_attempts SET execution_id = id", "ALTER COLUMN execution_id SET NOT NULL",
		"UPDATE subworkflow_links link", "PRIMARY KEY (tenant_id, parent_execution_id)",
		"default_node_attempt_execution_id", "default_subworkflow_parent_execution_id",
	} {
		if !strings.Contains(sql, contract) {
			t.Errorf("logical execution migration missing %q", contract)
		}
	}
}

// 验证节点注册表迁移建立全局不可变类型/版本模型及完整执行契约。
func TestNodeRegistryMigrationDefinesImmutableAuthority(t *testing.T) {
	t.Parallel()
	contents, err := os.ReadFile("../../migrations/000014_node_registry.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(contents)
	for _, contract := range []string{
		"CREATE TABLE node_types", "CREATE TABLE node_type_versions",
		"config_schema jsonb NOT NULL", "input_ports jsonb NOT NULL", "output_ports jsonb NOT NULL",
		"capabilities jsonb NOT NULL", "permissions jsonb NOT NULL", "executor_binding jsonb NOT NULL",
		"migration_metadata jsonb NOT NULL", "node_types_immutable", "node_type_versions_immutable",
		"BEFORE UPDATE OR DELETE",
	} {
		if !strings.Contains(sql, contract) {
			t.Errorf("node registry migration lacks %q", contract)
		}
	}
}
