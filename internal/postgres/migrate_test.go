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
	adminConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	adminConfig.ConnConfig.Database = "postgres"
	setup, err := pgxpool.NewWithConfig(ctx, adminConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer setup.Close()
	database := fmt.Sprintf("migrator_history_%d", time.Now().UnixNano())
	identifier := pgx.Identifier{database}.Sanitize()
	if _, err = setup.Exec(ctx, "CREATE DATABASE "+identifier); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, cleanupErr := setup.Exec(context.Background(), "DROP DATABASE IF EXISTS "+identifier+" WITH (FORCE)"); cleanupErr != nil {
			t.Errorf("drop test database: %v", cleanupErr)
		}
	}()
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.Database = database
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

// 验证真实 v25 数据库升级到 v26 后保留业务数据，删除本地 IAM，并建立可执行的组织约束与默认策略。
func TestFenixOrganizationScopeUpgradeFromV25(t *testing.T) {
	databaseURL := os.Getenv("SUPAFLOW_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("SUPAFLOW_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	adminConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	adminConfig.ConnConfig.Database = "postgres"
	admin, err := pgxpool.NewWithConfig(ctx, adminConfig)
	if err != nil {
		t.Fatal(err)
	}
	database := fmt.Sprintf("organization_upgrade_%d", time.Now().UnixNano())
	identifier := pgx.Identifier{database}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE DATABASE "+identifier); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.Database = database
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+identifier+" WITH (FORCE)")
		admin.Close()
	})
	migrations, err := LoadMigrations(os.DirFS("../.."), "migrations")
	if err != nil || len(migrations) != 26 {
		t.Fatalf("migrations=%d err=%v", len(migrations), err)
	}
	migrator := NewMigrator(pool)
	if err = migrator.Up(ctx, migrations[:25]); err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO tenants(id,name) VALUES('org-upgrade','Upgrade');
		INSERT INTO service_accounts(tenant_id,id,name) VALUES('org-upgrade','svc-upgrade','Service');
		INSERT INTO tenant_memberships(tenant_id,actor_id,actor_type,role) VALUES('org-upgrade','svc-upgrade','service_account','admin');
		INSERT INTO api_keys(id,tenant_id,name,key_hash,key_prefix,service_account_id)
		VALUES('key-upgrade','org-upgrade','Key',decode(repeat('11',32),'hex'),'prefix','svc-upgrade');
		INSERT INTO oidc_identities(id,issuer,subject) VALUES('oidc-upgrade','https://issuer.invalid','subject');
		INSERT INTO identity_sessions(id,session_hash,tenant_id,actor_id,expires_at)
		VALUES('session-upgrade',decode(repeat('22',32),'hex'),'org-upgrade','svc-upgrade',now()+interval '1 hour');
		INSERT INTO oidc_login_attempts(state_hash,browser_hash,nonce_hash,pkce_verifier,requested_tenant_id,expires_at)
		VALUES(decode(repeat('33',32),'hex'),decode(repeat('44',32),'hex'),decode(repeat('55',32),'hex'),repeat('v',43),'org-upgrade',now()+interval '1 hour');
		INSERT INTO workflows(tenant_id,id,slug,name,created_by) VALUES('org-upgrade','workflow-upgrade','upgrade','Upgrade','actor');
		INSERT INTO workflow_versions(id,tenant_id,workflow_id,version,publication_key,definition,definition_hash,published_by)
		VALUES('version-upgrade','org-upgrade','workflow-upgrade',1,'publication-upgrade','{}',decode(repeat('66',32),'hex'),'actor');
		INSERT INTO execution_plans(id,tenant_id,workflow_id,workflow_version_id,plan,plan_hash,compiler_version)
		VALUES('plan-upgrade','org-upgrade','workflow-upgrade','version-upgrade','{}',decode(repeat('77',32),'hex'),'test');
		INSERT INTO runs(id,tenant_id,workflow_id,workflow_version_id,execution_plan_id,idempotency_key,request_hash,input,plan_hash)
		VALUES('run-upgrade','org-upgrade','workflow-upgrade','version-upgrade','plan-upgrade','run-key',decode(repeat('88',32),'hex'),'{}',repeat('7',64));
		INSERT INTO run_events(id,tenant_id,run_id,sequence,event_type,payload)
		VALUES('event-upgrade','org-upgrade','run-upgrade',1,'run.created','{}');
		INSERT INTO credential_refs(tenant_id,id,name,provider,purpose,created_by)
		VALUES('org-upgrade','credential-upgrade','Credential','local','runtime','actor');
		INSERT INTO triggers(tenant_id,id,public_hash,workflow_id,kind,name,created_by)
		VALUES('org-upgrade','trigger-upgrade','public-hash-upgrade','workflow-upgrade','api','Trigger','actor');
		UPDATE tenant_quotas SET max_active_runs=7 WHERE tenant_id='org-upgrade';
		UPDATE retention_policies SET output_days=9 WHERE tenant_id='org-upgrade';`)
	if err != nil {
		t.Fatal(err)
	}
	if err = migrator.Up(ctx, migrations); err != nil {
		t.Fatal(err)
	}
	var workflowOrganization, runOrganization, eventOrganization, credentialOrganization, triggerOrganization string
	if err = pool.QueryRow(ctx, `SELECT w.organization_id,r.organization_id,e.organization_id,c.organization_id,t.organization_id
		FROM workflows w JOIN runs r ON r.organization_id=w.organization_id
		JOIN run_events e ON e.organization_id=r.organization_id AND e.run_id=r.id
		JOIN credential_refs c ON c.organization_id=w.organization_id
		JOIN triggers t ON t.organization_id=w.organization_id
		WHERE w.id='workflow-upgrade'`).Scan(&workflowOrganization, &runOrganization, &eventOrganization, &credentialOrganization, &triggerOrganization); err != nil {
		t.Fatal(err)
	}
	if workflowOrganization != "org-upgrade" || runOrganization != workflowOrganization || eventOrganization != workflowOrganization || credentialOrganization != workflowOrganization || triggerOrganization != workflowOrganization {
		t.Fatalf("organization values were not preserved: %q %q %q %q %q", workflowOrganization, runOrganization, eventOrganization, credentialOrganization, triggerOrganization)
	}
	var iamTables, tenantColumns, maxRuns, outputDays int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tablename=ANY($1)`, []string{"tenants", "api_keys", "service_accounts", "tenant_memberships", "oidc_identities", "identity_sessions", "oidc_login_attempts"}).Scan(&iamTables); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND column_name='tenant_id'`).Scan(&tenantColumns); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT q.max_active_runs,p.output_days FROM organization_quotas q JOIN retention_policies p USING(organization_id) WHERE organization_id='org-upgrade'`).Scan(&maxRuns, &outputDays); err != nil {
		t.Fatal(err)
	}
	if iamTables != 0 || tenantColumns != 0 || maxRuns != 7 || outputDays != 9 {
		t.Fatalf("iam=%d tenantColumns=%d quota=%d retention=%d", iamTables, tenantColumns, maxRuns, outputDays)
	}
	var compositeFKs, scopedIndexes int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM pg_constraint WHERE connamespace='public'::regnamespace AND contype='f' AND pg_get_constraintdef(oid) LIKE '%organization_id%'`).Scan(&compositeFKs); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM pg_indexes WHERE schemaname='public' AND indexdef LIKE '%organization_id%'`).Scan(&scopedIndexes); err != nil {
		t.Fatal(err)
	}
	if compositeFKs == 0 || scopedIndexes == 0 {
		t.Fatalf("compositeFKs=%d scopedIndexes=%d", compositeFKs, scopedIndexes)
	}
	var available int
	if err = pool.QueryRow(ctx, `SELECT GREATEST(0,max_pending_trigger_firings)::integer FROM organization_quotas WHERE organization_id='org-upgrade'`).Scan(&available); err != nil || available <= 0 {
		t.Fatalf("operational function inputs unavailable: %d %v", available, err)
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

// 验证 v26 是唯一收敛当前 schema 的前向迁移，并显式删除本地 IAM、重命名组织作用域与业务配额表。
func TestFenixOrganizationScopeMigrationContract(t *testing.T) {
	t.Parallel()
	contents, err := os.ReadFile("../../migrations/000026_fenix_organization_scope.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(contents)
	for _, contract := range []string{
		"DROP TABLE identity_sessions", "DROP TABLE oidc_login_attempts", "DROP TABLE api_keys",
		"DROP TABLE tenant_memberships", "DROP TABLE service_accounts", "DROP TABLE oidc_identities", "DROP TABLE tenants",
		"RENAME COLUMN tenant_id TO organization_id", "tenant_quotas RENAME TO organization_quotas",
		"tenant_quota_counters RENAME TO organization_quota_counters", "tenant_exports RENAME TO organization_exports",
		"trigger_schedule_tenant_state RENAME TO trigger_schedule_organization_state",
	} {
		if !strings.Contains(sql, contract) {
			t.Errorf("organization scope migration missing %q", contract)
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
