package platformops

import (
	"os"
	"strings"
	"testing"
)

// 验证 Phase 3 迁移包含租户隔离、幂等、防重放、配额和运维持久化约束。
func TestPlatformOpsMigrationContracts(t *testing.T) {
	contents, err := os.ReadFile("../../migrations/000006_platform_ops.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(contents)
	required := []string{
		"CREATE TABLE triggers", "PRIMARY KEY (tenant_id, id)", "UNIQUE (public_hash)",
		"char_length(public_hash) BETWEEN 16 AND 128", "CREATE TABLE webhook_replays",
		"PRIMARY KEY (tenant_id, trigger_id, request_id)", "CREATE TABLE credential_versions",
		"CREATE TABLE tenant_quota_counters", "CHECK (active_runs >= 0)",
		"CREATE TABLE outbox_dead_letters", "CREATE TABLE retention_policies",
		"CREATE TABLE worker_registrations",
	}
	for _, fragment := range required {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("platform migration missing %q", fragment)
		}
	}
}

// 验证生产查询明确使用租户谓词、跳锁与 topic 所有权过滤。
func TestPlatformOpsSQLCoordinationContracts(t *testing.T) {
	files := []string{"trigger_postgres.go", "outbox.go", "admission.go", "credential_postgres.go", "audit.go", "retention.go"}
	var source strings.Builder
	for _, file := range files {
		contents, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		source.Write(contents)
	}
	text := source.String()
	for _, fragment := range []string{"tenant_id=$1", "FOR UPDATE SKIP LOCKED", "topic = ANY($4::text[])", "ON CONFLICT", "insertAuditTx"} {
		if !strings.Contains(text, fragment) {
			t.Fatalf("platform SQL coordination missing %q", fragment)
		}
	}
}

// 验证 outbound webhook 迁移提供租户隔离、稳定幂等收据和订阅生命周期约束。
func TestOutboundWebhookMigrationContracts(t *testing.T) {
	contents, err := os.ReadFile("../../migrations/000011_outbound_webhooks.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(contents)
	for _, fragment := range []string{
		"CREATE TABLE outbound_webhook_subscriptions", "PRIMARY KEY (tenant_id, id)",
		"FOREIGN KEY (tenant_id, signing_credential_id)", "CREATE TABLE outbound_webhook_receipts",
		"PRIMARY KEY (tenant_id, subscription_id, message_id)", "UNIQUE (tenant_id, request_id)",
		"status IN ('pending', 'delivered')", "WHERE delivered_at IS NULL",
	} {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("outbound webhook migration missing %q", fragment)
		}
	}
}

// 验证 Trigger lifecycle 使用租户内幂等主键保存请求指纹和安全响应，以支持重放与冲突拒绝。
func TestTriggerLifecycleMigrationContracts(t *testing.T) {
	contents, err := os.ReadFile("../../migrations/000012_trigger_lifecycle.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(contents)
	for _, fragment := range []string{
		"CREATE TABLE trigger_mutations", "PRIMARY KEY (tenant_id, idempotency_key)",
		"request_hash bytea NOT NULL", "trigger_id text NOT NULL", "result jsonb",
		"operation IN ('create', 'update', 'state', 'delete')",
	} {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("trigger lifecycle migration missing %q", fragment)
		}
	}
}

// 验证已接受 firing 固化不可变执行目标，且删除 trigger 不级联删除历史命令。
func TestImmutableFiringTargetMigrationContract(t *testing.T) {
	contents, err := os.ReadFile("../../migrations/000022_firing_immutable_plan.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(contents)
	for _, fragment := range []string{
		"DROP CONSTRAINT trigger_firings_tenant_id_trigger_id_fkey",
		"ADD COLUMN workflow_version_id", "ADD COLUMN execution_plan_id", "ADD COLUMN plan_hash",
		"trigger_firings_workflow_version_fkey", "ON DELETE RESTRICT", "trigger_firings_bound_plan_check",
	} {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("immutable firing migration missing %q", fragment)
		}
	}
}

// 验证 Trigger lifecycle 的所有修改和查询都带 tenant 与乐观版本谓词。
func TestTriggerLifecycleSQLUsesTenantAndOptimisticConcurrency(t *testing.T) {
	contents, err := os.ReadFile("trigger_lifecycle_postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(contents)
	for _, fragment := range []string{
		"WHERE tenant_id=$1", "updated_at=$3", "ON CONFLICT (tenant_id,idempotency_key)",
		"subtle.ConstantTimeCompare", "DELETE FROM triggers WHERE tenant_id=$1",
	} {
		if !strings.Contains(source, fragment) {
			t.Fatalf("trigger lifecycle SQL missing %q", fragment)
		}
	}
}

// 验证 durability migration 同时定义 mutation lease fencing 与 inline output retention tombstone。
func TestDurabilityRetentionMigrationDefinesLeaseAndOutputLifecycle(t *testing.T) {
	contents, err := os.ReadFile("../../migrations/000016_durability_retention.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(contents)
	for _, required := range []string{
		"operation_id text", "lease_owner text", "lease_expires_at timestamptz",
		"ops_mutation_pending_lease", "WHERE status = 'pending'",
		"retention_until timestamptz", "retention_status text", "archived_at timestamptz",
		"deleted_at timestamptz", "node_outputs_payload_retention_check",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("migration missing %q", required)
		}
	}
}

// 验证每个迁移后新增租户都由数据库事务自动获得默认 retention 策略。
func TestDefaultRetentionPolicyMigrationCoversFutureTenants(t *testing.T) {
	contents, err := os.ReadFile("../../migrations/000018_default_retention_policy.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(contents)
	for _, required := range []string{
		"CREATE TRIGGER tenants_default_retention_policy", "AFTER INSERT ON tenants",
		"provision_default_retention_policy", "ON CONFLICT (tenant_id) DO NOTHING",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("default retention migration missing %q", required)
		}
	}
}

// 验证 trigger capacity 迁移在数据库边界提供租户硬上限、公平游标、失败释放与保留索引。
func TestTriggerCapacityMigrationDefinesHardGuardsAndRetention(t *testing.T) {
	contents, err := os.ReadFile("../../migrations/000023_trigger_capacity_retention.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(contents)
	for _, required := range []string{
		"max_triggers", "max_pending_trigger_firings", "max_pending_outbox_messages",
		"CREATE TABLE trigger_schedule_tenant_state",
		"triggers_capacity_guard", "trigger_firings_capacity_guard", "outbox_messages_capacity_guard",
		"USING ERRCODE = '53400'", "outbox_terminal_trigger_firing",
		"trigger_firings_completed_retention_idx", "outbox_messages_delivered_retention_idx",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("trigger capacity migration missing %q", required)
		}
	}
}

// 验证对象上传迁移持久化 owner lease，并提供可恢复的删除中状态与有界索引。
func TestObjectUploadIntentMigrationDefinesFencingAndDeletionRecovery(t *testing.T) {
	contents, err := os.ReadFile("../../migrations/000020_object_upload_intents.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(contents)
	for _, required := range []string{
		"CREATE TABLE object_upload_intents", "owner_token text NOT NULL",
		"lease_expires_at timestamptz NOT NULL", "UNIQUE (tenant_id, object_key)",
		"object_upload_intents_recovery_idx", "'deleting'", "managed_objects_deleting_idx",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("object upload migration missing %q", required)
		}
	}
}

// 验证上传 recovery 的过期判断只能使用 PostgreSQL now()，不能注入控制面时钟。
func TestObjectUploadRecoveryUsesDatabaseClockContract(t *testing.T) {
	contents, err := os.ReadFile("object_upload.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(contents)
	if !strings.Contains(source, "lease_expires_at <= now()") {
		t.Fatal("object upload recovery does not use the database clock")
	}
	if strings.Contains(source, "lease_expires_at <= $2") || strings.Contains(source, "tenantID string, now time.Time") {
		t.Fatal("object upload recovery accepts an application-clock fencing decision")
	}
}
