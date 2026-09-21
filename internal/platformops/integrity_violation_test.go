package platformops

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/postgres"
)

// 验证两条「调用方自己的值造成完整性冲突」的 SQLSTATE 被归一到稳定的服务错误，其余故障不被
// 误归类：23505（唯一键已被占用）是调用方改名即可修正的冲突，23503（引用了不存在的父行）是
// 换一个存在的 id 即可修正的参数错误。两者此前都以原始 PgError 冒到 HTTP 面被报成 500，
// 而 23514（组织配额未配置）这类服务端状态必须继续留在 500 一侧。
func TestIntegrityViolationErrorMatchesOnlyCallerCausedViolations(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want error
	}{
		{name: "唯一键冲突 23505", err: &pgconn.PgError{Code: "23505", Message: `duplicate key value violates unique constraint "triggers_organization_id_workflow_id_name_key"`}, want: ErrConflict},
		{name: "被包装的唯一键冲突", err: fmt.Errorf("create trigger: %w", &pgconn.PgError{Code: "23505"}), want: ErrConflict},
		{name: "外键指向不存在的父行 23503", err: &pgconn.PgError{Code: "23503", Message: `insert or update on table "triggers" violates foreign key constraint "triggers_organization_id_workflow_id_fkey"`}, want: ErrInvalidArgument},
		{name: "被包装的外键冲突", err: fmt.Errorf("create trigger: %w", &pgconn.PgError{Code: "23503"}), want: ErrInvalidArgument},
		{name: "检查约束 23514 不是调用方的输入冲突", err: &pgconn.PgError{Code: "23514", Message: "organization quota is not configured"}, want: nil},
		{name: "容量超限 53400 由各自的门禁单独归类", err: &pgconn.PgError{Code: "53400"}, want: nil},
		{name: "非 Postgres 错误", err: errors.New("connection reset by peer"), want: nil},
		{name: "nil", err: nil, want: nil},
	}
	for _, testCase := range cases {
		if got := integrityViolationError(testCase.err); got != testCase.want {
			t.Fatalf("%s got=%v want=%v", testCase.name, got, testCase.want)
		}
	}
}

// 验证 triggers 上的两条调用方输入冲突被翻译成稳定错误：同名重复创建（UNIQUE (organization_id,
// workflow_id, name)，SQLSTATE 23505）此前返回 500 internal_error，调用方看不出重名；workflowId
// 指向不存在的 workflow（外键 23503）同样返回 500，而它只需要改一个字段。改名、换 workflow、
// 以及同名但在另一个 workflow 下创建都必须照常成功。
func TestPostgresTriggerCreateClassifiesCallerConflicts(t *testing.T) {
	ctx, pool := integrityViolationPool(t)
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	organizationID := "integrity_trigger_" + suffix
	workflowID, otherWorkflowID := "integrity_flow_"+suffix, "integrity_flow_other_"+suffix
	if err := seedPlatformOrganization(ctx, pool, organizationID, workflowID); err != nil {
		t.Fatal(err)
	}
	if err := seedPlatformOrganization(ctx, pool, organizationID, otherWorkflowID); err != nil {
		t.Fatal(err)
	}
	store := NewPostgresTriggerStore(pool)
	create := func(workflow, name string) error {
		_, err := store.CreateTrigger(ctx, CreateTriggerRequest{OrganizationID: organizationID,
			ActorID: "operator", WorkflowID: workflow, Kind: TriggerAPI, Name: name})
		return err
	}
	if err := create(workflowID, "nightly"); err != nil {
		t.Fatalf("first create: %v", err)
	}
	if err := create(workflowID, "nightly"); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate name err=%v want ErrConflict", err)
	}
	if err := create("integrity_missing_"+suffix, "nightly"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("missing workflow err=%v want ErrInvalidArgument", err)
	}
	if err := create(otherWorkflowID, "nightly"); err != nil {
		t.Fatalf("same name under another workflow: %v", err)
	}
}

// 验证 outbound_webhook_subscriptions 上的 UNIQUE (organization_id, name) 同样被翻译成
// ErrConflict：同名重复创建此前返回 500，调用方无法从响应里看出是自己重名；换名字必须成功，
// 而 signingCredentialId 指向不存在或未授权的凭据仍走更精确的 ErrCredentialForbidden（403），
// 不被这条翻译吞掉。
func TestPostgresWebhookSubscriptionCreateClassifiesDuplicateName(t *testing.T) {
	ctx, pool := integrityViolationPool(t)
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	organizationID := "integrity_webhook_" + suffix
	if err := seedPlatformOrganization(ctx, pool, organizationID, "integrity_webhook_flow_"+suffix); err != nil {
		t.Fatal(err)
	}
	provider, err := NewLocalAESProvider("integrity-key", []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := NewCredentialService(NewPostgresCredentialStore(pool), provider)
	if err != nil {
		t.Fatal(err)
	}
	credential, err := credentials.Create(ctx, CreateCredentialRequest{OrganizationID: organizationID,
		ActorID: "operator", Name: "signer-" + suffix, Purpose: "outbound_webhook_signing", Secret: []byte("signing-material")})
	if err != nil {
		t.Fatal(err)
	}
	repository := NewOutboundWebhookRepository(pool)
	request := CreateOutboundWebhookRequest{OrganizationID: organizationID, ActorID: "operator",
		Name: "sink-" + suffix, EndpointURL: "https://events.example.test/hook",
		EventTypes: []string{"run.succeeded"}, SigningCredentialID: credential.ID, Timeout: time.Second}
	if _, err = repository.Create(ctx, request); err != nil {
		t.Fatalf("first create: %v", err)
	}
	if _, err = repository.Create(ctx, request); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate name err=%v want ErrConflict", err)
	}
	request.Name = "sink-other-" + suffix
	if _, err = repository.Create(ctx, request); err != nil {
		t.Fatalf("distinct name: %v", err)
	}
	request.Name = "sink-forbidden-" + suffix
	request.SigningCredentialID = "cred_missing_" + suffix
	if _, err = repository.Create(ctx, request); !errors.Is(err, ErrCredentialForbidden) {
		t.Fatalf("missing credential err=%v want ErrCredentialForbidden", err)
	}
}

// 集成测试用一次性数据库，迁移到最新后由调用方为每个用例生成唯一 organization。
func integrityViolationPool(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	databaseURL := os.Getenv("SUPAFLOW_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("SUPAFLOW_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	migrations, err := postgres.LoadMigrations(os.DirFS("../.."), "migrations")
	if err != nil {
		t.Fatalf("migrate integrity violation database: %v", err)
	}
	if err = postgres.NewMigrator(pool).Up(ctx, migrations); err != nil {
		t.Fatalf("migrate integrity violation database: %v", err)
	}
	return ctx, pool
}
