package identity

import (
	"os"
	"strings"
	"testing"
)

// 验证 API key 查询必须同时校验 tenant、service account、membership、过期与撤销状态。
func TestAPIKeySQLKeepsIdentityAndTenantContracts(t *testing.T) {
	t.Parallel()
	for _, contract := range []string{
		"JOIN tenants", "JOIN service_accounts", "JOIN tenant_memberships",
		"k.key_hash = $1", "k.revoked_at IS NULL", "k.expires_at > $2",
		"m.actor_type = 'service_account'", "m.status = 'active'",
	} {
		if !strings.Contains(resolveAPIKeySQL, contract) {
			t.Errorf("API key query lacks %q", contract)
		}
	}
}

// 验证 session 查询每次重新读取服务端 membership 角色并校验租户、身份、过期和撤销。
func TestSessionSQLKeepsIdentityAndTenantContracts(t *testing.T) {
	t.Parallel()
	for _, contract := range []string{
		"JOIN tenants", "JOIN tenant_memberships", "JOIN oidc_identities",
		"s.session_hash = $1", "s.revoked_at IS NULL", "s.expires_at > $2",
		"m.actor_type = 'user'", "m.status = 'active'",
	} {
		if !strings.Contains(resolveSessionSQL, contract) {
			t.Errorf("session query lacks %q", contract)
		}
	}
}

// 验证 000008 迁移提供成员、service account、摘要凭据、session 和 OIDC 身份的完整约束。
func TestProductionIdentityMigrationContract(t *testing.T) {
	t.Parallel()
	contents, err := os.ReadFile("../../migrations/000008_production_identity.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(contents)
	for _, contract := range []string{
		"CREATE TABLE service_accounts", "CREATE TABLE oidc_identities", "CREATE TABLE tenant_memberships",
		"CREATE TABLE identity_sessions", "CREATE TABLE oidc_login_attempts", "session_hash bytea NOT NULL UNIQUE",
		"CHECK (octet_length(session_hash) = 32)", "ADD COLUMN service_account_id", "api_keys_service_account_fk",
		"PRIMARY KEY (tenant_id, actor_id)", "role text NOT NULL CHECK",
	} {
		if !strings.Contains(sql, contract) {
			t.Errorf("identity migration lacks %q", contract)
		}
	}
	if strings.Contains(sql, "session_token") || strings.Contains(sql, "raw_key") {
		t.Fatal("identity migration must never persist raw bearer credentials")
	}
}
