package platformops

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/postgres"
)

// 验证同一组织下重名凭据创建被翻译成 ErrConflict：credential_refs 只有 UNIQUE
// (organization_id, name)（实测约束名 credential_refs_organization_id_name_key，SQLSTATE
// 23505），名字由调用方给定，重名是他改名即可修正的冲突。此前 23505 以普通存储错误冒到
// HTTP 面，调用方拿到 500 而看不出重名；不同名字则必须照常创建成功。
func TestPostgresCredentialCreateRejectsDuplicateName(t *testing.T) {
	databaseURL := os.Getenv("SUPAFLOW_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("SUPAFLOW_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	migrations, err := postgres.LoadMigrations(os.DirFS("../.."), "migrations")
	if err != nil {
		t.Fatal(err)
	}
	if err = postgres.NewMigrator(pool).Up(ctx, migrations); err != nil {
		t.Fatal(err)
	}
	provider, err := NewLocalAESProvider("credential-conflict-key", bytes.Repeat([]byte{5}, 32))
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := NewCredentialService(NewPostgresCredentialStore(pool), provider)
	if err != nil {
		t.Fatal(err)
	}
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	organizationID, name := "cred_conflict_"+suffix, "signer-"+suffix
	if err = seedPlatformOrganization(ctx, pool, organizationID, "workflow_"+suffix); err != nil {
		t.Fatal(err)
	}
	create := func(credentialName string) (Credential, error) {
		return credentials.Create(ctx, CreateCredentialRequest{
			OrganizationID: organizationID, ActorID: "admin", Name: credentialName,
			Purpose: "webhook_signing", Secret: []byte("secret-value"),
		})
	}
	if _, err = create(name); err != nil {
		t.Fatalf("first create: %v", err)
	}
	if _, err = create(name); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate name err=%v want ErrConflict", err)
	}
	if _, err = create(name + "-other"); err != nil {
		t.Fatalf("distinct name: %v", err)
	}
}
