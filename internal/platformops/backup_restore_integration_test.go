package platformops

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const restoreIntegrationDatabaseEnv = "SUPAFLOW_RESTORE_TEST_ADMIN_DATABASE_URL"

// 验证真实 PostgreSQL 工具链成功恢复、拒绝错误身份，并在归档 SQL 失败时整体回滚。
func TestExecCommandRunnerRealPostgresRestoreDrill(t *testing.T) {
	adminURL := strings.TrimSpace(os.Getenv(restoreIntegrationDatabaseEnv))
	if adminURL == "" {
		t.Skip(restoreIntegrationDatabaseEnv + " is not set; real pg_dump/pg_restore/psql drill is opt-in")
	}
	for _, tool := range []string{"pg_dump", "pg_restore", "psql"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s is set but required PostgreSQL client %q is unavailable in PATH", restoreIntegrationDatabaseEnv, tool)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("connect restore drill admin database: %v", err)
	}
	defer admin.Close(ctx)

	suffix := fmt.Sprintf("%d", time.Now().UTC().UnixNano())
	databaseNames := []string{
		"supaflow_restore_source_" + suffix,
		"supaflow_restore_success_" + suffix,
		"supaflow_restore_rollback_" + suffix,
	}
	created := make([]string, 0, len(databaseNames))
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		for index := len(created) - 1; index >= 0; index-- {
			name := created[index]
			_, _ = admin.Exec(cleanupCtx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
				WHERE datname=$1 AND pid<>pg_backend_pid()`, name)
			if _, dropErr := admin.Exec(cleanupCtx, "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()); dropErr != nil {
				t.Errorf("drop disposable restore database %q: %v", name, dropErr)
			}
		}
	}()
	for _, name := range databaseNames {
		if _, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()+" TEMPLATE template0"); err != nil {
			t.Fatalf("create disposable restore database %q: %v", name, err)
		}
		created = append(created, name)
	}
	sourceURL := restoreIntegrationDatabaseURL(t, adminURL, databaseNames[0])
	successURL := restoreIntegrationDatabaseURL(t, adminURL, databaseNames[1])
	rollbackURL := restoreIntegrationDatabaseURL(t, adminURL, databaseNames[2])

	source := restoreIntegrationConnect(t, ctx, sourceURL)
	restoreIntegrationExec(t, ctx, source, `CREATE TABLE a_restored_before (value text NOT NULL)`)
	restoreIntegrationExec(t, ctx, source, `INSERT INTO a_restored_before (value) VALUES ('restored')`)
	restoreIntegrationExec(t, ctx, source, `CREATE TABLE z_restore_failure (value text NOT NULL)`)
	sourceIdentity := restoreIntegrationIdentity(t, ctx, source)
	source.Close(ctx)

	root := t.TempDir()
	restore, err := NewBackupRestore(root, ExecCommandRunner{})
	if err != nil {
		t.Fatal(err)
	}
	archivePath, err := restore.Backup(ctx, sourceURL, "real-postgres.dump")
	if err != nil {
		t.Fatalf("create real custom-format archive: %v", err)
	}
	digest := testArchiveSHA256(t, archivePath)

	success := restoreIntegrationConnect(t, ctx, successURL)
	successIdentity := restoreIntegrationIdentity(t, ctx, success)
	success.Close(ctx)
	if err = restore.RestoreDrill(ctx, successURL, "real-postgres.dump", digest, successIdentity, sourceIdentity); err != nil {
		t.Fatalf("restore real archive into disposable scratch database: %v", err)
	}
	success = restoreIntegrationConnect(t, ctx, successURL)
	var restoredValue string
	if err = success.QueryRow(ctx, `SELECT value FROM a_restored_before`).Scan(&restoredValue); err != nil || restoredValue != "restored" {
		t.Fatalf("restored value=%q err=%v", restoredValue, err)
	}
	success.Close(ctx)

	err = restore.RestoreDrill(ctx, sourceURL, "real-postgres.dump", digest, successIdentity, sourceIdentity)
	if err == nil || !strings.Contains(err.Error(), "restore target identity") {
		t.Fatalf("identity guard did not reject source database as restore target: %v", err)
	}

	rollback := restoreIntegrationConnect(t, ctx, rollbackURL)
	restoreIntegrationExec(t, ctx, rollback, `CREATE FUNCTION reject_restore_failure() RETURNS event_trigger
		LANGUAGE plpgsql AS $$ BEGIN
		IF tg_tag = 'CREATE TABLE' AND current_query() LIKE '%z_restore_failure%' THEN
			RAISE EXCEPTION 'intentional restore failure';
		END IF;
		END $$`)
	restoreIntegrationExec(t, ctx, rollback, `CREATE EVENT TRIGGER reject_restore_failure
		ON ddl_command_start WHEN TAG IN ('CREATE TABLE') EXECUTE FUNCTION reject_restore_failure()`)
	rollbackIdentity := restoreIntegrationIdentity(t, ctx, rollback)
	rollback.Close(ctx)
	err = restore.RestoreDrill(ctx, rollbackURL, "real-postgres.dump", digest, rollbackIdentity, sourceIdentity)
	if err == nil || !strings.Contains(err.Error(), "intentional restore failure") {
		t.Fatalf("failing archive SQL was not reported: %v", err)
	}
	rollback = restoreIntegrationConnect(t, ctx, rollbackURL)
	defer rollback.Close(ctx)
	var priorTableMissing, failingTableMissing bool
	if err = rollback.QueryRow(ctx, `SELECT to_regclass('public.a_restored_before') IS NULL,
		to_regclass('public.z_restore_failure') IS NULL`).Scan(&priorTableMissing, &failingTableMissing); err != nil {
		t.Fatal(err)
	}
	if !priorTableMissing || !failingTableMissing {
		t.Fatalf("failed restore left partial tables: prior_missing=%v failing_missing=%v", priorTableMissing, failingTableMissing)
	}
}

func restoreIntegrationDatabaseURL(t *testing.T, adminURL, databaseName string) string {
	t.Helper()
	parsed, err := url.Parse(adminURL)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") || parsed.Host == "" {
		t.Fatalf("%s must be a PostgreSQL URL", restoreIntegrationDatabaseEnv)
	}
	parsed.Path = "/" + databaseName
	parsed.RawPath = ""
	return parsed.String()
}

func restoreIntegrationConnect(t *testing.T, ctx context.Context, databaseURL string) *pgx.Conn {
	t.Helper()
	connection, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect disposable restore database: %v", err)
	}
	return connection
}

func restoreIntegrationExec(t *testing.T, ctx context.Context, connection *pgx.Conn, statement string) {
	t.Helper()
	if _, err := connection.Exec(ctx, statement); err != nil {
		t.Fatalf("prepare restore integration database: %v", err)
	}
}

func restoreIntegrationIdentity(t *testing.T, ctx context.Context, connection *pgx.Conn) RestoreTargetIdentity {
	t.Helper()
	var identity RestoreTargetIdentity
	err := connection.QueryRow(ctx, `SELECT control.system_identifier::text,
		database.oid::bigint,current_database()
		FROM pg_control_system() control
		JOIN pg_database database ON database.datname=current_database()`).Scan(
		&identity.SystemIdentifier,
		&identity.DatabaseOID,
		&identity.DatabaseName,
	)
	if err != nil {
		t.Fatalf("read disposable restore database identity: %v", err)
	}
	return identity
}
