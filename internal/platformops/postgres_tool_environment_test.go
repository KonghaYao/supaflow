package platformops

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// 验证 URL 连接字段被拆成 libpq 环境变量，密码不会落入 pg_dump 参数。
func TestBackupPassesURLConnectionOnlyThroughEnvironment(t *testing.T) {
	binDir := t.TempDir()
	captureDir := t.TempDir()
	writeExecutable(t, filepath.Join(binDir, "pg_dump"), `#!/bin/sh
printf '%s\n' "$@" > "$CAPTURE_DIR/args"
env | grep '^PG' | sort > "$CAPTURE_DIR/environment"
`)
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("CAPTURE_DIR", captureDir)
	t.Setenv("PGHOSTADDR", "203.0.113.10")
	restore, err := NewBackupRestore(t.TempDir(), ExecCommandRunner{})
	if err != nil {
		t.Fatal(err)
	}
	databaseURL := "postgresql://restore-user:pa%24%24word@127.0.0.1:55432/scratch?sslmode=disable&connect_timeout=4"
	if _, err = restore.Backup(context.Background(), databaseURL, "backup.dump"); err != nil {
		t.Fatal(err)
	}
	args, err := os.ReadFile(filepath.Join(captureDir, "args"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(args), "pa$$word") || strings.Contains(string(args), databaseURL) {
		t.Fatalf("connection secret leaked into argv: %s", args)
	}
	environment, err := os.ReadFile(filepath.Join(captureDir, "environment"))
	if err != nil {
		t.Fatal(err)
	}
	value := string(environment)
	for _, expected := range []string{
		"PGCONNECT_TIMEOUT=4", "PGDATABASE=scratch", "PGHOST=127.0.0.1", "PGPASSWORD=pa$$word",
		"PGPORT=55432", "PGSSLMODE=disable", "PGUSER=restore-user",
	} {
		if !strings.Contains(value, expected+"\n") {
			t.Fatalf("missing libpq environment %q in:\n%s", expected, value)
		}
	}
	if strings.Contains(value, "PGHOSTADDR=") || strings.Contains(value, databaseURL) {
		t.Fatalf("inherited routing or whole URI reached subprocess:\n%s", value)
	}
}

// 验证不受支持或缺少目标库的 URL 会安全失败，错误不会回显密码。
func TestPostgresToolEnvironmentRejectsUnsafeURLWithoutSecretDisclosure(t *testing.T) {
	secret := "do-not-disclose"
	for _, databaseURL := range []string{
		"postgres://user:" + secret + "@127.0.0.1",
		"postgres://user:" + secret + "@127.0.0.1/db?unknown_option=value",
		"mysql://user:" + secret + "@127.0.0.1/db",
	} {
		_, err := postgresToolEnvironment(databaseURL)
		if !errors.Is(err, ErrInvalidArgument) || strings.Contains(err.Error(), secret) {
			t.Fatalf("unsafe URL produced a revealing or unexpected error: %v", err)
		}
	}
}

// 验证 PostgreSQL 工具异常回显环境密码时，公开错误仍会精确脱敏。
func TestBackupRedactsPasswordEchoedBySubprocess(t *testing.T) {
	binDir := t.TempDir()
	writeExecutable(t, filepath.Join(binDir, "pg_dump"), "#!/bin/sh\nprintf '%s\\n' \"$PGPASSWORD\" >&2\nexit 9\n")
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	restore, err := NewBackupRestore(t.TempDir(), ExecCommandRunner{})
	if err != nil {
		t.Fatal(err)
	}
	secret := "bare-password-value"
	_, err = restore.Backup(context.Background(), "postgres://user:"+secret+"@127.0.0.1/db", "backup.dump")
	if err == nil || strings.Contains(err.Error(), secret) || !strings.Contains(err.Error(), "[REDACTED]") {
		t.Fatalf("subprocess password was not safely redacted: %v", err)
	}
}
