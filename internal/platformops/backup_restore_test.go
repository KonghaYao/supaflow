package platformops

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// 验证 restore 先离线展开归档，再把身份 guard 与完整 SQL 放入同一 psql 单事务，且不留下明文临时文件。
func TestExecCommandRunnerBindsRestoreToOneGuardedTransaction(t *testing.T) {
	binDir := t.TempDir()
	captureDir := t.TempDir()
	writeExecutable(t, filepath.Join(binDir, "pg_restore"), `#!/bin/sh
printf '%s\n' "$@" > "$CAPTURE_DIR/pg_restore.args"
cat >&3 <<'SQL'
\restrict SAFE_restore_123
COPY public.sample (value) FROM stdin;
\connect-is-copy-data
\.
SELECT 42;
\unrestrict SAFE_restore_123
SQL
`)
	writeExecutable(t, filepath.Join(binDir, "psql"), `#!/bin/sh
printf '%s\n' "$@" > "$CAPTURE_DIR/psql.args"
cat > "$CAPTURE_DIR/input.sql"
exit "${PSQL_EXIT_CODE:-0}"
`)
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("CAPTURE_DIR", captureDir)
	archiveDir := t.TempDir()
	archive := filepath.Join(archiveDir, "backup.dump")
	if err := os.WriteFile(archive, []byte("archive"), 0o600); err != nil {
		t.Fatal(err)
	}
	expected := RestoreTargetIdentity{SystemIdentifier: "7500000000002", DatabaseOID: 200, DatabaseName: "scratch"}
	production := RestoreTargetIdentity{SystemIdentifier: "7500000000001", DatabaseOID: 100, DatabaseName: "production"}
	if err := (ExecCommandRunner{}).RunRestore(context.Background(), "postgres://scratch", archive, expected, production); err != nil {
		t.Fatal(err)
	}
	input, err := os.ReadFile(filepath.Join(captureDir, "input.sql"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(input)
	guardIndex, archiveIndex := strings.Index(text, "restore target identity"), strings.Index(text, "SELECT 42")
	if guardIndex < 0 || archiveIndex < 0 || guardIndex >= archiveIndex ||
		!strings.Contains(text, "actual_database_oid <> 200") ||
		!strings.Contains(text, "actual_system_identifier = '7500000000001'") {
		t.Fatalf("restore input is not guarded before archive SQL:\n%s", text)
	}
	psqlArgs, err := os.ReadFile(filepath.Join(captureDir, "psql.args"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(psqlArgs), "--single-transaction") || !strings.Contains(string(psqlArgs), "--set=ON_ERROR_STOP=1") {
		t.Fatalf("psql does not enforce atomic failure: %s", psqlArgs)
	}
	entries, err := os.ReadDir(archiveDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "backup.dump" {
		t.Fatalf("private expanded restore script remained on disk: %+v", entries)
	}
}

// 验证 psql 身份拒绝、连接错误或进程崩溃都会作为失败返回，归档 SQL 由单事务回滚且临时描述符可回收。
func TestExecCommandRunnerRestoreFailureIsFailClosed(t *testing.T) {
	binDir := t.TempDir()
	captureDir := t.TempDir()
	writeExecutable(t, filepath.Join(binDir, "pg_restore"), `#!/bin/sh
printf 'CREATE TABLE must_rollback(id bigint);\n' >&3
`)
	writeExecutable(t, filepath.Join(binDir, "psql"), `#!/bin/sh
cat > "$CAPTURE_DIR/failed-input.sql"
echo 'restore target identity does not match verified scratch database' >&2
exit 7
`)
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("CAPTURE_DIR", captureDir)
	archive := filepath.Join(t.TempDir(), "backup.dump")
	if err := os.WriteFile(archive, []byte("archive"), 0o600); err != nil {
		t.Fatal(err)
	}
	expected := RestoreTargetIdentity{SystemIdentifier: "7500000000002", DatabaseOID: 200, DatabaseName: "scratch"}
	production := RestoreTargetIdentity{SystemIdentifier: "7500000000001", DatabaseOID: 100, DatabaseName: "production"}
	err := (ExecCommandRunner{}).RunRestore(context.Background(), "postgres://changed-target", archive, expected, production)
	if err == nil || !strings.Contains(err.Error(), "psql failed") || !strings.Contains(err.Error(), "restore target identity") {
		t.Fatalf("restore failure was not preserved safely: %v", err)
	}
}

// 验证被替换的 archive 无法在身份 guard 后利用 psql 元命令换库、执行 shell 或包含第二段脚本。
func TestExecCommandRunnerRejectsDangerousRestoreMetaCommands(t *testing.T) {
	commands := []string{
		`\connect production`, `\c production`, `SELECT 'production' \gexec`,
		`\! /bin/false`, `\include /tmp/replaced.sql`, `\ir ../replaced.sql`,
	}
	for _, command := range commands {
		t.Run(strings.Fields(command)[0], func(t *testing.T) {
			binDir := t.TempDir()
			captureDir := t.TempDir()
			rendered := filepath.Join(captureDir, "rendered.sql")
			if err := os.WriteFile(rendered, []byte(command+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			writeExecutable(t, filepath.Join(binDir, "pg_restore"), `#!/bin/sh
cat "$RENDERED_SQL" >&3
`)
			writeExecutable(t, filepath.Join(binDir, "psql"), `#!/bin/sh
touch "$CAPTURE_DIR/psql-ran"
cat >/dev/null
`)
			t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
			t.Setenv("CAPTURE_DIR", captureDir)
			t.Setenv("RENDERED_SQL", rendered)
			archive := filepath.Join(t.TempDir(), "backup.dump")
			if err := os.WriteFile(archive, []byte("archive"), 0o600); err != nil {
				t.Fatal(err)
			}
			expected := RestoreTargetIdentity{SystemIdentifier: "7500000000002", DatabaseOID: 200, DatabaseName: "scratch"}
			production := RestoreTargetIdentity{SystemIdentifier: "7500000000001", DatabaseOID: 100, DatabaseName: "production"}
			err := (ExecCommandRunner{}).RunRestore(context.Background(), "postgres://scratch", archive, expected, production)
			if err == nil || !strings.Contains(err.Error(), "unsafe psql meta-command") {
				t.Fatalf("dangerous command %q was not rejected: %v", command, err)
			}
			if _, statErr := os.Stat(filepath.Join(captureDir, "psql-ran")); !os.IsNotExist(statErr) {
				t.Fatalf("psql ran after dangerous command %q: %v", command, statErr)
			}
		})
	}
}

// 验证归档不能先提交已执行的 DDL，再借后续错误伪造单事务恢复失败。
func TestExecCommandRunnerRejectsTransactionEscapeBeforeStartingPsql(t *testing.T) {
	binDir := t.TempDir()
	captureDir := t.TempDir()
	rendered := filepath.Join(captureDir, "rendered.sql")
	malicious := "CREATE TABLE escaped(id bigint); COMMIT; SELECT 1 / 0;\n"
	if err := os.WriteFile(rendered, []byte(malicious), 0o600); err != nil {
		t.Fatal(err)
	}
	writeExecutable(t, filepath.Join(binDir, "pg_restore"), `#!/bin/sh
cat "$RENDERED_SQL" >&3
`)
	writeExecutable(t, filepath.Join(binDir, "psql"), `#!/bin/sh
touch "$CAPTURE_DIR/psql-ran"
cat >/dev/null
`)
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("CAPTURE_DIR", captureDir)
	t.Setenv("RENDERED_SQL", rendered)
	archive := filepath.Join(t.TempDir(), "backup.dump")
	if err := os.WriteFile(archive, []byte("archive"), 0o600); err != nil {
		t.Fatal(err)
	}
	expected := RestoreTargetIdentity{SystemIdentifier: "7500000000002", DatabaseOID: 200, DatabaseName: "scratch"}
	production := RestoreTargetIdentity{SystemIdentifier: "7500000000001", DatabaseOID: 100, DatabaseName: "production"}
	err := (ExecCommandRunner{}).RunRestore(context.Background(), "postgres://scratch", archive, expected, production)
	if err == nil || !strings.Contains(err.Error(), "unsafe transaction control") {
		t.Fatalf("transaction escape was not rejected: %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(captureDir, "psql-ran")); !os.IsNotExist(statErr) {
		t.Fatalf("psql ran after transaction escape was detected: %v", statErr)
	}
}

// 验证所有可能结束、拆分或挂起外层事务的顶层 SQL 命令都被保守拒绝。
func TestValidateRenderedRestoreScriptRejectsTransactionControl(t *testing.T) {
	scripts := []string{
		"BEGIN;",
		"START TRANSACTION;",
		"commit;",
		"END;",
		"ROLLBACK;",
		"ABORT;",
		"PREPARE TRANSACTION 'restore_escape';",
		"SAVEPOINT before_escape;",
		"RELEASE SAVEPOINT before_escape;",
		"SELECT 1; /* harmless separator */ COMMIT;",
		"/* outer /* nested */ closed */ COMMIT;",
	}
	for _, script := range scripts {
		err := validateRenderedRestoreScript(strings.NewReader(script + "\n"))
		if err == nil || !strings.Contains(err.Error(), "unsafe transaction control") {
			t.Fatalf("transaction command %q was not rejected: %v", script, err)
		}
	}
}

// 验证事务关键字出现在字面量、标识符、注释、函数体和 COPY 数据中时不会误报。
func TestValidateRenderedRestoreScriptIgnoresTransactionWordsOutsideTopLevelSQL(t *testing.T) {
	script := `-- COMMIT;
/* START TRANSACTION;
/* nested COMMIT; */
ROLLBACK; */
SELECT 'COMMIT; BEGIN;', "ROLLBACK";
SELECT E'ABORT; SAVEPOINT escaped\'';
DO $restore_body$
BEGIN
  PERFORM 'COMMIT;';
END
$restore_body$;
COPY public.sample (value) FROM stdin;
COMMIT;
ROLLBACK;
\.
SELECT 42;
`
	if err := validateRenderedRestoreScript(strings.NewReader(script)); err != nil {
		t.Fatalf("safe non-top-level transaction words were rejected: %v", err)
	}
}

func writeExecutable(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o700); err != nil {
		t.Fatal(err)
	}
}
