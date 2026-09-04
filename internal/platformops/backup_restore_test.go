package platformops

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
mv "$REPLACEMENT_ARCHIVE" "$ARCHIVE_PATH"
cat > "$CAPTURE_DIR/archive.read"
cat <<'SQL'
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
env | grep '^PG' | sort > "$CAPTURE_DIR/psql.env"
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
	replacement := filepath.Join(archiveDir, "replacement.dump")
	if err := os.WriteFile(replacement, []byte("replaced"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ARCHIVE_PATH", archive)
	t.Setenv("REPLACEMENT_ARCHIVE", replacement)
	expected := RestoreTargetIdentity{SystemIdentifier: "7500000000002", DatabaseOID: 200, DatabaseName: "scratch"}
	production := RestoreTargetIdentity{SystemIdentifier: "7500000000001", DatabaseOID: 100, DatabaseName: "production"}
	if err := (ExecCommandRunner{}).RunRestore(context.Background(), testDatabaseEnvironment(t), archive, testArchiveSHA256(t, archive), expected, production); err != nil {
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
	if strings.Contains(string(psqlArgs), "process-only") {
		t.Fatalf("psql argv leaked connection password: %s", psqlArgs)
	}
	psqlEnvironment, err := os.ReadFile(filepath.Join(captureDir, "psql.env"))
	if err != nil || !strings.Contains(string(psqlEnvironment), "PGPASSWORD=process-only\n") {
		t.Fatalf("psql did not receive connection password only through environment: %s err=%v", psqlEnvironment, err)
	}
	pgRestoreArgs, err := os.ReadFile(filepath.Join(captureDir, "pg_restore.args"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(pgRestoreArgs), archive) || strings.Contains(string(pgRestoreArgs), "/dev/fd/") ||
		!strings.Contains(string(pgRestoreArgs), "--file=-") {
		t.Fatalf("pg_restore did not consume the verified archive only through stdin: %s", pgRestoreArgs)
	}
	consumedArchive, err := os.ReadFile(filepath.Join(captureDir, "archive.read"))
	if err != nil || string(consumedArchive) != "archive" {
		t.Fatalf("pg_restore read path replacement instead of verified descriptor: %q err=%v", consumedArchive, err)
	}
	entries, err := os.ReadDir(archiveDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "backup.dump" {
		t.Fatalf("private expanded restore script remained on disk: %+v", entries)
	}
}

// 验证攻击者在摘要校验后原地覆盖同一 inode，也不能改变 pg_restore 实际消费的已验证字节。
func TestExecCommandRunnerSnapshotsArchiveBeforeSameInodeOverwrite(t *testing.T) {
	binDir := t.TempDir()
	captureDir := t.TempDir()
	writeExecutable(t, filepath.Join(binDir, "pg_restore"), `#!/bin/sh
printf 'replaced' > "$ARCHIVE_PATH"
cat > "$CAPTURE_DIR/archive.read"
printf 'SELECT 42;\n'
`)
	writeExecutable(t, filepath.Join(binDir, "psql"), "#!/bin/sh\ncat >/dev/null\n")
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("CAPTURE_DIR", captureDir)
	archive := filepath.Join(t.TempDir(), "backup.dump")
	if err := os.WriteFile(archive, []byte("archive"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ARCHIVE_PATH", archive)
	expected := RestoreTargetIdentity{SystemIdentifier: "7500000000002", DatabaseOID: 200, DatabaseName: "scratch"}
	production := RestoreTargetIdentity{SystemIdentifier: "7500000000001", DatabaseOID: 100, DatabaseName: "production"}
	if err := (ExecCommandRunner{}).RunRestore(context.Background(), testDatabaseEnvironment(t), archive, testArchiveSHA256(t, archive), expected, production); err != nil {
		t.Fatal(err)
	}
	consumedArchive, err := os.ReadFile(filepath.Join(captureDir, "archive.read"))
	if err != nil || string(consumedArchive) != "archive" {
		t.Fatalf("pg_restore did not consume the immutable verified snapshot: %q err=%v", consumedArchive, err)
	}
}

// 验证 psql 身份拒绝、连接错误或进程崩溃都会作为失败返回，归档 SQL 由单事务回滚且临时描述符可回收。
func TestExecCommandRunnerRestoreFailureIsFailClosed(t *testing.T) {
	binDir := t.TempDir()
	captureDir := t.TempDir()
	writeExecutable(t, filepath.Join(binDir, "pg_restore"), `#!/bin/sh
printf 'CREATE TABLE must_rollback(id bigint);\n'
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
	err := (ExecCommandRunner{}).RunRestore(context.Background(), testDatabaseEnvironment(t), archive, testArchiveSHA256(t, archive), expected, production)
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
cat "$RENDERED_SQL"
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
			err := (ExecCommandRunner{}).RunRestore(context.Background(), testDatabaseEnvironment(t), archive, testArchiveSHA256(t, archive), expected, production)
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
cat "$RENDERED_SQL"
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
	err := (ExecCommandRunner{}).RunRestore(context.Background(), testDatabaseEnvironment(t), archive, testArchiveSHA256(t, archive), expected, production)
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

// 验证 archive 内容与可信摘要不一致时，在启动 pg_restore 之前失败。
func TestExecCommandRunnerRejectsRestoreArchiveDigestMismatch(t *testing.T) {
	binDir := t.TempDir()
	marker := filepath.Join(t.TempDir(), "pg-restore-ran")
	writeExecutable(t, filepath.Join(binDir, "pg_restore"), "#!/bin/sh\ntouch \"$PG_RESTORE_MARKER\"\n")
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("PG_RESTORE_MARKER", marker)
	archive := filepath.Join(t.TempDir(), "backup.dump")
	if err := os.WriteFile(archive, []byte("archive"), 0o600); err != nil {
		t.Fatal(err)
	}
	expected := RestoreTargetIdentity{SystemIdentifier: "7500000000002", DatabaseOID: 200, DatabaseName: "scratch"}
	production := RestoreTargetIdentity{SystemIdentifier: "7500000000001", DatabaseOID: 100, DatabaseName: "production"}
	err := (ExecCommandRunner{}).RunRestore(context.Background(), testDatabaseEnvironment(t), archive, strings.Repeat("0", 64), expected, production)
	if err == nil || !strings.Contains(err.Error(), "trusted digest") {
		t.Fatalf("mismatched archive digest was not rejected: %v", err)
	}
	if _, statErr := os.Stat(marker); !os.IsNotExist(statErr) {
		t.Fatalf("pg_restore ran after digest mismatch: %v", statErr)
	}
}

// 验证 restore 拒绝 symlink artifact，即使其目标内容与可信摘要一致也不能越出备份根。
func TestExecCommandRunnerRejectsRestoreArchiveSymlink(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(directory, "target.dump")
	link := filepath.Join(directory, "backup.dump")
	if err := os.WriteFile(target, []byte("archive"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	expected := RestoreTargetIdentity{SystemIdentifier: "7500000000002", DatabaseOID: 200, DatabaseName: "scratch"}
	production := RestoreTargetIdentity{SystemIdentifier: "7500000000001", DatabaseOID: 100, DatabaseName: "production"}
	err := (ExecCommandRunner{}).RunRestore(context.Background(), testDatabaseEnvironment(t), link, testArchiveSHA256(t, target), expected, production)
	if err == nil || !strings.Contains(err.Error(), "not a symlink") {
		t.Fatalf("symlink archive was not rejected: %v", err)
	}
}

// 验证可信摘要只接受规范的小写 SHA-256，避免编码歧义绕过运维证据绑定。
func TestOpenVerifiedRestoreArchiveRejectsNonCanonicalDigest(t *testing.T) {
	archive := filepath.Join(t.TempDir(), "backup.dump")
	if err := os.WriteFile(archive, []byte("archive"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, digest := range []string{"weak", strings.ToUpper(testArchiveSHA256(t, archive))} {
		opened, err := openVerifiedRestoreArchive(archive, digest)
		if opened != nil || err != ErrInvalidArgument {
			t.Fatalf("non-canonical digest %q accepted: file=%v err=%v", digest, opened, err)
		}
	}
}

func testArchiveSHA256(t *testing.T, path string) string {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(contents)
	return hex.EncodeToString(digest[:])
}

func testDatabaseEnvironment(t *testing.T) []string {
	t.Helper()
	environment, err := postgresToolEnvironment("postgres://restore-user:process-only@127.0.0.1/scratch?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	return environment
}

func writeExecutable(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o700); err != nil {
		t.Fatal(err)
	}
}
