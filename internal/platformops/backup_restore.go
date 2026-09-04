package platformops

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

var postgresSystemIdentifierPattern = regexp.MustCompile(`^[0-9]+$`)

// RestoreTargetIdentity is a PostgreSQL database identity, independent of DSN spelling or DNS.
type RestoreTargetIdentity struct {
	SystemIdentifier string
	DatabaseOID      uint32
	DatabaseName     string
}

// BackupRestoreRunner is the process boundary for backup and digest/identity-bound restore drills.
type BackupRestoreRunner interface {
	Run(context.Context, string, []string, []string) error
	RunRestore(context.Context, []string, string, string, RestoreTargetIdentity, RestoreTargetIdentity) error
}

// ExecCommandRunner executes PostgreSQL tools with bounded redacted diagnostics.
type ExecCommandRunner struct{}

// Run executes one fixed tool command supplied by BackupRestore.
func (ExecCommandRunner) Run(ctx context.Context, name string, args, environment []string) error {
	command := exec.CommandContext(ctx, name, args...)
	command.Env = postgresProcessEnvironment(environment)
	return runBoundedCommandRedacting(command, name, postgresEnvironmentSecrets(environment))
}

// RunRestore verifies and snapshots one archive, then renders it without a
// database connection. It sends an identity guard plus the complete script
// through one transaction and one psql connection. DNS, proxy, symlink, or
// path changes cannot redirect either pg_restore or psql after verification.
func (ExecCommandRunner) RunRestore(
	ctx context.Context,
	databaseEnvironment []string,
	archivePath string,
	expectedArchiveSHA256 string,
	expected RestoreTargetIdentity,
	forbidden RestoreTargetIdentity,
) error {
	guard, err := restoreIdentityGuard(expected, forbidden)
	if err != nil {
		return err
	}
	archiveSnapshot, err := openVerifiedRestoreArchive(archivePath, expectedArchiveSHA256)
	if err != nil {
		return err
	}
	defer archiveSnapshot.Close()
	script, err := os.CreateTemp(filepath.Dir(archivePath), ".supaflow-restore-*.sql")
	if err != nil {
		return fmt.Errorf("create private restore script: %w", err)
	}
	scriptPath := script.Name()
	// The open descriptor survives unlink and is reclaimed by the kernel even
	// after SIGKILL, so expanded backup contents never remain on the filesystem.
	if err = os.Remove(scriptPath); err != nil {
		_ = script.Close()
		return fmt.Errorf("unlink private restore script: %w", err)
	}
	defer script.Close()

	// pg_restore may reopen a named archive while probing its format. On
	// descriptor filesystems such as macOS /dev/fd, reopening shares the file
	// offset and can make the second probe miss the archive header. Supplying
	// the verified snapshot as stdin keeps the exact descriptor binding while
	// remaining portable across supported PostgreSQL clients.
	render := exec.CommandContext(ctx, "pg_restore", "--no-owner", "--no-privileges", "--file=-")
	render.Stdin = archiveSnapshot
	render.Stdout = script
	if err = runBoundedCommand(render, "pg_restore"); err != nil {
		return err
	}
	if _, err = script.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind private restore script: %w", err)
	}
	if err = validateRenderedRestoreScript(script); err != nil {
		return err
	}
	if _, err = script.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind validated restore script: %w", err)
	}

	apply := exec.CommandContext(ctx, "psql", "--no-psqlrc", "--single-transaction", "--set=ON_ERROR_STOP=1", "--quiet")
	apply.Env = postgresProcessEnvironment(databaseEnvironment)
	apply.Stdin = io.MultiReader(strings.NewReader(guard), script)
	return runBoundedCommandRedacting(apply, "psql", postgresEnvironmentSecrets(databaseEnvironment))
}

type restoreSQLLexState struct {
	singleQuote  bool
	escapeString bool
	doubleQuote  bool
	blockDepth   int
	dollarTag    string
}

type restoreSQLStatementState struct {
	atStart bool
}

var unsafeRestoreTransactionCommands = map[string]struct{}{
	"ABORT":     {},
	"BEGIN":     {},
	"COMMIT":    {},
	"END":       {},
	"PREPARE":   {},
	"RELEASE":   {},
	"ROLLBACK":  {},
	"SAVEPOINT": {},
	"START":     {},
}

func validateRenderedRestoreScript(script io.Reader) error {
	scanner := bufio.NewScanner(script)
	// Platform ingress keeps database-resident values bounded. A larger plain
	// SQL line is either a corrupted/replaced artifact or unsuitable for this
	// drill and is rejected rather than allocated without limit.
	scanner.Buffer(make([]byte, 64<<10), 128<<20)
	state := restoreSQLLexState{}
	statement := restoreSQLStatementState{atStart: true}
	copyData := false
	for scanner.Scan() {
		line := scanner.Text()
		if copyData {
			if line == `\.` {
				copyData = false
			}
			continue
		}
		neutralAtStart := state == (restoreSQLLexState{})
		if err := validateRestoreSQLLine(line, &state, &statement); err != nil {
			return err
		}
		if neutralAtStart && state == (restoreSQLLexState{}) && isCopyFromStdin(line) {
			copyData = true
		}
	}
	if err := scanner.Err(); err != nil {
		return errors.New("rendered restore script exceeds safe line limit or cannot be read")
	}
	if copyData || state.singleQuote || state.doubleQuote || state.blockDepth > 0 || state.dollarTag != "" {
		return errors.New("rendered restore script is truncated")
	}
	return nil
}

func validateRestoreSQLLine(
	line string,
	state *restoreSQLLexState,
	statement *restoreSQLStatementState,
) error {
	for index := 0; index < len(line); {
		if state.blockDepth > 0 {
			if index+1 < len(line) && line[index:index+2] == "/*" {
				state.blockDepth++
				index += 2
			} else if index+1 < len(line) && line[index:index+2] == "*/" {
				state.blockDepth--
				index += 2
			} else {
				index++
			}
			continue
		}
		if state.dollarTag != "" {
			if strings.HasPrefix(line[index:], state.dollarTag) {
				index += len(state.dollarTag)
				state.dollarTag = ""
			} else {
				index++
			}
			continue
		}
		if state.singleQuote {
			if state.escapeString && line[index] == '\\' && index+1 < len(line) {
				index += 2
				continue
			}
			if line[index] == '\'' {
				if index+1 < len(line) && line[index+1] == '\'' {
					index += 2
					continue
				}
				state.singleQuote = false
				state.escapeString = false
			}
			index++
			continue
		}
		if state.doubleQuote {
			if line[index] == '"' {
				if index+1 < len(line) && line[index+1] == '"' {
					index += 2
					continue
				}
				state.doubleQuote = false
			}
			index++
			continue
		}

		if index+1 < len(line) && line[index:index+2] == "--" {
			return nil
		}
		if index+1 < len(line) && line[index:index+2] == "/*" {
			state.blockDepth = 1
			index += 2
			continue
		}
		switch line[index] {
		case '\'':
			statement.atStart = false
			state.singleQuote = true
			state.escapeString = index > 0 && (line[index-1] == 'E' || line[index-1] == 'e')
			index++
		case '"':
			statement.atStart = false
			state.doubleQuote = true
			index++
		case '$':
			tag := restoreDollarTag(line[index:])
			if tag == "" {
				statement.atStart = false
				index++
			} else {
				statement.atStart = false
				state.dollarTag = tag
				index += len(tag)
			}
		case '\\':
			trimmed := strings.TrimSpace(line)
			fields := strings.Fields(trimmed)
			if index == len(line)-len(strings.TrimLeft(line, " \t")) && len(fields) == 2 &&
				(fields[0] == `\restrict` || fields[0] == `\unrestrict`) && safeRestoreRestrictionKey(fields[1]) {
				return nil
			}
			return errors.New("rendered restore script contains an unsafe psql meta-command")
		default:
			if isRestoreSQLSpace(line[index]) {
				index++
				continue
			}
			if line[index] == ';' {
				statement.atStart = true
				index++
				continue
			}
			if isRestoreSQLIdentifierStart(line[index]) {
				end := index + 1
				for end < len(line) && isRestoreSQLIdentifierPart(line[end]) {
					end++
				}
				if statement.atStart {
					keyword := strings.ToUpper(line[index:end])
					if _, unsafe := unsafeRestoreTransactionCommands[keyword]; unsafe {
						return errors.New("rendered restore script contains unsafe transaction control")
					}
					statement.atStart = false
				}
				index = end
				continue
			}
			statement.atStart = false
			index++
		}
	}
	return nil
}

func isRestoreSQLSpace(value byte) bool {
	return value == ' ' || value == '\t' || value == '\r' || value == '\n' || value == '\f'
}

func isRestoreSQLIdentifierStart(value byte) bool {
	return (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') || value == '_'
}

func isRestoreSQLIdentifierPart(value byte) bool {
	return isRestoreSQLIdentifierStart(value) || (value >= '0' && value <= '9') || value == '$'
}

func restoreDollarTag(value string) string {
	if len(value) < 2 || value[0] != '$' {
		return ""
	}
	for index := 1; index < len(value); index++ {
		if value[index] == '$' {
			return value[:index+1]
		}
		char := value[index]
		if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9' && index > 1) || char == '_') {
			return ""
		}
	}
	return ""
}

func safeRestoreRestrictionKey(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, char := range value {
		if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '_') {
			return false
		}
	}
	return true
}

func isCopyFromStdin(line string) bool {
	upper := strings.ToUpper(strings.TrimSpace(line))
	return strings.HasPrefix(upper, "COPY ") && strings.HasSuffix(upper, " FROM STDIN;")
}

func restoreIdentityGuard(expected, forbidden RestoreTargetIdentity) (string, error) {
	if !validRestoreIdentity(expected) || !validRestoreIdentity(forbidden) || expected == forbidden {
		return "", ErrInvalidArgument
	}
	expectedName := hex.EncodeToString([]byte(expected.DatabaseName))
	return fmt.Sprintf(`DO $supaflow_restore_guard$
DECLARE
  actual_system_identifier text;
  actual_database_oid bigint;
  actual_database_name_hex text;
BEGIN
  SELECT control.system_identifier::text, database.oid::bigint,
         encode(convert_to(current_database(), 'UTF8'), 'hex')
    INTO actual_system_identifier, actual_database_oid, actual_database_name_hex
  FROM pg_control_system() control
  JOIN pg_database database ON database.datname=current_database();
  IF actual_system_identifier <> '%s'
     OR actual_database_oid <> %d
     OR actual_database_name_hex <> '%s'
     OR (actual_system_identifier = '%s' AND actual_database_oid = %d) THEN
    RAISE EXCEPTION 'restore target identity does not match verified scratch database'
      USING ERRCODE = '55000';
  END IF;
END
$supaflow_restore_guard$;
`, expected.SystemIdentifier, expected.DatabaseOID, expectedName,
		forbidden.SystemIdentifier, forbidden.DatabaseOID), nil
}

func validRestoreIdentity(value RestoreTargetIdentity) bool {
	return postgresSystemIdentifierPattern.MatchString(value.SystemIdentifier) &&
		value.DatabaseOID > 0 && value.DatabaseName != "" && len(value.DatabaseName) <= 63
}

func runBoundedCommand(command *exec.Cmd, name string) error {
	return runBoundedCommandRedacting(command, name, nil)
}

func runBoundedCommandRedacting(command *exec.Cmd, name string, secrets []string) error {
	var stderr bytes.Buffer
	command.Stderr = &limitedWriter{writer: &stderr, remaining: 4096}
	if err := command.Run(); err != nil {
		diagnostic := strings.TrimSpace(RedactDiagnostic(stderr.String()))
		for _, secret := range secrets {
			if secret != "" {
				diagnostic = strings.ReplaceAll(diagnostic, secret, "[REDACTED]")
			}
		}
		if diagnostic == "" {
			diagnostic = "no diagnostic output"
		}
		return fmt.Errorf("%s failed: %s: %w", filepath.Base(name), diagnostic, err)
	}
	return nil
}

// BackupRestore safely constructs executable PostgreSQL backup/restore drills.
type BackupRestore struct {
	runner BackupRestoreRunner
	root   string
}

// NewBackupRestore restricts backup artifacts below root.
func NewBackupRestore(root string, runner BackupRestoreRunner) (*BackupRestore, error) {
	absolute, err := filepath.Abs(root)
	if err != nil || root == "" || runner == nil {
		return nil, ErrInvalidArgument
	}
	if err = os.MkdirAll(absolute, 0o700); err != nil {
		return nil, fmt.Errorf("create backup root: %w", err)
	}
	return &BackupRestore{runner: runner, root: absolute}, nil
}

// Backup runs pg_dump in custom format with the connection secret supplied only via environment.
func (b *BackupRestore) Backup(ctx context.Context, databaseURL, name string) (string, error) {
	path, err := b.artifactPath(name)
	if err != nil || databaseURL == "" {
		return "", ErrInvalidArgument
	}
	databaseEnvironment, err := postgresToolEnvironment(databaseURL)
	if err != nil {
		return "", ErrInvalidArgument
	}
	err = b.runner.Run(ctx, "pg_dump", []string{"--format=custom", "--no-owner", "--file", path}, databaseEnvironment)
	if err != nil {
		return "", err
	}
	return path, nil
}

// RestoreDrill restores only an archive bound to a trusted digest, through a
// session bound to the verified scratch identity.
func (b *BackupRestore) RestoreDrill(
	ctx context.Context,
	databaseURL string,
	name string,
	expectedArchiveSHA256 string,
	expected RestoreTargetIdentity,
	forbidden RestoreTargetIdentity,
) error {
	path, err := b.artifactPath(name)
	if err != nil || databaseURL == "" || !validRestoreIdentity(expected) ||
		!validRestoreIdentity(forbidden) || expected == forbidden || !validArchiveSHA256(expectedArchiveSHA256) {
		return ErrInvalidArgument
	}
	databaseEnvironment, err := postgresToolEnvironment(databaseURL)
	if err != nil {
		return ErrInvalidArgument
	}
	return b.runner.RunRestore(ctx, databaseEnvironment, path, expectedArchiveSHA256, expected, forbidden)
}

func (b *BackupRestore) artifactPath(name string) (string, error) {
	if !objectPathPart.MatchString(name) {
		return "", ErrInvalidArgument
	}
	path := filepath.Join(b.root, name)
	if filepath.Dir(path) != b.root {
		return "", ErrInvalidArgument
	}
	return path, nil
}

type limitedWriter struct {
	writer    *bytes.Buffer
	remaining int
}

func (w *limitedWriter) Write(value []byte) (int, error) {
	original := len(value)
	if len(value) > w.remaining {
		value = value[:w.remaining]
	}
	_, _ = w.writer.Write(value)
	w.remaining -= len(value)
	return original, nil
}
