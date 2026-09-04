// Package postgres provides the PostgreSQL persistence adapter for Supaflow.
package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const migrationLockID int64 = 0x53555041464c4f57 // "SUPAFLOW"

var migrationNamePattern = regexp.MustCompile(`^([0-9]{6})_([a-z0-9_]+)\.sql$`)

// Migration describes one immutable, forward-only SQL migration.
type Migration struct {
	Version  int64
	Name     string
	SQL      string
	Checksum [sha256.Size]byte
}

// LoadMigrations reads and validates ordered forward-only migrations from dir.
func LoadMigrations(source fs.FS, dir string) ([]Migration, error) {
	entries, err := fs.ReadDir(source, dir)
	if err != nil {
		return nil, fmt.Errorf("read migration directory: %w", err)
	}

	migrations := make([]Migration, 0, len(entries))
	seen := make(map[int64]string)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		matches := migrationNamePattern.FindStringSubmatch(entry.Name())
		if matches == nil {
			if strings.HasSuffix(entry.Name(), ".sql") {
				return nil, fmt.Errorf("invalid forward migration filename %q", entry.Name())
			}
			continue
		}
		version, err := strconv.ParseInt(matches[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse migration version %q: %w", entry.Name(), err)
		}
		if previous, ok := seen[version]; ok {
			return nil, fmt.Errorf("duplicate migration version %d in %q and %q", version, previous, entry.Name())
		}
		contents, err := fs.ReadFile(source, filepath.ToSlash(filepath.Join(dir, entry.Name())))
		if err != nil {
			return nil, fmt.Errorf("read migration %q: %w", entry.Name(), err)
		}
		if len(strings.TrimSpace(string(contents))) == 0 {
			return nil, fmt.Errorf("migration %q is empty", entry.Name())
		}
		seen[version] = entry.Name()
		migrations = append(migrations, Migration{
			Version:  version,
			Name:     entry.Name(),
			SQL:      string(contents),
			Checksum: sha256.Sum256(contents),
		})
	}
	sort.Slice(migrations, func(i, j int) bool { return migrations[i].Version < migrations[j].Version })
	for index, migration := range migrations {
		expected := int64(index + 1)
		if migration.Version != expected {
			return nil, fmt.Errorf("migration history has a gap: expected version %06d, found %06d", expected, migration.Version)
		}
	}
	return migrations, nil
}

// Migrator applies forward-only migrations under a session advisory lock.
type Migrator struct {
	pool *pgxpool.Pool
}

// NewMigrator creates a migration runner using pool.
func NewMigrator(pool *pgxpool.Pool) *Migrator {
	return &Migrator{pool: pool}
}

// Up applies all pending migrations and rejects modified applied migrations.
func (m *Migrator) Up(ctx context.Context, migrations []Migration) (err error) {
	if m == nil || m.pool == nil {
		return errors.New("postgres migrator requires a pool")
	}
	conn, err := m.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration connection: %w", err)
	}
	defer conn.Release()

	if _, err = conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLockID); err != nil {
		return fmt.Errorf("acquire migration advisory lock: %w", err)
	}
	defer func() {
		_, unlockErr := conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, migrationLockID)
		if unlockErr != nil && err == nil {
			err = fmt.Errorf("release migration advisory lock: %w", unlockErr)
		}
	}()

	if _, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version bigint PRIMARY KEY,
			name text NOT NULL,
			checksum text NOT NULL,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("ensure migration ledger: %w", err)
	}
	if err = validateAppliedMigrationHistory(ctx, conn.Conn(), migrations); err != nil {
		return err
	}

	for _, migration := range migrations {
		if err = applyMigration(ctx, conn.Conn(), migration); err != nil {
			return err
		}
	}
	return nil
}

func validateAppliedMigrationHistory(ctx context.Context, conn *pgx.Conn, migrations []Migration) error {
	available := make(map[int64]Migration, len(migrations))
	for _, migration := range migrations {
		available[migration.Version] = migration
	}
	rows, err := conn.Query(ctx, `SELECT version,name,checksum FROM schema_migrations ORDER BY version`)
	if err != nil {
		return fmt.Errorf("read applied migration history: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var version int64
		var name, checksum string
		if err = rows.Scan(&version, &name, &checksum); err != nil {
			return fmt.Errorf("scan applied migration history: %w", err)
		}
		migration, exists := available[version]
		if !exists {
			return fmt.Errorf("applied migration %06d %q is missing from source", version, name)
		}
		expectedChecksum := hex.EncodeToString(migration.Checksum[:])
		if migration.Name != name || expectedChecksum != checksum {
			return fmt.Errorf("migration %06d checksum or name differs from applied migration", version)
		}
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("iterate applied migration history: %w", err)
	}
	return nil
}

func applyMigration(ctx context.Context, conn *pgx.Conn, migration Migration) error {
	var recordedName, recordedChecksum string
	err := conn.QueryRow(ctx,
		`SELECT name, checksum FROM schema_migrations WHERE version = $1`, migration.Version,
	).Scan(&recordedName, &recordedChecksum)
	expectedChecksum := hex.EncodeToString(migration.Checksum[:])
	if err == nil {
		if recordedName != migration.Name || recordedChecksum != expectedChecksum {
			return fmt.Errorf("migration %06d checksum or name differs from applied migration", migration.Version)
		}
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("inspect migration %06d: %w", migration.Version, err)
	}

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin migration %06d: %w", migration.Version, err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err = tx.Exec(ctx, migration.SQL); err != nil {
		return fmt.Errorf("execute migration %06d: %w", migration.Version, err)
	}
	if _, err = tx.Exec(ctx,
		`INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, $3)`,
		migration.Version, migration.Name, expectedChecksum,
	); err != nil {
		return fmt.Errorf("record migration %06d: %w", migration.Version, err)
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration %06d: %w", migration.Version, err)
	}
	return nil
}
