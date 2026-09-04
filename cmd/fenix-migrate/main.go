// Command fenix-migrate performs the one-time, restartable Fenix-to-Supaflow migration.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/fenixmigration"
)

type commandOptions struct {
	sourceDatabaseURL string
	targetDatabaseURL string
	sourceRoot        string
	mappingPath       string
	reportPath        string
	actorID           string
	dryRun            bool
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fenix migration failed:", err)
		os.Exit(1)
	}
}

func run() error {
	options := parseFlags()
	if options.sourceDatabaseURL == "" || (!options.dryRun && options.targetDatabaseURL == "") || options.sourceRoot == "" || options.mappingPath == "" || options.reportPath == "" || options.actorID == "" {
		return errors.New("source/target database, source root, mapping, report, and actor are required")
	}
	mapping, err := readMapping(options.mappingPath)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Hour)
	defer cancel()
	sourcePool, err := openPool(ctx, options.sourceDatabaseURL, 4)
	if err != nil {
		return fmt.Errorf("connect Fenix source database: %w", err)
	}
	defer sourcePool.Close()
	reader, err := fenixmigration.NewSourceReader(sourcePool, options.sourceRoot)
	if err != nil {
		return err
	}
	snapshot, err := reader.Load(ctx)
	if err != nil {
		return err
	}
	var target fenixmigration.Target
	var targetPool *pgxpool.Pool
	if !options.dryRun {
		targetPool, err = openPool(ctx, options.targetDatabaseURL, 8)
		if err != nil {
			return fmt.Errorf("connect Supaflow target database: %w", err)
		}
		defer targetPool.Close()
		target, err = fenixmigration.NewPostgresTarget(targetPool)
		if err != nil {
			return err
		}
	}
	report, migrationErr := fenixmigration.Migrate(ctx, snapshot, mapping, target, fenixmigration.RunOptions{ActorID: options.actorID, DryRun: options.dryRun})
	finalSnapshot, finalErr := reader.Load(ctx)
	if finalErr == nil {
		report.FinalSourceWatermark = finalSnapshot.Watermark
		report.FinalSourceDatabaseFingerprint = finalSnapshot.DatabaseFingerprint
		report.FinalSourceFingerprint, finalErr = finalSnapshot.Fingerprint()
		report.SourceStable = report.SourceFingerprint != "" && report.SourceFingerprint == report.FinalSourceFingerprint &&
			report.SourceDatabaseFingerprint != "" && report.SourceDatabaseFingerprint == report.FinalSourceDatabaseFingerprint
	}
	if writeErr := writeReport(options.reportPath, report); writeErr != nil {
		return writeErr
	}
	if migrationErr != nil {
		return migrationErr
	}
	if finalErr != nil {
		return fmt.Errorf("re-read frozen Fenix source: %w", finalErr)
	}
	if !report.SourceStable {
		return errors.New("Fenix workflow source changed during migration; keep writes frozen and resume with the same mapping")
	}
	fmt.Printf("migration verified: definitions=%d drafts=%d triggers=%d terminalRuns=%d report=%s\n", report.PlannedDefinitions, report.PlannedDrafts, report.PlannedTriggers, report.PlannedRuns, options.reportPath)
	return nil
}

func parseFlags() commandOptions {
	var options commandOptions
	flag.StringVar(&options.sourceDatabaseURL, "source-database-url", strings.TrimSpace(os.Getenv("FENIX_DATABASE_URL")), "Fenix PostgreSQL URL (prefer FENIX_DATABASE_URL)")
	flag.StringVar(&options.targetDatabaseURL, "target-database-url", strings.TrimSpace(os.Getenv("DATABASE_URL")), "Supaflow PostgreSQL URL (prefer DATABASE_URL)")
	flag.StringVar(&options.sourceRoot, "source-workflow-root", "", "allowed root containing legacy YAML files")
	flag.StringVar(&options.mappingPath, "mapping", "", "strict JSON identity/credential mapping")
	flag.StringVar(&options.reportPath, "report", "", "atomic JSON evidence report path")
	flag.StringVar(&options.actorID, "actor", "", "audited administrator identity")
	flag.BoolVar(&options.dryRun, "dry-run", false, "preflight without target writes")
	flag.Parse()
	return options
}

func readMapping(path string) (fenixmigration.Mapping, error) {
	file, err := os.Open(path)
	if err != nil {
		return fenixmigration.Mapping{}, fmt.Errorf("open migration mapping: %w", err)
	}
	defer file.Close()
	content, err := io.ReadAll(io.LimitReader(file, 1<<20+1))
	if err != nil || len(content) > 1<<20 {
		return fenixmigration.Mapping{}, errors.New("mapping must be readable and at most one MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	var mapping fenixmigration.Mapping
	if err = decoder.Decode(&mapping); err != nil {
		return fenixmigration.Mapping{}, fmt.Errorf("decode migration mapping: %w", err)
	}
	if err = ensureEOF(decoder); err != nil {
		return fenixmigration.Mapping{}, err
	}
	return mapping, nil
}

func openPool(ctx context.Context, databaseURL string, maximum int32) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, errors.New("database URL is invalid")
	}
	config.MaxConns = maximum
	config.MinConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

func writeReport(path string, report fenixmigration.Report) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create report directory: %w", err)
	}
	file, err := os.CreateTemp(directory, ".fenix-migration-*.json")
	if err != nil {
		return fmt.Errorf("create migration report: %w", err)
	}
	temporary := file.Name()
	defer os.Remove(temporary)
	if err = file.Chmod(0o600); err == nil {
		var content []byte
		content, err = json.MarshalIndent(report, "", "  ")
		if err == nil {
			_, err = file.Write(append(content, '\n'))
		}
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("write migration report: %w", err)
	}
	if err = os.Rename(temporary, path); err != nil {
		return fmt.Errorf("publish migration report: %w", err)
	}
	return nil
}

func ensureEOF(decoder *json.Decoder) error {
	var value any
	if err := decoder.Decode(&value); !errors.Is(err, io.EOF) {
		return errors.New("mapping must contain exactly one JSON object")
	}
	return nil
}
