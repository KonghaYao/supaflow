// Command ops runs bounded Supaflow capacity, alert, backup, and restore drills.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/platformops"
)

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, platformops.RedactDiagnostic(err.Error()))
		os.Exit(1)
	}
}

func run() error {
	action := flag.String("action", "capacity", "capacity, alerts, backup, or restore")
	tenantID := flag.String("tenant", "", "tenant ID for capacity and alerts")
	artifact := flag.String("artifact", "", "validated backup artifact name")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if databaseURL == "" {
		return errors.New("DATABASE_URL is required")
	}
	switch *action {
	case "backup", "restore":
		root := strings.TrimSpace(os.Getenv("SUPAFLOW_BACKUP_ROOT"))
		if root == "" {
			root = "./data/backups"
		}
		tool, err := platformops.NewBackupRestore(root, platformops.ExecCommandRunner{})
		if err != nil {
			return err
		}
		if *action == "backup" {
			path, backupErr := tool.Backup(ctx, databaseURL, *artifact)
			if backupErr == nil {
				_, _ = fmt.Fprintln(os.Stdout, path)
			}
			return backupErr
		}
		scratchURL := strings.TrimSpace(os.Getenv("SUPAFLOW_RESTORE_DATABASE_URL"))
		if scratchURL == "" {
			return errors.New("SUPAFLOW_RESTORE_DATABASE_URL must identify a separate scratch database")
		}
		archiveSHA256 := strings.TrimSpace(os.Getenv("SUPAFLOW_RESTORE_ARCHIVE_SHA256"))
		if archiveSHA256 == "" {
			return errors.New("SUPAFLOW_RESTORE_ARCHIVE_SHA256 is required for restore")
		}
		return runVerifiedRestore(ctx, databaseURL, scratchURL, readPostgresDatabaseIdentity, func(production, scratch databaseIdentity) error {
			return tool.RestoreDrill(ctx, scratchURL, *artifact, archiveSHA256, scratch, production)
		})
	case "capacity", "alerts":
		if strings.TrimSpace(*tenantID) == "" {
			return errors.New("-tenant is required")
		}
		pool, err := pgxpool.New(ctx, databaseURL)
		if err != nil {
			return errors.New("database connection configuration is invalid")
		}
		defer pool.Close()
		snapshot, err := platformops.NewCapacityMonitor(pool).Snapshot(ctx, *tenantID, time.Now().UTC())
		if err != nil {
			return err
		}
		if *action == "alerts" {
			return json.NewEncoder(os.Stdout).Encode(map[string]any{"alerts": platformops.NewCapacityMonitor(pool).Alerts(snapshot, 1000, 0, 5*time.Minute)})
		}
		return json.NewEncoder(os.Stdout).Encode(snapshot)
	default:
		return errors.New("unsupported -action")
	}
}
