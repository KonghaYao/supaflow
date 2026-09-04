// Command migrate applies Supaflow's forward-only PostgreSQL migrations.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/postgres"
	"github.com/pazhou/supaflow/internal/workflow"
)

func main() {
	var migrationsDir string
	flag.StringVar(&migrationsDir, "dir", "migrations", "directory containing forward-only SQL migrations")
	flag.Parse()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	migrations, err := postgres.LoadMigrations(os.DirFS("."), migrationsDir)
	if err != nil {
		log.Fatalf("load migrations: %v", err)
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		log.Fatalf("configure database: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("connect to database: %v", err)
	}
	if err := postgres.NewMigrator(pool).Up(ctx, migrations); err != nil {
		log.Fatalf("migrate database: %v", err)
	}
	registry := workflow.BuiltinRegistry()
	if err := postgres.NewNodeRegistry(pool).EnsureRegistry(ctx, registry); err != nil {
		log.Fatalf("seed node registry: %v", err)
	}
	fmt.Printf("verified %d migration(s) and %d node version(s)\n", len(migrations), len(registry.Entries()))
}
