package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/workflow"
)

// 验证 PostgreSQL registry 幂等 seed、完整 projection 和已发布版本不可变约束。
func TestPostgresNodeRegistryLifecycle(t *testing.T) {
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
	migrations, err := LoadMigrations(os.DirFS("../.."), "migrations")
	if err != nil {
		t.Fatal(err)
	}
	if err = NewMigrator(pool).Up(ctx, migrations); err != nil {
		t.Fatal(err)
	}

	nodeType := fmt.Sprintf("test.registry.%d", time.Now().UTC().UnixNano())
	entry := workflow.NodeTypeVersion{
		Type: nodeType, Version: "1.2.3", Capabilities: workflow.CapabilitySet{Cancellable: true},
		DefaultTimeoutMS: 1234, MaximumOutputBytes: 5678,
		Permissions:     workflow.PermissionSet{Network: true},
		Executor:        workflow.ExecutorBinding{Kind: "test.registry", MinimumRunnerVersion: "1.1.0"},
		Inputs:          map[string]workflow.PortDefinition{"request": {Type: workflow.ValueTypeObject, Required: true}},
		Outputs:         map[string]workflow.PortDefinition{"result": {Type: workflow.ValueTypeAny}},
		ConfigSchema:    json.RawMessage(`{"type":"object","required":["value"]}`),
		SensitiveFields: []string{"/config/value"},
		ValidateConfig:  func(json.RawMessage) error { return nil },
	}
	cache, err := workflow.NewRegistry(entry)
	if err != nil {
		t.Fatal(err)
	}
	store := NewNodeRegistry(pool)
	if err = store.EnsureRegistry(ctx, cache); err != nil {
		t.Fatal(err)
	}
	if err = store.EnsureRegistry(ctx, cache); err != nil {
		t.Fatalf("idempotent seed: %v", err)
	}
	entries, err := store.ListNodeTypes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var projected *workflow.NodeTypeVersion
	for index := range entries {
		if entries[index].Type == nodeType {
			projected = &entries[index]
			break
		}
	}
	if projected == nil || projected.Version != entry.Version || projected.Executor != entry.Executor ||
		projected.DefaultTimeoutMS != entry.DefaultTimeoutMS || projected.Inputs["request"] != entry.Inputs["request"] ||
		len(projected.SensitiveFields) != 1 {
		t.Fatalf("projected entry=%+v", projected)
	}
	if _, err = pool.Exec(ctx, `UPDATE node_type_versions SET maximum_output_bytes=1 WHERE node_type=$1 AND version=$2`, nodeType, entry.Version); err == nil {
		t.Fatal("immutable node version accepted an update")
	}

	drifted := entry
	drifted.MaximumOutputBytes++
	driftedCache, err := workflow.NewRegistry(drifted)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.EnsureRegistry(ctx, driftedCache); err == nil {
		t.Fatal("same node version with a changed contract was accepted")
	}
}
