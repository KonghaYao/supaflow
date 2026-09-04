package executor

import (
	"encoding/json"
	"testing"

	"github.com/pazhou/supaflow/internal/workflow"
)

// 验证 worker 只接受与本地 adapter cache、executor 表和 runner 版本完全兼容的 DB registry。
func TestValidateNodeRegistryCompatibilityFailsClosed(t *testing.T) {
	cache := workflow.BuiltinRegistry()
	compatible := cache.Entries()
	if err := ValidateNodeRegistryCompatibility(compatible, cache, WorkerRunnerVersion); err != nil {
		t.Fatalf("compatible registry: %v", err)
	}

	tests := []struct {
		name   string
		mutate func([]workflow.NodeTypeVersion) []workflow.NodeTypeVersion
	}{
		{name: "contract drift", mutate: func(entries []workflow.NodeTypeVersion) []workflow.NodeTypeVersion {
			entries[0].MaximumOutputBytes++
			return entries
		}},
		{name: "runner too old", mutate: func(entries []workflow.NodeTypeVersion) []workflow.NodeTypeVersion {
			entries[0].Executor.MinimumRunnerVersion = "99.0.0"
			return entries
		}},
		{name: "unsupported executor", mutate: func(entries []workflow.NodeTypeVersion) []workflow.NodeTypeVersion {
			custom := workflow.NodeTypeVersion{
				Type: "custom", Version: "1.0.0", DefaultTimeoutMS: 1, MaximumOutputBytes: 1,
				Executor:     workflow.ExecutorBinding{Kind: "custom.unsupported", MinimumRunnerVersion: "1.0.0"},
				ConfigSchema: json.RawMessage(`{"type":"object"}`), SensitiveFields: []string{}, Migrations: []workflow.NodeMigrationMetadata{},
			}
			return append(entries, custom)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			entries := test.mutate(cache.Entries())
			if err := ValidateNodeRegistryCompatibility(entries, cache, WorkerRunnerVersion); err == nil {
				t.Fatal("incompatible registry was accepted")
			}
		})
	}
}
