package fenixmigration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// 验证迁移输出写入正式 projection metadata，普通 run event 不携带原始敏感 output。
func TestRunRequestProjectsOutputWithoutEmbeddingItInEvent(t *testing.T) {
	now := time.Now().UTC()
	raw := json.RawMessage(`{"result":"unique-result","api_token":"do-not-copy"}`)
	request, err := runRequest(SourceRun{
		ID: "run", WorkflowID: "flow", Status: "success", Input: json.RawMessage(`{}`),
		Output: raw, CreatedAt: now, StartedAt: now, FinishedAt: now,
	}, "organization", ImportedVersion{VersionID: "version", PlanID: "plan", PlanHash: strings.Repeat("a", 64)}, "admin")
	if err != nil {
		t.Fatal(err)
	}
	if request.Output == nil || !request.Output.Sensitive || request.Output.Size != int64(len(raw)) || len(request.Output.SHA256) != 64 {
		t.Fatalf("output=%+v", request.Output)
	}
	if bytes.Contains(request.Events[0].Payload, []byte("unique-result")) || bytes.Contains(request.Events[0].Payload, []byte("do-not-copy")) {
		t.Fatalf("event leaked raw output: %s", request.Events[0].Payload)
	}
	if !bytes.Contains(request.Events[0].Payload, []byte(request.Output.SHA256)) {
		t.Fatalf("event lacks safe projection metadata: %s", request.Events[0].Payload)
	}
}

// 验证无法验证为有界 JSON 的 legacy output 会在任何目标写入前成为 preflight blocker。
func TestUnsafeLegacyOutputBlocksMigrationBeforeWrites(t *testing.T) {
	now := time.Now().UTC()
	version := uint64(1)
	snapshot := Snapshot{
		Workflows: []SourceWorkflow{{
			ID: "flow", OrganizationID: "org",
			Versions: []SourceVersion{{Version: version, Status: "published", YAML: []byte(
				"schema_version: '1'\nname: flow\nnodes:\n  - {id: finish, type: end}\n"), SourceHash: "v1"}},
		}},
		Runs: []SourceRun{{ID: "run", OrganizationID: "org", WorkflowID: "flow", Version: &version,
			Status: "success", Input: json.RawMessage(`{}`), Output: json.RawMessage(`not-json`),
			CreatedAt: now, StartedAt: now, FinishedAt: now}},
	}
	target := newFakeTarget()
	_, err := Migrate(context.Background(), snapshot, Mapping{Organizations: map[string]string{"org": "organization"}}, target, RunOptions{ActorID: "admin"})
	var preflight *PreflightError
	hasOutputBlocker := false
	if errors.As(err, &preflight) {
		for _, blocker := range preflight.Blockers {
			hasOutputBlocker = hasOutputBlocker || blocker.Blocker.Code == "run_output"
		}
	}
	if err == nil || target.writes != 0 || !hasOutputBlocker {
		t.Fatalf("err=%v writes=%d", err, target.writes)
	}
}
