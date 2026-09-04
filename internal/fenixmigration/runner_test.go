package fenixmigration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/pazhou/supaflow/internal/platformops"
	"github.com/pazhou/supaflow/internal/workflow"
)

type fakeTarget struct {
	versions  map[string]ImportedVersion
	drafts    map[string]bool
	triggers  map[string]bool
	runs      map[string]bool
	writes    int
	tenantIDs []string
}

func newFakeTarget() *fakeTarget {
	return &fakeTarget{versions: map[string]ImportedVersion{}, drafts: map[string]bool{}, triggers: map[string]bool{}, runs: map[string]bool{}}
}
func (f *fakeTarget) EnsureTenant(_ context.Context, tenantID, _ string) error {
	f.tenantIDs = append(f.tenantIDs, tenantID)
	return nil
}
func (f *fakeTarget) ImportVersion(_ context.Context, tenant, workflowID string, version uint64, _ workflow.WorkflowDefinition, key string) (ImportedVersion, bool, error) {
	if value, ok := f.versions[key]; ok {
		return value, false, nil
	}
	value := ImportedVersion{VersionID: fmt.Sprintf("version-%d", version), PlanID: fmt.Sprintf("plan-%d", version), PlanHash: fmt.Sprintf("%064d", version), Version: version}
	f.versions[key], f.writes = value, f.writes+1
	return value, true, nil
}
func (f *fakeTarget) ImportDraft(_ context.Context, _, workflowID, _ string, _ workflow.WorkflowDefinition, key string) (bool, error) {
	if f.drafts[key] {
		return false, nil
	}
	f.drafts[key], f.writes = true, f.writes+1
	return true, nil
}
func (f *fakeTarget) ImportTrigger(_ context.Context, request platformops.ImportTriggerRequest) (bool, error) {
	if f.triggers[request.TriggerID] {
		return false, nil
	}
	f.triggers[request.TriggerID], f.writes = true, f.writes+1
	return true, nil
}
func (f *fakeTarget) ImportRun(_ context.Context, request platformops.ImportRunProjectionRequest) (bool, error) {
	if f.runs[request.RunID] {
		return false, nil
	}
	f.runs[request.RunID], f.writes = true, f.writes+1
	return true, nil
}

// 验证完整迁移顺序可从头安全重放，版本、草稿、触发器和终态历史都有独立幂等证据。
func TestMigrateIsRestartable(t *testing.T) {
	now := time.Now().UTC()
	yaml := []byte("schema_version: '1'\nname: flow\nnodes:\n  - {id: finish, type: end}\n")
	version := uint64(1)
	snapshot := Snapshot{
		Workflows: []SourceWorkflow{{ID: "flow", OrganizationID: "org", Name: "flow", Versions: []SourceVersion{{Version: 0, Status: "draft", YAML: yaml, SourceHash: "draft"}, {Version: 1, Status: "published", YAML: yaml, SourceHash: "v1"}}}},
		Triggers:  []SourceTrigger{{ID: "trigger", OrganizationID: "org", WorkflowID: "flow", Kind: "webhook", PublicHash: "abcdefghijklmnop", Config: json.RawMessage(`{}`), Enabled: true, CreatedAt: now}},
		Runs:      []SourceRun{{ID: "run", OrganizationID: "org", WorkflowID: "flow", Version: &version, Status: "SUCCESS", Input: json.RawMessage(`{"secret":"redact downstream"}`), Output: json.RawMessage(`{"ok":true}`), StartedAt: now, FinishedAt: now, CreatedAt: now}},
	}
	mapping := Mapping{Tenants: map[string]string{"org": "tenant"}, TriggerCredentials: map[string]string{"trigger": "credential"}}
	target := newFakeTarget()
	first, err := Migrate(context.Background(), snapshot, mapping, target, RunOptions{ActorID: "admin"})
	if err != nil {
		t.Fatal(err)
	}
	if first.CreatedDefinitions != 1 || first.CreatedDrafts != 1 || first.CreatedTriggers != 1 || first.CreatedRuns != 1 {
		t.Fatalf("first report = %#v", first)
	}
	second, err := Migrate(context.Background(), snapshot, mapping, target, RunOptions{ActorID: "admin"})
	if err != nil {
		t.Fatal(err)
	}
	if second.ReplayedDefinitions != 1 || second.ReplayedDrafts != 1 || second.ReplayedTriggers != 1 || second.ReplayedRuns != 1 {
		t.Fatalf("second report = %#v", second)
	}
}

// 验证 dry-run 会汇总所有 preflight blocker 且不触发任何目标写入。
func TestMigratePreflightHasNoPartialWrites(t *testing.T) {
	snapshot := Snapshot{Workflows: []SourceWorkflow{{ID: "flow", OrganizationID: "unmapped", Versions: []SourceVersion{{Version: 1, Status: "published", YAML: []byte("invalid")}}}}}
	target := newFakeTarget()
	_, err := Migrate(context.Background(), snapshot, Mapping{}, target, RunOptions{ActorID: "admin", DryRun: true})
	if err == nil || target.writes != 0 {
		t.Fatalf("err = %v, writes = %d", err, target.writes)
	}
}

// 验证只有触发器或历史运行的组织仍须显式映射租户，并会在目标写入前创建租户边界。
func TestMigrateRequiresAndEnsuresTenantWithoutWorkflowRows(t *testing.T) {
	snapshot := Snapshot{Triggers: []SourceTrigger{{ID: "trigger", OrganizationID: "org", WorkflowID: "flow", Kind: "event", Config: json.RawMessage(`{"eventType":"created"}`)}}}
	target := newFakeTarget()
	if _, err := Migrate(context.Background(), snapshot, Mapping{}, target, RunOptions{ActorID: "admin"}); err == nil {
		t.Fatal("expected missing tenant mapping blocker")
	}
	if target.writes != 0 || len(target.tenantIDs) != 0 {
		t.Fatalf("preflight performed writes: %#v", target)
	}

	report, err := Migrate(context.Background(), snapshot, Mapping{Tenants: map[string]string{"org": "tenant"}}, target, RunOptions{ActorID: "admin"})
	if err != nil {
		t.Fatal(err)
	}
	if report.CreatedTriggers != 1 || len(target.tenantIDs) != 1 || target.tenantIDs[0] != "tenant" {
		t.Fatalf("report = %#v, tenants = %#v", report, target.tenantIDs)
	}
}
