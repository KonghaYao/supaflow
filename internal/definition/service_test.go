package definition

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/pazhou/supaflow/internal/platform"
	"github.com/pazhou/supaflow/internal/workflow"
)

type fakePublisher struct {
	versions map[string]uint64
	replays  map[string]platform.PublishedPlan
}

func (p *fakePublisher) Publish(_ context.Context, tenantID, workflowID string, definition workflow.WorkflowDefinition, key string) (platform.PublishedPlan, bool, error) {
	if key == "conflict" {
		return platform.PublishedPlan{}, false, platform.ErrPublicationConflict
	}
	if replay, ok := p.replays[tenantID+"\x00"+key]; ok {
		return replay, false, nil
	}
	p.versions[tenantID+"\x00"+workflowID]++
	plan, err := workflow.Compile(definition, workflow.BuiltinRegistry(), workflow.CompilePolicy{
		AllowedCapabilities: workflow.CapabilitySet{Interruptible: true, Idempotent: true},
	})
	if err != nil {
		return platform.PublishedPlan{}, false, err
	}
	published := platform.PublishedPlan{ID: "plan-1", WorkflowID: workflowID, Version: p.versions[tenantID+"\x00"+workflowID], Hash: plan.Hash(), Plan: plan, PublishedAt: time.Unix(10, 0).UTC()}
	p.replays[tenantID+"\x00"+key] = published
	return published, true, nil
}

func definitionFor(id string) workflow.WorkflowDefinition {
	return workflow.WorkflowDefinition{SchemaVersion: workflow.DefinitionSchemaV1, ID: id,
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Type: workflow.NodeTypeStart, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
			{ID: "end", Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
		},
		Edges: []workflow.EdgeDefinition{{ID: "edge", FromNode: "start", FromPort: "input", ToNode: "end", ToPort: "result"}},
	}
}

func newTestService(t *testing.T) (*Service, *MemoryStore) {
	t.Helper()
	store := NewMemoryStore()
	service, err := NewService(store, CanonicalAdapter{}, &fakePublisher{versions: map[string]uint64{}, replays: map[string]platform.PublishedPlan{}}, workflow.BuiltinRegistry(), workflow.CompilePolicy{
		AllowedCapabilities: workflow.CapabilitySet{Streaming: true, Cancellable: true, Interruptible: true, Idempotent: true},
		AllowedPermissions:  workflow.PermissionSet{Network: true, Credentials: true, Filesystem: true, Compute: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	return service, store
}

// 验证草稿 CRUD 使用乐观 revision，过期写入不会覆盖较新的设计。
func TestDraftLifecycleAndOptimisticConflict(t *testing.T) {
	service, _ := newTestService(t)
	created, err := service.CreateDraft(context.Background(), "tenant-a", CreateDraftCommand{
		WorkflowID: "flow", ActorID: "editor-a", IdempotencyKey: "create-0001", Document: mustJSON(t, definitionFor("flow")),
	})
	if err != nil || created.Revision != 1 {
		t.Fatalf("create=%+v err=%v", created, err)
	}
	updated, err := service.UpdateDraft(context.Background(), "tenant-a", UpdateDraftCommand{
		WorkflowID: "flow", ActorID: "editor-a", IdempotencyKey: "update-0001", ExpectedRevision: 1, Document: mustJSON(t, definitionFor("flow")),
	})
	if err != nil || updated.Revision != 2 {
		t.Fatalf("update=%+v err=%v", updated, err)
	}
	_, err = service.UpdateDraft(context.Background(), "tenant-a", UpdateDraftCommand{
		WorkflowID: "flow", ActorID: "editor-b", IdempotencyKey: "update-0002", ExpectedRevision: 1, Document: mustJSON(t, definitionFor("flow")),
	})
	if !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("stale update err=%v", err)
	}
	if _, err := service.GetDraft(context.Background(), "tenant-b", "flow"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross tenant read err=%v", err)
	}
}

// 验证草稿变更幂等键只重放同一请求，复用于其他内容时明确冲突。
func TestDraftMutationIdempotency(t *testing.T) {
	service, _ := newTestService(t)
	command := CreateDraftCommand{WorkflowID: "flow", ActorID: "editor", IdempotencyKey: "create-0001", Document: mustJSON(t, definitionFor("flow"))}
	first, err := service.CreateDraft(context.Background(), "tenant-a", command)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := service.CreateDraft(context.Background(), "tenant-a", command)
	if err != nil || replay.ID != first.ID || replay.Revision != first.Revision {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	command.WorkflowID = "other"
	if _, err := service.CreateDraft(context.Background(), "tenant-a", command); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("key reuse err=%v", err)
	}
}

// 验证删除重试可安全重放，且不会因资源已经删除而变成失败。
func TestDeleteDraftIdempotency(t *testing.T) {
	service, _ := newTestService(t)
	_, err := service.CreateDraft(context.Background(), "tenant-a", CreateDraftCommand{
		WorkflowID: "flow", ActorID: "editor", IdempotencyKey: "create-0001", Document: mustJSON(t, definitionFor("flow")),
	})
	if err != nil {
		t.Fatal(err)
	}
	command := DeleteDraftCommand{WorkflowID: "flow", ActorID: "editor", IdempotencyKey: "delete-0001", ExpectedRevision: 1}
	if err := service.DeleteDraft(context.Background(), "tenant-a", command); err != nil {
		t.Fatal(err)
	}
	if err := service.DeleteDraft(context.Background(), "tenant-a", command); err != nil {
		t.Fatalf("delete replay err=%v", err)
	}
}

// 验证发布记录不可变版本，并支持结构化 diff 与回滚式重新发布。
func TestPublishVersionDiffAndRepublish(t *testing.T) {
	service, _ := newTestService(t)
	definition := definitionFor("flow")
	if _, _, err := service.Publish(context.Background(), "tenant-a", "flow", definition, "publish-0001"); err != nil {
		t.Fatal(err)
	}
	definition.Metadata = map[string]json.RawMessage{"label": json.RawMessage(`"v2"`)}
	if _, _, err := service.Publish(context.Background(), "tenant-a", "flow", definition, "publish-0002"); err != nil {
		t.Fatal(err)
	}
	versions, err := service.ListVersions(context.Background(), "tenant-a", "flow")
	if err != nil || len(versions) != 2 || versions[0].Version != 2 {
		t.Fatalf("versions=%+v err=%v", versions, err)
	}
	latest, err := service.LatestVersion(context.Background(), "tenant-a", "flow")
	if err != nil || latest.Version != 2 || latest.VersionID == "" || latest.PlanHash == "" {
		t.Fatalf("latest=%+v err=%v", latest, err)
	}
	diff, err := service.DiffVersions(context.Background(), "tenant-a", "flow", 1, 2)
	if err != nil || len(diff.Changes) == 0 {
		t.Fatalf("diff=%+v err=%v", diff, err)
	}
	republished, created, err := service.Republish(context.Background(), "tenant-a", "flow", 1, "republish-0001")
	if err != nil || !created || republished.Version != 3 {
		t.Fatalf("republish=%+v created=%v err=%v", republished, created, err)
	}
}

// 验证迁移 seam 必须顺序导入版本，从而保持 Fenix 的历史版本号。
func TestImportVersionPreservesSourceSequence(t *testing.T) {
	service, _ := newTestService(t)
	definition := definitionFor("flow")
	_, _, err := service.ImportVersion(context.Background(), "tenant-a", "flow", ImportVersionCommand{
		Definition: definition, SourceVersion: 2, IdempotencyKey: "import-0002",
	})
	if !errors.Is(err, ErrVersionSequenceConflict) {
		t.Fatalf("out of order import err=%v", err)
	}
	first, created, err := service.ImportVersion(context.Background(), "tenant-a", "flow", ImportVersionCommand{
		Definition: definition, SourceVersion: 1, IdempotencyKey: "import-0001",
	})
	if err != nil || !created || first.Version != 1 {
		t.Fatalf("first=%+v created=%v err=%v", first, created, err)
	}
	definition.Metadata = map[string]json.RawMessage{"source": json.RawMessage(`"v2"`)}
	second, created, err := service.ImportVersion(context.Background(), "tenant-a", "flow", ImportVersionCommand{
		Definition: definition, SourceVersion: 2, IdempotencyKey: "import-0002",
	})
	if err != nil || !created || second.Version != 2 {
		t.Fatalf("second=%+v created=%v err=%v", second, created, err)
	}
	replay, created, err := service.ImportVersion(context.Background(), "tenant-a", "flow", ImportVersionCommand{
		Definition: definitionFor("flow"), SourceVersion: 1, IdempotencyKey: "import-0001",
	})
	if err != nil || created || replay.Version != 1 || replay.Hash != first.Hash {
		t.Fatalf("historical replay=%+v created=%v err=%v", replay, created, err)
	}
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
