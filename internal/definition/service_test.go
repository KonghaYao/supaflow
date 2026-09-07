package definition

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pazhou/supaflow/internal/platform"
	"github.com/pazhou/supaflow/internal/workflow"
)

type fakePublisher struct {
	versions map[string]uint64
	replays  map[string]platform.PublishedPlan
}

func (p *fakePublisher) Publish(_ context.Context, organizationID, workflowID string, definition workflow.WorkflowDefinition, key string) (platform.PublishedPlan, bool, error) {
	if key == "conflict" {
		return platform.PublishedPlan{}, false, platform.ErrPublicationConflict
	}
	if replay, ok := p.replays[organizationID+"\x00"+key]; ok {
		return replay, false, nil
	}
	p.versions[organizationID+"\x00"+workflowID]++
	plan, err := workflow.Compile(definition, workflow.BuiltinRegistry(), workflow.CompilePolicy{
		AllowedCapabilities: workflow.CapabilitySet{Interruptible: true, Idempotent: true},
	})
	if err != nil {
		return platform.PublishedPlan{}, false, err
	}
	published := platform.PublishedPlan{ID: "plan-1", WorkflowID: workflowID, Version: p.versions[organizationID+"\x00"+workflowID], Hash: plan.Hash(), Plan: plan, PublishedAt: time.Unix(10, 0).UTC()}
	p.replays[organizationID+"\x00"+key] = published
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
	created, err := service.CreateDraft(context.Background(), "organization-a", CreateDraftCommand{
		WorkflowID: "flow", ActorID: "editor-a", IdempotencyKey: "create-0001", Document: mustJSON(t, definitionFor("flow")),
	})
	if err != nil || created.Revision != 1 {
		t.Fatalf("create=%+v err=%v", created, err)
	}
	updated, err := service.UpdateDraft(context.Background(), "organization-a", UpdateDraftCommand{
		WorkflowID: "flow", ActorID: "editor-a", IdempotencyKey: "update-0001", ExpectedRevision: 1, Document: mustJSON(t, definitionFor("flow")),
	})
	if err != nil || updated.Revision != 2 {
		t.Fatalf("update=%+v err=%v", updated, err)
	}
	_, err = service.UpdateDraft(context.Background(), "organization-a", UpdateDraftCommand{
		WorkflowID: "flow", ActorID: "editor-b", IdempotencyKey: "update-0002", ExpectedRevision: 1, Document: mustJSON(t, definitionFor("flow")),
	})
	if !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("stale update err=%v", err)
	}
	if _, err := service.GetDraft(context.Background(), "organization-b", "flow"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross organization read err=%v", err)
	}
}

// 验证草稿变更幂等键只重放同一请求，复用于其他内容时明确冲突。
func TestDraftMutationIdempotency(t *testing.T) {
	service, _ := newTestService(t)
	command := CreateDraftCommand{WorkflowID: "flow", ActorID: "editor", IdempotencyKey: "create-0001", Document: mustJSON(t, definitionFor("flow"))}
	first, err := service.CreateDraftWithResult(context.Background(), "organization-a", command)
	if err != nil || !first.Created {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	replay, err := service.CreateDraftWithResult(context.Background(), "organization-a", command)
	if err != nil || replay.Created || replay.Draft.ID != first.Draft.ID || replay.Draft.Revision != first.Draft.Revision {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	command.WorkflowID = "other"
	if _, err := service.CreateDraftWithResult(context.Background(), "organization-a", command); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("key reuse err=%v", err)
	}
}

// 验证并发提交同一创建命令时，原子存储结果只允许一个调用计为创建。
func TestConcurrentDraftCreateHasSingleCreator(t *testing.T) {
	service, _ := newTestService(t)
	command := CreateDraftCommand{WorkflowID: "flow", ActorID: "editor", IdempotencyKey: "concurrent-create", Document: mustJSON(t, definitionFor("flow"))}
	results := make([]CreateDraftResult, 2)
	errs := make([]error, 2)
	var wait sync.WaitGroup
	for index := range results {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			results[index], errs[index] = service.CreateDraftWithResult(context.Background(), "organization-a", command)
		}(index)
	}
	wait.Wait()
	created := 0
	for index, result := range results {
		if errs[index] != nil {
			t.Fatalf("result[%d]=%+v err=%v", index, result, errs[index])
		}
		if result.Created {
			created++
		}
	}
	if created != 1 || results[0].Draft.ID != results[1].Draft.ID {
		t.Fatalf("results=%+v created=%d", results, created)
	}
}

// 验证删除重试可安全重放，且不会因资源已经删除而变成失败。
func TestDeleteDraftIdempotency(t *testing.T) {
	service, _ := newTestService(t)
	_, err := service.CreateDraft(context.Background(), "organization-a", CreateDraftCommand{
		WorkflowID: "flow", ActorID: "editor", IdempotencyKey: "create-0001", Document: mustJSON(t, definitionFor("flow")),
	})
	if err != nil {
		t.Fatal(err)
	}
	command := DeleteDraftCommand{WorkflowID: "flow", ActorID: "editor", IdempotencyKey: "delete-0001", ExpectedRevision: 1}
	if err := service.DeleteDraft(context.Background(), "organization-a", command); err != nil {
		t.Fatal(err)
	}
	if err := service.DeleteDraft(context.Background(), "organization-a", command); err != nil {
		t.Fatalf("delete replay err=%v", err)
	}
}

// 验证发布记录不可变版本，并支持结构化 diff 与回滚式重新发布。
func TestPublishVersionDiffAndRepublish(t *testing.T) {
	service, _ := newTestService(t)
	definition := definitionFor("flow")
	if _, _, err := service.Publish(context.Background(), "organization-a", "flow", definition, "publish-0001"); err != nil {
		t.Fatal(err)
	}
	definition.Metadata = map[string]json.RawMessage{"label": json.RawMessage(`"v2"`)}
	if _, _, err := service.Publish(context.Background(), "organization-a", "flow", definition, "publish-0002"); err != nil {
		t.Fatal(err)
	}
	versions, err := service.ListVersions(context.Background(), "organization-a", "flow", 0, DefaultPageLimit)
	if err != nil || len(versions.Items) != 2 || versions.Items[0].Version != 2 {
		t.Fatalf("versions=%+v err=%v", versions, err)
	}
	latest, err := service.LatestVersion(context.Background(), "organization-a", "flow")
	if err != nil || latest.Version != 2 || latest.VersionID == "" || latest.PlanHash == "" {
		t.Fatalf("latest=%+v err=%v", latest, err)
	}
	diff, err := service.DiffVersions(context.Background(), "organization-a", "flow", 1, 2)
	if err != nil || len(diff.Changes) == 0 {
		t.Fatalf("diff=%+v err=%v", diff, err)
	}
	republished, created, err := service.Republish(context.Background(), "organization-a", "flow", 1, "republish-0001")
	if err != nil || !created || republished.Version != 3 {
		t.Fatalf("republish=%+v created=%v err=%v", republished, created, err)
	}
}

// 验证 Studio 列表使用稳定 cursor、强制 page limit，并且摘要不携带大 document/definition。
func TestDefinitionListsAreBoundedSummaryPages(t *testing.T) {
	service, _ := newTestService(t)
	for index, workflowID := range []string{"alpha", "beta", "gamma"} {
		if _, err := service.CreateDraft(context.Background(), "organization-a", CreateDraftCommand{
			WorkflowID: workflowID, ActorID: "editor", IdempotencyKey: fmt.Sprintf("create-%08d", index), Document: mustJSON(t, definitionFor(workflowID)),
		}); err != nil {
			t.Fatal(err)
		}
	}
	first, err := service.ListDrafts(context.Background(), "organization-a", "", 2)
	if err != nil || len(first.Items) != 2 || first.Items[0].WorkflowID != "alpha" || first.NextCursor != "beta" {
		t.Fatalf("first draft page=%+v err=%v", first, err)
	}
	second, err := service.ListDrafts(context.Background(), "organization-a", first.NextCursor, 2)
	if err != nil || len(second.Items) != 1 || second.Items[0].WorkflowID != "gamma" || second.NextCursor != "" {
		t.Fatalf("second draft page=%+v err=%v", second, err)
	}
	if _, err = service.ListDrafts(context.Background(), "organization-a", "", MaxPageLimit+1); err == nil {
		t.Fatal("oversized draft page was accepted")
	}

	for index := 0; index < 3; index++ {
		value := definitionFor("flow")
		value.Metadata = map[string]json.RawMessage{"revision": json.RawMessage(fmt.Sprintf("%d", index))}
		if _, _, err = service.Publish(context.Background(), "organization-a", "flow", value, fmt.Sprintf("publish-%08d", index)); err != nil {
			t.Fatal(err)
		}
	}
	versions, err := service.ListVersions(context.Background(), "organization-a", "flow", 0, 2)
	if err != nil || len(versions.Items) != 2 || versions.Items[0].Version != 3 || versions.NextCursor != 2 {
		t.Fatalf("version page=%+v err=%v", versions, err)
	}
	last, err := service.ListVersions(context.Background(), "organization-a", "flow", versions.NextCursor, 2)
	if err != nil || len(last.Items) != 1 || last.Items[0].Version != 1 || last.NextCursor != 0 {
		t.Fatalf("last version page=%+v err=%v", last, err)
	}
}

// 验证同租户的草稿硬配额在创建边界拒绝新资源。
func TestCreateDraftEnforcesOrganizationHardLimit(t *testing.T) {
	service, store := newTestService(t)
	command := CreateDraftCommand{WorkflowID: "existing", ActorID: "editor", IdempotencyKey: "create-existing", Document: mustJSON(t, definitionFor("existing"))}
	created, err := service.CreateDraft(context.Background(), "organization-a", command)
	if err != nil {
		t.Fatal(err)
	}
	for index := 1; index < MaxDraftsPerOrganization; index++ {
		workflowID := fmt.Sprintf("flow-%04d", index)
		store.drafts[scope("organization-a", workflowID)] = Draft{OrganizationID: "organization-a", WorkflowID: workflowID}
	}
	if replay, replayErr := service.CreateDraft(context.Background(), "organization-a", command); replayErr != nil || replay.ID != created.ID {
		t.Fatalf("replay at capacity=%+v err=%v", replay, replayErr)
	}
	_, err = service.CreateDraft(context.Background(), "organization-a", CreateDraftCommand{
		WorkflowID: "overflow", ActorID: "editor", IdempotencyKey: "create-overflow", Document: mustJSON(t, definitionFor("overflow")),
	})
	if !errors.Is(err, ErrOrganizationDraftLimit) {
		t.Fatalf("CreateDraft() error=%v, want ErrOrganizationDraftLimit", err)
	}
}

// 验证迁移 seam 必须顺序导入版本，从而保持 Fenix 的历史版本号。
func TestImportVersionPreservesSourceSequence(t *testing.T) {
	service, _ := newTestService(t)
	definition := definitionFor("flow")
	_, _, err := service.ImportVersion(context.Background(), "organization-a", "flow", ImportVersionCommand{
		Definition: definition, SourceVersion: 2, IdempotencyKey: "import-0002",
	})
	if !errors.Is(err, ErrVersionSequenceConflict) {
		t.Fatalf("out of order import err=%v", err)
	}
	first, created, err := service.ImportVersion(context.Background(), "organization-a", "flow", ImportVersionCommand{
		Definition: definition, SourceVersion: 1, IdempotencyKey: "import-0001",
	})
	if err != nil || !created || first.Version != 1 {
		t.Fatalf("first=%+v created=%v err=%v", first, created, err)
	}
	definition.Metadata = map[string]json.RawMessage{"source": json.RawMessage(`"v2"`)}
	second, created, err := service.ImportVersion(context.Background(), "organization-a", "flow", ImportVersionCommand{
		Definition: definition, SourceVersion: 2, IdempotencyKey: "import-0002",
	})
	if err != nil || !created || second.Version != 2 {
		t.Fatalf("second=%+v created=%v err=%v", second, created, err)
	}
	replay, created, err := service.ImportVersion(context.Background(), "organization-a", "flow", ImportVersionCommand{
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
