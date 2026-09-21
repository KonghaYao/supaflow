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
	// 复用同一个键提交另一份合法文档：文档必须与新的 workflow id 一致，否则会先被文档校验拦下，
	// 这条用例要断言的幂等键冲突就测不到了。
	command.Document = mustJSON(t, definitionFor("other"))
	if _, err := service.CreateDraftWithResult(context.Background(), "organization-a", command); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("key reuse err=%v", err)
	}
}

// 验证草稿在写入时就校验文档：无法解码、或文档 id 与资源不一致的文档不会落库，
// 否则它会先存下来，再让之后每次编译、试运行和发布草稿都失败。
func TestDraftWriteRejectsUnusableDocument(t *testing.T) {
	service, _ := newTestService(t)
	cases := []struct {
		name     string
		document json.RawMessage
	}{
		{name: "workflow id mismatch", document: mustJSON(t, definitionFor("other"))},
		{name: "unknown field", document: json.RawMessage(`{"schemaVersion":"v1","id":"flow","nodes":[],"edges":[],"unexpected":true}`)},
		{name: "unknown node field", document: json.RawMessage(`{"schemaVersion":"v1","id":"flow","nodes":[{"id":"start","type":"start","version":"1.0.0","config":{},"bogus":true}],"edges":[]}`)},
	}
	for index, testCase := range cases {
		_, err := service.CreateDraft(context.Background(), "organization-a", CreateDraftCommand{
			WorkflowID: "flow", ActorID: "editor", IdempotencyKey: fmt.Sprintf("unusable-create-%02d", index), Document: testCase.document,
		})
		var requestError *RequestError
		if !errors.As(err, &requestError) || requestError.Code != "invalid_workflow" {
			t.Fatalf("%s create err=%v", testCase.name, err)
		}
		if _, err := service.GetDraft(context.Background(), "organization-a", "flow"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("%s stored a rejected document: err=%v", testCase.name, err)
		}
	}
}

// 验证写入边界拒绝编译器永远不接受的 workflow id：workflow.ValidateDefinition 只认「字母或
// 数字开头、仅含字母数字下划线连字符」的 id，此前写入边界不校验，于是「建得出、永远编不过」的
// 草稿永久堆在库里，调用方要到 compile / dry-run / publish 才知道。拒绝时上报的 code、path 与
// message 必须与编译器逐字一致，否则同一条件在两步会读成两个不同的问题。
func TestDraftWriteRejectsUncompilableWorkflowID(t *testing.T) {
	service, store := newTestService(t)
	for index, workflowID := range []string{"my flow", "nested/id", "-leading", "trailing.", "点"} {
		document := mustJSON(t, definitionFor(workflowID))
		_, err := service.CreateDraft(context.Background(), "organization-a", CreateDraftCommand{
			WorkflowID: workflowID, ActorID: "editor", IdempotencyKey: fmt.Sprintf("invalid-id-create-%02d", index), Document: document,
		})
		var validation *workflow.ValidationError
		if !errors.As(err, &validation) || len(validation.Issues) != 1 {
			t.Fatalf("%s create err=%v", workflowID, err)
		}
		compiled := workflow.ValidateDefinition(definitionFor(workflowID), workflow.BuiltinRegistry(), workflow.CompilePolicy{})
		var expected *workflow.ValidationError
		if !errors.As(compiled, &expected) || expected.Issues[0] != validation.Issues[0] {
			t.Fatalf("%s write boundary issue=%+v compiler issue=%+v", workflowID, validation.Issues, expected)
		}
		if validation.Issues[0].Code != "invalid_id" || validation.Issues[0].Path != "id" {
			t.Fatalf("%s issue=%+v", workflowID, validation.Issues[0])
		}
		if _, err = service.GetDraft(context.Background(), "organization-a", workflowID); !errors.Is(err, ErrNotFound) {
			t.Fatalf("%s stored a draft the compiler will refuse: err=%v", workflowID, err)
		}
		// 更新路径同样在写库前拦住，不需要先写坏一次草稿。
		_, err = service.UpdateDraft(context.Background(), "organization-a", UpdateDraftCommand{
			WorkflowID: workflowID, ActorID: "editor", IdempotencyKey: fmt.Sprintf("invalid-id-update-%02d", index),
			ExpectedRevision: 1, Document: document,
		})
		if !errors.As(err, &validation) {
			t.Fatalf("%s update err=%v", workflowID, err)
		}
	}
	// 对照组：合法 id 必须照常写下来，边界校验不能把能编译的草稿挡在门外。
	if _, err := service.CreateDraft(context.Background(), "organization-a", CreateDraftCommand{
		WorkflowID: "Flow-1_ok", ActorID: "editor", IdempotencyKey: "valid-id-create", Document: mustJSON(t, definitionFor("Flow-1_ok")),
	}); err != nil {
		t.Fatalf("valid id create err=%v", err)
	}
	if _, err := store.GetDraft(context.Background(), "organization-a", "Flow-1_ok"); err != nil {
		t.Fatalf("valid id not stored: err=%v", err)
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
