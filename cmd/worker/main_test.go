package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/pazhou/supaflow/internal/executor"
	"github.com/pazhou/supaflow/internal/platform"
	"github.com/pazhou/supaflow/internal/platformops"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

// 验证 worker 的租户与 egress allowlist 配置去空、去重且保持顺序。
func TestParseList(t *testing.T) {
	items := parseList(" organization-a,organization-b,organization-a, ,organization-c ")
	if len(items) != 3 || items[0] != "organization-a" || items[2] != "organization-c" {
		t.Fatalf("items=%v", items)
	}
}

// 验证 worker 仅从 immutable node 配置和可信 adapter 映射提取最小凭据集合。
func TestCredentialGrantsForPinnedNodes(t *testing.T) {
	tests := []struct {
		name string
		node workflow.PlanNode
		want []nodeCredentialGrant
	}{
		{name: "http", node: workflow.PlanNode{Executor: workflow.ExecutorBinding{Kind: "http.request"}, Config: json.RawMessage(`{"credentialRef":"http-cred"}`)}, want: []nodeCredentialGrant{{reference: "http-cred", purpose: httpCredentialPurpose}}},
		{name: "model", node: workflow.PlanNode{Executor: workflow.ExecutorBinding{Kind: "eino.llm"}, Config: json.RawMessage(`{"name":"chat"}`)}, want: []nodeCredentialGrant{{reference: "model-cred", purpose: providerCredentialPurpose}}},
		{name: "isolation", node: workflow.PlanNode{Executor: workflow.ExecutorBinding{Kind: "isolated.shell"}, Config: json.RawMessage(`{"env":["TOKEN=env-cred","OTHER=second-cred","DUP=env-cred"]}`)}, want: []nodeCredentialGrant{{reference: "env-cred", purpose: isolationCredentialPurpose}, {reference: "second-cred", purpose: isolationCredentialPurpose}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			grants, err := credentialGrantsForNode(test.node, map[string]string{"chat": "model-cred"})
			if err != nil || len(grants) != len(test.want) {
				t.Fatalf("grants=%+v err=%v", grants, err)
			}
			for index := range grants {
				if grants[index] != test.want[index] {
					t.Fatalf("grants=%+v want=%+v", grants, test.want)
				}
			}
		})
	}
}

// 验证生产 worker 同时装配凭据、Eino、子流程和独立隔离进程，而非只注册节点名称。
func TestConfigureExecutionWiresEveryExternalAdapter(t *testing.T) {
	t.Setenv("SUPAFLOW_CREDENTIAL_MASTER_KEY", "0707070707070707070707070707070707070707070707070707070707070707")
	t.Setenv("SUPAFLOW_CREDENTIAL_KEY_ID", "worker-key-1")
	t.Setenv("SUPAFLOW_ISOLATION_BINARY", "/usr/bin/false")
	t.Setenv("SUPAFLOW_EINO_MODELS_JSON", `[{"name":"chat","credentialRef":"credential","model":"gpt"}]`)
	t.Setenv("SUPAFLOW_EINO_TOOLS_JSON", `[{"name":"pick","kind":"json.project"}]`)
	t.Setenv("SUPAFLOW_EINO_AGENTS_JSON", `[{"name":"analyst","model":"chat","tools":["pick"],"system":"Return verified values.","maxSteps":4}]`)
	catalog := platform.NewMemoryCatalog(workflow.BuiltinRegistry(), workflow.CompilePolicy{
		AllowedCapabilities: workflow.CapabilitySet{Streaming: true, Cancellable: true, Interruptible: true, Idempotent: true},
		AllowedPermissions:  workflow.PermissionSet{Network: true, Credentials: true, Compute: true},
	})
	runs := workflowruntime.NewService(workflowruntime.NewMemoryStore(), nil, nil)
	config, err := configureExecution(nil, catalog, runs, []string{"example.com"}, &recordingOutputs{}, func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if config.CredentialResolver == nil || config.Models == nil || config.Agents == nil || config.Tools == nil || config.Subworkflows == nil || config.Isolation == nil || config.Outputs == nil {
		t.Fatalf("external adapter wiring is incomplete: %+v", config)
	}
	if _, same := config.Agents.(*executor.EinoRegistry); same {
		t.Fatal("model registry must not masquerade as the agent registry")
	}
}

// 验证 Agent 引用未知 model/tool 时 worker 在启动阶段保守拒绝。
func TestConfigureExecutionRejectsUnknownAgentDependency(t *testing.T) {
	t.Setenv("SUPAFLOW_CREDENTIAL_MASTER_KEY", "0707070707070707070707070707070707070707070707070707070707070707")
	t.Setenv("SUPAFLOW_CREDENTIAL_KEY_ID", "worker-key-1")
	t.Setenv("SUPAFLOW_ISOLATION_BINARY", "/usr/bin/false")
	t.Setenv("SUPAFLOW_EINO_AGENTS_JSON", `[{"name":"bad","model":"missing","tools":["missing"],"system":"finite","maxSteps":2}]`)
	catalog := platform.NewMemoryCatalog(workflow.BuiltinRegistry(), workflow.CompilePolicy{})
	runs := workflowruntime.NewService(workflowruntime.NewMemoryStore(), nil, nil)
	if _, err := configureExecution(nil, catalog, runs, nil, &recordingOutputs{}, func(context.Context, string, []string) error { return nil }); err == nil {
		t.Fatal("expected invalid agent dependency error")
	}
}

// 验证 worker 启动配置拒绝向明文 HTTP（包括 loopback）发送模型凭据。
func TestConfigureExecutionRejectsPlaintextModelEndpoint(t *testing.T) {
	t.Setenv("SUPAFLOW_CREDENTIAL_MASTER_KEY", "0707070707070707070707070707070707070707070707070707070707070707")
	t.Setenv("SUPAFLOW_CREDENTIAL_KEY_ID", "worker-key-1")
	t.Setenv("SUPAFLOW_ISOLATION_BINARY", "/usr/bin/false")
	t.Setenv("SUPAFLOW_EINO_MODELS_JSON", `[{"name":"chat","credentialRef":"credential","model":"gpt","baseUrl":"http://127.0.0.1:8080/v1"}]`)
	catalog := platform.NewMemoryCatalog(workflow.BuiltinRegistry(), workflow.CompilePolicy{})
	runs := workflowruntime.NewService(workflowruntime.NewMemoryStore(), nil, nil)
	if _, err := configureExecution(nil, catalog, runs, nil, &recordingOutputs{}, func(context.Context, string, []string) error { return nil }); err == nil {
		t.Fatal("expected plaintext model endpoint to be rejected")
	}
}

// 验证 worker 对对象输出全局上限使用有限生产边界，错误配置在启动前 fail-closed。
func TestConfigureWorkerOutputsRejectsUnsafeMaximum(t *testing.T) {
	t.Setenv("SUPAFLOW_OBJECT_STORE_DIR", t.TempDir())
	for _, value := range []string{"0", "not-a-number", "1073741825"} {
		t.Setenv("SUPAFLOW_OUTPUT_MAX_BYTES", value)
		if _, err := configureWorkerOutputs(nil); err == nil {
			t.Fatalf("SUPAFLOW_OUTPUT_MAX_BYTES=%q unexpectedly accepted", value)
		}
	}
}

type managedOutputBackendStub struct {
	value          []byte
	metadata       platformops.ManagedObject
	organizationID string
	runID          string
	calls          int
}

func (*managedOutputBackendStub) Store(context.Context, string, string, string, *time.Time, io.Reader) (platformops.ManagedObject, error) {
	return platformops.ManagedObject{}, errors.New("not implemented")
}

func (s *managedOutputBackendStub) OpenRunOutput(_ context.Context, organizationID, runID string) (io.ReadCloser, platformops.ManagedObject, error) {
	s.organizationID, s.runID, s.calls = organizationID, runID, s.calls+1
	return io.NopCloser(bytes.NewReader(s.value)), s.metadata, nil
}

func (*managedOutputBackendStub) Discard(context.Context, string, string) error { return nil }

// 验证 worker 只经租户/run 关系读取对象输出，并校验 immutable metadata、大小与 JSON 边界。
func TestManagedOutputsReadRunOutputEnforcesProjectionBoundary(t *testing.T) {
	value := []byte(`{"secret":true}`)
	digest := sha256.Sum256(value)
	expected := workflowruntime.RunOutput{External: true, Sensitive: true, ContentType: "application/json", Size: int64(len(value)), SHA256: hex.EncodeToString(digest[:])}
	backend := &managedOutputBackendStub{value: value, metadata: platformops.ManagedObject{
		OrganizationID: "organization_a", RunID: "run_child", Kind: "output", Status: "active",
		ContentType: expected.ContentType, Size: expected.Size, SHA256: expected.SHA256,
	}}
	manager := &managedOutputs{manager: backend, maximum: 1024}
	output, err := manager.ReadRunOutput(context.Background(), "organization_a", "run_child", expected)
	if err != nil || string(output) != string(value) || backend.organizationID != "organization_a" || backend.runID != "run_child" {
		t.Fatalf("output=%s scope=%s/%s error=%v", output, backend.organizationID, backend.runID, err)
	}

	backend.metadata.Size++
	if _, err = manager.ReadRunOutput(context.Background(), "organization_a", "run_child", expected); err == nil {
		t.Fatal("mismatched immutable output metadata was accepted")
	}
	oversized := expected
	oversized.Size = 2048
	before := backend.calls
	if _, err = manager.ReadRunOutput(context.Background(), "organization_a", "run_child", oversized); err == nil || backend.calls != before {
		t.Fatal("oversized output reached object storage")
	}
}

type recordingOutputs struct{}

type workerNodeTypeCatalogStub struct {
	entries []workflow.NodeTypeVersion
	calls   int
}

func (s *workerNodeTypeCatalogStub) ListNodeTypes(context.Context) ([]workflow.NodeTypeVersion, error) {
	s.calls++
	return s.entries, nil
}

// 验证 worker 启动检查实际读取持久化 registry，并在开始 claim 前拒绝契约漂移。
func TestValidateWorkerNodeRegistryReadsDurableProjection(t *testing.T) {
	cache := workflow.BuiltinRegistry()
	projection := &workerNodeTypeCatalogStub{entries: cache.Entries()}
	if err := validateWorkerNodeRegistry(context.Background(), projection, cache); err != nil {
		t.Fatal(err)
	}
	if projection.calls != 1 {
		t.Fatalf("projection calls=%d", projection.calls)
	}
	projection.entries[0].Executor.MinimumRunnerVersion = "99.0.0"
	if err := validateWorkerNodeRegistry(context.Background(), projection, cache); err == nil {
		t.Fatal("incompatible durable registry was accepted")
	}
}

func (*recordingOutputs) StoreOutput(context.Context, workflowruntime.OrganizationID, string, string, []byte) (executor.ExternalOutput, error) {
	return executor.ExternalOutput{}, nil
}

func (*recordingOutputs) DiscardOutput(context.Context, workflowruntime.OrganizationID, string) error {
	return nil
}

func (*recordingOutputs) ReadRunOutput(context.Context, workflowruntime.OrganizationID, string, workflowruntime.RunOutput) (json.RawMessage, error) {
	return json.RawMessage(`null`), nil
}
