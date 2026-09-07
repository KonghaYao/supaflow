package load

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/pazhou/supaflow/internal/executor"
	"github.com/pazhou/supaflow/internal/platform"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

// 验证 100/500/1000 节点（含生产硬上限）的编译、发布和完整执行基线，防止大图出现非线性退化或调度遗漏。
func TestNodeScaleBaseline(t *testing.T) {
	for _, size := range []int{100, 500, workflow.MaxDefinitionNodes} {
		t.Run(fmt.Sprintf("nodes_%d", size), func(t *testing.T) {
			startedAt := time.Now()
			definition := linearDefinition(size)
			registry := workflow.BuiltinRegistry()
			policy := workflow.CompilePolicy{AllowedCapabilities: workflow.CapabilitySet{Idempotent: true}}
			catalog := platform.NewMemoryCatalog(registry, policy)
			published, _, err := catalog.Publish(context.Background(), "scale-organization", definition.ID, definition, fmt.Sprintf("publish-%d", size))
			if err != nil {
				t.Fatal(err)
			}
			publishedAt := time.Now()
			store := workflowruntime.NewMemoryStore()
			service := workflowruntime.NewService(store, nil, nil)
			created, err := service.StartRun(context.Background(), "scale-organization", workflowruntime.StartRunRequest{
				Plan: workflowruntime.PlanRef{
					ID: published.ID, Hash: published.Hash, EntryNodeID: "start", Executor: "builtin.start",
				},
				IdempotencyKey: fmt.Sprintf("run-%d", size), Input: json.RawMessage(`{"value":1}`),
			})
			if err != nil {
				t.Fatal(err)
			}
			planExecutor, err := executor.NewPlanExecutor(catalog, executor.Config{})
			if err != nil {
				t.Fatal(err)
			}
			resumeTokens, err := workflowruntime.NewResumeTokenIssuer([]byte("scale-test-resume-token-secret-32-bytes"))
			if err != nil {
				t.Fatal(err)
			}
			orchestrator, err := workflowruntime.NewOrchestrator(store, planExecutor, nil, nil, workflowruntime.OrchestratorConfig{
				WorkerID: "scale-worker", LeaseDuration: time.Minute,
				RetryPolicy:  workflowruntime.RetryPolicy{MaxAttempts: 1, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond},
				ResumeTokens: resumeTokens,
			})
			if err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(20 * time.Second)
			advances := 0
			for {
				run, getErr := service.GetRun(context.Background(), "scale-organization", created.Run.ID)
				if getErr != nil {
					t.Fatal(getErr)
				}
				if workflowruntime.IsTerminalRun(run.Status) {
					if run.Status != workflowruntime.RunSuccess {
						t.Fatalf("run status = %s", run.Status)
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("%d-node execution exceeded 20s after %d advances", size, advances)
				}
				result, advanceErr := orchestrator.Advance(context.Background(), "scale-organization")
				if advanceErr != nil {
					t.Fatal(advanceErr)
				}
				if !result.Worked {
					t.Fatal("orchestrator became idle before terminal state")
				}
				advances++
			}
			if advances != size {
				t.Fatalf("advances = %d, want %d", advances, size)
			}
			t.Logf("nodes=%d compile_publish=%s execute=%s", size, publishedAt.Sub(startedAt), time.Since(publishedAt))
		})
	}
}

// BenchmarkPlanScale records canonical compile/publication allocation baselines for production-sized DAGs.
func BenchmarkPlanScale(b *testing.B) {
	for _, size := range []int{100, 500, workflow.MaxDefinitionNodes} {
		definition := linearDefinition(size)
		b.Run(fmt.Sprintf("nodes_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				if _, err := workflow.Compile(definition, workflow.BuiltinRegistry(), workflow.CompilePolicy{AllowedCapabilities: workflow.CapabilitySet{Idempotent: true}}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func linearDefinition(size int) workflow.WorkflowDefinition {
	nodes := make([]workflow.NodeDefinition, 0, size)
	edges := make([]workflow.EdgeDefinition, 0, size-1)
	nodes = append(nodes, workflow.NodeDefinition{ID: "start", Type: workflow.NodeTypeStart, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)})
	previous, previousPort := "start", "input"
	for index := 1; index < size-1; index++ {
		id := fmt.Sprintf("transform_%04d", index)
		nodes = append(nodes, workflow.NodeDefinition{ID: id, Type: workflow.NodeTypeTransform, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{"select":{"value":"$.missing"}}`)})
		edges = append(edges, workflow.EdgeDefinition{ID: fmt.Sprintf("edge_%04d", index), FromNode: previous, FromPort: previousPort, ToNode: id, ToPort: "input"})
		previous, previousPort = id, "output"
	}
	nodes = append(nodes, workflow.NodeDefinition{ID: "end", Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)})
	edges = append(edges, workflow.EdgeDefinition{ID: fmt.Sprintf("edge_%04d", size-1), FromNode: previous, FromPort: previousPort, ToNode: "end", ToPort: "result"})
	return workflow.WorkflowDefinition{SchemaVersion: workflow.DefinitionSchemaV1, ID: fmt.Sprintf("scale_%d", size), Nodes: nodes, Edges: edges}
}
