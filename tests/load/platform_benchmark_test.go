package load

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/pazhou/supaflow/internal/definition"
	"github.com/pazhou/supaflow/internal/platform"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

// BenchmarkDraftAutosave measures optimistic Studio document persistence without transport overhead.
func BenchmarkDraftAutosave(b *testing.B) {
	document, err := json.Marshal(benchmarkDefinition())
	if err != nil {
		b.Fatal(err)
	}
	registry := workflow.BuiltinRegistry()
	policy := workflow.CompilePolicy{AllowedCapabilities: workflow.CapabilitySet{Idempotent: true}}
	catalog := platform.NewMemoryCatalog(registry, policy)
	service, err := definition.NewService(definition.NewMemoryStore(), definition.CanonicalAdapter{}, catalog, registry, policy)
	if err != nil {
		b.Fatal(err)
	}
	draft, err := service.CreateDraft(context.Background(), "benchmark-tenant", definition.CreateDraftCommand{
		WorkflowID: "benchmark-flow", ActorID: "loadtest", IdempotencyKey: "draft-create-0001", Document: document,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		draft, err = service.UpdateDraft(context.Background(), "benchmark-tenant", definition.UpdateDraftCommand{
			WorkflowID: "benchmark-flow", ActorID: "loadtest", ExpectedRevision: draft.Revision,
			IdempotencyKey: fmt.Sprintf("autosave-%016d", iteration), Document: document,
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkImmutablePublish measures canonical compile, hash, and immutable catalog publication.
func BenchmarkImmutablePublish(b *testing.B) {
	catalog := platform.NewMemoryCatalog(workflow.BuiltinRegistry(), workflow.CompilePolicy{
		AllowedCapabilities: workflow.CapabilitySet{Idempotent: true},
	})
	definition := benchmarkDefinition()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if _, _, err := catalog.Publish(context.Background(), "benchmark-tenant", definition.ID, definition, fmt.Sprintf("publish-%016d", iteration)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEventCommit measures the state/output/event atomic completion path.
func BenchmarkEventCommit(b *testing.B) {
	store := workflowruntime.NewMemoryStore()
	service := workflowruntime.NewService(store, nil, nil)
	issuer, err := workflowruntime.NewResumeTokenIssuer([]byte("benchmark-event-token-secret-32-bytes"))
	if err != nil {
		b.Fatal(err)
	}
	orchestrator, err := workflowruntime.NewOrchestrator(store, finalExecutor{}, nil, nil, workflowruntime.OrchestratorConfig{
		WorkerID: "benchmark-worker", LeaseDuration: time.Minute, ResumeTokens: issuer,
		RetryPolicy: workflowruntime.RetryPolicy{MaxAttempts: 1, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		created, startErr := service.StartRun(context.Background(), "benchmark-tenant", workflowruntime.StartRunRequest{
			Plan:  workflowruntime.PlanRef{ID: "benchmark-plan", Hash: "benchmark-hash", EntryNodeID: "end", Executor: "builtin.end"},
			Input: json.RawMessage(`{"value":1}`), IdempotencyKey: fmt.Sprintf("event-%016d", iteration),
		})
		if startErr != nil {
			b.Fatal(startErr)
		}
		result, advanceErr := orchestrator.Advance(context.Background(), "benchmark-tenant")
		if advanceErr != nil || !result.Worked || result.Run == nil || result.Run.ID != created.Run.ID {
			b.Fatalf("advance result = %#v, err = %v", result, advanceErr)
		}
	}
}

type finalExecutor struct{}

func (finalExecutor) Execute(context.Context, workflowruntime.ExecutionRequest) (workflowruntime.ExecutionResult, error) {
	return workflowruntime.ExecutionResult{Final: true, Output: &workflowruntime.Output{
		ContentType: "application/json", Size: 11, Inline: json.RawMessage(`{"value":1}`),
	}}, nil
}

func (finalExecutor) Cancel(context.Context, workflowruntime.ExecutionRequest) error { return nil }
