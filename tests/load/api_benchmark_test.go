package load

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/pazhou/supaflow/internal/api"
	"github.com/pazhou/supaflow/internal/platform"
	"github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

const benchmarkAPIKey = "benchmark-api-key-at-least-24-bytes"

func benchmarkDefinition() workflow.WorkflowDefinition {
	return workflow.WorkflowDefinition{
		SchemaVersion: workflow.DefinitionSchemaV1,
		ID:            "benchmark-flow",
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Type: workflow.NodeTypeStart, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
			{ID: "end", Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
		},
		Edges: []workflow.EdgeDefinition{{ID: "edge", FromNode: "start", FromPort: "input", ToNode: "end", ToPort: "result"}},
	}
}

func benchmarkHandler(tb testing.TB) (http.Handler, platform.PublishedPlan) {
	tb.Helper()
	auth, err := api.NewMasterKeyAuthenticator(benchmarkAPIKey)
	if err != nil {
		tb.Fatal(err)
	}
	catalog := platform.NewMemoryCatalog(workflow.BuiltinRegistry(), workflow.CompilePolicy{
		AllowedCapabilities: workflow.CapabilitySet{Idempotent: true},
	})
	published, _, err := catalog.Publish(context.Background(), "benchmark-tenant", "benchmark-flow", benchmarkDefinition(), "benchmark-publish")
	if err != nil {
		tb.Fatal(err)
	}
	resumeTokens, err := runtime.NewResumeTokenIssuer([]byte("benchmark-resume-token-secret-32bytes"))
	if err != nil {
		tb.Fatal(err)
	}
	handler, err := api.NewHandler(runtime.NewService(runtime.NewMemoryStore(), nil, nil), catalog, auth, resumeTokens)
	if err != nil {
		tb.Fatal(err)
	}
	return handler.Routes(), published
}

// BenchmarkStartRunHTTP measures the authenticated idempotent run-start write path under parallel load.
func BenchmarkStartRunHTTP(b *testing.B) {
	handler, published := benchmarkHandler(b)
	payload, err := json.Marshal(map[string]any{
		"planId": published.ID, "planHash": published.Hash,
		"input": map[string]any{"payload": "small representative input"},
	})
	if err != nil {
		b.Fatal(err)
	}
	var sequence atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := sequence.Add(1)
			req := httptest.NewRequest(http.MethodPost, "/v1/runs", bytes.NewReader(payload))
			req.Header.Set("Authorization", "Bearer "+benchmarkAPIKey)
			req.Header.Set("X-Fenix-Organization-Id", "benchmark-tenant")
			req.Header.Set("Idempotency-Key", fmt.Sprintf("load-%016d", id))
			res := httptest.NewRecorder()
			handler.ServeHTTP(res, req)
			if res.Code != http.StatusAccepted {
				b.Fatalf("status=%d body=%s", res.Code, res.Body.String())
			}
		}
	})
}
