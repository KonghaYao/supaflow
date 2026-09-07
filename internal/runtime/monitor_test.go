package runtime

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// 验证监控投影递归隐藏输入凭据，并且敏感输出只保留元数据。
func TestRedactRunDetailsHidesSecretsAndSensitiveOutput(t *testing.T) {
	details := RunDetails{
		Run: Run{Input: json.RawMessage(`{"nested":{"api_key":"key-value"},"safe":1}`)},
		Nodes: []NodeDiagnostic{{
			Attempts: []AttemptDiagnostic{{Attempt: Attempt{Input: json.RawMessage(`{"authorization":"Bearer private","value":2}`), ResumeData: json.RawMessage(`{"token":"private"}`)}}},
			Output:   &RunOutput{Inline: json.RawMessage(`{"private":true}`), Sensitive: true, Size: 16, SHA256: "digest"},
		}},
	}
	redactRunDetails(&details)
	if string(details.Run.Input) != `{"nested":{"api_key":"[REDACTED]"},"safe":1}` {
		t.Fatalf("unexpected redacted run input %s", details.Run.Input)
	}
	attempt := details.Nodes[0].Attempts[0].Attempt
	if string(attempt.Input) != `{"authorization":"[REDACTED]","value":2}` || attempt.ResumeData != nil {
		t.Fatalf("attempt retained sensitive data: input=%s resume=%s", attempt.Input, attempt.ResumeData)
	}
	if details.Nodes[0].Output.Inline != nil || details.Nodes[0].Output.Size != 16 {
		t.Fatalf("sensitive output projection is unsafe: %+v", details.Nodes[0].Output)
	}
}

// 验证列表过滤的租户边界和状态约束由内存规格实现保持。
func TestMemoryMonitorListRunsIsOrganizationScoped(t *testing.T) {
	store := NewMemoryStore()
	service := NewService(store, &fakeClock{now: time.Unix(100, 0)}, &sequenceIDs{})
	_, _ = service.StartRun(context.Background(), "organization_a", StartRunRequest{Plan: PlanRef{ID: "plan-a", Hash: "hash-a"}, Input: json.RawMessage(`{}`), IdempotencyKey: "organization-a-run"})
	_, _ = service.StartRun(context.Background(), "organization_b", StartRunRequest{Plan: PlanRef{ID: "plan-b", Hash: "hash-b"}, Input: json.RawMessage(`{}`), IdempotencyKey: "organization-b-run"})
	items, err := service.ListRuns(context.Background(), "organization_a", RunFilter{Status: RunPending, Limit: 20})
	if err != nil || len(items) != 1 || items[0].Run.OrganizationID != "organization_a" {
		t.Fatalf("items=%+v err=%v", items, err)
	}
}
