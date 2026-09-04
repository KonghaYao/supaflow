package runtime

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// 验证 webhook token 只能恢复 webhook 中断、接受任意 JSON，并提供单次消费与丢包重放。
func TestWebhookResumeIsSeparateFromApproval(t *testing.T) {
	now := time.Date(2026, 9, 3, 8, 0, 0, 0, time.UTC)
	service, store, clock := testService(now)
	issuer := testResumeTokens(t)
	service.SetResumeTokenIssuer(issuer)
	_, err := service.StartRun(context.Background(), "tenant_a", startRequest("webhook-run"))
	if err != nil {
		t.Fatal(err)
	}
	executor := executorFunc{execute: func(context.Context, ExecutionRequest) (ExecutionResult, error) {
		return ExecutionResult{Suspend: &Suspension{Kind: "webhook", ExpiresIn: time.Hour}}, nil
	}}
	orchestrator, err := NewOrchestrator(store, executor, clock, &sequenceIDs{}, OrchestratorConfig{
		WorkerID: "worker", LeaseDuration: time.Minute,
		RetryPolicy: RetryPolicy{MaxAttempts: 1, BaseDelay: time.Second, MaxDelay: time.Second}, ResumeTokens: issuer,
	})
	if err != nil {
		t.Fatal(err)
	}
	suspended, err := orchestrator.Advance(context.Background(), "tenant_a")
	if err != nil || suspended.Interrupt == nil {
		t.Fatalf("suspend=%+v err=%v", suspended, err)
	}
	token, _ := issuer.Issue(*suspended.Interrupt)
	command := ResumeWebhookCommand{InterruptID: suspended.Interrupt.ID, ResumeToken: token, IdempotencyKey: "webhook-resume-key", Data: json.RawMessage(`{"event":"paid","amount":42}`)}
	resolved, err := service.ResumeWebhook(context.Background(), command)
	if err != nil || resolved.Run.Status != RunPending || string(resolved.Interrupt.Decision) != string(command.Data) {
		t.Fatalf("resolved=%+v err=%v", resolved, err)
	}
	// Lost-response recovery must remain available after expiry because the
	// idempotency record was durably committed before the client retried.
	clock.Set(now.Add(2 * time.Hour))
	replay, err := service.ResumeWebhook(context.Background(), command)
	if err != nil || replay.Run.Version != resolved.Run.Version {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	conflict := command
	conflict.Data = json.RawMessage(`{"event":"refunded"}`)
	if _, err = service.ResumeWebhook(context.Background(), conflict); !IsCode(err, CodeIdempotencyConflict) {
		t.Fatalf("conflict error=%v", err)
	}
	if _, err = service.ResolveApproval(context.Background(), "tenant_a", ResolveApprovalCommand{ActorID: "approver", InterruptID: suspended.Interrupt.ID, ResumeToken: token, IdempotencyKey: "approval-key-after", Decision: ApprovalDecision{Approved: true}}); !IsCode(err, CodeApprovalInvalid) {
		t.Fatalf("approval path consumed webhook result: %v", err)
	}
}

// 验证 token 验签会拒绝篡改和到期值，并完整恢复绑定身份。
func TestResumeTokenVerifyRejectsTamperingAndExpiry(t *testing.T) {
	issuer := testResumeTokens(t)
	now := time.Date(2026, 9, 3, 8, 0, 0, 0, time.UTC)
	interrupt := Interrupt{TenantID: "tenant", ID: "interrupt", RunID: "run", NodeID: "node", AttemptID: "attempt", Kind: "webhook", ExpiresAt: now.Add(time.Minute)}
	token, _ := issuer.Issue(interrupt)
	claims, err := issuer.Verify(token, now)
	if err != nil || claims.InterruptID != interrupt.ID || claims.TenantID != interrupt.TenantID || claims.Kind != "webhook" {
		t.Fatalf("claims=%+v err=%v", claims, err)
	}
	if _, err = issuer.Verify(token+"x", now); err == nil {
		t.Fatal("tampered token was accepted")
	}
	if _, err = issuer.Verify(token, interrupt.ExpiresAt); err == nil {
		t.Fatal("expired token was accepted")
	}
}
