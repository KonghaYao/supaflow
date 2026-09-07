package runtime

import (
	"testing"
	"time"
)

// 验证恢复令牌密钥必须达到生产安全下限。
func TestResumeTokenIssuerRejectsShortSecret(t *testing.T) {
	if _, err := NewResumeTokenIssuer([]byte("too-short")); err == nil {
		t.Fatal("expected short secret to be rejected")
	}
}

// 验证令牌确定性绑定审批中断的全部身份字段和过期时间。
func TestResumeTokenIssuerBindsInterruptClaims(t *testing.T) {
	issuer, err := NewResumeTokenIssuer([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	base := Interrupt{OrganizationID: "organization-a", ID: "int-1", RunID: "run-1", NodeID: "approve",
		AttemptID: "attempt-1", Kind: "approval", ExpiresAt: time.Unix(1234, 567).UTC()}
	want, err := issuer.Issue(base)
	if err != nil {
		t.Fatal(err)
	}
	again, _ := issuer.Issue(base)
	if again != want {
		t.Fatal("same claims must produce the same token")
	}
	mutations := []func(*Interrupt){
		func(v *Interrupt) { v.OrganizationID = "organization-b" }, func(v *Interrupt) { v.RunID = "run-2" },
		func(v *Interrupt) { v.NodeID = "other" }, func(v *Interrupt) { v.AttemptID = "attempt-2" },
		func(v *Interrupt) { v.ID = "int-2" }, func(v *Interrupt) { v.Kind = "wait" },
		func(v *Interrupt) { v.ExpiresAt = v.ExpiresAt.Add(time.Second) },
	}
	for index, mutate := range mutations {
		changed := base
		mutate(&changed)
		got, issueErr := issuer.Issue(changed)
		if issueErr != nil {
			t.Fatalf("mutation %d: %v", index, issueErr)
		}
		if got == want {
			t.Fatalf("mutation %d did not affect token", index)
		}
	}
}
