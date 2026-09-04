package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pazhou/supaflow/internal/identity"
	"github.com/pazhou/supaflow/internal/platformops"
)

// 验证高风险 API 将成功、拒绝和失败映射为不可变审计结果。
func TestMutationAuditOutcomes(t *testing.T) {
	recorder := &recordingAudit{}
	handler := &Handler{audit: recorder}
	tests := []struct {
		status  int
		outcome platformops.AuditOutcome
	}{
		{status: http.StatusAccepted, outcome: platformops.AuditAllowed},
		{status: http.StatusForbidden, outcome: platformops.AuditDenied},
		{status: http.StatusConflict, outcome: platformops.AuditFailed},
	}
	for _, item := range tests {
		request := httptest.NewRequest(http.MethodPost, "/v1/runs/run-1/cancel", nil)
		request = request.WithContext(identity.ContextWithPrincipal(request.Context(), Principal{TenantID: "tenant-a", ActorID: "operator-a", Role: RoleOperator}))
		response := httptest.NewRecorder()
		handler.auditMutations(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(item.status) })).ServeHTTP(response, request)
		if got := recorder.records[len(recorder.records)-1]; got.Outcome != item.outcome || got.TenantID != "tenant-a" || got.ResourceID != "run-1" {
			t.Fatalf("record=%+v", got)
		}
	}
}

type recordingAudit struct{ records []platformops.AuditRecord }

func (r *recordingAudit) Record(_ context.Context, record platformops.AuditRecord) (platformops.AuditRecord, error) {
	r.records = append(r.records, record)
	return record, nil
}
