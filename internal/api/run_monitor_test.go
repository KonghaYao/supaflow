package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/pazhou/supaflow/internal/platform"
)

// 验证 Run 列表由认证 tenant 隔离并支持 workflow/status 过滤。
func TestRunMonitorListIsTenantScopedAndFilterable(t *testing.T) {
	handler := testHandler(t)
	publish := requestJSON(t, handler, http.MethodPost, "/v1/workflows/flow/publish", tenantAKey, "monitor-publish-key", minimalDefinition("flow"))
	var plan platform.PublishedPlan
	if err := json.Unmarshal(publish.Body.Bytes(), &plan); err != nil {
		t.Fatal(err)
	}
	requestJSON(t, handler, http.MethodPost, "/v1/runs", tenantAKey, "monitor-run-key", map[string]any{
		"planId": plan.ID, "planHash": plan.Hash, "input": map[string]any{"apiToken": "must-hide"},
	})
	visible := requestJSON(t, handler, http.MethodGet, "/v1/runs?status=PENDING&limit=20", tenantAKey, "", nil)
	hidden := requestJSON(t, handler, http.MethodGet, "/v1/runs?limit=20", tenantBKey, "", nil)
	var visibleItems, hiddenItems []runSummaryResponse
	if err := json.Unmarshal(visible.Body.Bytes(), &visibleItems); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(hidden.Body.Bytes(), &hiddenItems); err != nil {
		t.Fatal(err)
	}
	if visible.Code != http.StatusOK || len(visibleItems) != 1 || hidden.Code != http.StatusOK || len(hiddenItems) != 0 {
		t.Fatalf("visible=%d %s hidden=%d %s", visible.Code, visible.Body.String(), hidden.Code, hidden.Body.String())
	}
}

// 验证详情接口返回节点尝试并递归清理输入中的凭据字段。
func TestRunMonitorDetailsRedactsAttemptInput(t *testing.T) {
	handler := testHandler(t)
	publish := requestJSON(t, handler, http.MethodPost, "/v1/workflows/flow/publish", tenantAKey, "detail-publish-key", minimalDefinition("flow"))
	var plan platform.PublishedPlan
	if err := json.Unmarshal(publish.Body.Bytes(), &plan); err != nil {
		t.Fatal(err)
	}
	started := requestJSON(t, handler, http.MethodPost, "/v1/runs", tenantAKey, "detail-run-key", map[string]any{
		"planId": plan.ID, "planHash": plan.Hash, "input": map[string]any{"nested": map[string]any{"password": "must-hide"}},
	})
	var run runResponse
	if err := json.Unmarshal(started.Body.Bytes(), &run); err != nil {
		t.Fatal(err)
	}
	details := requestJSON(t, handler, http.MethodGet, "/v1/runs/"+run.ID+"/details", tenantAKey, "", nil)
	if details.Code != http.StatusOK || !json.Valid(details.Body.Bytes()) {
		t.Fatalf("status=%d body=%s", details.Code, details.Body.String())
	}
	if contains := string(details.Body.Bytes()); !strings.Contains(contains, `"password":"[REDACTED]"`) || strings.Contains(contains, "must-hide") {
		t.Fatalf("details did not redact secrets: %s", contains)
	}
	denied := requestJSON(t, handler, http.MethodGet, "/v1/runs/"+run.ID+"/details", tenantBKey, "", nil)
	if denied.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant details status=%d body=%s", denied.Code, denied.Body.String())
	}
}
