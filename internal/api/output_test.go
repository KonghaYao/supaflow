package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pazhou/supaflow/internal/platform"
	"github.com/pazhou/supaflow/internal/platformops"
	"github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

type outputFixture struct {
	value    []byte
	metadata platformops.ManagedObject
}

func (f outputFixture) OpenRunOutput(_ context.Context, tenantID, runID string) (io.ReadCloser, platformops.ManagedObject, error) {
	if tenantID != f.metadata.TenantID || runID != f.metadata.RunID {
		return nil, platformops.ManagedObject{}, platformops.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(f.value)), f.metadata, nil
}

// 验证对象输出只能通过认证租户的 Run 引用流式读取，响应携带大小和哈希且不泄露 object key。
func TestRunOutputDownloadIsTenantScopedAndAudited(t *testing.T) {
	value := []byte(`{"answer":42}`)
	digest := sha256.Sum256(value)
	store := runtime.NewMemoryStore()
	service := runtime.NewService(store, nil, nil)
	started, err := service.StartRun(context.Background(), "tenant-a", runtime.StartRunRequest{
		Plan:  runtime.PlanRef{ID: "plan", Hash: strings.Repeat("a", 64), EntryNodeID: "end", Executor: "end"},
		Input: json.RawMessage(`{}`), IdempotencyKey: "output-download-run",
	})
	if err != nil {
		t.Fatal(err)
	}
	work, err := store.ClaimAttempt(context.Background(), runtime.ClaimAttemptRequest{TenantID: "tenant-a", WorkerID: "worker", Now: time.Now(), LeaseDuration: time.Minute})
	if err != nil || work == nil {
		t.Fatalf("work=%+v err=%v", work, err)
	}
	_, err = store.CompleteAttempt(context.Background(), runtime.CompleteAttemptRequest{TenantID: "tenant-a", RunID: started.Run.ID,
		AttemptID: work.Attempt.ID, WorkerID: "worker", FencingToken: work.Lease.FencingToken, Now: time.Now(), RunStatus: runtime.RunSuccess,
		Output: &runtime.Output{ID: "output", TenantID: "tenant-a", RunID: started.Run.ID, AttemptID: work.Attempt.ID,
			ObjectRef: "opaque", ContentType: "application/json", Size: int64(len(value)), SHA256: hex.EncodeToString(digest[:])}})
	if err != nil {
		t.Fatal(err)
	}
	auth, _ := NewMasterKeyAuthenticator(tenantAKey)
	resume, _ := runtime.NewResumeTokenIssuer([]byte("test-resume-token-secret-32-bytes!"))
	catalog := platform.NewMemoryCatalog(workflow.BuiltinRegistry(), workflow.CompilePolicy{})
	handler, _ := NewHandler(service, catalog, auth, resume)
	audit := &recordingAudit{}
	handler.SetAudit(audit)
	handler.SetOutputReader(outputFixture{value: value, metadata: platformops.ManagedObject{TenantID: "tenant-a", RunID: started.Run.ID,
		ContentType: "application/json", Size: int64(len(value)), SHA256: hex.EncodeToString(digest[:]), Status: "active"}})
	request := func(key string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/v1/runs/"+started.Run.ID+"/output", nil)
		authenticatedTestRequest(req, key)
		res := httptest.NewRecorder()
		handler.Routes().ServeHTTP(res, req)
		return res
	}
	allowed := request(tenantAKey)
	if allowed.Code != http.StatusOK || !bytes.Equal(allowed.Body.Bytes(), value) || allowed.Header().Get("X-Content-SHA256") != hex.EncodeToString(digest[:]) {
		t.Fatalf("status=%d headers=%v body=%s", allowed.Code, allowed.Header(), allowed.Body.Bytes())
	}
	if strings.Contains(allowed.Body.String(), "opaque") || len(audit.records) != 1 || audit.records[0].Action != "run.output.read" {
		t.Fatalf("unsafe body or audit=%+v", audit.records)
	}
	other := request(tenantBKey)
	if other.Code != http.StatusNotFound {
		t.Fatalf("cross tenant status=%d body=%s", other.Code, other.Body.String())
	}
}
