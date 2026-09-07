package api

import (
	"io/fs"
	"net/http"
	"testing"
	"testing/fstest"

	"github.com/pazhou/supaflow/internal/platform"
	"github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

func studioAPIHandler(t *testing.T, assets fs.FS) http.Handler {
	t.Helper()
	auth, err := NewMasterKeyAuthenticator(tenantAKey)
	if err != nil {
		t.Fatal(err)
	}
	catalog := platform.NewMemoryCatalog(workflow.BuiltinRegistry(), workflow.CompilePolicy{})
	issuer, err := runtime.NewResumeTokenIssuer([]byte("test-resume-token-secret-32-bytes!"))
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(runtime.NewService(runtime.NewMemoryStore(), nil, nil), catalog, auth, issuer)
	if err != nil {
		t.Fatal(err)
	}
	handler.SetStudioAssets(assets)
	return handler.Routes()
}

// 验证 Studio 入口和 SPA fallback 都返回 index 且禁止缓存身份相关页面。
func TestStudioAssetsAndSPAFallback(t *testing.T) {
	handler := studioAPIHandler(t, fstest.MapFS{
		"index.html":    {Data: []byte("<html>studio</html>")},
		"assets/app.js": {Data: []byte("export const app = true")},
	})
	for _, target := range []string{"/studio/", "/studio/workflows/flow"} {
		response := requestJSON(t, handler, http.MethodGet, target, "", "", nil)
		if response.Code != http.StatusOK || response.Header().Get("Cache-Control") != "private, no-cache, no-store" {
			t.Fatalf("target=%s status=%d cache=%q body=%s", target, response.Code, response.Header().Get("Cache-Control"), response.Body.String())
		}
	}
}

// 验证指纹静态资源可长期缓存并带有浏览器安全响应头。
func TestStudioFingerprintAssetCaching(t *testing.T) {
	handler := studioAPIHandler(t, fstest.MapFS{"index.html": {Data: []byte("index")}, "assets/app.js": {Data: []byte("app")}})
	response := requestJSON(t, handler, http.MethodGet, "/studio/assets/app.js", "", "", nil)
	if response.Code != http.StatusOK || response.Header().Get("Cache-Control") != "public, max-age=31536000, immutable" {
		t.Fatalf("status=%d cache=%q", response.Code, response.Header().Get("Cache-Control"))
	}
	if response.Header().Get("Content-Security-Policy") == "" || response.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("security headers=%v", response.Header())
	}
}
