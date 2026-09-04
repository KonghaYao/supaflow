package supaflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"testing"
)

// 验证 Go SDK 透传认证并在 EOF 校验声明的输出大小与 SHA-256。
func TestDownloadRunOutputVerifiesBoundedStream(t *testing.T) {
	value := []byte(`{"answer":42}`)
	digest := sha256.Sum256(value)
	client := testClient(t, func(request *http.Request) (*http.Response, error) {
		if request.URL.EscapedPath() != "/v1/runs/run%2F1/output" || request.Header.Get("X-API-Key") == "" {
			t.Fatalf("request=%s headers=%v", request.URL, request.Header)
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{
			"Content-Type":     []string{"application/json"},
			"Content-Length":   []string{"13"},
			"X-Content-Sha256": []string{hex.EncodeToString(digest[:])},
		}, Body: io.NopCloser(strings.NewReader(string(value)))}, nil
	})
	output, err := client.DownloadRunOutput(context.Background(), "run/1", 1024)
	if err != nil {
		t.Fatal(err)
	}
	read, err := io.ReadAll(output.Body)
	_ = output.Body.Close()
	if err != nil || string(read) != string(value) || output.SHA256 != hex.EncodeToString(digest[:]) {
		t.Fatalf("output=%+v bytes=%s err=%v", output, read, err)
	}
}

// 验证 SDK 在读取 body 前拒绝超过调用方预算的 external output。
func TestDownloadRunOutputRejectsDeclaredOversize(t *testing.T) {
	client := testClient(t, func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{
			"Content-Type": []string{"application/json"}, "Content-Length": []string{"2048"},
			"X-Content-Sha256": []string{strings.Repeat("a", 64)},
		}, Body: io.NopCloser(strings.NewReader("{}"))}, nil
	})
	if _, err := client.DownloadRunOutput(context.Background(), "run", 1024); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("error=%v", err)
	}
}
