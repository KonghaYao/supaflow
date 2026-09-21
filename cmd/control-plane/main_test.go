package main

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// 验证 supervisor 生产并发与单次 delivery deadline 只能配置为正的有限值。
func TestSupervisorConfigFromEnv(t *testing.T) {
	t.Setenv("SUPAFLOW_DELIVERY_CONCURRENCY", "7")
	t.Setenv("SUPAFLOW_DELIVERY_TIMEOUT_MS", "9000")
	config, err := supervisorConfigFromEnv()
	if err != nil || config.DeliveryConcurrency != 7 || config.DeliveryTimeout != 9*time.Second {
		t.Fatalf("config=%+v err=%v", config, err)
	}
	t.Setenv("SUPAFLOW_DELIVERY_CONCURRENCY", "0")
	if _, err = supervisorConfigFromEnv(); err == nil {
		t.Fatal("zero delivery concurrency was accepted")
	}
}

// 验证 SSE 生产配额、退避和写 deadline 可由环境显式收紧。
func TestEventStreamConfigFromEnv(t *testing.T) {
	t.Setenv("SUPAFLOW_SSE_GLOBAL_LIMIT", "20")
	t.Setenv("SUPAFLOW_SSE_ORGANIZATION_LIMIT", "5")
	t.Setenv("SUPAFLOW_SSE_POLL_MIN_MS", "100")
	t.Setenv("SUPAFLOW_SSE_POLL_MAX_MS", "800")
	t.Setenv("SUPAFLOW_SSE_WRITE_TIMEOUT_MS", "1500")
	config, err := eventStreamConfigFromEnv()
	if err != nil || config.GlobalLimit != 20 || config.OrganizationLimit != 5 || config.PollMax != 800*time.Millisecond || config.WriteTimeout != 1500*time.Millisecond {
		t.Fatalf("config=%+v err=%v", config, err)
	}
}

// 验证部署注入的 AES/KMS seam 密钥只接受精确 32 字节的 hex 或 base64。
func TestDecodeKey(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	for _, encoded := range []string{hex.EncodeToString(key), base64.StdEncoding.EncodeToString(key)} {
		decoded, err := decodeKey(encoded)
		if err != nil || !bytes.Equal(decoded, key) {
			t.Fatalf("decode key err=%v", err)
		}
	}
	if _, err := decodeKey("too-short"); err == nil {
		t.Fatal("expected invalid master key rejection")
	}
}

// 验证 /ops 的 5xx 日志行带的是响应里那个 requestId：中间件在调用处理器之前先把标识固定到
// 请求上（调用方自带就沿用，否则生成一个），处理器写 problem 时读到的就是同一个值。此前日志行
// 只有 path 与 status，调用方拿着响应里的 id 也检索不到任何东西。
func TestOperationsFailureLogCarriesResponseRequestID(t *testing.T) {
	logs := &bytes.Buffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(logs, nil)))
	defer slog.SetDefault(previous)

	// 处理器模拟 /ops 的 problem 响应：读固定下来的标识并回带，与 httpapi.writeProblem 同形。
	var written string
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		written = r.Header.Get("X-Request-ID")
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"status":500,"code":"internal_error","requestId":"` + written + `"}`))
	})
	request := httptest.NewRequest(http.MethodPost, "/ops/triggers", nil)
	response := httptest.NewRecorder()
	logOperationsFailures(next).ServeHTTP(response, request)
	if written == "" || !strings.Contains(response.Body.String(), written) {
		t.Fatalf("requestId=%q body=%s", written, response.Body.String())
	}
	if !strings.Contains(logs.String(), written) || !strings.Contains(logs.String(), "operations request failed") {
		t.Fatalf("log=%s missing requestId=%s", logs.String(), written)
	}

	// 调用方自带标识时响应与日志沿用该值，不另行生成。
	var echoed string
	callerRequest := httptest.NewRequest(http.MethodPost, "/ops/triggers", nil)
	callerRequest.Header.Set("X-Request-ID", "caller-supplied-ops-id")
	callerNext := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		echoed = r.Header.Get("X-Request-ID")
		w.WriteHeader(http.StatusInternalServerError)
	})
	logOperationsFailures(callerNext).ServeHTTP(httptest.NewRecorder(), callerRequest)
	if echoed != "caller-supplied-ops-id" || !strings.Contains(logs.String(), "caller-supplied-ops-id") {
		t.Fatalf("echoed=%q log=%s", echoed, logs.String())
	}
}
