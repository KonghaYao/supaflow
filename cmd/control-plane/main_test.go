package main

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"testing"
	"time"

	"github.com/pazhou/supaflow/internal/api"
)

// 验证 API key 配置将租户和角色绑定到服务端认证主体。
func TestParseAPIKeys(t *testing.T) {
	keys, err := parseAPIKeys(`[{"key":"a-key-that-is-at-least-24-bytes","tenantId":"tenant-a","actorId":"service-a","role":"admin"}]`)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0].Principal.TenantID != "tenant-a" || keys[0].Principal.Role != api.RoleAdmin {
		t.Fatalf("keys=%+v", keys)
	}
}

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
	t.Setenv("SUPAFLOW_SSE_TENANT_LIMIT", "5")
	t.Setenv("SUPAFLOW_SSE_POLL_MIN_MS", "100")
	t.Setenv("SUPAFLOW_SSE_POLL_MAX_MS", "800")
	t.Setenv("SUPAFLOW_SSE_WRITE_TIMEOUT_MS", "1500")
	config, err := eventStreamConfigFromEnv()
	if err != nil || config.GlobalLimit != 20 || config.TenantLimit != 5 || config.PollMax != 800*time.Millisecond || config.WriteTimeout != 1500*time.Millisecond {
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

// 验证空 key 列表不会启动一个绕过认证或无法使用的服务。
func TestParseAPIKeysRejectsEmptyList(t *testing.T) {
	if _, err := parseAPIKeys(`[]`); err == nil {
		t.Fatal("expected empty configuration to fail")
	}
}
