package api

import (
	"testing"
	"time"
)

// 验证 SSE 配额同时约束全局与单租户，并在连接释放后恢复容量。
func TestEventStreamLimiterEnforcesGlobalAndTenantCaps(t *testing.T) {
	limiter, err := newEventStreamLimiter(EventStreamConfig{
		GlobalLimit: 2, TenantLimit: 1, PollMin: 10 * time.Millisecond,
		PollMax: time.Second, WriteTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	releaseA, ok := limiter.acquire("tenant_a")
	if !ok {
		t.Fatal("first tenant stream was rejected")
	}
	if _, ok = limiter.acquire("tenant_a"); ok {
		t.Fatal("tenant cap was bypassed")
	}
	releaseB, ok := limiter.acquire("tenant_b")
	if !ok {
		t.Fatal("second tenant stream was rejected")
	}
	if _, ok = limiter.acquire("tenant_c"); ok {
		t.Fatal("global cap was bypassed")
	}
	releaseA()
	if releaseC, acquired := limiter.acquire("tenant_c"); !acquired {
		t.Fatal("released stream capacity was not reusable")
	} else {
		releaseC()
	}
	releaseB()
}
