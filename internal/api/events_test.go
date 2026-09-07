package api

import (
	"testing"
	"time"
)

// 验证 SSE 配额同时约束全局与单租户，并在连接释放后恢复容量。
func TestEventStreamLimiterEnforcesGlobalAndOrganizationCaps(t *testing.T) {
	limiter, err := newEventStreamLimiter(EventStreamConfig{
		GlobalLimit: 2, OrganizationLimit: 1, PollMin: 10 * time.Millisecond,
		PollMax: time.Second, WriteTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	releaseA, ok := limiter.acquire("organization_a")
	if !ok {
		t.Fatal("first organization stream was rejected")
	}
	if _, ok = limiter.acquire("organization_a"); ok {
		t.Fatal("organization cap was bypassed")
	}
	releaseB, ok := limiter.acquire("organization_b")
	if !ok {
		t.Fatal("second organization stream was rejected")
	}
	if _, ok = limiter.acquire("organization_c"); ok {
		t.Fatal("global cap was bypassed")
	}
	releaseA()
	if releaseC, acquired := limiter.acquire("organization_c"); !acquired {
		t.Fatal("released stream capacity was not reusable")
	} else {
		releaseC()
	}
	releaseB()
}
