package load

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// 验证 32 路并发的一千次认证 Run API 启动都在时限内成功，且不会共享幂等键或丢失响应。
func TestStartRunAPIPressure(t *testing.T) {
	handler, published := benchmarkHandler(t)
	payload, err := json.Marshal(map[string]any{
		"planId": published.ID, "planHash": published.Hash, "input": map[string]any{"pressure": true},
	})
	if err != nil {
		t.Fatal(err)
	}
	const requests = 1000
	const concurrency = 32
	jobs := make(chan int)
	errors := make(chan string, requests)
	started := time.Now()
	var workers sync.WaitGroup
	for worker := 0; worker < concurrency; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				req := httptest.NewRequest(http.MethodPost, "/v1/runs", bytes.NewReader(payload))
				req.Header.Set("X-API-Key", benchmarkAPIKey)
				req.Header.Set("Idempotency-Key", fmt.Sprintf("pressure-%08d", index))
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, req)
				if response.Code != http.StatusAccepted {
					errors <- fmt.Sprintf("request %d status=%d body=%s", index, response.Code, response.Body.String())
				}
			}
		}()
	}
	for index := 0; index < requests; index++ {
		jobs <- index
	}
	close(jobs)
	workers.Wait()
	close(errors)
	for message := range errors {
		t.Error(message)
	}
	elapsed := time.Since(started)
	if elapsed > 10*time.Second {
		t.Fatalf("1000 requests exceeded pressure-test budget: %s", elapsed)
	}
	t.Logf("requests=%d concurrency=%d elapsed=%s throughput=%.0f req/s", requests, concurrency, elapsed, requests/elapsed.Seconds())
}
