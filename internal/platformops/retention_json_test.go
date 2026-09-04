package platformops

import (
	"bytes"
	"errors"
	"testing"
)

// 验证导出达到硬上限时拒绝继续写入，避免大租户导出耗尽进程内存或临时磁盘。
func TestBoundedJSONWriterRejectsWriteBeyondLimit(t *testing.T) {
	var destination bytes.Buffer
	writer := &boundedJSONWriter{writer: &destination, remaining: 4}

	written, err := writer.Write([]byte("1234"))
	if err != nil || written != 4 {
		t.Fatalf("write within limit = (%d, %v), want (4, nil)", written, err)
	}
	written, err = writer.Write([]byte("5"))
	if !errors.Is(err, ErrRateLimited) || written != 0 {
		t.Fatalf("write beyond limit = (%d, %v), want (0, ErrRateLimited)", written, err)
	}
	if got := destination.String(); got != "1234" {
		t.Fatalf("destination = %q, want exact bounded payload", got)
	}
}
