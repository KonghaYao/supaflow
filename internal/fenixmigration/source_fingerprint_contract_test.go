package fenixmigration

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"
)

const sourceFingerprintContractHash = "6cc56eaef78c85666e58c4f16de4fbdd158f08485ce0a5e46f378bfe7494601f"

// 验证迁移器与 Fenix cleanup gate 共同发布的 fingerprint v1 契约没有漂移。
func TestSourceFingerprintV1Contract(t *testing.T) {
	digest := sha256.Sum256([]byte(strings.TrimSpace(sourceFingerprintSQL)))
	if actual := fmt.Sprintf("%x", digest); actual != sourceFingerprintContractHash {
		t.Fatalf("source fingerprint contract changed: got %s want %s", actual, sourceFingerprintContractHash)
	}
}
