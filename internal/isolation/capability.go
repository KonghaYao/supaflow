package isolation

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"
)

const capabilityCheckFlag = "--supaflow-capability-check"

// IsCapabilityCheck reports whether args request the trusted startup probe.
func IsCapabilityCheck(args []string) bool {
	return len(args) > 0 && args[0] == capabilityCheckFlag
}

// CapabilityCheckArgs returns the helper argument used by worker startup.
func CapabilityCheckArgs() []string { return []string{capabilityCheckFlag} }

// CheckCapabilities exercises both supported interpreters through the real
// bwrap, cgroup-v2 and bounded-tmpfs production path.
func CheckCapabilities(ctx context.Context) error {
	nonce := strconv.FormatInt(time.Now().UnixNano(), 10) + "-" + strconv.Itoa(os.Getpid())
	checks := []Request{
		capabilityRequest(nonce+"-shell", "shell", `printf null`),
		capabilityRequest(nonce+"-python", "python", `print("null")`),
	}
	for _, request := range checks {
		response := Run(ctx, request)
		if response.Code != "" {
			return fmt.Errorf("%s capability probe failed: %s", request.Language, response.Code)
		}
	}
	return nil
}

func capabilityRequest(attemptID, language, source string) Request {
	return Request{
		TenantID: "capability-probe", RunID: "capability-probe", AttemptID: attemptID,
		FencingToken: 1, Language: language, Source: source, Network: false,
		Input: []byte("null"), Timeout: 5 * time.Second, OutputLimit: 1024,
		Limits: ResourceLimits{Processes: 4, WorkspaceBytes: 1 << 20, WorkspaceInodes: 64},
	}
}
