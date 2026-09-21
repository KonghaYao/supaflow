package isolation

import (
	"os"
	"testing"
)

// TestMain lets the test binary double as cmd/isolation so the launcher path stays
// reachable from tests.
//
// Both sandboxCommand and localSandboxCommand re-exec os.Executable(), which under
// `go test` is this binary. Without this hook the Go test flag parser rejects the
// launcher flag, the re-exec exits immediately, and every sandboxed run fails for a
// reason that has nothing to do with the boundary under test — the failure mode that
// previously let the Darwin launcher ship broken while its tests stayed green.
func TestMain(m *testing.M) {
	if IsResourceLimitLauncher(os.Args[1:]) {
		os.Exit(RunResourceLimitLauncher(os.Args[1:]))
	}
	os.Exit(m.Run())
}
