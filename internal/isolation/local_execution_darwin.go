//go:build darwin

package isolation

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
)

// prepareProductionBoundary returns the degraded boundary used only on Darwin hosts
// where the operator explicitly set SUPAFLOW_ISOLATION_ALLOW_UNSAFE_LOCAL_EXECUTION.
//
// macOS provides no bubblewrap equivalent and no cgroup v2, so this is a plain
// temporary workspace. Measured against the Linux boundary it drops the mount, PID,
// IPC, UTS and network namespaces, the read-only system bind mounts, the workspace
// byte and inode quotas, the die-with-parent guarantee, and the per-invocation process
// ceiling (RLIMIT_NPROC is per-host-user on Darwin, which is why the production path
// refuses this host outright).
//
// It keeps the per-attempt workspace, the process group that cancellation kills, the
// rlimit ceilings the launcher installs on the interpreter, and the timeout plus
// bounded output enforced by runWithBoundary. User code still runs as the worker's
// own user and can reach every file, socket and credential that user can, so this
// belongs to local development on a disposable machine only. Callers that leave the
// variable unset keep the fail-closed startup probe.
func prepareProductionBoundary(_ Request, _ resolvedLimits) (*executionBoundary, error) {
	if !allowUnsafeLocalExecution() {
		return nil, errResourceLimitsUnavailable
	}
	root := strings.TrimSpace(os.Getenv(workspaceRootEnv))
	if root == "" {
		root = os.TempDir()
	}
	if !filepath.IsAbs(root) {
		return nil, errResourceLimitsUnavailable
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, err
	}
	workspace, err := os.MkdirTemp(root, "attempt-")
	if err != nil {
		return nil, err
	}
	return &executionBoundary{
		workspace:        workspace,
		sandboxWorkspace: workspace,
		configure: func(cmd *exec.Cmd) error {
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			return nil
		},
		cleanup: func() { _ = os.RemoveAll(workspace) },
	}, nil
}
