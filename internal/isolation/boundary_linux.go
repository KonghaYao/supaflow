//go:build linux

package isolation

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	cgroupRootEnv    = "SUPAFLOW_ISOLATION_CGROUP_ROOT"
	workspaceRootEnv = "SUPAFLOW_ISOLATION_WORKSPACE_ROOT"
)

type attemptCgroup struct {
	path string
	file *os.File
}

func prepareProductionBoundary(request Request, limits resolvedLimits) (*executionBoundary, error) {
	workspaceRoot := strings.TrimSpace(os.Getenv(workspaceRootEnv))
	if !filepath.IsAbs(workspaceRoot) {
		return nil, errResourceLimitsUnavailable
	}
	if err := os.MkdirAll(workspaceRoot, 0o700); err != nil {
		return nil, fmt.Errorf("create workspace root: %w", err)
	}
	workspace, err := os.MkdirTemp(workspaceRoot, "attempt-")
	if err != nil {
		return nil, err
	}
	cgroup, err := createAttemptCgroup(request, limits)
	if err != nil {
		_ = os.RemoveAll(workspace)
		return nil, err
	}
	return &executionBoundary{
		workspace: workspace, sandboxWorkspace: "/workspace",
		configure: func(cmd *exec.Cmd) error {
			cmd.SysProcAttr = &syscall.SysProcAttr{
				Setpgid: true, Pdeathsig: syscall.SIGKILL,
				UseCgroupFD: true, CgroupFD: int(cgroup.file.Fd()),
			}
			return nil
		},
		monitor: func(ctx context.Context, cmd *exec.Cmd) <-chan error {
			return monitorWorkspaceInodes(ctx, cmd, limits.workspaceInodes)
		},
		verify: func() error {
			return checkWorkspaceInodes(workspace, limits.workspaceInodes)
		},
		cleanup: func() {
			cgroup.close()
			_ = os.RemoveAll(workspace)
		},
	}, nil
}

func checkWorkspaceInodes(root string, limit uint64) error {
	var count uint64
	err := filepath.WalkDir(root, func(path string, _ fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			return nil
		}
		count++
		if count > limit {
			return errWorkspaceQuotaExceeded
		}
		return nil
	})
	return err
}

func createAttemptCgroup(request Request, limits resolvedLimits) (*attemptCgroup, error) {
	root := strings.TrimSpace(os.Getenv(cgroupRootEnv))
	cleanRoot := filepath.Clean(root)
	if !filepath.IsAbs(root) || cleanRoot == "/sys/fs/cgroup" || !strings.HasPrefix(cleanRoot, "/sys/fs/cgroup/") {
		return nil, errResourceLimitsUnavailable
	}
	controllers, err := os.ReadFile(filepath.Join(cleanRoot, "cgroup.controllers"))
	if err != nil || !containsWord(string(controllers), "pids") || !containsWord(string(controllers), "memory") {
		return nil, errResourceLimitsUnavailable
	}
	enabled, err := os.ReadFile(filepath.Join(cleanRoot, "cgroup.subtree_control"))
	if err != nil || !containsWord(string(enabled), "pids") || !containsWord(string(enabled), "memory") {
		return nil, errResourceLimitsUnavailable
	}
	digest := sha256.Sum256([]byte(request.TenantID + "\x00" + request.RunID + "\x00" + request.AttemptID + "\x00" + strconv.FormatUint(request.FencingToken, 10)))
	path := filepath.Join(cleanRoot, fmt.Sprintf("attempt-%x", digest[:12]))
	if err = os.Mkdir(path, 0o700); err != nil {
		return nil, errResourceLimitsUnavailable
	}
	failed := true
	defer func() {
		if failed {
			_ = os.Remove(path)
		}
	}()
	settings := map[string]string{
		"pids.max":         strconv.FormatUint(limits.processes, 10),
		"memory.max":       strconv.FormatUint(limits.addressSpaceBytes, 10),
		"memory.oom.group": "1",
	}
	for name, value := range settings {
		if err = os.WriteFile(filepath.Join(path, name), []byte(value), 0o600); err != nil {
			return nil, errResourceLimitsUnavailable
		}
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, errResourceLimitsUnavailable
	}
	failed = false
	return &attemptCgroup{path: path, file: file}, nil
}

func (group *attemptCgroup) close() {
	_ = os.WriteFile(filepath.Join(group.path, "cgroup.kill"), []byte("1"), 0o600)
	_ = group.file.Close()
	for attempt := 0; attempt < 20; attempt++ {
		if err := os.Remove(group.path); err == nil || errors.Is(err, fs.ErrNotExist) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func containsWord(value, expected string) bool {
	for _, word := range strings.Fields(value) {
		if word == expected {
			return true
		}
	}
	return false
}

func monitorWorkspaceInodes(ctx context.Context, command *exec.Cmd, limit uint64) <-chan error {
	result := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		root := fmt.Sprintf("/proc/%d/root/workspace", command.Process.Pid)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				var count uint64
				err := filepath.WalkDir(root, func(_ string, entry fs.DirEntry, walkErr error) error {
					if walkErr != nil {
						return walkErr
					}
					if entry.Name() != "workspace" {
						count++
					}
					if count > limit {
						return errWorkspaceQuotaExceeded
					}
					return nil
				})
				if errors.Is(err, errWorkspaceQuotaExceeded) {
					result <- err
					return
				}
				// The namespace is not visible until bwrap finishes setup. Other
				// lookup failures are retried; process exit closes the monitor via ctx.
			}
		}
	}()
	return result
}
