package isolation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"
)

const maxSourceBytes = 1 << 20

// workspaceRootEnv bounds where attempt workspaces are created on every platform.
const workspaceRootEnv = "SUPAFLOW_ISOLATION_WORKSPACE_ROOT"

// allowUnsafeLocalExecutionEnv authorizes the degraded darwin runner. It only
// affects hosts that cannot provide the production sandbox at all; Linux always
// takes the bwrap plus cgroup path regardless of this value. See
// local_execution_darwin.go for the exact boundaries that survive the trade.
const allowUnsafeLocalExecutionEnv = "SUPAFLOW_ISOLATION_ALLOW_UNSAFE_LOCAL_EXECUTION"

// allowUnsafeLocalExecution reports whether the operator explicitly accepted the
// degraded runner. Only the exact opt-in values enable it, so an unset, empty or
// misspelled value keeps the fail-closed startup probe.
func allowUnsafeLocalExecution() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(allowUnsafeLocalExecutionEnv))) {
	case "1", "true":
		return true
	default:
		return false
	}
}

const (
	defaultCPUTime           = 30 * time.Second
	defaultAddressSpaceBytes = 256 << 20
	defaultProcesses         = 32
	defaultOpenFiles         = 64
	defaultWorkspaceBytes    = 16 << 20
	defaultWorkspaceInodes   = 1024
)

// Run executes one request with OS-enforced filesystem, network, CPU, address
// space, process, file-size, and open-file limits. It fails closed when the
// host cannot enforce every boundary.
func Run(ctx context.Context, request Request) Response {
	return runWithBoundary(ctx, request, sandboxCommand, prepareProductionBoundary)
}

type commandBuilder func(string, string, []string, resolvedLimits) ([]string, error)

func runWithBuilder(ctx context.Context, request Request, build commandBuilder) Response {
	return runWithBoundary(ctx, request, build, prepareTestBoundary)
}

type executionBoundary struct {
	workspace        string
	sandboxWorkspace string
	configure        func(*exec.Cmd) error
	monitor          func(context.Context, *exec.Cmd) <-chan error
	verify           func() error
	cleanup          func()
}

var errWorkspaceQuotaExceeded = errors.New("workspace quota exceeded")

type boundaryBuilder func(Request, resolvedLimits) (*executionBoundary, error)

func runWithBoundary(ctx context.Context, request Request, build commandBuilder, prepare boundaryBuilder) Response {
	if err := validateRequest(request); err != nil {
		return Response{Code: err.Error()}
	}
	limits, err := resolveResourceLimits(request)
	if err != nil {
		return Response{Code: "invalid_limits"}
	}
	boundary, err := prepare(request, limits)
	if err != nil {
		if errors.Is(err, errResourceLimitsUnavailable) {
			return Response{Code: "resource_limits_unavailable"}
		}
		return Response{Code: "workspace_unavailable"}
	}
	defer boundary.cleanup()
	workspace := boundary.workspace
	ext := ".sh"
	interpreter := "/bin/sh"
	if request.Language == "python" {
		ext, interpreter = ".py", pythonInterpreter()
	}
	if interpreter == "" {
		return Response{Code: "interpreter_unavailable"}
	}
	sourcePath := filepath.Join(workspace, "main"+ext)
	if err = os.WriteFile(sourcePath, []byte(request.Source), 0o600); err != nil {
		return Response{Code: "workspace_unavailable"}
	}
	command, err := build(workspace, interpreter, append([]string{sourcePath}, request.Args...), limits)
	if err != nil {
		if errors.Is(err, errResourceLimitsUnavailable) {
			return Response{Code: "resource_limits_unavailable"}
		}
		return Response{Code: "sandbox_unavailable"}
	}
	executionCtx, cancel := context.WithTimeout(ctx, request.Timeout)
	defer cancel()
	cmd := exec.Command(command[0], command[1:]...)
	cmd.Dir = workspace
	cmd.Env = isolatedEnvironment(boundary.sandboxWorkspace, request.Environment)
	cmd.Stdin = bytes.NewReader(request.Input)
	stdout := &boundedBuffer{limit: request.OutputLimit}
	stderr := &boundedBuffer{limit: min(request.OutputLimit, 64<<10)}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	if err = boundary.configure(cmd); err != nil {
		return Response{Code: "resource_limits_unavailable"}
	}
	err = runCommand(executionCtx, cmd, boundary.monitor, 1500*time.Millisecond)
	if err == nil && boundary.verify != nil {
		err = boundary.verify()
	}
	if errors.Is(stdout.err, errOutputLimit) {
		return Response{Code: "output_limit_exceeded"}
	}
	if ctx.Err() != nil {
		return Response{Code: "cancelled"}
	}
	if executionCtx.Err() != nil {
		return Response{Code: "timeout"}
	}
	if err != nil {
		if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOSYS) {
			return Response{Code: "resource_limits_unavailable"}
		}
		if strings.Contains(stderr.String(), resourceLimitFailureMarker) {
			return Response{Code: "resource_limits_unavailable"}
		}
		if strings.Contains(stderr.String(), "sandbox_apply: Operation not permitted") {
			return Response{Code: "sandbox_unavailable"}
		}
		return Response{Code: "execution_failed"}
	}
	output := bytes.TrimSpace(stdout.Bytes())
	if len(output) == 0 {
		output = []byte("null")
	}
	if !json.Valid(output) {
		encoded, _ := json.Marshal(string(output))
		output = encoded
	}
	return Response{Output: output}
}

func prepareTestBoundary(_ Request, _ resolvedLimits) (*executionBoundary, error) {
	workspace, err := os.MkdirTemp("", "supaflow-isolation-test-")
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

func runCommand(ctx context.Context, cmd *exec.Cmd, monitor func(context.Context, *exec.Cmd) <-chan error, grace time.Duration) error {
	if err := cmd.Start(); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	var violation <-chan error
	if monitor != nil {
		violation = monitor(ctx, cmd)
	}
	select {
	case err := <-done:
		return err
	case err := <-violation:
		terminateProcessGroup(cmd, syscall.SIGKILL)
		<-done
		return err
	case <-ctx.Done():
		terminateProcessGroup(cmd, syscall.SIGTERM)
		timer := time.NewTimer(grace)
		defer timer.Stop()
		select {
		case <-done:
			return ctx.Err()
		case <-timer.C:
			terminateProcessGroup(cmd, syscall.SIGKILL)
			<-done
			return ctx.Err()
		}
	}
}

func terminateProcessGroup(cmd *exec.Cmd, signal syscall.Signal) {
	if cmd.Process == nil {
		return
	}
	if err := syscall.Kill(-cmd.Process.Pid, signal); err != nil {
		_ = cmd.Process.Signal(signal)
	}
}

type resolvedLimits struct {
	cpuSeconds        uint64
	addressSpaceBytes uint64
	processes         uint64
	openFiles         uint64
	fileSizeBytes     uint64
	workspaceBytes    uint64
	workspaceInodes   uint64
}

func resolveResourceLimits(request Request) (resolvedLimits, error) {
	cpuCap := min(defaultCPUTime, time.Duration(ceilSeconds(request.Timeout))*time.Second)
	cpuTime := request.Limits.CPUTime
	if cpuTime == 0 {
		cpuTime = cpuCap
	}
	addressSpace := request.Limits.AddressSpaceBytes
	if addressSpace == 0 {
		addressSpace = defaultAddressSpaceBytes
	}
	processes := request.Limits.Processes
	if processes == 0 {
		processes = defaultProcesses
	}
	openFiles := request.Limits.OpenFiles
	if openFiles == 0 {
		openFiles = defaultOpenFiles
	}
	fileSize := request.Limits.FileSizeBytes
	if fileSize == 0 {
		fileSize = uint64(request.OutputLimit)
	}
	workspaceBytes := request.Limits.WorkspaceBytes
	if workspaceBytes == 0 {
		workspaceBytes = defaultWorkspaceBytes
	}
	workspaceInodes := request.Limits.WorkspaceInodes
	if workspaceInodes == 0 {
		workspaceInodes = defaultWorkspaceInodes
	}
	if cpuTime <= 0 || cpuTime > cpuCap || addressSpace > defaultAddressSpaceBytes || processes > defaultProcesses || openFiles > defaultOpenFiles || fileSize > uint64(request.OutputLimit) || workspaceBytes > defaultWorkspaceBytes || workspaceInodes > defaultWorkspaceInodes {
		return resolvedLimits{}, fmt.Errorf("resource limit exceeds production cap")
	}
	return resolvedLimits{
		cpuSeconds:        ceilSeconds(cpuTime),
		addressSpaceBytes: addressSpace,
		processes:         processes,
		openFiles:         openFiles,
		fileSizeBytes:     fileSize,
		workspaceBytes:    workspaceBytes,
		workspaceInodes:   workspaceInodes,
	}, nil
}

// ValidateResourceLimits reports whether requested limits only tighten the
// finite production caps used by the helper.
func ValidateResourceLimits(timeout time.Duration, outputLimit int64, limits ResourceLimits) error {
	_, err := resolveResourceLimits(Request{Timeout: timeout, OutputLimit: outputLimit, Limits: limits})
	return err
}

func ceilSeconds(value time.Duration) uint64 {
	seconds := uint64(value / time.Second)
	if value%time.Second != 0 {
		seconds++
	}
	return seconds
}

func validateRequest(request Request) error {
	if request.OrganizationID == "" || request.RunID == "" || request.AttemptID == "" || request.FencingToken == 0 {
		return fmt.Errorf("invalid_scope")
	}
	if request.Language != "shell" && request.Language != "python" {
		return fmt.Errorf("unsupported_language")
	}
	if request.Source == "" || len(request.Source) > maxSourceBytes {
		return fmt.Errorf("invalid_source")
	}
	if request.Network {
		return fmt.Errorf("network_denied")
	}
	if request.Timeout <= 0 || request.OutputLimit <= 0 {
		return fmt.Errorf("invalid_limits")
	}
	if !json.Valid(request.Input) {
		return fmt.Errorf("invalid_input")
	}
	for key := range request.Environment {
		if !safeEnvironmentName(key) {
			return fmt.Errorf("invalid_environment")
		}
	}
	return nil
}

func pythonInterpreter() string {
	for _, candidate := range []string{"/usr/bin/python3", "/opt/homebrew/bin/python3"} {
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate
		}
	}
	return ""
}

func isolatedEnvironment(workspace string, allowed map[string]string) []string {
	env := []string{"HOME=" + workspace, "TMPDIR=" + workspace, "LANG=C", "LC_ALL=C", "PATH=/usr/bin:/bin"}
	keys := make([]string, 0, len(allowed))
	for key := range allowed {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := allowed[key]
		env = append(env, key+"="+value)
	}
	return env
}

func safeEnvironmentName(value string) bool {
	if value == "" || !((value[0] >= 'A' && value[0] <= 'Z') || value[0] == '_') {
		return false
	}
	for _, char := range value[1:] {
		if !((char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '_') {
			return false
		}
	}
	reserved := map[string]struct{}{
		"BASH_ENV": {}, "CDPATH": {}, "ENV": {}, "GLOBIGNORE": {}, "HOME": {}, "IFS": {},
		"LANG": {}, "LC_ALL": {}, "PATH": {}, "SHELLOPTS": {}, "TMPDIR": {},
	}
	if _, denied := reserved[value]; denied {
		return false
	}
	return !strings.HasPrefix(value, "LD_") && !strings.HasPrefix(value, "DYLD_") &&
		!strings.HasPrefix(value, "PYTHON")
}

func sandboxCommand(workspace, interpreter string, args []string, limits resolvedLimits) ([]string, error) {
	switch runtime.GOOS {
	case "darwin":
		// RLIMIT_NPROC is per-host-user on Darwin, so it cannot safely express a
		// per-invocation process limit. Running without that boundary is forbidden
		// unless the operator opts into the degraded local runner, which keeps every
		// boundary Darwin can still enforce and abandons the ones it cannot.
		if !allowUnsafeLocalExecution() {
			return nil, errResourceLimitsUnavailable
		}
		return localSandboxCommand(interpreter, args, limits)
	case "linux":
		path, err := exec.LookPath("bwrap")
		if err != nil {
			return nil, err
		}
		launcher, err := os.Executable()
		if err != nil {
			return nil, errResourceLimitsUnavailable
		}
		launcher, err = filepath.EvalSymlinks(launcher)
		if err != nil {
			return nil, errResourceLimitsUnavailable
		}
		if len(args) == 0 {
			return nil, errResourceLimitsUnavailable
		}
		sourceName := "/workspace/main" + filepath.Ext(args[0])
		launcherArgs := resourceLimitLauncherArgs(limits, interpreter, append([]string{sourceName}, args[1:]...))
		command := []string{
			path, "--unshare-all", "--new-session", "--die-with-parent", "--proc", "/proc",
			"--ro-bind", "/usr", "/usr", "--ro-bind", "/bin", "/bin",
			"--ro-bind", "/lib", "/lib",
			"--size", fmt.Sprintf("%d", limits.workspaceBytes), "--tmpfs", "/workspace",
			"--ro-bind", args[0], sourceName, "--chdir", "/workspace",
		}
		if _, statErr := os.Stat("/lib64"); statErr == nil {
			command = append(command, "--ro-bind", "/lib64", "/lib64")
		}
		command = append(command, "--ro-bind", launcher, resourceLimitLauncherPath, resourceLimitLauncherPath)
		return append(command, launcherArgs...), nil
	default:
		return nil, fmt.Errorf("unsupported sandbox host")
	}
}

var errResourceLimitsUnavailable = errors.New("resource limits unavailable")

// localSandboxCommand builds the degraded command used only where the host cannot
// provide the production sandbox and the operator explicitly authorized the trade.
// The helper re-executes itself in launcher mode, so the interpreter still inherits
// every ceiling resourceLimitsToApply reports for this platform. Unlike the production
// path it gets no mount, PID, IPC or UTS namespace, no filesystem confinement, no
// workspace byte or inode quota, and no per-invocation process ceiling: user code
// runs as the worker's own user and can read or write anything that user can.
func localSandboxCommand(interpreter string, args []string, limits resolvedLimits) ([]string, error) {
	if len(args) == 0 {
		return nil, errResourceLimitsUnavailable
	}
	launcher, err := os.Executable()
	if err != nil {
		return nil, errResourceLimitsUnavailable
	}
	launcher, err = filepath.EvalSymlinks(launcher)
	if err != nil {
		return nil, errResourceLimitsUnavailable
	}
	return append([]string{launcher}, resourceLimitLauncherArgs(limits, interpreter, args)...), nil
}

var errOutputLimit = errors.New("output limit exceeded")

type boundedBuffer struct {
	buffer bytes.Buffer
	limit  int64
	err    error
}

func (b *boundedBuffer) Len() int       { return b.buffer.Len() }
func (b *boundedBuffer) Bytes() []byte  { return b.buffer.Bytes() }
func (b *boundedBuffer) String() string { return b.buffer.String() }

func (b *boundedBuffer) Write(value []byte) (int, error) {
	remaining := b.limit - int64(b.Len())
	if remaining <= 0 {
		b.err = errOutputLimit
		return 0, b.err
	}
	if int64(len(value)) > remaining {
		_, _ = b.buffer.Write(value[:remaining])
		b.err = errOutputLimit
		return int(remaining), b.err
	}
	return b.buffer.Write(value)
}
