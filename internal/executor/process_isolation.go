package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/pazhou/supaflow/internal/isolation"
)

// EnvironmentResolver returns only explicitly granted environment values.
// Implementations must scope every lookup by tenant and attempt.
type EnvironmentResolver interface {
	ResolveEnvironment(context.Context, IsolationRequest, []string) (map[string]string, error)
}

// IsolationCapabilityProbe validates the real helper runtime during startup.
type IsolationCapabilityProbe func(context.Context, string, []string) error

// ProcessIsolationConfig configures the trusted isolation helper process.
type ProcessIsolationConfig struct {
	Binary      string
	HelperArgs  []string
	Environment EnvironmentResolver
	Timeout     time.Duration
	OutputLimit int64
	Limits      isolation.ResourceLimits
	CancelGrace time.Duration
	// CapabilityProbe exists for hermetic tests and custom process launchers.
	// Production callers leave it nil to execute the real helper startup probe.
	CapabilityProbe IsolationCapabilityProbe
}

// ProcessIsolationRunner invokes cmd/isolation rather than executing user code
// in the control-plane process.
type ProcessIsolationRunner struct{ cfg ProcessIsolationConfig }

// NewProcessIsolationRunner validates finite process limits and an absolute helper path.
func NewProcessIsolationRunner(cfg ProcessIsolationConfig) (*ProcessIsolationRunner, error) {
	if !filepath.IsAbs(cfg.Binary) || cfg.Timeout <= 0 || cfg.OutputLimit <= 0 {
		return nil, fmt.Errorf("executor: absolute isolation binary and positive limits are required")
	}
	if err := isolation.ValidateResourceLimits(cfg.Timeout, cfg.OutputLimit, cfg.Limits); err != nil {
		return nil, fmt.Errorf("executor: invalid isolation resource limits")
	}
	info, err := os.Stat(cfg.Binary)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return nil, fmt.Errorf("executor: isolation binary must be an executable regular file")
	}
	if cfg.CancelGrace <= 0 {
		cfg.CancelGrace = 1500 * time.Millisecond
	}
	probe := cfg.CapabilityProbe
	if probe == nil {
		probe = probeIsolationCapabilities
	}
	probeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err = probe(probeCtx, cfg.Binary, cfg.HelperArgs); err != nil {
		return nil, fmt.Errorf("executor: isolation capabilities are unavailable")
	}
	return &ProcessIsolationRunner{cfg: cfg}, nil
}

// Run sends one fenced invocation to the isolation helper over bounded JSON IPC.
func (r *ProcessIsolationRunner) Run(ctx context.Context, request IsolationRequest) (json.RawMessage, error) {
	environment := map[string]string{}
	if len(request.EnvAllowlist) > 0 {
		if r.cfg.Environment == nil {
			return nil, fmt.Errorf("environment resolver is unavailable")
		}
		resolved, err := r.cfg.Environment.ResolveEnvironment(ctx, request, request.EnvAllowlist)
		if err != nil {
			return nil, fmt.Errorf("resolve isolated environment")
		}
		environment = resolved
	}
	protocolRequest := isolation.Request{
		TenantID: string(request.TenantID), RunID: request.RunID, AttemptID: request.AttemptID,
		FencingToken: request.FencingToken, Language: request.Language, Source: request.Source,
		Args: request.Args, Environment: environment, Network: request.Network, Input: request.Input,
		Timeout: r.cfg.Timeout, OutputLimit: r.cfg.OutputLimit, Limits: r.cfg.Limits,
	}
	encoded, err := json.Marshal(protocolRequest)
	if err != nil {
		return nil, fmt.Errorf("encode isolation request: %w", err)
	}
	processCtx, cancel := context.WithTimeout(ctx, r.cfg.Timeout+r.cfg.CancelGrace+time.Second)
	defer cancel()
	command := exec.Command(r.cfg.Binary, r.cfg.HelperArgs...)
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Stdin = bytes.NewReader(encoded)
	var stdout bytes.Buffer
	command.Stdout = &stdout
	if err = runIsolationHelper(processCtx, command, r.cfg.CancelGrace); err != nil {
		return nil, fmt.Errorf("isolation helper failed")
	}
	if int64(stdout.Len()) > r.cfg.OutputLimit+4096 {
		return nil, fmt.Errorf("isolation protocol output exceeded limit")
	}
	var response isolation.Response
	decoder := json.NewDecoder(&stdout)
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&response); err != nil {
		return nil, fmt.Errorf("invalid isolation response")
	}
	if response.Code != "" {
		return nil, fmt.Errorf("isolated execution: %s", response.Code)
	}
	return response.Output, nil
}

func probeIsolationCapabilities(ctx context.Context, binary string, helperArgs []string) error {
	args := append(append([]string(nil), helperArgs...), isolation.CapabilityCheckArgs()...)
	command := exec.CommandContext(ctx, binary, args...)
	command.Stdout = nil
	command.Stderr = nil
	return command.Run()
}

func runIsolationHelper(ctx context.Context, command *exec.Cmd, grace time.Duration) error {
	if err := command.Start(); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		terminateHelperGroup(command, syscall.SIGTERM)
		timer := time.NewTimer(grace)
		defer timer.Stop()
		select {
		case <-done:
			return ctx.Err()
		case <-timer.C:
			terminateHelperGroup(command, syscall.SIGKILL)
			<-done
			return ctx.Err()
		}
	}
}

func terminateHelperGroup(command *exec.Cmd, signal syscall.Signal) {
	if command.Process == nil {
		return
	}
	if err := syscall.Kill(-command.Process.Pid, signal); err != nil {
		_ = command.Process.Signal(signal)
	}
}

var _ IsolationRunner = (*ProcessIsolationRunner)(nil)
