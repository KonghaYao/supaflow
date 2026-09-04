package executor

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/pazhou/supaflow/internal/workflow"
)

// WorkerRunnerVersion is the executor protocol version implemented by this
// worker binary. Node versions may require a newer runner and fail startup.
const WorkerRunnerVersion = "1.0.0"

var supportedNodeExecutors = map[string]struct{}{
	"builtin.start": {}, "builtin.end": {}, "builtin.human-approval": {},
	"builtin.condition": {}, "builtin.switch": {}, "builtin.loop": {},
	"builtin.transform": {}, "builtin.subworkflow": {}, "builtin.delay": {},
	"builtin.wait": {}, "builtin.webhook-resume": {}, "http.request": {},
	"eino.llm": {}, "eino.agent": {}, "eino.tool": {},
	"isolated.shell": {}, "isolated.python": {},
}

// ValidateNodeRegistryCompatibility rejects a worker whose executable adapter
// cache cannot exactly honor every immutable version in the durable registry.
func ValidateNodeRegistryCompatibility(entries []workflow.NodeTypeVersion, cache *workflow.Registry, runnerVersion string) error {
	if cache == nil || len(entries) == 0 {
		return errors.New("executor: durable node registry and adapter cache are required")
	}
	if _, err := parseSemanticVersion(runnerVersion); err != nil {
		return fmt.Errorf("executor: invalid worker runner version: %w", err)
	}
	if err := cache.VerifyProjection(entries); err != nil {
		return fmt.Errorf("executor: durable node registry differs from adapter cache: %w", err)
	}
	seen := make(map[string]struct{}, len(entries))
	for _, persisted := range entries {
		key := persisted.Type + "@" + persisted.Version
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("executor: duplicate durable node version %s", key)
		}
		seen[key] = struct{}{}
		if _, supported := supportedNodeExecutors[persisted.Executor.Kind]; !supported {
			return fmt.Errorf("executor: unsupported durable executor %s for %s", persisted.Executor.Kind, key)
		}
		compatible, err := semanticVersionAtLeast(runnerVersion, persisted.Executor.MinimumRunnerVersion)
		if err != nil {
			return fmt.Errorf("executor: invalid minimum runner version for %s: %w", key, err)
		}
		if !compatible {
			return fmt.Errorf("executor: worker runner %s is older than %s required by %s", runnerVersion, persisted.Executor.MinimumRunnerVersion, key)
		}
	}
	return nil
}

func semanticVersionAtLeast(current, minimum string) (bool, error) {
	currentParts, err := parseSemanticVersion(current)
	if err != nil {
		return false, err
	}
	minimumParts, err := parseSemanticVersion(minimum)
	if err != nil {
		return false, err
	}
	for index := range currentParts {
		if currentParts[index] != minimumParts[index] {
			return currentParts[index] > minimumParts[index], nil
		}
	}
	return true, nil
}

func parseSemanticVersion(value string) ([3]uint64, error) {
	var result [3]uint64
	parts := strings.Split(value, ".")
	if len(parts) != len(result) {
		return result, errors.New("version must contain major, minor, and patch")
	}
	for index, part := range parts {
		if part == "" || (len(part) > 1 && part[0] == '0') {
			return result, errors.New("version components must be canonical unsigned integers")
		}
		parsed, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			return result, errors.New("version components must be unsigned integers")
		}
		result[index] = parsed
	}
	return result, nil
}
