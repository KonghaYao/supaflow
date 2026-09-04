package isolation

import (
	"os"
	"strconv"
)

const (
	resourceLimitLauncherFlag  = "--supaflow-apply-resource-limits"
	resourceLimitLauncherPath  = "/supaflow-isolation-limit-launcher"
	resourceLimitFailureMarker = "supaflow_resource_limits_unavailable"
)

func resourceLimitLauncherArgs(limits resolvedLimits, interpreter string, args []string) []string {
	launcherArgs := []string{
		resourceLimitLauncherFlag,
		strconv.FormatUint(limits.cpuSeconds, 10),
		strconv.FormatUint(limits.addressSpaceBytes, 10),
		strconv.FormatUint(limits.processes, 10),
		strconv.FormatUint(limits.openFiles, 10),
		strconv.FormatUint(limits.fileSizeBytes, 10),
		interpreter,
	}
	return append(launcherArgs, args...)
}

// IsResourceLimitLauncher reports whether args request the trusted internal
// launcher mode. The isolation command must check this before decoding stdin.
func IsResourceLimitLauncher(args []string) bool {
	return len(args) > 0 && args[0] == resourceLimitLauncherFlag
}

// RunResourceLimitLauncher applies hard OS limits and replaces the launcher
// with the requested interpreter. It returns only when setup or exec fails.
func RunResourceLimitLauncher(args []string) int {
	if len(args) < 7 || args[0] != resourceLimitLauncherFlag {
		return 125
	}
	values := make([]uint64, 5)
	for index := range values {
		value, err := strconv.ParseUint(args[index+1], 10, 64)
		if err != nil || value == 0 {
			return 125
		}
		values[index] = value
	}
	limits := resolvedLimits{
		cpuSeconds: values[0], addressSpaceBytes: values[1], processes: values[2],
		openFiles: values[3], fileSizeBytes: values[4],
	}
	if err := execWithResourceLimits(limits, args[6], args[7:]); err != nil {
		_, _ = os.Stderr.WriteString(resourceLimitFailureMarker + "\n")
		return 125
	}
	return 125
}
