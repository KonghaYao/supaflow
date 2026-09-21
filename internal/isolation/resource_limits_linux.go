//go:build linux

package isolation

import "golang.org/x/sys/unix"

// resourceLimitsToApply lists the ceilings the production launcher installs inside
// the sandbox. The per-invocation process ceiling is intentionally absent: on Linux
// it comes from the attempt cgroup, not from RLIMIT_NPROC.
func resourceLimitsToApply(limits resolvedLimits) []resourceLimit {
	return []resourceLimit{
		{name: "cpu", resource: unix.RLIMIT_CPU, value: limits.cpuSeconds},
		{name: "address_space", resource: unix.RLIMIT_AS, value: limits.addressSpaceBytes},
		{name: "open_files", resource: unix.RLIMIT_NOFILE, value: limits.openFiles},
		{name: "file_size", resource: unix.RLIMIT_FSIZE, value: limits.fileSizeBytes},
	}
}
