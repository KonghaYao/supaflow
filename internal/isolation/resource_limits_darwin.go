//go:build darwin

package isolation

import "golang.org/x/sys/unix"

// resourceLimitsToApply lists the ceilings the degraded Darwin launcher can actually
// install. It is deliberately shorter than the Linux list, and every omission is a
// boundary the operator gave up by enabling the local runner:
//
//   - address_space is absent because the Darwin kernel rejects RLIMIT_AS with
//     EINVAL (measured on macOS 26), so the 256 MiB address-space ceiling the Linux
//     boundary enforces has no macOS equivalent.
//   - processes is absent for the reason documented on sandboxCommand: RLIMIT_NPROC
//     is per-host-user on Darwin and cannot express a per-invocation ceiling.
//   - file_size is absent because macOS cannot start a Python interpreter beneath it.
//     /usr/bin/python3 links libxcselect.dylib, which rebuilds its xcrun_db cache
//     whenever the interpreter's HOME changes, and the runner gives every attempt a
//     fresh HOME (see isolatedEnvironment), so it happens on every attempt. The rebuild
//     writes a new file in place of the old one, growing it in power-of-two steps to the
//     accumulated cache size (~1.4 MB here, measured 1,442,395 bytes; it grows as the
//     cache learns about more tools, so treat any exact figure as a snapshot, not a
//     constant). That write lands in the Darwin per-user temp directory and not the
//     attempt workspace, because libxcselect ignores TMPDIR. Any RLIMIT_FSIZE below the
//     cache size kills the interpreter with SIGXFSZ before user code runs, and it leaves
//     the partially written temp file behind: 79 such orphans totalling ~20 MB were the
//     measured residue of this comment's own investigation. The production ceiling is the
//     request OutputLimit, which is 1 MiB for worker runs, so the ceiling and Python are
//     mutually exclusive here. PYTHONPYCACHEPREFIX, PYTHONDONTWRITEBYTECODE and the
//     interpreter's own -B, -S, -E, -I and -s flags all leave that write in place, so
//     nothing the runner controls can move it out of the interpreter's startup path. The
//     same xcselect dependency breaks every other developer tool the platform shims,
//     which makes dropping the ceiling the honest choice: the degraded runner already
//     abandons filesystem confinement, so it cannot claim a per-file byte limit that its
//     own interpreter is forbidden to exceed.
//
// The remaining ceilings, the request timeout and the bounded output are all an
// attempt is bounded by; disk exhaustion is not defended against on Darwin.
//
// Keeping these lists per platform rather than filtering at runtime means a Linux
// host can never silently lose a ceiling it is supposed to enforce.
func resourceLimitsToApply(limits resolvedLimits) []resourceLimit {
	return []resourceLimit{
		{name: "cpu", resource: unix.RLIMIT_CPU, value: limits.cpuSeconds},
		{name: "open_files", resource: unix.RLIMIT_NOFILE, value: limits.openFiles},
	}
}
