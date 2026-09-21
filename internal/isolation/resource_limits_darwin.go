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
//     /usr/bin/python3 links libxcselect.dylib, which rewrites a ~1.4 MB xcrun_db
//     cache into TMPDIR on every invocation (measured: 1,431,272 bytes on macOS 26);
//     any lower RLIMIT_FSIZE kills the interpreter with SIGXFSZ before user code runs,
//     and the production ceiling is the request OutputLimit, which is 1 MiB for worker
//     runs. Neither TMPDIR, HOME, PYTHONPYCACHEPREFIX, PYTHONDONTWRITEBYTECODE nor -B
//     moves that write out of the process, so the ceiling and Python are mutually
//     exclusive here. The same xcselect dependency breaks every other developer tool
//     the platform shims, which makes dropping the ceiling the honest choice: the
//     degraded runner already abandons filesystem confinement, so it cannot claim a
//     per-file byte limit that its own interpreter is forbidden to exceed.
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
