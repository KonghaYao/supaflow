//go:build linux

package isolation

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func applyResourceLimits(limits resolvedLimits) error {
	resources := [...]struct {
		name     string
		resource int
		value    uint64
	}{
		{name: "cpu", resource: unix.RLIMIT_CPU, value: limits.cpuSeconds},
		{name: "address_space", resource: unix.RLIMIT_AS, value: limits.addressSpaceBytes},
		{name: "open_files", resource: unix.RLIMIT_NOFILE, value: limits.openFiles},
		{name: "file_size", resource: unix.RLIMIT_FSIZE, value: limits.fileSizeBytes},
	}
	for _, item := range resources {
		var current unix.Rlimit
		if err := unix.Getrlimit(item.resource, &current); err != nil {
			return fmt.Errorf("get %s limit: %w", item.name, err)
		}
		// Never widen a stricter limit inherited from the worker/container.
		value := min(item.value, current.Cur, current.Max)
		if value == 0 {
			return fmt.Errorf("%s hard limit is zero", item.name)
		}
		if err := unix.Setrlimit(item.resource, &unix.Rlimit{Cur: value, Max: value}); err != nil {
			return fmt.Errorf("set %s limit: %w", item.name, err)
		}
	}
	return nil
}

func execWithResourceLimits(limits resolvedLimits, interpreter string, args []string) error {
	argv := append([]string{interpreter}, args...)
	environment := os.Environ()
	if err := applyResourceLimits(limits); err != nil {
		return err
	}
	return unix.Exec(interpreter, argv, environment)
}
