//go:build linux || darwin

package isolation

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// resourceLimit is one OS ceiling the launcher installs before it execs the interpreter.
type resourceLimit struct {
	name     string
	resource int
	value    uint64
}

// applyResourceLimits installs every ceiling this platform reports as enforceable.
// A platform that cannot express a ceiling omits it from resourceLimitsToApply; this
// function never swallows a Setrlimit failure, so a host that stops honouring a
// listed ceiling still fails closed instead of running unbounded.
func applyResourceLimits(limits resolvedLimits) error {
	for _, item := range resourceLimitsToApply(limits) {
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
