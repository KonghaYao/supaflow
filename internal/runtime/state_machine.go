package runtime

var runTransitions = map[RunStatus]map[RunStatus]struct{}{
	RunPending: {
		RunRunning: {}, RunCancelling: {}, RunCancelled: {}, RunError: {},
	},
	RunRunning: {
		RunSuspended: {}, RunCancelling: {}, RunSuccess: {}, RunFailed: {}, RunError: {}, RunCancelled: {},
	},
	RunSuspended: {
		RunPending: {}, RunRunning: {}, RunCancelling: {}, RunCancelled: {}, RunError: {},
	},
	RunCancelling: {
		RunCancelled: {}, RunError: {},
	},
}

// CanTransitionRun reports whether a run transition is legal. Same-state writes
// are not transitions and must be handled explicitly as idempotent operations.
func CanTransitionRun(from, to RunStatus) bool {
	_, ok := runTransitions[from][to]
	return ok
}

// IsTerminalRun reports whether status cannot transition to another run state.
func IsTerminalRun(status RunStatus) bool {
	switch status {
	case RunSuccess, RunFailed, RunError, RunCancelled:
		return true
	default:
		return false
	}
}

func requireRunTransition(from, to RunStatus) error {
	if !CanTransitionRun(from, to) {
		return errorf(CodeInvalidTransition, "run cannot transition from %s to %s", from, to)
	}
	return nil
}
