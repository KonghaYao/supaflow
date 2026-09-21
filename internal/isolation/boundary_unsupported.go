//go:build !linux && !darwin

package isolation

func prepareProductionBoundary(Request, resolvedLimits) (*executionBoundary, error) {
	return nil, errResourceLimitsUnavailable
}
