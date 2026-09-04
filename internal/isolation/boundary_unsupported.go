//go:build !linux

package isolation

func prepareProductionBoundary(Request, resolvedLimits) (*executionBoundary, error) {
	return nil, errResourceLimitsUnavailable
}
