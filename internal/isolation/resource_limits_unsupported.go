//go:build !linux && !darwin

package isolation

func execWithResourceLimits(resolvedLimits, string, []string) error {
	return errResourceLimitsUnavailable
}
