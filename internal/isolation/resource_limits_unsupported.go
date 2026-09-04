//go:build !linux

package isolation

func execWithResourceLimits(resolvedLimits, string, []string) error {
	return errResourceLimitsUnavailable
}
