package runtime

import "context"

// Telemetry starts privacy-safe runtime operations. Implementations own organization
// hashing and must never export raw organization identifiers.
type Telemetry interface {
	Start(context.Context, string, string) (context.Context, func(error))
}
