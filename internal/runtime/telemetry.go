package runtime

import "context"

// Telemetry starts privacy-safe runtime operations. Implementations own tenant
// hashing and must never export raw tenant identifiers.
type Telemetry interface {
	Start(context.Context, string, string) (context.Context, func(error))
}
