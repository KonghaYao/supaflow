package platformops

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

func integrationRunOutput() *ImportedRunOutput {
	value := json.RawMessage(`{"legacyOutput":true}`)
	digest := sha256.Sum256(value)
	return &ImportedRunOutput{
		Value: value, ContentType: "application/json", Size: int64(len(value)),
		SHA256: hex.EncodeToString(digest[:]), Sensitive: false,
	}
}
