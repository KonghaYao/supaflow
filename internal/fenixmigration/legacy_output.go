package fenixmigration

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"

	"github.com/pazhou/supaflow/internal/platformops"
)

const maximumLegacyInlineOutput = 4 << 20

func classifyLegacyOutput(raw json.RawMessage) (*platformops.ImportedRunOutput, error) {
	if !json.Valid(raw) || len(raw) > maximumLegacyInlineOutput {
		return nil, errors.New("legacy output is invalid or exceeds the safe inline migration limit")
	}
	var value any
	if json.Unmarshal(raw, &value) != nil {
		return nil, errors.New("legacy output cannot be classified")
	}
	digest := sha256.Sum256(raw)
	return &platformops.ImportedRunOutput{
		Value: append(json.RawMessage(nil), raw...), ContentType: "application/json",
		Size: int64(len(raw)), SHA256: hex.EncodeToString(digest[:]), Sensitive: legacyOutputSensitive(value),
	}, nil
}

func legacyOutputSensitive(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		for key, nested := range typed {
			normalized := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(key, "-", ""), "_", ""))
			for _, marker := range []string{"secret", "token", "password", "authorization", "apikey", "connectionstring"} {
				if strings.Contains(normalized, marker) {
					return true
				}
			}
			if legacyOutputSensitive(nested) {
				return true
			}
		}
	case []any:
		for _, nested := range typed {
			if legacyOutputSensitive(nested) {
				return true
			}
		}
	}
	return false
}
