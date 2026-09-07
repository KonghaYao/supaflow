package runtime

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

const minimumResumeTokenSecretBytes = 32

type resumeTokenClaims struct {
	Version        int            `json:"v"`
	OrganizationID OrganizationID `json:"organizationId"`
	RunID          string         `json:"runId"`
	NodeID         string         `json:"nodeId"`
	AttemptID      string         `json:"attemptId"`
	InterruptID    string         `json:"interruptId"`
	Kind           string         `json:"kind"`
	ExpiresAt      int64          `json:"expiresAt"`
}

// VerifiedResumeToken is the authenticated identity scope carried by a resume
// token. It is produced only after constant-time signature and expiry checks.
type VerifiedResumeToken struct {
	OrganizationID OrganizationID
	RunID          string
	NodeID         string
	AttemptID      string
	InterruptID    string
	Kind           string
	ExpiresAt      time.Time
}

// ResumeTokenIssuer deterministically issues organization-scoped, expiring approval
// tokens. Callers may persist only Digest(token), never the raw token.
type ResumeTokenIssuer struct {
	secret []byte
}

// NewResumeTokenIssuer copies and validates an HMAC secret.
func NewResumeTokenIssuer(secret []byte) (*ResumeTokenIssuer, error) {
	if len(secret) < minimumResumeTokenSecretBytes {
		return nil, errors.New("resume token secret must contain at least 32 bytes")
	}
	return &ResumeTokenIssuer{secret: append([]byte(nil), secret...)}, nil
}

// Issue creates a token bound to every identity-bearing interrupt field.
func (i *ResumeTokenIssuer) Issue(interrupt Interrupt) (string, error) {
	if i == nil || len(i.secret) < minimumResumeTokenSecretBytes {
		return "", errors.New("resume token issuer is not configured")
	}
	if interrupt.OrganizationID == "" || interrupt.RunID == "" || interrupt.NodeID == "" ||
		interrupt.AttemptID == "" || interrupt.ID == "" || interrupt.Kind == "" || interrupt.ExpiresAt.IsZero() {
		return "", errors.New("interrupt token claims are incomplete")
	}
	claims, err := json.Marshal(resumeTokenClaims{
		Version: 1, OrganizationID: interrupt.OrganizationID, RunID: interrupt.RunID, NodeID: interrupt.NodeID,
		AttemptID: interrupt.AttemptID, InterruptID: interrupt.ID, Kind: interrupt.Kind,
		// PostgreSQL timestamps preserve microseconds, so canonicalize here to
		// guarantee worker and control-plane issue the identical token.
		ExpiresAt: interrupt.ExpiresAt.UTC().UnixMicro(),
	})
	if err != nil {
		return "", err
	}
	payload := base64.RawURLEncoding.EncodeToString(claims)
	mac := hmac.New(sha256.New, i.secret)
	_, _ = mac.Write([]byte(payload))
	return payload + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

// Verify authenticates and decodes a resume token without consulting mutable
// request data. The caller must still consume its digest transactionally.
func (i *ResumeTokenIssuer) Verify(token string, now time.Time) (VerifiedResumeToken, error) {
	verified, err := i.verifyClaims(token)
	if err != nil {
		return VerifiedResumeToken{}, err
	}
	if !now.Before(verified.ExpiresAt) {
		return VerifiedResumeToken{}, errors.New("resume token is expired")
	}
	return verified, nil
}

// verifyClaims authenticates immutable claims without enforcing their expiry.
// Webhook resume uses this before a transactional idempotency lookup so a
// previously committed response remains replayable after token expiry.
func (i *ResumeTokenIssuer) verifyClaims(token string) (VerifiedResumeToken, error) {
	if i == nil || len(i.secret) < minimumResumeTokenSecretBytes {
		return VerifiedResumeToken{}, errors.New("resume token issuer is not configured")
	}
	payload, signature, found := strings.Cut(token, ".")
	if !found || payload == "" || signature == "" {
		return VerifiedResumeToken{}, errors.New("resume token is invalid")
	}
	providedMAC, err := base64.RawURLEncoding.DecodeString(signature)
	if err != nil {
		return VerifiedResumeToken{}, errors.New("resume token is invalid")
	}
	mac := hmac.New(sha256.New, i.secret)
	_, _ = mac.Write([]byte(payload))
	if !hmac.Equal(providedMAC, mac.Sum(nil)) {
		return VerifiedResumeToken{}, errors.New("resume token is invalid")
	}
	encoded, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return VerifiedResumeToken{}, errors.New("resume token is invalid")
	}
	var claims resumeTokenClaims
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&claims); err != nil || claims.Version != 1 || claims.OrganizationID == "" ||
		claims.RunID == "" || claims.NodeID == "" || claims.AttemptID == "" || claims.InterruptID == "" || claims.Kind == "" {
		return VerifiedResumeToken{}, errors.New("resume token is invalid")
	}
	expiresAt := time.UnixMicro(claims.ExpiresAt).UTC()
	return VerifiedResumeToken{
		OrganizationID: claims.OrganizationID, RunID: claims.RunID, NodeID: claims.NodeID,
		AttemptID: claims.AttemptID, InterruptID: claims.InterruptID,
		Kind: claims.Kind, ExpiresAt: expiresAt,
	}, nil
}

// ResumeTokenDigest returns the only resume-token representation permitted in
// durable state, events, and logs.
func ResumeTokenDigest(token string) [sha256.Size]byte {
	return sha256.Sum256([]byte(token))
}
