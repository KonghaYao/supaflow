package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/json"
)

// ResumeWebhookCommand is the unauthenticated transport input authorized by a
// signed, expiring token. Organization and run scope never come from the body.
type ResumeWebhookCommand struct {
	InterruptID    string
	ResumeToken    string
	IdempotencyKey string
	Data           json.RawMessage
}

// ResumeWebhook consumes a webhook-only token and makes the suspended attempt
// runnable with the exact JSON callback value as ResumeData.
func (s *Service) ResumeWebhook(ctx context.Context, command ResumeWebhookCommand) (ResumeWebhookResult, error) {
	if command.ResumeToken == "" {
		return ResumeWebhookResult{}, errorf(CodeApprovalInvalid, "webhook resume token is required")
	}
	if len(command.IdempotencyKey) < 8 || len(command.IdempotencyKey) > 128 {
		return ResumeWebhookResult{}, errorf(CodeInvalidArgument, "idempotency key must contain 8 to 128 characters")
	}
	if len(command.Data) == 0 {
		command.Data = json.RawMessage(`null`)
	}
	if len(command.Data) > maxWebhookResumeBytes || !json.Valid(command.Data) {
		return ResumeWebhookResult{}, errorf(CodeInvalidArgument, "webhook data must be valid JSON and no larger than 256 KiB")
	}
	claims, err := s.verifyWebhookToken(command.ResumeToken)
	if err != nil {
		return ResumeWebhookResult{}, err
	}
	if command.InterruptID == "" || command.InterruptID != claims.InterruptID {
		return ResumeWebhookResult{}, errorf(CodeApprovalInvalid, "webhook resume token is invalid")
	}
	digest := ResumeTokenDigest(command.ResumeToken)
	requestHash, err := webhookRequestHash(claims.InterruptID, digest, command.Data)
	if err != nil {
		return ResumeWebhookResult{}, err
	}
	return s.store.ResumeWebhook(ctx, ResumeWebhookRequest{
		OrganizationID: claims.OrganizationID, InterruptID: claims.InterruptID, RunID: claims.RunID,
		NodeID: claims.NodeID, AttemptID: claims.AttemptID, TokenDigest: digest,
		IdempotencyKey: command.IdempotencyKey, RequestHash: requestHash,
		Data: cloneJSON(command.Data), Now: s.clock.Now(),
	})
}

func (s *Service) verifyWebhookToken(token string) (VerifiedResumeToken, error) {
	// The service intentionally shares the same issuer used by workers. It is
	// injected after construction to keep public tokens out of Store methods.
	if s.resumeTokens == nil {
		return VerifiedResumeToken{}, errorf(CodeConflict, "resume token verification is unavailable")
	}
	claims, err := s.resumeTokens.verifyClaims(token)
	if err != nil || claims.Kind != "webhook" {
		return VerifiedResumeToken{}, errorf(CodeApprovalInvalid, "webhook resume token is invalid")
	}
	return claims, nil
}

func webhookRequestHash(interruptID string, digest [32]byte, data json.RawMessage) ([32]byte, error) {
	canonical, err := json.Marshal(json.RawMessage(data))
	if err != nil {
		return [32]byte{}, errorf(CodeInvalidArgument, "webhook data is invalid")
	}
	h := sha256.New()
	h.Write([]byte(interruptID))
	h.Write([]byte{0})
	h.Write(digest[:])
	h.Write([]byte{0})
	h.Write(canonical)
	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result, nil
}
