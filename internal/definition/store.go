package definition

import "context"

// MutationRequest is the atomic persistence command for an idempotent draft mutation.
type MutationRequest struct {
	Draft            Draft
	ExpectedRevision uint64
	IdempotencyKey   string
	RequestHash      [32]byte
}

// Store is the tenant-scoped persistence port for drafts and version history.
type Store interface {
	CreateDraft(context.Context, MutationRequest) (CreateDraftResult, error)
	GetDraft(context.Context, string, string) (Draft, error)
	ListDrafts(context.Context, string, string, int) (DraftPage, error)
	UpdateDraft(context.Context, MutationRequest) (Draft, error)
	DeleteDraft(context.Context, MutationRequest) error
	RecordVersion(context.Context, Version) error
	ListVersions(context.Context, string, string, uint64, int) (VersionPage, error)
	GetVersion(context.Context, string, string, uint64) (Version, error)
	GetLatestVersion(context.Context, string, string) (Version, error)
}
