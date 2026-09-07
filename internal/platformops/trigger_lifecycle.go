package platformops

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"time"
)

// ListTriggersRequest selects a bounded organization-owned trigger collection.
type ListTriggersRequest struct {
	OrganizationID string
	WorkflowID     string
	Kind           TriggerKind
	Enabled        *bool
	Limit          int
}

// UpdateTriggerRequest changes mutable trigger configuration using optimistic concurrency.
type UpdateTriggerRequest struct {
	OrganizationID      string
	ActorID             string
	TriggerID           string
	Name                string
	Config              json.RawMessage
	ScheduleInterval    time.Duration
	MisfirePolicy       MisfirePolicy
	FirstFireAt         *time.Time
	SigningCredentialID string
	EventType           string
	ExpectedUpdatedAt   time.Time
	IdempotencyKey      string
	RequestHash         [sha256.Size]byte
}

// ChangeTriggerStateRequest enables or disables one trigger idempotently.
type ChangeTriggerStateRequest struct {
	OrganizationID    string
	ActorID           string
	TriggerID         string
	Enabled           bool
	ExpectedUpdatedAt time.Time
	IdempotencyKey    string
	RequestHash       [sha256.Size]byte
	Now               time.Time
}

// DeleteTriggerRequest deletes one trigger after an optimistic revision check.
type DeleteTriggerRequest struct {
	OrganizationID    string
	ActorID           string
	TriggerID         string
	ExpectedUpdatedAt time.Time
	IdempotencyKey    string
	RequestHash       [sha256.Size]byte
}

// TriggerLifecycleStore is the durable CRUD read/write port used by Studio.
type TriggerLifecycleStore interface {
	ListTriggers(context.Context, ListTriggersRequest) ([]Trigger, error)
	UpdateTrigger(context.Context, UpdateTriggerRequest) (Trigger, error)
	SetTriggerEnabledIdempotently(context.Context, ChangeTriggerStateRequest) (Trigger, error)
	DeleteTrigger(context.Context, DeleteTriggerRequest) error
}

// Get returns one organization-owned trigger without secret material.
func (s *TriggerService) Get(ctx context.Context, organizationID, triggerID string) (Trigger, error) {
	if organizationID == "" || triggerID == "" {
		return Trigger{}, ErrInvalidArgument
	}
	return s.store.GetTrigger(ctx, organizationID, triggerID)
}

// List returns bounded organization-owned trigger definitions.
func (s *TriggerService) List(ctx context.Context, request ListTriggersRequest) ([]Trigger, error) {
	if request.OrganizationID == "" || request.Limit <= 0 || request.Limit > 200 {
		return nil, ErrInvalidArgument
	}
	store, ok := s.store.(TriggerLifecycleStore)
	if !ok {
		return nil, ErrConflict
	}
	return store.ListTriggers(ctx, request)
}

// Update validates mutable configuration and applies an optimistic idempotent update.
func (s *TriggerService) Update(ctx context.Context, request UpdateTriggerRequest) (Trigger, error) {
	if request.OrganizationID == "" || request.ActorID == "" || request.TriggerID == "" || request.ExpectedUpdatedAt.IsZero() || !validMutationKey(request.IdempotencyKey) {
		return Trigger{}, ErrInvalidArgument
	}
	current, err := s.store.GetTrigger(ctx, request.OrganizationID, request.TriggerID)
	if err != nil {
		return Trigger{}, err
	}
	if err = validateTriggerRequest(CreateTriggerRequest{OrganizationID: request.OrganizationID, ActorID: request.ActorID,
		WorkflowID: current.WorkflowID, Kind: current.Kind, Name: request.Name, Config: request.Config,
		ScheduleInterval: request.ScheduleInterval, MisfirePolicy: request.MisfirePolicy, FirstFireAt: request.FirstFireAt,
		SigningCredentialID: request.SigningCredentialID, EventType: request.EventType}); err != nil {
		return Trigger{}, err
	}
	request.RequestHash = triggerMutationHash("update", request)
	store, ok := s.store.(TriggerLifecycleStore)
	if !ok {
		return Trigger{}, ErrConflict
	}
	return store.UpdateTrigger(ctx, request)
}

// ChangeState idempotently enables or disables a trigger at an expected revision.
func (s *TriggerService) ChangeState(ctx context.Context, request ChangeTriggerStateRequest) (Trigger, error) {
	if request.OrganizationID == "" || request.ActorID == "" || request.TriggerID == "" || request.ExpectedUpdatedAt.IsZero() ||
		request.Now.IsZero() || !validMutationKey(request.IdempotencyKey) {
		return Trigger{}, ErrInvalidArgument
	}
	request.RequestHash = triggerMutationHash("state", struct {
		OrganizationID    string
		ActorID           string
		TriggerID         string
		Enabled           bool
		ExpectedUpdatedAt time.Time
		IdempotencyKey    string
	}{request.OrganizationID, request.ActorID, request.TriggerID, request.Enabled, request.ExpectedUpdatedAt, request.IdempotencyKey})
	store, ok := s.store.(TriggerLifecycleStore)
	if !ok {
		return Trigger{}, ErrConflict
	}
	return store.SetTriggerEnabledIdempotently(ctx, request)
}

// Delete idempotently removes one trigger at an expected revision.
func (s *TriggerService) Delete(ctx context.Context, request DeleteTriggerRequest) error {
	if request.OrganizationID == "" || request.ActorID == "" || request.TriggerID == "" || request.ExpectedUpdatedAt.IsZero() || !validMutationKey(request.IdempotencyKey) {
		return ErrInvalidArgument
	}
	request.RequestHash = triggerMutationHash("delete", request)
	store, ok := s.store.(TriggerLifecycleStore)
	if !ok {
		return ErrConflict
	}
	return store.DeleteTrigger(ctx, request)
}

func validMutationKey(value string) bool { return len(value) >= 8 && len(value) <= 128 }

func triggerMutationHash(action string, value any) [sha256.Size]byte {
	encoded, err := json.Marshal(struct {
		Action string `json:"action"`
		Value  any    `json:"value"`
	}{Action: action, Value: value})
	if err != nil {
		return sha256.Sum256([]byte(action))
	}
	return sha256.Sum256(encoded)
}
