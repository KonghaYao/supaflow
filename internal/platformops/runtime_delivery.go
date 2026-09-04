package platformops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/platform"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

// RunStarter is the narrow durable-runtime seam used by trigger delivery.
type RunStarter interface {
	StartRun(context.Context, workflowruntime.TenantID, workflowruntime.StartRunRequest) (workflowruntime.CreateRunResult, error)
}

// TriggerRunDelivery turns a committed trigger.fired outbox message into one idempotent run.
type TriggerRunDelivery struct {
	pool    *pgxpool.Pool
	catalog platform.PlanCatalog
	runs    RunStarter
}

// NewTriggerRunDelivery validates the production trigger delivery dependencies.
func NewTriggerRunDelivery(pool *pgxpool.Pool, catalog platform.PlanCatalog, runs RunStarter) (*TriggerRunDelivery, error) {
	if pool == nil || catalog == nil || runs == nil {
		return nil, ErrInvalidArgument
	}
	return &TriggerRunDelivery{pool: pool, catalog: catalog, runs: runs}, nil
}

// Deliver resolves the immutable plan bound by the acceptance transaction and
// creates a run keyed by firing ID. It never consults mutable trigger state.
func (d *TriggerRunDelivery) Deliver(ctx context.Context, message DeliveryMessage) error {
	if message.Topic != "trigger.fired" || message.TenantID == "" {
		return ErrInvalidArgument
	}
	var envelope struct {
		FiringID          string          `json:"firingId"`
		TriggerID         string          `json:"triggerId"`
		WorkflowID        string          `json:"workflowId"`
		WorkflowVersionID string          `json:"workflowVersionId"`
		ExecutionPlanID   string          `json:"executionPlanId"`
		PlanHash          string          `json:"planHash"`
		Payload           json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(message.Payload, &envelope); err != nil || envelope.FiringID == "" || envelope.TriggerID == "" ||
		envelope.WorkflowID == "" || envelope.WorkflowVersionID == "" || envelope.ExecutionPlanID == "" ||
		envelope.PlanHash == "" || !json.Valid(envelope.Payload) {
		return ErrInvalidArgument
	}
	published, err := d.catalog.Resolve(ctx, message.TenantID, envelope.ExecutionPlanID, envelope.PlanHash)
	if err != nil {
		return fmt.Errorf("resolve trigger execution plan: %w", err)
	}
	if published.WorkflowID != envelope.WorkflowID || published.Version == 0 {
		return errors.New("trigger execution target failed immutable identity verification")
	}
	order := published.Plan.TopologicalOrder()
	if len(order) == 0 {
		return errors.New("trigger execution plan has no entry node")
	}
	entry, found := published.Plan.Node(order[0])
	if !found {
		return errors.New("trigger execution plan entry is unavailable")
	}
	_, err = d.runs.StartRun(ctx, workflowruntime.TenantID(message.TenantID), workflowruntime.StartRunRequest{
		Plan:  workflowruntime.PlanRef{ID: published.ID, Hash: published.Hash, WorkflowID: published.WorkflowID, EntryNodeID: entry.ID, Executor: entry.Executor.Kind},
		Input: envelope.Payload, IdempotencyKey: "trigger:" + envelope.FiringID,
	})
	if err != nil {
		return fmt.Errorf("start trigger run: %w", err)
	}
	_, err = d.pool.Exec(ctx, `UPDATE trigger_firings SET status='dispatched'
		WHERE tenant_id=$1 AND id=$2 AND trigger_id=$3 AND workflow_version_id=$4 AND execution_plan_id=$5 AND plan_hash=$6`,
		message.TenantID, envelope.FiringID, envelope.TriggerID, envelope.WorkflowVersionID, envelope.ExecutionPlanID, envelope.PlanHash)
	if err != nil {
		return fmt.Errorf("mark trigger firing dispatched: %w", err)
	}
	return nil
}
