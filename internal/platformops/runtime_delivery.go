package platformops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/definition"
	"github.com/pazhou/supaflow/internal/platform"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

// LatestVersionResolver resolves the immutable publication selected by a workflow trigger.
type LatestVersionResolver interface {
	LatestVersion(context.Context, string, string) (definition.Version, error)
}

// RunStarter is the narrow durable-runtime seam used by trigger delivery.
type RunStarter interface {
	StartRun(context.Context, workflowruntime.TenantID, workflowruntime.StartRunRequest) (workflowruntime.CreateRunResult, error)
}

// TriggerRunDelivery turns a committed trigger.fired outbox message into one idempotent run.
type TriggerRunDelivery struct {
	pool     *pgxpool.Pool
	versions LatestVersionResolver
	catalog  platform.PlanCatalog
	runs     RunStarter
}

// NewTriggerRunDelivery validates the production trigger delivery dependencies.
func NewTriggerRunDelivery(pool *pgxpool.Pool, versions LatestVersionResolver, catalog platform.PlanCatalog, runs RunStarter) (*TriggerRunDelivery, error) {
	if pool == nil || versions == nil || catalog == nil || runs == nil {
		return nil, ErrInvalidArgument
	}
	return &TriggerRunDelivery{pool: pool, versions: versions, catalog: catalog, runs: runs}, nil
}

// Deliver resolves the latest immutable plan and creates a run keyed by firing ID.
func (d *TriggerRunDelivery) Deliver(ctx context.Context, message DeliveryMessage) error {
	if message.Topic != "trigger.fired" || message.TenantID == "" {
		return ErrInvalidArgument
	}
	var envelope struct {
		FiringID  string          `json:"firingId"`
		TriggerID string          `json:"triggerId"`
		Payload   json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(message.Payload, &envelope); err != nil || envelope.FiringID == "" || envelope.TriggerID == "" || !json.Valid(envelope.Payload) {
		return ErrInvalidArgument
	}
	var workflowID string
	if err := d.pool.QueryRow(ctx, `SELECT workflow_id FROM triggers WHERE tenant_id=$1 AND id=$2`, message.TenantID, envelope.TriggerID).Scan(&workflowID); err != nil {
		return fmt.Errorf("resolve trigger workflow: %w", err)
	}
	version, err := d.versions.LatestVersion(ctx, message.TenantID, workflowID)
	if err != nil {
		return fmt.Errorf("resolve trigger workflow version: %w", err)
	}
	published, err := d.catalog.Resolve(ctx, message.TenantID, version.PlanID, version.PlanHash)
	if err != nil {
		return fmt.Errorf("resolve trigger execution plan: %w", err)
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
	_, err = d.pool.Exec(ctx, `UPDATE trigger_firings SET status='dispatched' WHERE tenant_id=$1 AND id=$2 AND trigger_id=$3`, message.TenantID, envelope.FiringID, envelope.TriggerID)
	if err != nil {
		return fmt.Errorf("mark trigger firing dispatched: %w", err)
	}
	return nil
}
