package platformops

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/postgres"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

// 验证 supervisor 所用 topic dispatcher 将 firing 幂等变成 run，且不会消费其他 outbox topic。
func TestPostgresTriggerRunDeliveryIsIdempotentAndTopicScoped(t *testing.T) {
	databaseURL := os.Getenv("SUPAFLOW_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("SUPAFLOW_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	organizationID, workflowID := "delivery_organization_"+suffix, "delivery_flow_"+suffix
	if err = seedPlatformOrganization(ctx, pool, organizationID, workflowID); err != nil {
		t.Fatal(err)
	}
	registry := workflow.BuiltinRegistry()
	policy := workflow.CompilePolicy{AllowedCapabilities: workflow.CapabilitySet{Streaming: true, Cancellable: true, Interruptible: true, Idempotent: true}, AllowedPermissions: workflow.PermissionSet{Network: true, Credentials: true}}
	catalog := postgres.NewPlanCatalog(pool, registry, policy)
	definitionValue := workflow.WorkflowDefinition{SchemaVersion: workflow.DefinitionSchemaV1, ID: workflowID,
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Type: workflow.NodeTypeStart, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
			{ID: "end", Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
		},
		Edges: []workflow.EdgeDefinition{{ID: "edge", FromNode: "start", FromPort: "input", ToNode: "end", ToPort: "result"}},
	}
	versionOne, _, err := catalog.Publish(ctx, organizationID, workflowID, definitionValue, "delivery-publication")
	if err != nil {
		t.Fatal(err)
	}
	runs := workflowruntime.NewService(postgres.NewRuntimeStore(pool), nil, nil)
	delivery, err := NewTriggerRunDelivery(pool, catalog, runs)
	if err != nil {
		t.Fatal(err)
	}
	triggerStore := NewPostgresTriggerStore(pool)
	triggers, err := NewTriggerService(triggerStore, nil, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	trigger, err := triggers.Create(ctx, CreateTriggerRequest{OrganizationID: organizationID, ActorID: "operator", WorkflowID: workflowID, Kind: TriggerAPI, Name: "api"})
	if err != nil {
		t.Fatal(err)
	}
	firing, created, err := triggers.Fire(ctx, FireRequest{OrganizationID: organizationID, TriggerID: trigger.ID, ExpectedKind: TriggerAPI, DeduplicationKey: "delivery-request", Payload: json.RawMessage(`{"value":1}`)})
	if err != nil || !created {
		t.Fatalf("firing=%+v created=%v err=%v", firing, created, err)
	}
	definitionValue.Metadata = map[string]json.RawMessage{"revision": json.RawMessage(`2`)}
	versionTwo, _, err := catalog.Publish(ctx, organizationID, workflowID, definitionValue, "delivery-publication-v2")
	if err != nil || versionTwo.ID == versionOne.ID {
		t.Fatalf("publish v2=%+v err=%v", versionTwo, err)
	}
	if err = triggers.Delete(ctx, DeleteTriggerRequest{OrganizationID: organizationID, ActorID: "operator", TriggerID: trigger.ID,
		ExpectedUpdatedAt: trigger.UpdatedAt, IdempotencyKey: "delete-accepted-trigger"}); err != nil {
		t.Fatalf("delete accepted trigger: %v", err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO outbox_messages (id,organization_id,aggregate_type,aggregate_id,topic,payload) VALUES ($1,$2,'run','unrelated','run.created','{}')`, "unrelated_"+suffix, organizationID); err != nil {
		t.Fatal(err)
	}
	dispatcher, err := NewOutboxDispatcher(NewPostgresTopicDeliveryStore(pool, "trigger.fired"), delivery, "delivery-worker", time.Minute, time.Second, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	result, err := dispatcher.DispatchOne(ctx, organizationID, time.Now().UTC())
	if err != nil || !result.Delivered {
		t.Fatalf("delivery=%+v err=%v", result, err)
	}
	if replay, replayErr := dispatcher.DispatchOne(ctx, organizationID, time.Now().UTC()); replayErr != nil || replay.MessageID != "" {
		t.Fatalf("replay delivery=%+v err=%v", replay, replayErr)
	}
	var runCount, unrelatedPending int
	var deliveredPlanID string
	if err = pool.QueryRow(ctx, `SELECT count(*),max(execution_plan_id) FROM runs
		WHERE organization_id=$1 AND idempotency_key=$2`, organizationID, "trigger:"+firing.ID).Scan(&runCount, &deliveredPlanID); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM outbox_messages WHERE organization_id=$1 AND id=$2 AND topic='run.created' AND delivered_at IS NULL`, organizationID, "unrelated_"+suffix).Scan(&unrelatedPending); err != nil {
		t.Fatal(err)
	}
	if runCount != 1 || unrelatedPending != 1 || deliveredPlanID != versionOne.ID || deliveredPlanID == versionTwo.ID {
		t.Fatalf("run count=%d plan=%s v1=%s v2=%s unrelated pending=%d", runCount, deliveredPlanID, versionOne.ID, versionTwo.ID, unrelatedPending)
	}
}
