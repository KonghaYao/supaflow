package main

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/api"
	"github.com/pazhou/supaflow/internal/definition"
	"github.com/pazhou/supaflow/internal/identity"
	"github.com/pazhou/supaflow/internal/platform"
	"github.com/pazhou/supaflow/internal/platformops"
	platformhttp "github.com/pazhou/supaflow/internal/platformops/httpapi"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

type operationsRuntime struct {
	handler    *platformhttp.Handler
	supervisor *platformops.Supervisor
	telemetry  *platformops.Telemetry
	audit      *platformops.AuditService
	outputs    *platformops.OutputManager
}

func configureOperations(ctx context.Context, pool *pgxpool.Pool, catalog platform.PlanCatalog, definitions *definition.Service, runs *workflowruntime.Service) (operationsRuntime, error) {
	masterKey, err := decodeKey(strings.TrimSpace(os.Getenv("SUPAFLOW_CREDENTIAL_MASTER_KEY")))
	if err != nil {
		return operationsRuntime{}, errors.New("SUPAFLOW_CREDENTIAL_MASTER_KEY must encode exactly 32 bytes")
	}
	keyID := strings.TrimSpace(os.Getenv("SUPAFLOW_CREDENTIAL_KEY_ID"))
	if keyID == "" {
		return operationsRuntime{}, errors.New("SUPAFLOW_CREDENTIAL_KEY_ID is required")
	}
	provider, err := platformops.NewLocalAESProvider(keyID, masterKey)
	clearKey(masterKey)
	if err != nil {
		return operationsRuntime{}, err
	}
	credentials, err := platformops.NewCredentialService(platformops.NewPostgresCredentialStore(pool), provider)
	if err != nil {
		return operationsRuntime{}, err
	}
	triggerStore := platformops.NewPostgresTriggerStore(pool)
	triggers, err := platformops.NewTriggerService(triggerStore, credentials, 5*time.Minute)
	if err != nil {
		return operationsRuntime{}, err
	}
	admission, err := platformops.NewAdmission(pool, 5*time.Second)
	if err != nil {
		return operationsRuntime{}, err
	}
	audit, err := platformops.NewAuditService(pool)
	if err != nil {
		return operationsRuntime{}, err
	}
	objectRoot := strings.TrimSpace(os.Getenv("SUPAFLOW_OBJECT_STORE_DIR"))
	if objectRoot == "" {
		objectRoot = "./data/objects"
	}
	objects, err := platformops.NewFileObjectStore(objectRoot)
	if err != nil {
		return operationsRuntime{}, err
	}
	outputs, err := platformops.NewOutputManager(pool, objects, 1<<30)
	if err != nil {
		return operationsRuntime{}, err
	}
	retention, err := platformops.NewRetention(pool, objects, 1<<30)
	if err != nil {
		return operationsRuntime{}, err
	}
	readiness, err := platformops.NewReadiness(platformops.ReadinessCheck{Name: "postgres", Check: pool.Ping})
	if err != nil {
		return operationsRuntime{}, err
	}
	delivery, err := platformops.NewTriggerRunDelivery(pool, catalog, runs)
	if err != nil {
		return operationsRuntime{}, err
	}
	webhookRepository := platformops.NewOutboundWebhookRepository(pool)
	mutations, err := platformops.NewPostgresMutationLedger(pool)
	if err != nil {
		return operationsRuntime{}, err
	}
	webhookClient := platformops.NewSafeOutboundWebhookClient(splitList(os.Getenv("SUPAFLOW_WEBHOOK_EGRESS_ALLOWLIST")))
	webhookDelivery, err := platformops.NewOutboundWebhookDelivery(webhookRepository, credentials, webhookClient)
	if err != nil {
		return operationsRuntime{}, err
	}
	deliveryRouter, err := platformops.NewDeliveryRouter(delivery, webhookDelivery)
	if err != nil {
		return operationsRuntime{}, err
	}
	deliveryStore := platformops.NewPostgresDeliveryStore(pool)
	dispatcher, err := platformops.NewOutboxDispatcher(deliveryStore, deliveryRouter, "control-plane", 30*time.Second, time.Second, 5*time.Minute)
	if err != nil {
		return operationsRuntime{}, err
	}
	supervisorConfig, err := supervisorConfigFromEnv()
	if err != nil {
		return operationsRuntime{}, err
	}
	supervisor, err := platformops.NewSupervisor(pool, triggers, triggerStore, dispatcher, retention, supervisorConfig, slog.Default())
	if err != nil {
		return operationsRuntime{}, err
	}
	handler, err := platformhttp.NewHandler(platformhttp.Services{
		Triggers: triggers, Credentials: credentials, Admission: admission, Audit: audit,
		Retention: retention, Delivery: platformops.NewPostgresDeliveryStore(pool),
		Capacity: platformops.NewCapacityMonitor(pool), Workers: platformops.NewWorkerRegistry(pool), Readiness: readiness,
		Webhooks:  webhookRepository,
		Mutations: mutations,
	})
	if err != nil {
		return operationsRuntime{}, err
	}
	telemetry, err := configureTelemetry(ctx)
	if err != nil {
		return operationsRuntime{}, err
	}
	if telemetry != nil {
		dispatcher.SetTelemetry(telemetry)
	}
	return operationsRuntime{handler: handler, supervisor: supervisor, telemetry: telemetry, audit: audit, outputs: outputs}, nil
}

func supervisorConfigFromEnv() (platformops.SupervisorConfig, error) {
	readPositive := func(name string, fallback int) (int, error) {
		value := strings.TrimSpace(os.Getenv(name))
		if value == "" {
			return fallback, nil
		}
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed <= 0 {
			return 0, fmt.Errorf("%s must be a positive integer", name)
		}
		return parsed, nil
	}
	concurrency, err := readPositive("SUPAFLOW_DELIVERY_CONCURRENCY", 4)
	if err != nil {
		return platformops.SupervisorConfig{}, err
	}
	timeoutMS, err := readPositive("SUPAFLOW_DELIVERY_TIMEOUT_MS", 20000)
	if err != nil || timeoutMS > 30000 {
		return platformops.SupervisorConfig{}, errors.New("SUPAFLOW_DELIVERY_TIMEOUT_MS must be between 1 and 30000")
	}
	batch, err := readPositive("SUPAFLOW_DELIVERY_BATCH", 100)
	if err != nil {
		return platformops.SupervisorConfig{}, err
	}
	return platformops.SupervisorConfig{
		ScheduleInterval: time.Second, DeadlineInterval: time.Second, DeliveryInterval: time.Second,
		ObjectRecoveryInterval: time.Minute, RetentionInterval: time.Hour,
		DeliveryTimeout: time.Duration(timeoutMS) * time.Millisecond, ScheduleBatch: 100,
		DeliveryBatch: batch, DeliveryConcurrency: concurrency, RetentionBatch: 500,
	}, nil
}

func eventStreamConfigFromEnv() (api.EventStreamConfig, error) {
	read := func(name string, fallback int) (int, error) {
		value := strings.TrimSpace(os.Getenv(name))
		if value == "" {
			return fallback, nil
		}
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed <= 0 {
			return 0, fmt.Errorf("%s must be a positive integer", name)
		}
		return parsed, nil
	}
	global, err := read("SUPAFLOW_SSE_GLOBAL_LIMIT", 512)
	if err != nil {
		return api.EventStreamConfig{}, err
	}
	tenant, err := read("SUPAFLOW_SSE_TENANT_LIMIT", 64)
	if err != nil {
		return api.EventStreamConfig{}, err
	}
	pollMin, err := read("SUPAFLOW_SSE_POLL_MIN_MS", 250)
	if err != nil {
		return api.EventStreamConfig{}, err
	}
	pollMax, err := read("SUPAFLOW_SSE_POLL_MAX_MS", 2000)
	if err != nil {
		return api.EventStreamConfig{}, err
	}
	writeTimeout, err := read("SUPAFLOW_SSE_WRITE_TIMEOUT_MS", 5000)
	if err != nil {
		return api.EventStreamConfig{}, err
	}
	return api.EventStreamConfig{GlobalLimit: global, TenantLimit: tenant,
		PollMin: time.Duration(pollMin) * time.Millisecond, PollMax: time.Duration(pollMax) * time.Millisecond,
		WriteTimeout: time.Duration(writeTimeout) * time.Millisecond}, nil
}

func splitList(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func configureTelemetry(ctx context.Context) (*platformops.Telemetry, error) {
	endpoint := strings.TrimSpace(os.Getenv("SUPAFLOW_OTEL_ENDPOINT"))
	if endpoint == "" {
		return nil, nil
	}
	tenantKey, err := decodeKey(strings.TrimSpace(os.Getenv("SUPAFLOW_TENANT_HASH_KEY")))
	if err != nil {
		return nil, errors.New("SUPAFLOW_TENANT_HASH_KEY must encode exactly 32 bytes when telemetry is enabled")
	}
	defer clearKey(tenantKey)
	return platformops.NewTelemetry(ctx, platformops.TelemetryConfig{
		ServiceName: "supaflow-control-plane", Endpoint: endpoint,
		Insecure:      strings.EqualFold(strings.TrimSpace(os.Getenv("SUPAFLOW_OTEL_INSECURE")), "true"),
		TenantHashKey: tenantKey, MetricInterval: 15 * time.Second,
	})
}

func decodeKey(value string) ([]byte, error) {
	if value == "" {
		return nil, errors.New("key is empty")
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		decoded, err = base64.StdEncoding.DecodeString(value)
	}
	if err != nil || len(decoded) != 32 {
		clearKey(decoded)
		return nil, errors.New("invalid key")
	}
	return decoded, nil
}

func clearKey(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

func mountHandlers(core http.Handler, ops *platformhttp.Handler, auth *api.Authenticator, identityRoutes *identity.HTTPHandler, telemetry *platformops.Telemetry) http.Handler {
	public := func(handler http.Handler) http.Handler { return handler }
	if telemetry != nil {
		public = telemetry.HTTPMiddleware
	}
	root := http.NewServeMux()
	root.Handle("/hooks/resume/", public(core))
	webhookRoutes := ops.WebhookRoutes()
	if telemetry != nil {
		webhookRoutes = platformops.NewDeferredTenantTelemetryHTTPMiddleware(telemetry, webhookRoutes)
	}
	root.Handle("/hooks/", webhookRoutes)
	root.Handle("/ops/", auth.Middleware(ops.Routes()))
	if identityRoutes != nil {
		root.Handle("/auth/", public(identityRoutes.Routes()))
	}
	root.Handle("/v1/", core)
	root.Handle("/", public(core))
	return root
}
