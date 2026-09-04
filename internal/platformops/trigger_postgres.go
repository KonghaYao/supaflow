package platformops

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresTriggerStore persists trigger definitions, replay guards, firings, and outbox atomically.
type PostgresTriggerStore struct{ pool *pgxpool.Pool }

// NewPostgresTriggerStore creates a durable trigger store.
func NewPostgresTriggerStore(pool *pgxpool.Pool) *PostgresTriggerStore {
	return &PostgresTriggerStore{pool: pool}
}

// CreateTrigger stores a validated trigger definition.
func (s *PostgresTriggerStore) CreateTrigger(ctx context.Context, request CreateTriggerRequest) (Trigger, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Trigger{}, triggerError("begin create trigger", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if request.IdempotencyKey != "" {
		replayed, found, claimErr := claimTriggerMutation(ctx, tx, request.TenantID, request.IdempotencyKey, "create", request.RequestHash, "")
		if claimErr != nil {
			return Trigger{}, claimErr
		}
		if found {
			return replayed, nil
		}
	}
	id, err := newID("trg")
	if err != nil {
		return Trigger{}, err
	}
	publicHash, err := newID("hook")
	if err != nil {
		return Trigger{}, err
	}
	config := request.Config
	if len(config) == 0 {
		config = json.RawMessage(`{}`)
	}
	var interval any
	if request.ScheduleInterval > 0 {
		interval = int64(request.ScheduleInterval / time.Second)
	}
	status := "enabled"
	row := tx.QueryRow(ctx, `
		INSERT INTO triggers (
		    tenant_id, id, public_hash, workflow_id, kind, name, status, config,
		    schedule_interval_seconds, misfire_policy, next_fire_at,
		    signing_credential_id, event_type, created_by
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NULLIF($12,''),NULLIF($13,''),$14)
		RETURNING tenant_id,id,public_hash,workflow_id,kind,name,status,config,
		          schedule_interval_seconds,COALESCE(misfire_policy,''),next_fire_at,
		          COALESCE(signing_credential_id,''),COALESCE(event_type,''),created_by,created_at,updated_at`,
		request.TenantID, id, publicHash, request.WorkflowID, request.Kind, request.Name, status, config,
		interval, nullableString(string(request.MisfirePolicy)), request.FirstFireAt,
		request.SigningCredentialID, request.EventType, request.ActorID)
	trigger, err := scanTrigger(row)
	if err != nil {
		return Trigger{}, triggerError("create trigger", err)
	}
	if request.IdempotencyKey != "" {
		if err = completeTriggerMutation(ctx, tx, request.TenantID, request.IdempotencyKey, trigger); err != nil {
			return Trigger{}, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return Trigger{}, triggerError("commit create trigger", err)
	}
	return trigger, nil
}

// GetTrigger returns a tenant-scoped trigger without secret material.
func (s *PostgresTriggerStore) GetTrigger(ctx context.Context, tenantID, triggerID string) (Trigger, error) {
	trigger, err := scanTrigger(s.pool.QueryRow(ctx, `
		SELECT tenant_id,id,public_hash,workflow_id,kind,name,status,config,
		       schedule_interval_seconds,COALESCE(misfire_policy,''),next_fire_at,
		       COALESCE(signing_credential_id,''),COALESCE(event_type,''),created_by,created_at,updated_at
		FROM triggers WHERE tenant_id=$1 AND id=$2`, tenantID, triggerID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Trigger{}, ErrNotFound
	}
	if err != nil {
		return Trigger{}, triggerError("get trigger", err)
	}
	return trigger, nil
}

// ResolvePublicWebhook maps a public route hash to server-owned tenant context.
func (s *PostgresTriggerStore) ResolvePublicWebhook(ctx context.Context, publicHash string) (Trigger, error) {
	trigger, err := scanTrigger(s.pool.QueryRow(ctx, `
		SELECT tenant_id,id,public_hash,workflow_id,kind,name,status,config,
		       schedule_interval_seconds,COALESCE(misfire_policy,''),next_fire_at,
		       COALESCE(signing_credential_id,''),COALESCE(event_type,''),created_by,created_at,updated_at
		FROM triggers WHERE public_hash=$1 AND kind='webhook' AND status='enabled'`, publicHash))
	if errors.Is(err, pgx.ErrNoRows) {
		return Trigger{}, ErrNotFound
	}
	if err != nil {
		return Trigger{}, triggerError("resolve public webhook", err)
	}
	return trigger, nil
}

// SetTriggerEnabled atomically changes the enabled state.
func (s *PostgresTriggerStore) SetTriggerEnabled(ctx context.Context, tenantID, triggerID string, enabled bool, now time.Time) (Trigger, error) {
	status := "disabled"
	if enabled {
		status = "enabled"
	}
	trigger, err := scanTrigger(s.pool.QueryRow(ctx, `
		UPDATE triggers SET status=$3,updated_at=$4
		WHERE tenant_id=$1 AND id=$2
		RETURNING tenant_id,id,public_hash,workflow_id,kind,name,status,config,
		          schedule_interval_seconds,COALESCE(misfire_policy,''),next_fire_at,
		          COALESCE(signing_credential_id,''),COALESCE(event_type,''),created_by,created_at,updated_at`,
		tenantID, triggerID, status, now))
	if errors.Is(err, pgx.ErrNoRows) {
		return Trigger{}, ErrNotFound
	}
	if err != nil {
		return Trigger{}, triggerError("set trigger state", err)
	}
	return trigger, nil
}

// Fire atomically deduplicates and enqueues one trigger firing.
func (s *PostgresTriggerStore) Fire(ctx context.Context, request FireRequest) (Firing, bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Firing{}, false, triggerError("begin firing", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err = requireEnabledTrigger(ctx, tx, request.TenantID, request.TriggerID, request.ExpectedKind); err != nil {
		return Firing{}, false, err
	}
	firing, created, err := insertFiringTx(ctx, tx, request)
	if err != nil {
		return Firing{}, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Firing{}, false, triggerError("commit firing", err)
	}
	return firing, created, nil
}

// FireWebhook consumes replay identity and enqueues the firing in one transaction.
func (s *PostgresTriggerStore) FireWebhook(ctx context.Context, request FireRequest, requestID string, digest [sha256.Size]byte, expiresAt time.Time) (Firing, bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Firing{}, false, triggerError("begin webhook firing", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err = requireEnabledTrigger(ctx, tx, request.TenantID, request.TriggerID, TriggerWebhook); err != nil {
		return Firing{}, false, err
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO webhook_replays (tenant_id,trigger_id,request_id,request_digest,expires_at)
		VALUES ($1,$2,$3,$4,$5) ON CONFLICT (tenant_id,trigger_id,request_id) DO NOTHING`,
		request.TenantID, request.TriggerID, requestID, digest[:], expiresAt)
	if err != nil {
		return Firing{}, false, triggerError("record webhook replay guard", err)
	}
	if tag.RowsAffected() != 1 {
		return Firing{}, false, ErrReplay
	}
	firing, created, err := insertFiringTx(ctx, tx, request)
	if err != nil {
		return Firing{}, false, err
	}
	metadata, _ := json.Marshal(map[string]any{
		"created": created, "requestIdHash": AuditIdentifier(requestID),
	})
	if _, err = insertAuditTx(ctx, tx, AuditRecord{
		TenantID: request.TenantID, ActorID: "public-webhook", Action: "trigger.webhook.receive",
		ResourceType: "trigger", ResourceID: request.TriggerID, Outcome: AuditAllowed, Metadata: metadata,
	}); err != nil {
		return Firing{}, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Firing{}, false, triggerError("commit webhook firing", err)
	}
	return firing, created, nil
}

// PurgeExpiredWebhookReplays deletes bounded expired replay guards after their signature window.
func (s *PostgresTriggerStore) PurgeExpiredWebhookReplays(ctx context.Context, before time.Time, limit int) (int, error) {
	if before.IsZero() || limit <= 0 || limit > 10000 {
		return 0, ErrInvalidArgument
	}
	tag, err := s.pool.Exec(ctx, `
		WITH expired AS (
		    SELECT tenant_id,trigger_id,request_id FROM webhook_replays
		    WHERE expires_at <= $1 ORDER BY expires_at,tenant_id,trigger_id,request_id LIMIT $2
		)
		DELETE FROM webhook_replays r USING expired e
		WHERE r.tenant_id=e.tenant_id AND r.trigger_id=e.trigger_id AND r.request_id=e.request_id`, before, limit)
	if err != nil {
		return 0, triggerError("purge webhook replay guards", err)
	}
	return int(tag.RowsAffected()), nil
}

// FireDueSchedules locks due schedules, advances their clocks, and enqueues occurrences atomically.
func (s *PostgresTriggerStore) FireDueSchedules(ctx context.Context, options ScheduleOptions) ([]Firing, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, triggerError("begin schedule poll", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	rows, err := tx.Query(ctx, `
		SELECT tenant_id,id,schedule_interval_seconds,misfire_policy,next_fire_at
		FROM triggers
		WHERE kind='schedule' AND status='enabled' AND next_fire_at <= $1
		ORDER BY next_fire_at,tenant_id,id FOR UPDATE SKIP LOCKED LIMIT $2`, options.Now, options.Limit)
	if err != nil {
		return nil, triggerError("select due schedules", err)
	}
	type dueSchedule struct {
		tenantID, id string
		interval     int
		policy       MisfirePolicy
		next         time.Time
	}
	due := make([]dueSchedule, 0, options.Limit)
	for rows.Next() {
		var item dueSchedule
		if err = rows.Scan(&item.tenantID, &item.id, &item.interval, &item.policy, &item.next); err != nil {
			rows.Close()
			return nil, triggerError("scan due schedule", err)
		}
		due = append(due, item)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, triggerError("read due schedules", err)
	}
	rows.Close()
	firings := make([]Firing, 0, len(due))
	for _, item := range due {
		occurrences, next := scheduleOccurrences(item.next, time.Duration(item.interval)*time.Second, item.policy, options)
		if _, err = tx.Exec(ctx, `UPDATE triggers SET next_fire_at=$3,updated_at=$4 WHERE tenant_id=$1 AND id=$2`, item.tenantID, item.id, next, options.Now); err != nil {
			return nil, triggerError("advance schedule", err)
		}
		for _, occurrence := range occurrences {
			payload, _ := json.Marshal(map[string]string{"scheduledAt": occurrence.Format(time.RFC3339Nano)})
			firing, created, fireErr := insertFiringTx(ctx, tx, FireRequest{
				TenantID: item.tenantID, TriggerID: item.id, ExpectedKind: TriggerSchedule,
				DeduplicationKey: "schedule:" + occurrence.Format(time.RFC3339Nano), SourceTime: &occurrence, Payload: payload,
			})
			if fireErr != nil {
				return nil, fireErr
			}
			if created {
				firings = append(firings, firing)
			}
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, triggerError("commit schedule poll", err)
	}
	return firings, nil
}

// FireEvent atomically fans one source event out to matching enabled triggers.
func (s *PostgresTriggerStore) FireEvent(ctx context.Context, tenantID, eventType, eventID string, payload json.RawMessage, at time.Time) ([]Firing, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, triggerError("begin event firing", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	rows, err := tx.Query(ctx, `SELECT id FROM triggers WHERE tenant_id=$1 AND kind='event' AND status='enabled' AND event_type=$2 ORDER BY id FOR UPDATE`, tenantID, eventType)
	if err != nil {
		return nil, triggerError("select event triggers", err)
	}
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return nil, triggerError("scan event trigger", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	firings := make([]Firing, 0, len(ids))
	for _, id := range ids {
		firing, created, fireErr := insertFiringTx(ctx, tx, FireRequest{
			TenantID: tenantID, TriggerID: id, ExpectedKind: TriggerEvent,
			DeduplicationKey: "event:" + eventID, SourceTime: &at, Payload: payload,
		})
		if fireErr != nil {
			return nil, fireErr
		}
		if created {
			firings = append(firings, firing)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, triggerError("commit event firing", err)
	}
	return firings, nil
}

func insertFiringTx(ctx context.Context, tx pgx.Tx, request FireRequest) (Firing, bool, error) {
	id, err := newID("fire")
	if err != nil {
		return Firing{}, false, err
	}
	payload := request.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	var firing Firing
	err = tx.QueryRow(ctx, `
		INSERT INTO trigger_firings (tenant_id,id,trigger_id,deduplication_key,source_time,payload)
		VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (tenant_id,trigger_id,deduplication_key) DO NOTHING
		RETURNING tenant_id,id,trigger_id,deduplication_key,source_time,payload,created_at`,
		request.TenantID, id, request.TriggerID, request.DeduplicationKey, request.SourceTime, payload).Scan(
		&firing.TenantID, &firing.ID, &firing.TriggerID, &firing.DeduplicationKey,
		&firing.SourceTime, &firing.Payload, &firing.CreatedAt)
	created := err == nil
	if errors.Is(err, pgx.ErrNoRows) {
		err = tx.QueryRow(ctx, `
			SELECT tenant_id,id,trigger_id,deduplication_key,source_time,payload,created_at
			FROM trigger_firings
			WHERE tenant_id=$1 AND trigger_id=$2 AND deduplication_key=$3
			  AND payload=$4::jsonb AND source_time IS NOT DISTINCT FROM $5`,
			request.TenantID, request.TriggerID, request.DeduplicationKey, payload, request.SourceTime).Scan(
			&firing.TenantID, &firing.ID, &firing.TriggerID, &firing.DeduplicationKey,
			&firing.SourceTime, &firing.Payload, &firing.CreatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return Firing{}, false, ErrConflict
		}
	}
	if err != nil {
		return Firing{}, false, triggerError("persist firing", err)
	}
	if created {
		envelope, _ := json.Marshal(map[string]any{
			"firingId": firing.ID, "triggerId": firing.TriggerID,
			"deduplicationKey": firing.DeduplicationKey, "payload": json.RawMessage(firing.Payload),
		})
		if err = insertPlatformOutboxTx(ctx, tx, firing.TenantID, "trigger", firing.TriggerID, "trigger.fired", envelope); err != nil {
			return Firing{}, false, err
		}
	}
	return firing, created, nil
}

func requireEnabledTrigger(ctx context.Context, tx pgx.Tx, tenantID, triggerID string, kind TriggerKind) error {
	var foundKind, status string
	err := tx.QueryRow(ctx, `SELECT kind,status FROM triggers WHERE tenant_id=$1 AND id=$2 FOR UPDATE`, tenantID, triggerID).Scan(&foundKind, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return triggerError("lock trigger", err)
	}
	if foundKind != string(kind) || status != "enabled" {
		return ErrConflict
	}
	return nil
}

func scheduleOccurrences(due time.Time, interval time.Duration, policy MisfirePolicy, options ScheduleOptions) ([]time.Time, time.Time) {
	steps := int(options.Now.Sub(due)/interval) + 1
	if steps < 1 {
		steps = 1
	}
	next := due.Add(time.Duration(steps) * interval)
	switch policy {
	case MisfireSkip:
		if options.Now.Sub(due) > options.MisfireGrace {
			return nil, next
		}
		return []time.Time{due}, next
	case MisfireFireOnce:
		return []time.Time{due}, next
	case MisfireCatchUp:
		count := steps
		if count > options.MaxCatchUp {
			count = options.MaxCatchUp
		}
		next = due.Add(time.Duration(count) * interval)
		occurrences := make([]time.Time, 0, count)
		for index := 0; index < count; index++ {
			occurrences = append(occurrences, due.Add(time.Duration(index)*interval))
		}
		return occurrences, next
	default:
		return nil, next
	}
}

func scanTrigger(row pgx.Row) (Trigger, error) {
	var trigger Trigger
	var kind, status, policy string
	var seconds *int64
	err := row.Scan(&trigger.TenantID, &trigger.ID, &trigger.PublicHash, &trigger.WorkflowID, &kind, &trigger.Name,
		&status, &trigger.Config, &seconds, &policy, &trigger.NextFireAt,
		&trigger.SigningCredentialID, &trigger.EventType, &trigger.CreatedBy, &trigger.CreatedAt, &trigger.UpdatedAt)
	trigger.Kind, trigger.Enabled, trigger.MisfirePolicy = TriggerKind(kind), status == "enabled", MisfirePolicy(policy)
	if seconds != nil {
		trigger.ScheduleInterval = time.Duration(*seconds) * time.Second
	}
	return trigger, err
}

func insertPlatformOutboxTx(ctx context.Context, tx pgx.Tx, tenantID, aggregateType, aggregateID, topic string, payload json.RawMessage) error {
	id, err := newID("out")
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO outbox_messages (id,tenant_id,aggregate_type,aggregate_id,topic,payload) VALUES ($1,$2,$3,$4,$5,$6)`, id, tenantID, aggregateType, aggregateID, topic, payload)
	if err != nil {
		return fmt.Errorf("enqueue platform event: %w", err)
	}
	return nil
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
