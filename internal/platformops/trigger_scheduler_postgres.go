package platformops

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

type dueSchedule struct {
	tenantID string
	id       string
	interval int
	policy   MisfirePolicy
	next     time.Time
	dbNow    time.Time
}

// FireDueSchedules gives at most one schedule to each due tenant per round.
// It repeats fair rounds up to the global limit, while a saturated tenant is
// removed from later rounds and each trigger is processed at most once per
// tick. Every tenant commits independently, so one noisy tenant cannot roll
// back or starve another tenant's occurrence.
func (s *PostgresTriggerStore) FireDueSchedules(ctx context.Context, options ScheduleOptions) ([]Firing, error) {
	firings := make([]Firing, 0, options.Limit)
	roundTenants := make([]string, 0, options.Limit)
	blockedTenants := make([]string, 0)
	processedTriggerTenants := make([]string, 0, options.Limit)
	processedTriggerIDs := make([]string, 0, options.Limit)
	var failures []error
	for attempts := 0; attempts < options.Limit; {
		excluded := append(append(make([]string, 0, len(roundTenants)+len(blockedTenants)), roundTenants...), blockedTenants...)
		batch, tenantID, triggerID, found, err := s.fireNextDueTenant(
			ctx, options, excluded, processedTriggerTenants, processedTriggerIDs,
		)
		if !found {
			if err != nil {
				failures = append(failures, err)
			}
			if len(roundTenants) == 0 {
				break
			}
			roundTenants = roundTenants[:0]
			continue
		}
		attempts++
		roundTenants = append(roundTenants, tenantID)
		processedTriggerTenants = append(processedTriggerTenants, tenantID)
		processedTriggerIDs = append(processedTriggerIDs, triggerID)
		firings = append(firings, batch...)
		if err != nil {
			failures = append(failures, err)
			blockedTenants = append(blockedTenants, tenantID)
		}
	}
	return firings, errors.Join(failures...)
}

func (s *PostgresTriggerStore) fireNextDueTenant(
	ctx context.Context,
	options ScheduleOptions,
	excludedTenants, processedTriggerTenants, processedTriggerIDs []string,
) ([]Firing, string, string, bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, "", "", false, triggerError("begin fair schedule poll", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	item, err := selectFairDueSchedule(ctx, tx, excludedTenants, processedTriggerTenants, processedTriggerIDs)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, "", "", false, nil
	}
	if err != nil {
		return nil, "", "", false, triggerError("select fair due schedule", err)
	}
	work, err := tx.Begin(ctx)
	if err != nil {
		return nil, item.tenantID, item.id, true, triggerError("begin schedule occurrence savepoint", err)
	}
	firings, workErr := fireScheduleItem(ctx, work, item, options)
	if workErr != nil {
		_ = work.Rollback(ctx)
	} else if err = work.Commit(ctx); err != nil {
		return nil, item.tenantID, item.id, true, triggerError("release schedule occurrence savepoint", err)
	}
	if _, err = tx.Exec(ctx, `UPDATE trigger_schedule_tenant_state
		SET last_served_at=clock_timestamp() WHERE tenant_id=$1`, item.tenantID); err != nil {
		return nil, item.tenantID, item.id, true, triggerError("advance schedule tenant fairness cursor", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, item.tenantID, item.id, true, triggerError("commit fair schedule poll", err)
	}
	return firings, item.tenantID, item.id, true, workErr
}

func selectFairDueSchedule(
	ctx context.Context,
	tx pgx.Tx,
	excludedTenants, processedTriggerTenants, processedTriggerIDs []string,
) (dueSchedule, error) {
	var item dueSchedule
	err := tx.QueryRow(ctx, `
		WITH database_clock AS MATERIALIZED (SELECT clock_timestamp() AS now)
		SELECT trigger.tenant_id,trigger.id,trigger.schedule_interval_seconds,
		       trigger.misfire_policy,trigger.next_fire_at,database_clock.now
		FROM database_clock, trigger_schedule_tenant_state tenant
		JOIN LATERAL (
		    SELECT candidate.tenant_id,candidate.id
		    FROM triggers candidate, database_clock due_clock
		    WHERE candidate.tenant_id=tenant.tenant_id AND candidate.kind='schedule'
		      AND candidate.status='enabled' AND candidate.next_fire_at <= due_clock.now
		      AND NOT EXISTS (
		          SELECT 1 FROM unnest($2::text[],$3::text[]) processed(tenant_id,trigger_id)
		          WHERE processed.tenant_id=candidate.tenant_id AND processed.trigger_id=candidate.id
		      )
		    ORDER BY candidate.next_fire_at,candidate.id LIMIT 1
		) selected ON true
		JOIN triggers trigger
		  ON trigger.tenant_id=selected.tenant_id AND trigger.id=selected.id
		WHERE NOT (tenant.tenant_id = ANY($1::text[]))
		ORDER BY tenant.last_served_at NULLS FIRST,trigger.next_fire_at,tenant.tenant_id
		FOR UPDATE OF tenant,trigger SKIP LOCKED LIMIT 1`, excludedTenants,
		processedTriggerTenants, processedTriggerIDs).Scan(
		&item.tenantID, &item.id, &item.interval, &item.policy, &item.next, &item.dbNow)
	return item, err
}

func fireScheduleItem(ctx context.Context, tx pgx.Tx, item dueSchedule, options ScheduleOptions) ([]Firing, error) {
	available, err := tenantTriggerIngressCapacityTx(ctx, tx, item.tenantID)
	if err != nil {
		return nil, err
	}
	bounded := options
	bounded.Now = item.dbNow
	bounded.MaxCatchUp = min(bounded.MaxCatchUp, available)
	if bounded.MaxCatchUp == 0 && item.policy != MisfireSkip {
		return nil, triggerCapacityError()
	}
	occurrences, next := scheduleOccurrences(item.next, time.Duration(item.interval)*time.Second, item.policy, bounded)
	if len(occurrences) > available {
		return nil, triggerCapacityError()
	}
	if _, err = tx.Exec(ctx, `UPDATE triggers SET next_fire_at=$3,updated_at=clock_timestamp()
		WHERE tenant_id=$1 AND id=$2`, item.tenantID, item.id, next); err != nil {
		return nil, triggerError("advance schedule", err)
	}
	firings := make([]Firing, 0, len(occurrences))
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
	return firings, nil
}
