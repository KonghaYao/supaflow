package platformops

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

type dueSchedule struct {
	organizationID string
	id             string
	interval       int
	policy         MisfirePolicy
	next           time.Time
	dbNow          time.Time
}

// FireDueSchedules gives at most one schedule to each due organization per round.
// It repeats fair rounds up to the global limit, while a saturated organization is
// removed from later rounds and each trigger is processed at most once per
// tick. Every organization commits independently, so one noisy organization cannot roll
// back or starve another organization's occurrence.
func (s *PostgresTriggerStore) FireDueSchedules(ctx context.Context, options ScheduleOptions) ([]Firing, error) {
	firings := make([]Firing, 0, options.Limit)
	roundOrganizations := make([]string, 0, options.Limit)
	blockedOrganizations := make([]string, 0)
	processedTriggerOrganizations := make([]string, 0, options.Limit)
	processedTriggerIDs := make([]string, 0, options.Limit)
	var failures []error
	for attempts := 0; attempts < options.Limit; {
		excluded := append(append(make([]string, 0, len(roundOrganizations)+len(blockedOrganizations)), roundOrganizations...), blockedOrganizations...)
		batch, organizationID, triggerID, found, err := s.fireNextDueOrganization(
			ctx, options, excluded, processedTriggerOrganizations, processedTriggerIDs,
		)
		if !found {
			if err != nil {
				failures = append(failures, err)
			}
			if len(roundOrganizations) == 0 {
				break
			}
			roundOrganizations = roundOrganizations[:0]
			continue
		}
		attempts++
		roundOrganizations = append(roundOrganizations, organizationID)
		processedTriggerOrganizations = append(processedTriggerOrganizations, organizationID)
		processedTriggerIDs = append(processedTriggerIDs, triggerID)
		firings = append(firings, batch...)
		if err != nil {
			failures = append(failures, err)
			blockedOrganizations = append(blockedOrganizations, organizationID)
		}
	}
	return firings, errors.Join(failures...)
}

func (s *PostgresTriggerStore) fireNextDueOrganization(
	ctx context.Context,
	options ScheduleOptions,
	excludedOrganizations, processedTriggerOrganizations, processedTriggerIDs []string,
) ([]Firing, string, string, bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, "", "", false, triggerError("begin fair schedule poll", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	item, err := selectFairDueSchedule(ctx, tx, excludedOrganizations, processedTriggerOrganizations, processedTriggerIDs)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, "", "", false, nil
	}
	if err != nil {
		return nil, "", "", false, triggerError("select fair due schedule", err)
	}
	work, err := tx.Begin(ctx)
	if err != nil {
		return nil, item.organizationID, item.id, true, triggerError("begin schedule occurrence savepoint", err)
	}
	firings, workErr := fireScheduleItem(ctx, work, item, options)
	if workErr != nil {
		_ = work.Rollback(ctx)
	} else if err = work.Commit(ctx); err != nil {
		return nil, item.organizationID, item.id, true, triggerError("release schedule occurrence savepoint", err)
	}
	if _, err = tx.Exec(ctx, `UPDATE trigger_schedule_organization_state
		SET last_served_at=clock_timestamp() WHERE organization_id=$1`, item.organizationID); err != nil {
		return nil, item.organizationID, item.id, true, triggerError("advance schedule organization fairness cursor", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, item.organizationID, item.id, true, triggerError("commit fair schedule poll", err)
	}
	return firings, item.organizationID, item.id, true, workErr
}

func selectFairDueSchedule(
	ctx context.Context,
	tx pgx.Tx,
	excludedOrganizations, processedTriggerOrganizations, processedTriggerIDs []string,
) (dueSchedule, error) {
	var item dueSchedule
	err := tx.QueryRow(ctx, `
		WITH database_clock AS MATERIALIZED (SELECT clock_timestamp() AS now)
		SELECT trigger.organization_id,trigger.id,trigger.schedule_interval_seconds,
		       trigger.misfire_policy,trigger.next_fire_at,database_clock.now
		FROM database_clock, trigger_schedule_organization_state organization
		JOIN LATERAL (
		    SELECT candidate.organization_id,candidate.id
		    FROM triggers candidate, database_clock due_clock
		    WHERE candidate.organization_id=organization.organization_id AND candidate.kind='schedule'
		      AND candidate.status='enabled' AND candidate.next_fire_at <= due_clock.now
		      AND NOT EXISTS (
		          SELECT 1 FROM unnest($2::text[],$3::text[]) processed(organization_id,trigger_id)
		          WHERE processed.organization_id=candidate.organization_id AND processed.trigger_id=candidate.id
		      )
		    ORDER BY candidate.next_fire_at,candidate.id LIMIT 1
		) selected ON true
		JOIN triggers trigger
		  ON trigger.organization_id=selected.organization_id AND trigger.id=selected.id
		WHERE NOT (organization.organization_id = ANY($1::text[]))
		ORDER BY organization.last_served_at NULLS FIRST,trigger.next_fire_at,organization.organization_id
		FOR UPDATE OF organization,trigger SKIP LOCKED LIMIT 1`, excludedOrganizations,
		processedTriggerOrganizations, processedTriggerIDs).Scan(
		&item.organizationID, &item.id, &item.interval, &item.policy, &item.next, &item.dbNow)
	return item, err
}

func fireScheduleItem(ctx context.Context, tx pgx.Tx, item dueSchedule, options ScheduleOptions) ([]Firing, error) {
	available, err := organizationTriggerIngressCapacityTx(ctx, tx, item.organizationID)
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
		WHERE organization_id=$1 AND id=$2`, item.organizationID, item.id, next); err != nil {
		return nil, triggerError("advance schedule", err)
	}
	firings := make([]Firing, 0, len(occurrences))
	for _, occurrence := range occurrences {
		payload, _ := json.Marshal(map[string]string{"scheduledAt": occurrence.Format(time.RFC3339Nano)})
		firing, created, fireErr := insertFiringTx(ctx, tx, FireRequest{
			OrganizationID: item.organizationID, TriggerID: item.id, ExpectedKind: TriggerSchedule,
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
