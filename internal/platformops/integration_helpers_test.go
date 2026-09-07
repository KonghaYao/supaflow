package platformops

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func seedPlatformOrganization(ctx context.Context, pool *pgxpool.Pool, organizationID, workflowID string) error {
	_, err := pool.Exec(ctx, `INSERT INTO workflows (organization_id,id,slug,name,created_by) VALUES ($1,$2,$2,$2,'integration')`, organizationID, workflowID)
	return err
}

func seedTriggerPublication(ctx context.Context, pool *pgxpool.Pool, organizationID, workflowID, suffix string) error {
	return seedTriggerPublicationWithKey(ctx, pool, organizationID, workflowID, suffix, "trigger-publication-"+suffix)
}

func seedTriggerPublicationWithKey(ctx context.Context, pool *pgxpool.Pool, organizationID, workflowID, suffix, publicationKey string) error {
	hash := sha256.Sum256([]byte("trigger-publication-" + suffix))
	versionID, planID := "trigger_version_"+suffix, "trigger_plan_"+suffix
	if _, err := pool.Exec(ctx, `INSERT INTO workflow_versions
		(id,organization_id,workflow_id,version,publication_key,definition,definition_hash,published_by)
		VALUES ($1,$2,$3,1,$4,'{}',$5,'integration')`, versionID, organizationID, workflowID, publicationKey, hash[:]); err != nil {
		return err
	}
	_, err := pool.Exec(ctx, `INSERT INTO execution_plans
		(id,organization_id,workflow_id,workflow_version_id,plan,plan_hash,compiler_version)
		VALUES ($1,$2,$3,$4,'{}',$5,'integration')`, planID, organizationID, workflowID, versionID, hash[:])
	return err
}

func seedRunningAttempt(ctx context.Context, pool *pgxpool.Pool, organizationID, workflowID, suffix string) (string, string, error) {
	hash := sha256.Sum256([]byte("seed-" + suffix))
	versionID, planID, runID := "version_"+suffix, "plan_"+suffix, "run_"+suffix
	nodeStateID, attemptID := "node_"+suffix, "attempt_"+suffix
	tx, err := pool.Begin(ctx)
	if err != nil {
		return "", "", err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	statements := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO workflow_versions (id,organization_id,workflow_id,version,publication_key,definition,definition_hash,published_by)
			VALUES ($1,$2,$3,(SELECT COALESCE(MAX(version),0)+1 FROM workflow_versions WHERE organization_id=$2 AND workflow_id=$3),$4,'{}',$5,'integration')`, []any{versionID, organizationID, workflowID, "publication_" + suffix, hash[:]}},
		{`INSERT INTO execution_plans (id,organization_id,workflow_id,workflow_version_id,plan,plan_hash,compiler_version) VALUES ($1,$2,$3,$4,'{}',$5,'integration')`, []any{planID, organizationID, workflowID, versionID, hash[:]}},
		{`INSERT INTO runs (id,organization_id,workflow_id,workflow_version_id,execution_plan_id,idempotency_key,request_hash,input,plan_hash,status) VALUES ($1,$2,$3,$4,$5,$6,$7,'{}',$8,'running')`, []any{runID, organizationID, workflowID, versionID, planID, "run-key-" + suffix, hash[:], fmt.Sprintf("%x", hash)}},
		{`INSERT INTO node_states (id,organization_id,run_id,node_id,status,attempt_count,latest_attempt_id) VALUES ($1,$2,$3,'node','running',1,$4)`, []any{nodeStateID, organizationID, runID, attemptID}},
		{`INSERT INTO node_attempts (id,organization_id,run_id,node_state_id,node_id,executor,input,attempt_number,status,worker_id,fencing_token) VALUES ($1,$2,$3,$4,'node','integration','{}',1,'running','worker',1)`, []any{attemptID, organizationID, runID, nodeStateID}},
		{`INSERT INTO worker_leases (attempt_id,organization_id,run_id,worker_id,fencing_token,acquired_at,expires_at) VALUES ($1,$2,$3,'worker',1,now(),now()+interval '1 hour')`, []any{attemptID, organizationID, runID}},
	}
	for _, statement := range statements {
		if _, err = tx.Exec(ctx, statement.sql, statement.args...); err != nil {
			return "", "", err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return "", "", err
	}
	return attemptID, runID, nil
}

func stringsContainSecret(value string) bool {
	return bytes.Contains([]byte(value), []byte("Bearer-secret"))
}

func countOrganizationFirings(firings []Firing, organizationID string) int {
	count := 0
	for _, firing := range firings {
		if firing.OrganizationID == organizationID {
			count++
		}
	}
	return count
}

func timePointer(value time.Time) *time.Time { return &value }
