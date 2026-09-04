package platformops

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func seedPlatformTenant(ctx context.Context, pool *pgxpool.Pool, tenantID, workflowID string) error {
	if _, err := pool.Exec(ctx, `INSERT INTO tenants (id,name) VALUES ($1,$1)`, tenantID); err != nil {
		return err
	}
	_, err := pool.Exec(ctx, `INSERT INTO workflows (tenant_id,id,slug,name,created_by) VALUES ($1,$2,$2,$2,'integration')`, tenantID, workflowID)
	return err
}

func seedTriggerPublication(ctx context.Context, pool *pgxpool.Pool, tenantID, workflowID, suffix string) error {
	hash := sha256.Sum256([]byte("trigger-publication-" + suffix))
	versionID, planID := "trigger_version_"+suffix, "trigger_plan_"+suffix
	if _, err := pool.Exec(ctx, `INSERT INTO workflow_versions
		(id,tenant_id,workflow_id,version,publication_key,definition,definition_hash,published_by)
		VALUES ($1,$2,$3,1,$4,'{}',$5,'integration')`, versionID, tenantID, workflowID, "trigger-publication-"+suffix, hash[:]); err != nil {
		return err
	}
	_, err := pool.Exec(ctx, `INSERT INTO execution_plans
		(id,tenant_id,workflow_id,workflow_version_id,plan,plan_hash,compiler_version)
		VALUES ($1,$2,$3,$4,'{}',$5,'integration')`, planID, tenantID, workflowID, versionID, hash[:])
	return err
}

func seedRunningAttempt(ctx context.Context, pool *pgxpool.Pool, tenantID, workflowID, suffix string) (string, string, error) {
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
		{`INSERT INTO workflow_versions (id,tenant_id,workflow_id,version,publication_key,definition,definition_hash,published_by)
			VALUES ($1,$2,$3,(SELECT COALESCE(MAX(version),0)+1 FROM workflow_versions WHERE tenant_id=$2 AND workflow_id=$3),$4,'{}',$5,'integration')`, []any{versionID, tenantID, workflowID, "publication_" + suffix, hash[:]}},
		{`INSERT INTO execution_plans (id,tenant_id,workflow_id,workflow_version_id,plan,plan_hash,compiler_version) VALUES ($1,$2,$3,$4,'{}',$5,'integration')`, []any{planID, tenantID, workflowID, versionID, hash[:]}},
		{`INSERT INTO runs (id,tenant_id,workflow_id,workflow_version_id,execution_plan_id,idempotency_key,request_hash,input,plan_hash,status) VALUES ($1,$2,$3,$4,$5,$6,$7,'{}',$8,'running')`, []any{runID, tenantID, workflowID, versionID, planID, "run-key-" + suffix, hash[:], fmt.Sprintf("%x", hash)}},
		{`INSERT INTO node_states (id,tenant_id,run_id,node_id,status,attempt_count,latest_attempt_id) VALUES ($1,$2,$3,'node','running',1,$4)`, []any{nodeStateID, tenantID, runID, attemptID}},
		{`INSERT INTO node_attempts (id,tenant_id,run_id,node_state_id,node_id,executor,input,attempt_number,status,worker_id,fencing_token) VALUES ($1,$2,$3,$4,'node','integration','{}',1,'running','worker',1)`, []any{attemptID, tenantID, runID, nodeStateID}},
		{`INSERT INTO worker_leases (attempt_id,tenant_id,run_id,worker_id,fencing_token,acquired_at,expires_at) VALUES ($1,$2,$3,'worker',1,now(),now()+interval '1 hour')`, []any{attemptID, tenantID, runID}},
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

func countTenantFirings(firings []Firing, tenantID string) int {
	count := 0
	for _, firing := range firings {
		if firing.TenantID == tenantID {
			count++
		}
	}
	return count
}

func timePointer(value time.Time) *time.Time { return &value }
