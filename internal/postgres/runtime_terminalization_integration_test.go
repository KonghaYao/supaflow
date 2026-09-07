package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

// 验证 FAILED/ERROR 终结并行 DAG 时会取消其他分支并一次性释放全部 lease。
func TestPostgresFailureTerminalizesParallelWork(t *testing.T) {
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
	migrations, err := LoadMigrations(os.DirFS("../.."), "migrations")
	if err != nil {
		t.Fatal(err)
	}
	if err = NewMigrator(pool).Up(ctx, migrations); err != nil {
		t.Fatal(err)
	}
	for _, terminal := range []workflowruntime.RunStatus{workflowruntime.RunFailed, workflowruntime.RunError} {
		t.Run(string(terminal), func(t *testing.T) {
			organizationID, runID, attemptA, attemptB := seedParallelRuntime(t, ctx, pool, string(terminal))
			store := NewRuntimeStore(pool)
			result, failErr := store.FailAttempt(ctx, workflowruntime.FailAttemptRequest{
				OrganizationID: workflowruntime.OrganizationID(organizationID), RunID: runID, AttemptID: attemptA,
				WorkerID: "worker_a", FencingToken: 1, Now: time.Now().UTC(),
				TerminalStatus: terminal, ErrorCode: "terminal-test",
			})
			if failErr != nil || result.Status != terminal {
				t.Fatalf("result=%+v err=%v", result, failErr)
			}
			var secondStatus, secondNodeStatus string
			var liveAttempts, liveNodes, leases int
			if err = pool.QueryRow(ctx, `SELECT status FROM node_attempts WHERE organization_id=$1 AND id=$2`, organizationID, attemptB).Scan(&secondStatus); err != nil {
				t.Fatal(err)
			}
			if err = pool.QueryRow(ctx, `SELECT status FROM node_states WHERE organization_id=$1 AND run_id=$2 AND node_id='branch_b'`, organizationID, runID).Scan(&secondNodeStatus); err != nil {
				t.Fatal(err)
			}
			if err = pool.QueryRow(ctx, `SELECT
				(SELECT count(*) FROM node_attempts WHERE organization_id=$1 AND run_id=$2 AND status IN ('pending','retry_ready','running','suspended')),
				(SELECT count(*) FROM node_states WHERE organization_id=$1 AND run_id=$2 AND status IN ('pending','ready','running','suspended')),
				(SELECT count(*) FROM worker_leases WHERE organization_id=$1 AND run_id=$2)`, organizationID, runID).Scan(&liveAttempts, &liveNodes, &leases); err != nil {
				t.Fatal(err)
			}
			if secondStatus != "cancelled" || secondNodeStatus != "cancelled" || liveAttempts != 0 || liveNodes != 0 || leases != 0 {
				t.Fatalf("orphaned branch: attempt=%s node=%s liveAttempts=%d liveNodes=%d leases=%d",
					secondStatus, secondNodeStatus, liveAttempts, liveNodes, leases)
			}
		})
	}
}

// 验证快钟副本不能提前接管 live lease 或把尚未到期的 Run 终结为超时。
func TestPostgresCoordinationUsesDatabaseClock(t *testing.T) {
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
	migrations, err := LoadMigrations(os.DirFS("../.."), "migrations")
	if err != nil {
		t.Fatal(err)
	}
	if err = NewMigrator(pool).Up(ctx, migrations); err != nil {
		t.Fatal(err)
	}
	organizationID, runID, _, _ := seedParallelRuntime(t, ctx, pool, "clock")
	if _, err = pool.Exec(ctx, `UPDATE runs SET deadline_at=clock_timestamp()+interval '1 hour'
		WHERE organization_id=$1 AND id=$2`, organizationID, runID); err != nil {
		t.Fatal(err)
	}
	store := NewRuntimeStore(pool)
	fastClock := time.Now().UTC().Add(24 * time.Hour)
	if expired, expireErr := store.ExpireRun(ctx, workflowruntime.OrganizationID(organizationID), runID, fastClock); expireErr != nil || expired {
		t.Fatalf("fast host clock expired live run: expired=%t err=%v", expired, expireErr)
	}
	work, claimErr := store.ClaimAttempt(ctx, workflowruntime.ClaimAttemptRequest{
		OrganizationID: workflowruntime.OrganizationID(organizationID), WorkerID: "fast-worker",
		Now: fastClock, LeaseDuration: time.Hour,
	})
	if claimErr != nil || work != nil {
		t.Fatalf("fast host clock took live lease: work=%+v err=%v", work, claimErr)
	}
}

// 验证父 Run 的终态失败会递归取消已经运行的 subworkflow，避免不可达子任务继续产生副作用。
func TestPostgresFailureCancelsSubworkflowDescendants(t *testing.T) {
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
	migrations, err := LoadMigrations(os.DirFS("../.."), "migrations")
	if err != nil {
		t.Fatal(err)
	}
	if err = NewMigrator(pool).Up(ctx, migrations); err != nil {
		t.Fatal(err)
	}
	organizationID, parentRunID, failingAttemptID, parentAttemptID := seedParallelRuntime(t, ctx, pool, "descendant")
	childRunID, childAttemptID := seedChildRuntime(t, ctx, pool, organizationID, parentRunID, parentAttemptID)
	store := NewRuntimeStore(pool)
	if _, err = store.FailAttempt(ctx, workflowruntime.FailAttemptRequest{
		OrganizationID: workflowruntime.OrganizationID(organizationID), RunID: parentRunID, AttemptID: failingAttemptID,
		WorkerID: "worker_a", FencingToken: 1, TerminalStatus: workflowruntime.RunFailed,
		ErrorCode: "parent-failed", Now: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	var childStatus, childAttemptStatus, childAdmissionStatus string
	var childLeases int
	if err = pool.QueryRow(ctx, `SELECT status FROM runs WHERE organization_id=$1 AND id=$2`, organizationID, childRunID).Scan(&childStatus); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT status FROM node_attempts WHERE organization_id=$1 AND id=$2`, organizationID, childAttemptID).Scan(&childAttemptStatus); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT status FROM admission_entries WHERE organization_id=$1 AND resource_type='run' AND resource_id=$2`, organizationID, childRunID).Scan(&childAdmissionStatus); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM worker_leases WHERE organization_id=$1 AND run_id=$2`, organizationID, childRunID).Scan(&childLeases); err != nil {
		t.Fatal(err)
	}
	if childStatus != "cancelled" || childAttemptStatus != "cancelled" || childAdmissionStatus != "released" || childLeases != 0 {
		t.Fatalf("orphaned child: run=%s attempt=%s admission=%s leases=%d",
			childStatus, childAttemptStatus, childAdmissionStatus, childLeases)
	}
}

// 验证 approval/webhook interrupt 到期会释放父 Run 容量并递归取消已启动的 subworkflow。
func TestPostgresInterruptExpiryTerminalizesDescendants(t *testing.T) {
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
	migrations, err := LoadMigrations(os.DirFS("../.."), "migrations")
	if err != nil {
		t.Fatal(err)
	}
	if err = NewMigrator(pool).Up(ctx, migrations); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"approval", "webhook"} {
		t.Run(kind, func(t *testing.T) {
			organizationID, parentRunID, expiringAttemptID, parentAttemptID := seedParallelRuntime(t, ctx, pool, "interrupt_"+kind)
			childRunID, _ := seedChildRuntime(t, ctx, pool, organizationID, parentRunID, parentAttemptID)
			var nodeStateID string
			if err = pool.QueryRow(ctx, `UPDATE node_attempts SET status='suspended',updated_at=clock_timestamp()
				WHERE organization_id=$1 AND id=$2 RETURNING node_state_id`, organizationID, expiringAttemptID).Scan(&nodeStateID); err != nil {
				t.Fatal(err)
			}
			if _, err = pool.Exec(ctx, `UPDATE node_states SET status='suspended',updated_at=clock_timestamp()
				WHERE organization_id=$1 AND id=$2`, organizationID, nodeStateID); err != nil {
				t.Fatal(err)
			}
			if _, err = pool.Exec(ctx, `DELETE FROM worker_leases WHERE organization_id=$1 AND attempt_id=$2`,
				organizationID, expiringAttemptID); err != nil {
				t.Fatal(err)
			}
			token := sha256.Sum256([]byte("expired_" + parentRunID))
			if _, err = pool.Exec(ctx, `INSERT INTO interrupts
				(id,organization_id,run_id,node_state_id,attempt_id,kind,prompt,resume_token_hash,expires_at)
				VALUES($1,$2,$3,$4,$5,$6,'{}',$7,clock_timestamp()-interval '1 minute')`,
				"interrupt_"+parentRunID, organizationID, parentRunID, nodeStateID, expiringAttemptID, kind, token[:]); err != nil {
				t.Fatal(err)
			}
			store := NewRuntimeStore(pool)
			if count, expireErr := store.ExpireRuns(ctx, workflowruntime.OrganizationID(organizationID), time.Now().UTC().Add(-24*time.Hour), 10); expireErr != nil || count != 1 {
				t.Fatalf("expiry count=%d err=%v", count, expireErr)
			}
			var parentStatus, childStatus, parentAdmission, childAdmission string
			if err = pool.QueryRow(ctx, `SELECT
				(SELECT status FROM runs WHERE organization_id=$1 AND id=$2),
				(SELECT status FROM runs WHERE organization_id=$1 AND id=$3),
				(SELECT status FROM admission_entries WHERE organization_id=$1 AND resource_type='run' AND resource_id=$2),
				(SELECT status FROM admission_entries WHERE organization_id=$1 AND resource_type='run' AND resource_id=$3)`,
				organizationID, parentRunID, childRunID).Scan(&parentStatus, &childStatus, &parentAdmission, &childAdmission); err != nil {
				t.Fatal(err)
			}
			if parentStatus != "error" || childStatus != "cancelled" || parentAdmission != "released" || childAdmission != "released" {
				t.Fatalf("terminal states parent=%s child=%s admissions=%s/%s", parentStatus, childStatus, parentAdmission, childAdmission)
			}
		})
	}
}

func seedParallelRuntime(t *testing.T, ctx context.Context, pool *pgxpool.Pool, discriminator string) (string, string, string, string) {
	t.Helper()
	suffix := fmt.Sprintf("%s_%d", discriminator, time.Now().UnixNano())
	organizationID, workflowID := "terminal_organization_"+suffix, "terminal_workflow_"+suffix
	versionID, planID, runID := "terminal_version_"+suffix, "terminal_plan_"+suffix, "terminal_run_"+suffix
	nodeA, nodeB := "terminal_node_a_"+suffix, "terminal_node_b_"+suffix
	attemptA, attemptB := "terminal_attempt_a_"+suffix, "terminal_attempt_b_"+suffix
	digest := sha256.Sum256([]byte(suffix))
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO workflows(organization_id,id,slug,name,created_by) VALUES($1,$2,$2,$2,'test')`, []any{organizationID, workflowID}},
		{`INSERT INTO workflow_versions(id,organization_id,workflow_id,version,publication_key,definition,definition_hash,published_by)
			VALUES($1,$2,$3,1,$4,$5,$6,'test')`, []any{versionID, organizationID, workflowID, "publication_" + suffix, json.RawMessage(`{}`), digest[:]}},
		{`INSERT INTO execution_plans(id,organization_id,workflow_id,workflow_version_id,plan,plan_hash,compiler_version)
			VALUES($1,$2,$3,$4,$5,$6,'test')`, []any{planID, organizationID, workflowID, versionID, json.RawMessage(`{}`), digest[:]}},
		{`INSERT INTO runs(id,organization_id,workflow_id,workflow_version_id,execution_plan_id,idempotency_key,request_hash,input,plan_hash,status)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'running')`, []any{runID, organizationID, workflowID, versionID, planID, "run_" + suffix, digest[:], json.RawMessage(`{}`), fmt.Sprintf("%x", digest)}},
		{`INSERT INTO admission_entries(organization_id,id,resource_type,resource_id,status,admitted_at)
			VALUES($1,$2,'run',$3,'active',clock_timestamp())`, []any{organizationID, "terminal_admission_" + suffix, runID}},
		{`UPDATE organization_quota_counters SET active_runs=active_runs+1 WHERE organization_id=$1`, []any{organizationID}},
	}
	for _, statement := range statements {
		if _, err = tx.Exec(ctx, statement.query, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	for index, ids := range [][2]string{{nodeA, attemptA}, {nodeB, attemptB}} {
		nodeID := []string{"branch_a", "branch_b"}[index]
		workerID := []string{"worker_a", "worker_b"}[index]
		if _, err = tx.Exec(ctx, `INSERT INTO node_states(id,organization_id,run_id,node_id,status,attempt_count,latest_attempt_id)
			VALUES($1,$2,$3,$4,'running',1,$5)`, ids[0], organizationID, runID, nodeID, ids[1]); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(ctx, `INSERT INTO node_attempts(id,organization_id,run_id,node_state_id,node_id,executor,input,attempt_number,status,worker_id,fencing_token)
			VALUES($1,$2,$3,$4,$5,'test',$6,1,'running',$7,1)`, ids[1], organizationID, runID, ids[0], nodeID, json.RawMessage(`{}`), workerID); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(ctx, `INSERT INTO worker_leases(attempt_id,organization_id,run_id,worker_id,fencing_token,acquired_at,expires_at)
			VALUES($1,$2,$3,$4,1,now(),now()+interval '1 hour')`, ids[1], organizationID, runID, workerID); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return organizationID, runID, attemptA, attemptB
}

func seedChildRuntime(t *testing.T, ctx context.Context, pool *pgxpool.Pool, organizationID, parentRunID, parentAttemptID string) (string, string) {
	t.Helper()
	childRunID := "child_" + parentRunID
	childAttemptID := "child_" + parentAttemptID
	var workflowID, versionID, planID string
	if err := pool.QueryRow(ctx, `SELECT workflow_id,workflow_version_id,execution_plan_id FROM runs
		WHERE organization_id=$1 AND id=$2`, organizationID, parentRunID).Scan(&workflowID, &versionID, &planID); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(childRunID))
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err = tx.Exec(ctx, `INSERT INTO runs(id,organization_id,workflow_id,workflow_version_id,execution_plan_id,
		idempotency_key,request_hash,input,plan_hash,status) VALUES($1,$2,$3,$4,$5,$6,$7,'{}',$8,'running')`,
		childRunID, organizationID, workflowID, versionID, planID, "child_"+parentRunID, digest[:], fmt.Sprintf("%x", digest)); err != nil {
		t.Fatal(err)
	}
	childNodeStateID := "child_node_" + parentRunID
	if _, err = tx.Exec(ctx, `INSERT INTO node_states(id,organization_id,run_id,node_id,status,attempt_count,latest_attempt_id)
		VALUES($1,$2,$3,'child_node','running',1,$4)`, childNodeStateID, organizationID, childRunID, childAttemptID); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO node_attempts(id,organization_id,run_id,node_state_id,node_id,executor,input,
		attempt_number,status,worker_id,fencing_token) VALUES($1,$2,$3,$4,'child_node','test','{}',1,'running','child_worker',1)`,
		childAttemptID, organizationID, childRunID, childNodeStateID); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO worker_leases(attempt_id,organization_id,run_id,worker_id,fencing_token,acquired_at,expires_at)
		VALUES($1,$2,$3,'child_worker',1,clock_timestamp(),clock_timestamp()+interval '1 hour')`, childAttemptID, organizationID, childRunID); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO admission_entries(organization_id,id,resource_type,resource_id,status,admitted_at)
		VALUES($1,$2,'run',$3,'active',clock_timestamp())`, organizationID, "child_admission_"+parentRunID, childRunID); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `UPDATE organization_quota_counters SET active_runs=active_runs+1 WHERE organization_id=$1`, organizationID); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO subworkflow_links(organization_id,parent_run_id,parent_node_id,parent_attempt_id,parent_execution_id,child_run_id)
		VALUES($1,$2,'branch_b',$3,$3,$4)`, organizationID, parentRunID, parentAttemptID, childRunID); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return childRunID, childAttemptID
}
