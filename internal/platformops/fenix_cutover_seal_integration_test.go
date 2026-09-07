package platformops

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/postgres"
)

type targetAttestation struct {
	schemaVersion                             int
	installationID, contentHash               string
	definitionCount, draftCount, triggerCount int64
	workflowCount, runCount                   int64
}

// 验证 seal acquisition 在并发写入前建立持久屏障，所有证明行均拒绝变更且只有持 token 的操作员可释放。
func TestFenixCutoverSealClosesTargetAttestationRace(t *testing.T) {
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
	migrations, err := postgres.LoadMigrations(os.DirFS("../.."), "migrations")
	if err != nil {
		t.Fatal(err)
	}
	if err = postgres.NewMigrator(pool).Up(ctx, migrations); err != nil {
		t.Fatal(err)
	}

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	organizationID, workflowID := "seal_organization_"+suffix, "seal_workflow_"+suffix
	tokenDigest := sha256.Sum256([]byte("cutover-seal-token-" + suffix))
	token := fmt.Sprintf("%x", tokenDigest)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `SELECT release_fenix_cutover_seal_v2(installation_id,$1)
			FROM fenix_cutover_seals WHERE singleton=true AND released_at IS NULL`, token)
	})
	if err = seedPlatformOrganization(ctx, pool, organizationID, workflowID); err != nil {
		t.Fatal(err)
	}
	workflowHash := sha256.Sum256([]byte("seal-workflow-" + suffix))
	if _, err = pool.Exec(ctx, `INSERT INTO workflow_imports
		(organization_id,workflow_id,source_system,source_workflow_id,source_import_hash,imported_by)
		VALUES($1,$2,'fenix',$2,$3,'integration')`, organizationID, workflowID, workflowHash[:]); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO workflow_drafts
		(id,organization_id,workflow_id,document,updated_by) VALUES ($1,$2,$3,'{}','integration')`,
		"seal_draft_"+suffix, organizationID, workflowID); err != nil {
		t.Fatal(err)
	}
	if err = seedTriggerPublicationWithKey(ctx, pool, organizationID, workflowID, "seal_"+suffix, "fenix-version-"+suffix); err != nil {
		t.Fatal(err)
	}
	versionID, planID := "trigger_version_seal_"+suffix, "trigger_plan_seal_"+suffix
	if err = seedAttestedCutoverChildren(ctx, pool, organizationID, workflowID, versionID, planID, suffix); err != nil {
		t.Fatal(err)
	}

	attestation := readTargetAttestation(t, ctx, pool)
	attestedMutations := []struct {
		name, mutateSQL, restoreSQL string
		args                        []any
	}{
		{"workflow metadata", `UPDATE workflows SET description='changed' WHERE organization_id=$1 AND id=$2`, `UPDATE workflows SET description='' WHERE organization_id=$1 AND id=$2`, []any{organizationID, workflowID}},
		{"workflow import marker", `UPDATE workflow_imports SET imported_by='changed' WHERE organization_id=$1 AND workflow_id=$2`, `UPDATE workflow_imports SET imported_by='integration' WHERE organization_id=$1 AND workflow_id=$2`, []any{organizationID, workflowID}},
		{"definition mutation", `UPDATE definition_mutations SET response_snapshot='{"changed":true}' WHERE organization_id=$1 AND id=$2`, `UPDATE definition_mutations SET response_snapshot='{}' WHERE organization_id=$1 AND id=$2`, []any{organizationID, "seal_definition_mutation_" + suffix}},
		{"node attempt", `UPDATE node_attempts SET input='{"changed":true}' WHERE organization_id=$1 AND id=$2`, `UPDATE node_attempts SET input='{}' WHERE organization_id=$1 AND id=$2`, []any{organizationID, "seal_attempt_" + suffix}},
	}
	for _, mutation := range attestedMutations {
		if _, err = pool.Exec(ctx, mutation.mutateSQL, mutation.args...); err != nil {
			t.Fatalf("mutate %s: %v", mutation.name, err)
		}
		if changed := readTargetAttestation(t, ctx, pool); changed.contentHash == attestation.contentHash {
			t.Fatalf("%s is absent from v2 attestation", mutation.name)
		}
		if _, err = pool.Exec(ctx, mutation.restoreSQL, mutation.args...); err != nil {
			t.Fatalf("restore %s: %v", mutation.name, err)
		}
	}
	attestation = readTargetAttestation(t, ctx, pool)
	_, err = pool.Exec(ctx, `SELECT * FROM acquire_fenix_cutover_seal_v2($1,$2,$3,$4,$5,$6,$7,$8)`,
		attestation.installationID, attestation.contentHash, attestation.workflowCount, attestation.definitionCount+1, attestation.draftCount,
		attestation.triggerCount, attestation.runCount, token)
	assertPostgresCode(t, err, "55000")

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var acquired targetAttestation
	var acquiredAt time.Time
	err = tx.QueryRow(ctx, `SELECT * FROM acquire_fenix_cutover_seal_v2($1,$2,$3,$4,$5,$6,$7,$8)`,
		attestation.installationID, attestation.contentHash, attestation.workflowCount, attestation.definitionCount,
		attestation.draftCount, attestation.triggerCount, attestation.runCount, token,
	).Scan(&acquired.schemaVersion, &acquired.installationID, &acquired.contentHash,
		&acquired.workflowCount, &acquired.definitionCount, &acquired.draftCount, &acquired.triggerCount, &acquired.runCount, &acquiredAt)
	if err != nil || acquired != attestation || acquiredAt.IsZero() {
		t.Fatalf("acquire result=%+v at=%v err=%v", acquired, acquiredAt, err)
	}

	writeResult := make(chan error, 1)
	go func() {
		writeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, writeErr := pool.Exec(writeCtx, `UPDATE workflows SET name=name WHERE organization_id=$1 AND id=$2`, organizationID, workflowID)
		writeResult <- writeErr
	}()
	select {
	case writeErr := <-writeResult:
		t.Fatalf("concurrent writer escaped acquisition lock: %v", writeErr)
	case <-time.After(150 * time.Millisecond):
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	assertPostgresCode(t, <-writeResult, "55000")

	storedTokenHash := []byte(nil)
	if err = pool.QueryRow(ctx, `SELECT token_hash FROM fenix_cutover_seals WHERE singleton=true`).Scan(&storedTokenHash); err != nil {
		t.Fatal(err)
	}
	expectedTokenHash := sha256.Sum256([]byte(token))
	if !bytes.Equal(storedTokenHash, expectedTokenHash[:]) || bytes.Equal(storedTokenHash, []byte(token)) {
		t.Fatal("seal token was not persisted exclusively as its SHA-256 digest")
	}

	hash := sha256.Sum256([]byte("blocked-version-" + suffix))
	blockedMutations := []struct {
		name string
		sql  string
		args []any
	}{
		{"workflow", `UPDATE workflows SET name=name WHERE organization_id=$1 AND id=$2`, []any{organizationID, workflowID}},
		{"workflow import", `UPDATE workflow_imports SET imported_by=imported_by WHERE organization_id=$1 AND workflow_id=$2`, []any{organizationID, workflowID}},
		{"draft", `UPDATE workflow_drafts SET revision=revision+1 WHERE organization_id=$1 AND workflow_id=$2`, []any{organizationID, workflowID}},
		{"version", `INSERT INTO workflow_versions
			(id,organization_id,workflow_id,version,publication_key,definition,definition_hash,published_by)
			VALUES ($1,$2,$3,2,$4,'{}',$5,'integration')`, []any{"blocked_version_" + suffix, organizationID, workflowID, "blocked-publication-" + suffix, hash[:]}},
		{"plan", `UPDATE execution_plans SET compiler_version=compiler_version WHERE organization_id=$1 AND id=$2`, []any{organizationID, planID}},
		{"definition mutation", `UPDATE definition_mutations SET response_snapshot=response_snapshot WHERE organization_id=$1 AND id=$2`, []any{organizationID, "seal_definition_mutation_" + suffix}},
		{"trigger", `INSERT INTO triggers
			(organization_id,id,public_hash,workflow_id,kind,name,created_by)
			VALUES ($1,$2,$3,$4,'manual',$5,'integration')`, []any{organizationID, "blocked_trigger_" + suffix, "blocked-public-hash-" + suffix, workflowID, "blocked-trigger-" + suffix}},
		{"run", `INSERT INTO runs
			(id,organization_id,workflow_id,workflow_version_id,execution_plan_id,idempotency_key,request_hash,input,plan_hash)
			VALUES ($1,$2,$3,'unreachable','unreachable',$4,$5,'{}',$6)`, []any{"blocked_run_" + suffix, organizationID, workflowID, "blocked-run-" + suffix, hash[:], fmt.Sprintf("%x", hash)}},
		{"run event", `UPDATE run_events SET payload=payload WHERE organization_id=$1 AND id=$2`, []any{organizationID, "seal_event_" + suffix}},
		{"node state", `UPDATE node_states SET node_id=node_id WHERE organization_id=$1 AND id=$2`, []any{organizationID, "seal_state_" + suffix}},
		{"node attempt", `UPDATE node_attempts SET input=input WHERE organization_id=$1 AND id=$2`, []any{organizationID, "seal_attempt_" + suffix}},
		{"node output", `UPDATE node_outputs SET inline_value=inline_value WHERE organization_id=$1 AND id=$2`, []any{organizationID, "seal_output_" + suffix}},
	}
	for _, mutation := range blockedMutations {
		_, mutationErr := pool.Exec(ctx, mutation.sql, mutation.args...)
		if !isPostgresCode(mutationErr, "55000") {
			t.Errorf("%s mutation error=%v, want active-seal SQLSTATE 55000", mutation.name, mutationErr)
		}
	}

	wrongToken := strings.Repeat("f", 64)
	if wrongToken == token {
		wrongToken = strings.Repeat("e", 64)
	}
	_, err = pool.Exec(ctx, `SELECT release_fenix_cutover_seal_v2($1,$2)`, attestation.installationID, wrongToken)
	assertPostgresCode(t, err, "28000")
	if _, err = pool.Exec(ctx, `SELECT release_fenix_cutover_seal_v2($1,$2)`, attestation.installationID, token); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE workflows SET name=$3 WHERE organization_id=$1 AND id=$2`, organizationID, workflowID, "released"); err != nil {
		t.Fatalf("write after authenticated release: %v", err)
	}
	if _, err = pool.Exec(ctx, `SELECT release_fenix_cutover_seal_v2($1,$2)`, attestation.installationID, token); err != nil {
		t.Fatalf("idempotent release: %v", err)
	}
}

func seedAttestedCutoverChildren(
	ctx context.Context,
	pool *pgxpool.Pool,
	organizationID, workflowID, versionID, planID, suffix string,
) error {
	hash := sha256.Sum256([]byte("seal-attested-children-" + suffix))
	runID, stateID, attemptID := "seal_run_"+suffix, "seal_state_"+suffix, "seal_attempt_"+suffix
	statements := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO definition_mutations
			(id,organization_id,idempotency_key,action,workflow_id,request_hash,response_snapshot)
			VALUES ($1,$2,$3,'create',$4,$5,'{}')`, []any{"seal_definition_mutation_" + suffix, organizationID, "fenix-draft-" + suffix, workflowID, hash[:]}},
		{`INSERT INTO runs
			(id,organization_id,workflow_id,workflow_version_id,execution_plan_id,idempotency_key,request_hash,input,plan_hash,status,finished_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,'{}',$8,'success',clock_timestamp())`, []any{runID, organizationID, workflowID, versionID, planID, "import:fenix:" + runID, hash[:], fmt.Sprintf("%x", hash)}},
		{`INSERT INTO node_states (id,organization_id,run_id,node_id,status,attempt_count,latest_attempt_id)
			VALUES ($1,$2,$3,'node','success',1,$4)`, []any{stateID, organizationID, runID, attemptID}},
		{`INSERT INTO node_attempts
			(id,organization_id,run_id,node_state_id,node_id,executor,input,attempt_number,status,fencing_token,finished_at)
			VALUES ($1,$2,$3,$4,'node','integration','{}',1,'succeeded',1,clock_timestamp())`, []any{attemptID, organizationID, runID, stateID}},
		{`INSERT INTO run_events (id,organization_id,run_id,sequence,event_type,payload)
			VALUES ($1,$2,$3,1,'run.succeeded','{}')`, []any{"seal_event_" + suffix, organizationID, runID}},
		{`INSERT INTO node_outputs
			(id,organization_id,run_id,node_state_id,attempt_id,content_type,inline_value,size_bytes,content_hash)
			VALUES ($1,$2,$3,$4,$5,'application/json','{}',2,$6)`, []any{"seal_output_" + suffix, organizationID, runID, stateID, attemptID, hash[:]}},
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement.sql, statement.args...); err != nil {
			return err
		}
	}
	return nil
}

func readTargetAttestation(t *testing.T, ctx context.Context, pool *pgxpool.Pool) targetAttestation {
	t.Helper()
	var value targetAttestation
	if err := pool.QueryRow(ctx, `SELECT * FROM fenix_target_attestation_v2()`).Scan(
		&value.schemaVersion, &value.installationID, &value.contentHash,
		&value.workflowCount, &value.definitionCount, &value.draftCount, &value.triggerCount, &value.runCount,
	); err != nil {
		t.Fatal(err)
	}
	return value
}

func assertPostgresCode(t *testing.T, err error, code string) {
	t.Helper()
	if !isPostgresCode(err, code) {
		t.Fatalf("postgres error=%v, want SQLSTATE %s", err, code)
	}
}

func isPostgresCode(err error, code string) bool {
	var postgresError *pgconn.PgError
	return errors.As(err, &postgresError) && postgresError.Code == code
}
