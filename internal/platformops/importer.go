package platformops

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var importedPublicHashPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{16,128}$`)

// ImportTriggerRequest preserves an existing external trigger identity without secret material.
type ImportTriggerRequest struct {
	TenantID            string
	ActorID             string
	SourceSystem        string
	TriggerID           string
	PublicHash          string
	WorkflowID          string
	Kind                TriggerKind
	Name                string
	Enabled             bool
	Config              json.RawMessage
	ScheduleInterval    time.Duration
	MisfirePolicy       MisfirePolicy
	NextFireAt          *time.Time
	SigningCredentialID string
	EventType           string
}

// ImportedRunEvent is one source event with an explicit contiguous cursor.
type ImportedRunEvent struct {
	ID        string
	Sequence  uint64
	Type      string
	Payload   json.RawMessage
	CreatedAt time.Time
}

// ImportedRunOutput is the bounded, classified legacy terminal output written
// through the normal runtime output projection.
type ImportedRunOutput struct {
	Value       json.RawMessage
	ContentType string
	Size        int64
	SHA256      string
	Sensitive   bool
}

// ImportRunProjectionRequest imports immutable terminal history after definitions/plans exist.
type ImportRunProjectionRequest struct {
	TenantID          string
	ActorID           string
	SourceSystem      string
	RunID             string
	WorkflowID        string
	WorkflowVersionID string
	ExecutionPlanID   string
	PlanHash          string
	Status            string
	Input             json.RawMessage
	Output            *ImportedRunOutput
	CreatedAt         time.Time
	StartedAt         *time.Time
	FinishedAt        time.Time
	Events            []ImportedRunEvent
}

// AdminImporter is the privileged, idempotent Phase4 seam for trigger and terminal history import.
// It accepts credential references only; callers must migrate secret values through CredentialService.
type AdminImporter struct{ pool *pgxpool.Pool }

// NewAdminImporter creates a PostgreSQL administrative importer.
func NewAdminImporter(pool *pgxpool.Pool) (*AdminImporter, error) {
	if pool == nil {
		return nil, ErrInvalidArgument
	}
	return &AdminImporter{pool: pool}, nil
}

// ImportTrigger preserves trigger ID/publicHash and rejects a conflicting replay.
func (i *AdminImporter) ImportTrigger(ctx context.Context, request ImportTriggerRequest) (Trigger, bool, error) {
	if err := validateImportedTrigger(request); err != nil {
		return Trigger{}, false, err
	}
	fingerprint := request
	fingerprint.ActorID = ""
	requestHash, err := canonicalImportHash(fingerprint)
	if err != nil {
		return Trigger{}, false, err
	}
	tx, err := i.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Trigger{}, false, fmt.Errorf("begin trigger import: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	config := request.Config
	if len(config) == 0 {
		config = json.RawMessage(`{}`)
	}
	status := "disabled"
	if request.Enabled {
		status = "enabled"
	}
	var interval any
	if request.ScheduleInterval > 0 {
		interval = int64(request.ScheduleInterval / time.Second)
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO triggers (
		    tenant_id,id,public_hash,workflow_id,kind,name,status,config,
		    schedule_interval_seconds,misfire_policy,next_fire_at,signing_credential_id,
		    event_type,created_by,source_system,source_import_hash
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NULLIF($12,''),NULLIF($13,''),$14,$15,$16)
		ON CONFLICT (tenant_id,id) DO NOTHING`, request.TenantID, request.TriggerID,
		request.PublicHash, request.WorkflowID, request.Kind, request.Name, status, config,
		interval, nullableString(string(request.MisfirePolicy)), request.NextFireAt,
		request.SigningCredentialID, request.EventType, request.ActorID, request.SourceSystem, requestHash[:])
	if err != nil {
		return Trigger{}, false, fmt.Errorf("import trigger: %w", err)
	}
	created := tag.RowsAffected() == 1
	if !created {
		var existingHash []byte
		if err = tx.QueryRow(ctx, `SELECT source_import_hash FROM triggers WHERE tenant_id=$1 AND id=$2`, request.TenantID, request.TriggerID).Scan(&existingHash); err != nil {
			return Trigger{}, false, ErrConflict
		}
		if subtle.ConstantTimeCompare(existingHash, requestHash[:]) != 1 {
			return Trigger{}, false, ErrConflict
		}
	}
	trigger, err := scanTrigger(tx.QueryRow(ctx, `
		SELECT tenant_id,id,public_hash,workflow_id,kind,name,status,config,
		       schedule_interval_seconds,COALESCE(misfire_policy,''),next_fire_at,
		       COALESCE(signing_credential_id,''),COALESCE(event_type,''),created_by,created_at,updated_at
		FROM triggers WHERE tenant_id=$1 AND id=$2`, request.TenantID, request.TriggerID))
	if err != nil {
		return Trigger{}, false, fmt.Errorf("read imported trigger: %w", err)
	}
	if created {
		if _, err = insertAuditTx(ctx, tx, AuditRecord{
			TenantID: request.TenantID, ActorID: request.ActorID, Action: "trigger.import",
			ResourceType: "trigger", ResourceID: request.TriggerID, Outcome: AuditAllowed,
		}); err != nil {
			return Trigger{}, false, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return Trigger{}, false, fmt.Errorf("commit trigger import: %w", err)
	}
	return trigger, created, nil
}

// ImportRunProjection stores terminal run history and contiguous events without producing deliveries.
func (i *AdminImporter) ImportRunProjection(ctx context.Context, request ImportRunProjectionRequest) (bool, error) {
	if err := validateImportedRun(request); err != nil {
		return false, err
	}
	fingerprint := request
	fingerprint.ActorID = ""
	requestHash, err := canonicalImportHash(fingerprint)
	if err != nil {
		return false, err
	}
	tx, err := i.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("begin run projection import: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	idempotencyKey := "import:" + request.SourceSystem + ":" + request.RunID
	tag, err := tx.Exec(ctx, `
		INSERT INTO runs (
		    id,tenant_id,workflow_id,workflow_version_id,execution_plan_id,idempotency_key,
		    request_hash,input,plan_hash,status,next_event_sequence,started_at,finished_at,created_at,updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$13)
		ON CONFLICT (tenant_id,id) DO NOTHING`, request.RunID, request.TenantID, request.WorkflowID,
		request.WorkflowVersionID, request.ExecutionPlanID, idempotencyKey, requestHash[:], request.Input,
		request.PlanHash, request.Status, len(request.Events), request.StartedAt, request.FinishedAt, request.CreatedAt)
	if err != nil {
		return false, fmt.Errorf("import run projection: %w", err)
	}
	created := tag.RowsAffected() == 1
	if !created {
		var existingHash []byte
		if err = tx.QueryRow(ctx, `SELECT request_hash FROM runs WHERE tenant_id=$1 AND id=$2`, request.TenantID, request.RunID).Scan(&existingHash); err != nil || subtle.ConstantTimeCompare(existingHash, requestHash[:]) != 1 {
			return false, ErrConflict
		}
		if err = tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit run import replay: %w", err)
		}
		return false, nil
	}
	if request.Output != nil {
		if err = importRunOutputTx(ctx, tx, request); err != nil {
			return false, err
		}
	}
	for _, event := range request.Events {
		payload, redactErr := redactJSON(event.Payload)
		if redactErr != nil {
			return false, ErrInvalidArgument
		}
		if _, err = tx.Exec(ctx, `
			INSERT INTO run_events (id,tenant_id,run_id,sequence,event_type,payload,created_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7)`, event.ID, request.TenantID, request.RunID,
			event.Sequence, event.Type, payload, event.CreatedAt); err != nil {
			return false, fmt.Errorf("import run event %d: %w", event.Sequence, err)
		}
	}
	if _, err = insertAuditTx(ctx, tx, AuditRecord{
		TenantID: request.TenantID, ActorID: request.ActorID, Action: "run_history.import",
		ResourceType: "run", ResourceID: request.RunID, Outcome: AuditAllowed,
	}); err != nil {
		return false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit run projection import: %w", err)
	}
	return true, nil
}

func importRunOutputTx(ctx context.Context, tx pgx.Tx, request ImportRunProjectionRequest) error {
	if err := lockTenantRetention(ctx, tx, request.TenantID); err != nil {
		return err
	}
	output := request.Output
	digest, err := hex.DecodeString(output.SHA256)
	if err != nil || len(digest) != sha256.Size {
		return ErrInvalidArgument
	}
	identity := sha256.Sum256([]byte(request.TenantID + "\x00" + request.RunID + "\x00legacy-output"))
	suffix := hex.EncodeToString(identity[:16])
	nodeStateID, attemptID, outputID := "node_"+suffix, "att_"+suffix, "out_"+suffix
	const nodeID = "legacy-output"
	if _, err = tx.Exec(ctx, `
		INSERT INTO node_states
		(id,tenant_id,run_id,node_id,status,attempt_count,latest_attempt_id,created_at,updated_at)
		VALUES ($1,$2,$3,$4,'success',1,$5,$6,$7)`, nodeStateID, request.TenantID, request.RunID,
		nodeID, attemptID, request.CreatedAt, request.FinishedAt); err != nil {
		return fmt.Errorf("import output node state: %w", err)
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO node_attempts
		(id,tenant_id,run_id,node_state_id,node_id,executor,input,attempt_number,status,
		 available_at,started_at,finished_at,created_at,updated_at)
		VALUES ($1,$2,$3,$4,$5,'migration.fenix',$6,1,'succeeded',$7,$7,$8,$7,$8)`,
		attemptID, request.TenantID, request.RunID, nodeStateID, nodeID, request.Input,
		request.CreatedAt, request.FinishedAt); err != nil {
		return fmt.Errorf("import output attempt: %w", err)
	}
	sensitivity := "internal"
	if output.Sensitive {
		sensitivity = "sensitive"
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO node_outputs
		(id,tenant_id,run_id,node_state_id,attempt_id,content_type,inline_value,size_bytes,
		 content_hash,sensitivity,created_at,retention_until)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,
		 $11::timestamptz + COALESCE((SELECT output_days FROM retention_policies WHERE tenant_id=$2),30) * interval '1 day')`,
		outputID, request.TenantID, request.RunID, nodeStateID, attemptID, output.ContentType,
		output.Value, output.Size, digest, sensitivity, request.FinishedAt); err != nil {
		return fmt.Errorf("import run output: %w", err)
	}
	if _, err = tx.Exec(ctx, `UPDATE node_states SET output_id=$3 WHERE tenant_id=$1 AND id=$2`, request.TenantID, nodeStateID, outputID); err != nil {
		return fmt.Errorf("project imported node output: %w", err)
	}
	if _, err = tx.Exec(ctx, `UPDATE runs SET output_id=$3 WHERE tenant_id=$1 AND id=$2`, request.TenantID, request.RunID, outputID); err != nil {
		return fmt.Errorf("project imported run output: %w", err)
	}
	return nil
}

func validateImportedTrigger(request ImportTriggerRequest) error {
	if request.TriggerID == "" || request.SourceSystem == "" || !importedPublicHashPattern.MatchString(request.PublicHash) {
		return ErrInvalidArgument
	}
	return validateTriggerRequest(CreateTriggerRequest{
		TenantID: request.TenantID, ActorID: request.ActorID, WorkflowID: request.WorkflowID,
		Kind: request.Kind, Name: request.Name, Config: request.Config,
		ScheduleInterval: request.ScheduleInterval, MisfirePolicy: request.MisfirePolicy,
		FirstFireAt: request.NextFireAt, SigningCredentialID: request.SigningCredentialID, EventType: request.EventType,
	})
}

func validateImportedRun(request ImportRunProjectionRequest) error {
	if request.TenantID == "" || request.ActorID == "" || request.SourceSystem == "" ||
		request.RunID == "" || request.WorkflowID == "" || request.WorkflowVersionID == "" ||
		request.ExecutionPlanID == "" || request.PlanHash == "" || request.CreatedAt.IsZero() ||
		request.FinishedAt.IsZero() || !json.Valid(request.Input) {
		return ErrInvalidArgument
	}
	if request.Output == nil || !json.Valid(request.Output.Value) || request.Output.ContentType != "application/json" ||
		request.Output.Size != int64(len(request.Output.Value)) || !validImportedOutputDigest(request.Output) {
		return ErrInvalidArgument
	}
	if request.Status != "success" && request.Status != "failed" && request.Status != "error" && request.Status != "cancelled" {
		return ErrInvalidArgument
	}
	for index, event := range request.Events {
		if event.ID == "" || event.Type == "" || event.CreatedAt.IsZero() || event.Sequence != uint64(index+1) || !validJSONObject(event.Payload) {
			return ErrInvalidArgument
		}
	}
	return nil
}

func validImportedOutputDigest(output *ImportedRunOutput) bool {
	claimed, err := hex.DecodeString(output.SHA256)
	if err != nil || len(claimed) != sha256.Size {
		return false
	}
	actual := sha256.Sum256(output.Value)
	return subtle.ConstantTimeCompare(claimed, actual[:]) == 1
}

func canonicalImportHash(value any) ([sha256.Size]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("encode import fingerprint: %w", err)
	}
	return sha256.Sum256(encoded), nil
}
