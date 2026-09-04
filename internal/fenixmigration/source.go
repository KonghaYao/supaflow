package fenixmigration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const maxDefinitionBytes = 4 << 20

// Watermark fingerprints mutable Fenix workflow tables before and after import.
type Watermark struct {
	WorkflowCount  int64     `json:"workflowCount"`
	VersionCount   int64     `json:"versionCount"`
	TriggerCount   int64     `json:"triggerCount"`
	RunCount       int64     `json:"runCount"`
	BoardCount     int64     `json:"boardCount"`
	EventCount     int64     `json:"eventCount"`
	JobCount       int64     `json:"jobCount"`
	OutputCount    int64     `json:"outputCount"`
	SnapshotCount  int64     `json:"snapshotCount"`
	ActiveRunCount int64     `json:"activeRunCount"`
	LatestChange   time.Time `json:"latestChange"`
}

// SourceVersion is one frozen Fenix draft or published YAML record.
type SourceVersion struct {
	Version    uint64
	Status     string
	FilePath   string
	CreatedAt  time.Time
	YAML       []byte
	SourceHash string
}

// SourceWorkflow is one tenant-owned legacy workflow and its ordered history.
type SourceWorkflow struct {
	ID             string
	OrganizationID string
	Name           string
	Description    string
	StoragePath    string
	Versions       []SourceVersion
}

// SourceTrigger excludes the legacy secret value by construction.
type SourceTrigger struct {
	ID             string
	OrganizationID string
	WorkflowID     string
	Kind           string
	PublicHash     string
	HasSecret      bool
	Config         json.RawMessage
	Enabled        bool
	CreatedAt      time.Time
}

// SourceRun is a terminal Fenix history projection. Non-terminal runs are not migrated.
type SourceRun struct {
	ID             string
	OrganizationID string
	WorkflowID     string
	Version        *uint64
	Status         string
	Input          json.RawMessage
	Output         json.RawMessage
	TriggeredBy    string
	StartedAt      time.Time
	FinishedAt     time.Time
	CreatedAt      time.Time
}

// Snapshot is a repeatable migration input and its initial freeze watermark.
type Snapshot struct {
	Watermark           Watermark
	Workflows           []SourceWorkflow
	Triggers            []SourceTrigger
	Runs                []SourceRun
	DatabaseFingerprint string `json:"-"`
}

// Fingerprint returns a stable digest over the database cleanup digest and all
// loaded migration projections, including the exact YAML bytes.
func (s Snapshot) Fingerprint() (string, error) {
	payload := struct {
		DatabaseFingerprint string
		Watermark           Watermark
		Workflows           []SourceWorkflow
		Triggers            []SourceTrigger
		Runs                []SourceRun
	}{s.DatabaseFingerprint, s.Watermark, s.Workflows, s.Triggers, s.Runs}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode source snapshot fingerprint: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

// SourceReader reads only the old Fenix tables and a configured workflow root.
type SourceReader struct {
	pool *pgxpool.Pool
	root string
}

// NewSourceReader resolves the allowed YAML root and rejects broad or missing paths.
func NewSourceReader(pool *pgxpool.Pool, root string) (*SourceReader, error) {
	if pool == nil || strings.TrimSpace(root) == "" {
		return nil, errors.New("source database and workflow root are required")
	}
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		return nil, fmt.Errorf("resolve source workflow root: %w", err)
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.IsDir() {
		return nil, errors.New("source workflow root must be an existing directory")
	}
	return &SourceReader{pool: pool, root: filepath.Clean(resolved)}, nil
}

// Watermark returns a non-sensitive summary used to prove Fenix stayed frozen.
func (r *SourceReader) Watermark(ctx context.Context) (Watermark, error) {
	var result Watermark
	err := r.pool.QueryRow(ctx, `
		SELECT
		  (SELECT count(*) FROM workflow),
		  (SELECT count(*) FROM workflow_version),
		  (SELECT count(*) FROM workflow_trigger),
		  (SELECT count(*) FROM workflow_run),
		  (SELECT count(*) FROM workflow_board),
		  (SELECT count(*) FROM workflow_event),
		  (SELECT count(*) FROM workflow_job),
		  (SELECT count(*) FROM workflow_node_output),
		  (SELECT count(*) FROM workflow_snapshot),
		  (SELECT count(*) FROM workflow_run WHERE completed_at IS NULL
		    OR lower(status) NOT IN ('success','failed','error','cancelled')),
		  GREATEST(
		    COALESCE((SELECT max(updated_at) FROM workflow), to_timestamp(0)),
		    COALESCE((SELECT max(created_at) FROM workflow_version), to_timestamp(0)),
		    COALESCE((SELECT max(updated_at) FROM workflow_trigger), to_timestamp(0)),
		    COALESCE((SELECT max(created_at) FROM workflow_run), to_timestamp(0))
		  )`).Scan(&result.WorkflowCount, &result.VersionCount, &result.TriggerCount, &result.RunCount,
		&result.BoardCount, &result.EventCount, &result.JobCount, &result.OutputCount, &result.SnapshotCount,
		&result.ActiveRunCount, &result.LatestChange)
	if err != nil {
		return Watermark{}, fmt.Errorf("read Fenix workflow freeze watermark: %w", err)
	}
	return result, nil
}

// Load obtains a consistent database snapshot and then reads its referenced YAML files.
func (r *SourceReader) Load(ctx context.Context) (Snapshot, error) {
	watermark, err := r.Watermark(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	if watermark.ActiveRunCount != 0 {
		return Snapshot{}, errors.New("Fenix source contains non-terminal workflow runs; finish or cancel them before migration")
	}
	fingerprint, err := r.databaseFingerprint(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	workflows, err := r.loadWorkflows(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	triggers, err := r.loadTriggers(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	runs, err := r.loadRuns(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	return Snapshot{Watermark: watermark, Workflows: workflows, Triggers: triggers, Runs: runs, DatabaseFingerprint: fingerprint}, nil
}

func (r *SourceReader) databaseFingerprint(ctx context.Context) (string, error) {
	var fingerprint string
	if err := r.pool.QueryRow(ctx, sourceFingerprintSQL).Scan(&fingerprint); err != nil {
		return "", fmt.Errorf("fingerprint Fenix workflow source: %w", err)
	}
	if len(fingerprint) != sha256.Size*2 {
		return "", errors.New("Fenix workflow source fingerprint is invalid")
	}
	return fingerprint, nil
}

func (r *SourceReader) loadWorkflows(ctx context.Context) ([]SourceWorkflow, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT w.id::text,w.organization_id,w.name,COALESCE(w.description,''),COALESCE(w.storage_path,''),
		       v.version,v.status,v.file_path,v.created_at
		FROM workflow w
		LEFT JOIN workflow_version v ON v.workflow_id=w.id
		ORDER BY w.organization_id,w.id,v.version`)
	if err != nil {
		return nil, fmt.Errorf("read Fenix workflow definitions: %w", err)
	}
	defer rows.Close()
	result := make([]SourceWorkflow, 0)
	index := make(map[string]int)
	for rows.Next() {
		var item SourceWorkflow
		var version *int32
		var status, filePath *string
		var createdAt *time.Time
		if err = rows.Scan(&item.ID, &item.OrganizationID, &item.Name, &item.Description, &item.StoragePath, &version, &status, &filePath, &createdAt); err != nil {
			return nil, fmt.Errorf("scan Fenix workflow definition: %w", err)
		}
		key := item.OrganizationID + "\x00" + item.ID
		position, exists := index[key]
		if !exists {
			position = len(result)
			index[key] = position
			result = append(result, item)
		}
		if version == nil {
			continue
		}
		if *version < 0 || status == nil || filePath == nil || createdAt == nil {
			return nil, errors.New("Fenix workflow version contains invalid fields")
		}
		content, readErr := r.readDefinition(item.StoragePath, *filePath)
		if readErr != nil {
			return nil, fmt.Errorf("read Fenix workflow %s version %d: %w", item.ID, *version, readErr)
		}
		hash := fmt.Sprintf("%x", sha256Bytes(content))
		result[position].Versions = append(result[position].Versions, SourceVersion{Version: uint64(*version), Status: *status, FilePath: *filePath, CreatedAt: *createdAt, YAML: content, SourceHash: hash})
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate Fenix workflows: %w", err)
	}
	return result, nil
}

func (r *SourceReader) loadTriggers(ctx context.Context) ([]SourceTrigger, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id::text,organization_id,workflow_id::text,type,public_hash,(secret IS NOT NULL AND secret <> ''),
		       COALESCE(config,'{}'::jsonb),enabled,created_at
		FROM workflow_trigger ORDER BY organization_id,id`)
	if err != nil {
		return nil, fmt.Errorf("read Fenix workflow triggers: %w", err)
	}
	defer rows.Close()
	result := make([]SourceTrigger, 0)
	for rows.Next() {
		var item SourceTrigger
		if err = rows.Scan(&item.ID, &item.OrganizationID, &item.WorkflowID, &item.Kind, &item.PublicHash, &item.HasSecret, &item.Config, &item.Enabled, &item.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan Fenix trigger: %w", err)
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (r *SourceReader) loadRuns(ctx context.Context) ([]SourceRun, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT r.id::text,w.organization_id,r.workflow_id::text,r.version,r.status,
		       COALESCE(r.input,'{}'::jsonb),COALESCE(r.output,'null'::jsonb),r.triggered_by,
		       r.started_at,r.completed_at,r.created_at
		FROM workflow_run r JOIN workflow w ON w.id=r.workflow_id
		WHERE r.completed_at IS NOT NULL AND lower(r.status) IN ('success','failed','error','cancelled')
		ORDER BY w.organization_id,r.created_at,r.id`)
	if err != nil {
		return nil, fmt.Errorf("read Fenix terminal workflow runs: %w", err)
	}
	defer rows.Close()
	result := make([]SourceRun, 0)
	for rows.Next() {
		var item SourceRun
		var version *int32
		if err = rows.Scan(&item.ID, &item.OrganizationID, &item.WorkflowID, &version, &item.Status, &item.Input, &item.Output, &item.TriggeredBy, &item.StartedAt, &item.FinishedAt, &item.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan Fenix terminal run: %w", err)
		}
		if version != nil && *version > 0 {
			converted := uint64(*version)
			item.Version = &converted
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (r *SourceReader) readDefinition(storagePath, filePath string) ([]byte, error) {
	if filePath == "" || filepath.IsAbs(filePath) || strings.ContainsRune(filePath, '\x00') {
		return nil, errors.New("definition file path must be relative")
	}
	root, err := filepath.EvalSymlinks(r.root)
	if err != nil {
		return nil, fmt.Errorf("resolve configured workflow root: %w", err)
	}
	storage := storagePath
	if !filepath.IsAbs(storage) {
		storage = filepath.Join(root, storage)
	}
	candidate, err := filepath.EvalSymlinks(filepath.Join(storage, filePath))
	if err != nil {
		return nil, err
	}
	relative, err := filepath.Rel(root, candidate)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return nil, errors.New("definition path escapes configured workflow root")
	}
	file, err := os.Open(candidate)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	content, err := io.ReadAll(io.LimitReader(file, maxDefinitionBytes+1))
	if err != nil {
		return nil, err
	}
	if len(content) > maxDefinitionBytes {
		return nil, errors.New("definition exceeds four MiB")
	}
	return content, nil
}

func sha256Bytes(value []byte) [32]byte {
	// Kept local so source loading never serializes or logs workflow contents.
	return sha256.Sum256(value)
}
