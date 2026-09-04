package fenixmigration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/pazhou/supaflow/internal/platformops"
	"github.com/pazhou/supaflow/internal/workflow"
)

// Mapping contains explicit cross-system identities and pre-created credentials.
type Mapping struct {
	Tenants             map[string]string            `json:"tenants"`
	HTTPNodeCredentials map[string]map[string]string `json:"httpNodeCredentials,omitempty"`
	Subworkflows        map[string]PlanReference     `json:"subworkflows,omitempty"`
	TriggerCredentials  map[string]string            `json:"triggerCredentials,omitempty"`
}

// RunOptions controls one bounded, restartable migration execution.
type RunOptions struct {
	ActorID string
	DryRun  bool
}

// Report is safe to persist as migration evidence; it contains no definitions or secrets.
type Report struct {
	DryRun                         bool      `json:"dryRun"`
	StartedAt                      time.Time `json:"startedAt"`
	FinishedAt                     time.Time `json:"finishedAt"`
	SourceWatermark                Watermark `json:"sourceWatermark"`
	FinalSourceWatermark           Watermark `json:"finalSourceWatermark"`
	SourceFingerprint              string    `json:"sourceFingerprint"`
	FinalSourceFingerprint         string    `json:"finalSourceFingerprint"`
	SourceDatabaseFingerprint      string    `json:"sourceDatabaseFingerprint"`
	FinalSourceDatabaseFingerprint string    `json:"finalSourceDatabaseFingerprint"`
	SourceStable                   bool      `json:"sourceStable"`
	TargetAttestationSchemaVersion int       `json:"targetAttestationSchemaVersion"`
	TargetInstallationID           string    `json:"targetInstallationId"`
	TargetContentHash              string    `json:"targetContentHash"`
	TargetDefinitionCount          int       `json:"targetDefinitionCount"`
	TargetDraftCount               int       `json:"targetDraftCount"`
	TargetTriggerCount             int       `json:"targetTriggerCount"`
	TargetRunCount                 int       `json:"targetRunCount"`
	PlannedDefinitions             int       `json:"plannedDefinitions"`
	PlannedDrafts                  int       `json:"plannedDrafts"`
	PlannedTriggers                int       `json:"plannedTriggers"`
	PlannedRuns                    int       `json:"plannedRuns"`
	CreatedDefinitions             int       `json:"createdDefinitions"`
	ReplayedDefinitions            int       `json:"replayedDefinitions"`
	CreatedDrafts                  int       `json:"createdDrafts"`
	ReplayedDrafts                 int       `json:"replayedDrafts"`
	CreatedTriggers                int       `json:"createdTriggers"`
	ReplayedTriggers               int       `json:"replayedTriggers"`
	CreatedRuns                    int       `json:"createdRuns"`
	ReplayedRuns                   int       `json:"replayedRuns"`
}

// WorkflowBlocker ties a conversion blocker to an immutable source version.
type WorkflowBlocker struct {
	OrganizationID string  `json:"organizationId"`
	WorkflowID     string  `json:"workflowId"`
	Version        uint64  `json:"version"`
	Blocker        Blocker `json:"blocker"`
}

// PreflightError reports every known blocker before the first target write.
type PreflightError struct{ Blockers []WorkflowBlocker }

// Error summarizes preflight failure without exposing YAML.
func (e *PreflightError) Error() string {
	return fmt.Sprintf("Fenix migration preflight found %d blocker(s)", len(e.Blockers))
}

type preparedVersion struct {
	organizationID string
	tenantID       string
	workflowID     string
	version        SourceVersion
	definition     workflow.WorkflowDefinition
}

type preparedWorkflow struct {
	source   SourceWorkflow
	tenantID string
	versions []preparedVersion
	draft    *preparedVersion
}

// Migrate runs preflight over the complete snapshot before making idempotent writes.
func Migrate(ctx context.Context, snapshot Snapshot, mapping Mapping, target Target, options RunOptions) (Report, error) {
	report := Report{DryRun: options.DryRun, StartedAt: time.Now().UTC(), SourceWatermark: snapshot.Watermark,
		SourceDatabaseFingerprint: snapshot.DatabaseFingerprint}
	report.SourceFingerprint, _ = snapshot.Fingerprint()
	if strings.TrimSpace(options.ActorID) == "" {
		return report, errors.New("migration actor is required")
	}
	prepared, blockers := preflight(snapshot, mapping)
	for _, item := range prepared {
		report.PlannedDefinitions += len(item.versions)
		if item.draft != nil {
			report.PlannedDrafts++
		}
	}
	report.PlannedTriggers, report.PlannedRuns = len(snapshot.Triggers), len(snapshot.Runs)
	if len(blockers) > 0 {
		report.FinishedAt = time.Now().UTC()
		return report, &PreflightError{Blockers: blockers}
	}
	if options.DryRun {
		report.FinishedAt = time.Now().UTC()
		return report, nil
	}
	if target == nil {
		return report, errors.New("migration target is required")
	}
	ensured := make(map[string]bool)
	for _, organizationID := range sourceOrganizations(snapshot) {
		tenantID := mapping.Tenants[organizationID]
		if ensured[tenantID] {
			continue
		}
		if err := target.EnsureTenant(ctx, tenantID, "Migrated Fenix tenant "+organizationID); err != nil {
			return report, err
		}
		ensured[tenantID] = true
	}
	versions := make(map[string]ImportedVersion)
	latest := make(map[string]ImportedVersion)
	for _, item := range prepared {
		for _, version := range item.versions {
			key := migrationKey("version", version.organizationID, version.workflowID, fmt.Sprint(version.version.Version), version.version.SourceHash)
			imported, created, err := target.ImportVersion(ctx, version.tenantID, version.workflowID, version.version.Version, version.definition, key)
			if err != nil {
				return report, fmt.Errorf("import workflow %s version %d: %w", version.workflowID, version.version.Version, err)
			}
			if created {
				report.CreatedDefinitions++
			} else {
				report.ReplayedDefinitions++
			}
			versions[versionKey(version.tenantID, version.workflowID, version.version.Version)] = imported
			latest[workflowKey(version.tenantID, version.workflowID)] = imported
		}
		if item.draft != nil {
			key := migrationKey("draft", item.source.OrganizationID, item.source.ID, item.draft.version.SourceHash)
			created, err := target.ImportDraft(ctx, item.tenantID, item.source.ID, options.ActorID, item.draft.definition, key)
			if err != nil {
				return report, fmt.Errorf("import workflow %s draft: %w", item.source.ID, err)
			}
			if created {
				report.CreatedDrafts++
			} else {
				report.ReplayedDrafts++
			}
		}
	}
	for _, source := range snapshot.Triggers {
		tenantID := mapping.Tenants[source.OrganizationID]
		request, err := triggerRequest(source, tenantID, mapping.TriggerCredentials[source.ID], options.ActorID)
		if err != nil {
			return report, err
		}
		created, err := target.ImportTrigger(ctx, request)
		if err != nil {
			return report, fmt.Errorf("import trigger %s: %w", source.ID, err)
		}
		if created {
			report.CreatedTriggers++
		} else {
			report.ReplayedTriggers++
		}
	}
	for _, source := range snapshot.Runs {
		tenantID := mapping.Tenants[source.OrganizationID]
		imported := latest[workflowKey(tenantID, source.WorkflowID)]
		if source.Version != nil {
			imported = versions[versionKey(tenantID, source.WorkflowID, *source.Version)]
		}
		request, err := runRequest(source, tenantID, imported, options.ActorID)
		if err != nil {
			return report, fmt.Errorf("classify run %s output: %w", source.ID, err)
		}
		created, err := target.ImportRun(ctx, request)
		if err != nil {
			return report, fmt.Errorf("import run %s: %w", source.ID, err)
		}
		if created {
			report.CreatedRuns++
		} else {
			report.ReplayedRuns++
		}
	}
	attestation, err := target.Attest(ctx)
	if err != nil {
		return report, fmt.Errorf("attest migration target: %w", err)
	}
	if err := validateTargetAttestation(attestation); err != nil {
		return report, err
	}
	if attestation.DefinitionCount != report.PlannedDefinitions ||
		attestation.DraftCount != report.PlannedDrafts ||
		attestation.TriggerCount != report.PlannedTriggers ||
		attestation.RunCount != report.PlannedRuns {
		return report, errors.New("migration target attestation does not match the complete source plan")
	}
	report.TargetAttestationSchemaVersion = attestation.SchemaVersion
	report.TargetInstallationID = attestation.InstallationID
	report.TargetContentHash = attestation.ContentHash
	report.TargetDefinitionCount = attestation.DefinitionCount
	report.TargetDraftCount = attestation.DraftCount
	report.TargetTriggerCount = attestation.TriggerCount
	report.TargetRunCount = attestation.RunCount
	report.FinishedAt = time.Now().UTC()
	return report, nil
}

func preflight(snapshot Snapshot, mapping Mapping) ([]preparedWorkflow, []WorkflowBlocker) {
	prepared := make([]preparedWorkflow, 0, len(snapshot.Workflows))
	blockers := make([]WorkflowBlocker, 0)
	knownVersions := make(map[string]bool)
	knownLatest := make(map[string]bool)
	for _, source := range snapshot.Workflows {
		tenantID := mapping.Tenants[source.OrganizationID]
		if tenantID == "" {
			blockers = append(blockers, workflowBlock(source, 0, Blocker{Code: "tenant_mapping", Message: "organization requires an explicit target tenant"}))
			continue
		}
		item := preparedWorkflow{source: source, tenantID: tenantID}
		expected := uint64(1)
		for _, version := range source.Versions {
			if version.Status == "published" && version.Version != expected {
				blockers = append(blockers, workflowBlock(source, version.Version, Blocker{Code: "version_sequence", Message: "published versions must be contiguous from one"}))
				expected = version.Version
			}
			if version.Status != "published" && !(version.Status == "draft" && version.Version == 0) {
				blockers = append(blockers, workflowBlock(source, version.Version, Blocker{Code: "version_status", Message: "version status is invalid"}))
				continue
			}
			options := Options{Subworkflows: mapping.Subworkflows, HTTPNodeCredential: mapping.HTTPNodeCredentials[source.ID]}
			converted, err := ConvertYAML(source.ID, version.YAML, options)
			if err != nil {
				var blocked *BlockedError
				if errors.As(err, &blocked) {
					for _, detail := range blocked.Blockers {
						blockers = append(blockers, workflowBlock(source, version.Version, detail))
					}
				} else {
					blockers = append(blockers, workflowBlock(source, version.Version, Blocker{Code: "invalid_yaml", Message: err.Error()}))
				}
				continue
			}
			if _, err = workflow.Compile(converted.Definition, workflow.BuiltinRegistry(), migrationCompilePolicy()); err != nil {
				blockers = append(blockers, workflowBlock(source, version.Version, Blocker{Code: "compile", Message: err.Error()}))
				continue
			}
			value := preparedVersion{organizationID: source.OrganizationID, tenantID: tenantID, workflowID: source.ID, version: version, definition: converted.Definition}
			if version.Version == 0 {
				item.draft = &value
			} else {
				item.versions = append(item.versions, value)
				expected++
				knownVersions[versionKey(tenantID, source.ID, version.Version)] = true
			}
		}
		prepared = append(prepared, item)
		if len(item.versions) > 0 {
			knownLatest[workflowKey(tenantID, source.ID)] = true
		}
	}
	for _, trigger := range snapshot.Triggers {
		tenantID := mapping.Tenants[trigger.OrganizationID]
		if tenantID == "" {
			blockers = append(blockers, WorkflowBlocker{OrganizationID: trigger.OrganizationID, WorkflowID: trigger.WorkflowID, Blocker: Blocker{NodeID: trigger.ID, Code: "tenant_mapping", Message: "organization requires an explicit target tenant"}})
			continue
		}
		if _, err := triggerRequest(trigger, tenantID, mapping.TriggerCredentials[trigger.ID], "preflight"); err != nil {
			blockers = append(blockers, WorkflowBlocker{OrganizationID: trigger.OrganizationID, WorkflowID: trigger.WorkflowID, Blocker: Blocker{NodeID: trigger.ID, Code: "trigger", Message: err.Error()}})
		}
	}
	for _, run := range snapshot.Runs {
		tenantID := mapping.Tenants[run.OrganizationID]
		if tenantID == "" {
			blockers = append(blockers, WorkflowBlocker{OrganizationID: run.OrganizationID, WorkflowID: run.WorkflowID, Blocker: Blocker{NodeID: run.ID, Code: "tenant_mapping", Message: "organization requires an explicit target tenant"}})
			continue
		}
		versionMissing := run.Version != nil && !knownVersions[versionKey(tenantID, run.WorkflowID, *run.Version)]
		if run.Version == nil && !knownLatest[workflowKey(tenantID, run.WorkflowID)] {
			versionMissing = true
		}
		if versionMissing {
			blockers = append(blockers, WorkflowBlocker{OrganizationID: run.OrganizationID, WorkflowID: run.WorkflowID, Blocker: Blocker{NodeID: run.ID, Code: "run_version", Message: "terminal run must reference an imported published version"}})
		}
		if normalizeRunStatus(run.Status) == "" {
			blockers = append(blockers, WorkflowBlocker{OrganizationID: run.OrganizationID, WorkflowID: run.WorkflowID, Blocker: Blocker{NodeID: run.ID, Code: "run_status", Message: "run status is not terminal"}})
		}
		if _, err := classifyLegacyOutput(run.Output); err != nil {
			blockers = append(blockers, WorkflowBlocker{OrganizationID: run.OrganizationID, WorkflowID: run.WorkflowID, Blocker: Blocker{NodeID: run.ID, Code: "run_output", Message: err.Error()}})
		}
	}
	sort.Slice(blockers, func(i, j int) bool {
		left, right := blockers[i], blockers[j]
		if left.OrganizationID != right.OrganizationID {
			return left.OrganizationID < right.OrganizationID
		}
		if left.WorkflowID != right.WorkflowID {
			return left.WorkflowID < right.WorkflowID
		}
		if left.Version != right.Version {
			return left.Version < right.Version
		}
		if left.Blocker.NodeID != right.Blocker.NodeID {
			return left.Blocker.NodeID < right.Blocker.NodeID
		}
		return left.Blocker.Code < right.Blocker.Code
	})
	return prepared, blockers
}

func sourceOrganizations(snapshot Snapshot) []string {
	unique := make(map[string]struct{})
	for _, source := range snapshot.Workflows {
		unique[source.OrganizationID] = struct{}{}
	}
	for _, source := range snapshot.Triggers {
		unique[source.OrganizationID] = struct{}{}
	}
	for _, source := range snapshot.Runs {
		unique[source.OrganizationID] = struct{}{}
	}
	organizations := make([]string, 0, len(unique))
	for organizationID := range unique {
		organizations = append(organizations, organizationID)
	}
	sort.Strings(organizations)
	return organizations
}

func triggerRequest(source SourceTrigger, tenantID, credentialID, actorID string) (platformops.ImportTriggerRequest, error) {
	kind := platformops.TriggerKind(strings.ToLower(source.Kind))
	request := platformops.ImportTriggerRequest{TenantID: tenantID, ActorID: actorID, SourceSystem: "fenix", TriggerID: source.ID, PublicHash: source.PublicHash, WorkflowID: source.WorkflowID, Kind: kind, Name: "fenix-" + source.ID, Enabled: source.Enabled, Config: source.Config, SigningCredentialID: credentialID}
	if kind == platformops.TriggerWebhook && credentialID == "" {
		return request, errors.New("webhook trigger requires a pre-created signing credential mapping")
	}
	if kind == platformops.TriggerSchedule {
		var config struct {
			IntervalSeconds int64     `json:"intervalSeconds"`
			MisfirePolicy   string    `json:"misfirePolicy"`
			NextFireAt      time.Time `json:"nextFireAt"`
		}
		if json.Unmarshal(source.Config, &config) != nil || config.IntervalSeconds <= 0 || config.NextFireAt.IsZero() {
			return request, errors.New("schedule trigger config requires intervalSeconds and nextFireAt")
		}
		request.ScheduleInterval, request.MisfirePolicy, request.NextFireAt = time.Duration(config.IntervalSeconds)*time.Second, platformops.MisfirePolicy(config.MisfirePolicy), &config.NextFireAt
	}
	if kind == platformops.TriggerEvent {
		var config struct {
			EventType string `json:"eventType"`
		}
		_ = json.Unmarshal(source.Config, &config)
		request.EventType = config.EventType
		if request.EventType == "" {
			return request, errors.New("event trigger config requires eventType")
		}
	}
	return request, nil
}

func runRequest(source SourceRun, tenantID string, version ImportedVersion, actorID string) (platformops.ImportRunProjectionRequest, error) {
	output, err := classifyLegacyOutput(source.Output)
	if err != nil {
		return platformops.ImportRunProjectionRequest{}, err
	}
	payload, _ := json.Marshal(map[string]any{"triggeredBy": source.TriggeredBy, "outputMetadata": map[string]any{
		"contentType": output.ContentType, "size": output.Size, "sha256": output.SHA256, "sensitive": output.Sensitive,
	}})
	event := platformops.ImportedRunEvent{ID: migrationKey("event", tenantID, source.ID), Sequence: 1, Type: "run.migrated", Payload: payload, CreatedAt: source.FinishedAt}
	started := source.StartedAt
	return platformops.ImportRunProjectionRequest{TenantID: tenantID, ActorID: actorID, SourceSystem: "fenix", RunID: source.ID, WorkflowID: source.WorkflowID, WorkflowVersionID: version.VersionID, ExecutionPlanID: version.PlanID, PlanHash: version.PlanHash, Status: normalizeRunStatus(source.Status), Input: source.Input, Output: output, CreatedAt: source.CreatedAt, StartedAt: &started, FinishedAt: source.FinishedAt, Events: []platformops.ImportedRunEvent{event}}, nil
}

func normalizeRunStatus(value string) string {
	switch strings.ToLower(value) {
	case "success", "failed", "error", "cancelled":
		return strings.ToLower(value)
	default:
		return ""
	}
}
func workflowKey(tenant, workflowID string) string { return tenant + "\x00" + workflowID }
func versionKey(tenant, workflowID string, version uint64) string {
	return fmt.Sprintf("%s\x00%d", workflowKey(tenant, workflowID), version)
}
func workflowBlock(source SourceWorkflow, version uint64, blocker Blocker) WorkflowBlocker {
	return WorkflowBlocker{OrganizationID: source.OrganizationID, WorkflowID: source.ID, Version: version, Blocker: blocker}
}
