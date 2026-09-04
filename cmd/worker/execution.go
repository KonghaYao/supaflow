package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/executor"
	"github.com/pazhou/supaflow/internal/isolation"
	"github.com/pazhou/supaflow/internal/platform"
	"github.com/pazhou/supaflow/internal/platformops"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

const (
	httpCredentialPurpose      = "http_headers"
	providerCredentialPurpose  = "llm_provider"
	isolationCredentialPurpose = "isolated_environment"
)

type modelConfig struct {
	Name          string `json:"name"`
	CredentialRef string `json:"credentialRef"`
	BaseURL       string `json:"baseUrl,omitempty"`
	Model         string `json:"model"`
	TimeoutMS     int64  `json:"timeoutMs,omitempty"`
}

type toolConfig = executor.TrustedToolConfig
type agentConfig = executor.AgentConfig

type attemptCredentials struct {
	service            *platformops.CredentialService
	adapterCredentials map[string]string
}

func configureExecution(pool *pgxpool.Pool, catalog platform.PlanCatalog, runs *workflowruntime.Service, allowedHosts []string, outputs executor.OutputManager, capabilityProbe executor.IsolationCapabilityProbe) (executor.Config, error) {
	credentials, err := configureAttemptCredentials(pool)
	if err != nil {
		return executor.Config{}, err
	}
	modelsRegistry, err := executor.NewCredentialEinoRegistry(credentials)
	if err != nil {
		return executor.Config{}, err
	}
	models, err := parseModelConfigs(strings.TrimSpace(envValue("SUPAFLOW_EINO_MODELS_JSON")))
	if err != nil {
		return executor.Config{}, err
	}
	for _, model := range models {
		timeout := time.Duration(model.TimeoutMS) * time.Millisecond
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		if err = modelsRegistry.RegisterOpenAIProvider(executor.OpenAIModelConfig{
			Name: model.Name, CredentialRef: model.CredentialRef, BaseURL: model.BaseURL,
			Model: model.Model, Timeout: timeout,
		}); err != nil {
			return executor.Config{}, err
		}
		credentials.adapterCredentials[model.Name] = model.CredentialRef
	}
	toolConfigs, err := parseToolConfigs(strings.TrimSpace(envValue("SUPAFLOW_EINO_TOOLS_JSON")))
	if err != nil {
		return executor.Config{}, err
	}
	toolsRegistry, err := executor.NewTrustedToolRegistry(toolConfigs)
	if err != nil {
		return executor.Config{}, err
	}
	agentConfigs, err := parseAgentConfigs(strings.TrimSpace(envValue("SUPAFLOW_EINO_AGENTS_JSON")))
	if err != nil {
		return executor.Config{}, err
	}
	agentsRegistry, err := executor.NewBoundedAgentRegistry(modelsRegistry, toolsRegistry, agentConfigs, modelsRegistry.HasModel)
	if err != nil {
		return executor.Config{}, err
	}
	for _, agent := range agentConfigs {
		if reference := credentials.adapterCredentials[agent.Model]; reference != "" {
			credentials.adapterCredentials[agent.Name] = reference
		}
	}
	subflows, err := executor.NewRuntimeSubworkflowRunner(catalog, runs, outputs)
	if err != nil {
		return executor.Config{}, err
	}
	isolationBinary := strings.TrimSpace(envValue("SUPAFLOW_ISOLATION_BINARY"))
	if !filepath.IsAbs(isolationBinary) {
		return executor.Config{}, errors.New("SUPAFLOW_ISOLATION_BINARY must be an absolute path")
	}
	isolationConfig, err := loadIsolationProcessConfig(isolationBinary, credentials, capabilityProbe)
	if err != nil {
		return executor.Config{}, err
	}
	isolationRunner, err := executor.NewProcessIsolationRunner(isolationConfig)
	if err != nil {
		return executor.Config{}, err
	}
	return executor.Config{
		AllowedHosts: allowedHosts, CredentialResolver: credentials, CredentialAuthorizer: credentials,
		Models: modelsRegistry, Agents: agentsRegistry, Tools: toolsRegistry, Subworkflows: subflows, Isolation: isolationRunner, Outputs: outputs,
	}, nil
}

func loadIsolationProcessConfig(binary string, credentials executor.EnvironmentResolver, probe executor.IsolationCapabilityProbe) (executor.ProcessIsolationConfig, error) {
	read := func(name string, fallback uint64) (uint64, error) {
		value := strings.TrimSpace(envValue(name))
		if value == "" {
			return fallback, nil
		}
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil || parsed == 0 {
			return 0, errors.New(name + " must be a positive integer")
		}
		return parsed, nil
	}
	processes, err := read("SUPAFLOW_ISOLATION_MAX_PIDS", 32)
	if err != nil {
		return executor.ProcessIsolationConfig{}, err
	}
	memory, err := read("SUPAFLOW_ISOLATION_MEMORY_BYTES", 256<<20)
	if err != nil {
		return executor.ProcessIsolationConfig{}, err
	}
	workspaceBytes, err := read("SUPAFLOW_ISOLATION_WORKSPACE_BYTES", 16<<20)
	if err != nil {
		return executor.ProcessIsolationConfig{}, err
	}
	workspaceInodes, err := read("SUPAFLOW_ISOLATION_WORKSPACE_INODES", 1024)
	if err != nil {
		return executor.ProcessIsolationConfig{}, err
	}
	return executor.ProcessIsolationConfig{
		Binary: binary, Environment: credentials, Timeout: 30 * time.Second, OutputLimit: 1 << 20,
		Limits: isolation.ResourceLimits{
			AddressSpaceBytes: memory, Processes: processes,
			WorkspaceBytes: workspaceBytes, WorkspaceInodes: workspaceInodes,
		},
		CapabilityProbe: probe,
	}, nil
}

func configureAttemptCredentials(pool *pgxpool.Pool) (*attemptCredentials, error) {
	masterKey, err := decodeWorkerKey(strings.TrimSpace(envValue("SUPAFLOW_CREDENTIAL_MASTER_KEY")))
	if err != nil {
		return nil, errors.New("SUPAFLOW_CREDENTIAL_MASTER_KEY must encode exactly 32 bytes")
	}
	defer clear(masterKey)
	keyID := strings.TrimSpace(envValue("SUPAFLOW_CREDENTIAL_KEY_ID"))
	provider, err := platformops.NewLocalAESProvider(keyID, masterKey)
	if err != nil {
		return nil, errors.New("SUPAFLOW_CREDENTIAL_KEY_ID and master key are required")
	}
	service, err := platformops.NewCredentialService(platformops.NewPostgresCredentialStore(pool), provider)
	if err != nil {
		return nil, err
	}
	return &attemptCredentials{service: service, adapterCredentials: make(map[string]string)}, nil
}

func (r *attemptCredentials) ResolveHTTPHeaders(ctx context.Context, scope executor.AttemptScope, credentialID string) (http.Header, error) {
	secret, err := r.resolve(ctx, scope, credentialID, httpCredentialPurpose)
	if err != nil {
		return nil, err
	}
	defer clear(secret)
	var values map[string]string
	decoder := json.NewDecoder(bytes.NewReader(secret))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&values) != nil || len(values) == 0 {
		return nil, errors.New("credential payload is invalid")
	}
	headers := make(http.Header, len(values))
	for name, value := range values {
		if strings.TrimSpace(name) == "" || strings.ContainsAny(name, "\r\n") || strings.ContainsAny(value, "\r\n") {
			return nil, errors.New("credential payload is invalid")
		}
		headers.Set(name, value)
	}
	return headers, nil
}

func (r *attemptCredentials) ResolveProviderCredential(ctx context.Context, scope executor.AttemptScope, credentialID string) ([]byte, error) {
	return r.resolve(ctx, scope, credentialID, providerCredentialPurpose)
}

func (r *attemptCredentials) ResolveEnvironment(ctx context.Context, request executor.IsolationRequest, bindings []string) (map[string]string, error) {
	scope := executor.AttemptScope{TenantID: request.TenantID, RunID: request.RunID, AttemptID: request.AttemptID, FencingToken: request.FencingToken}
	result := make(map[string]string, len(bindings))
	for _, binding := range bindings {
		name, credentialID, ok := strings.Cut(binding, "=")
		if !ok || !validEnvironmentName(name) || strings.TrimSpace(credentialID) == "" {
			return nil, errors.New("isolated environment binding is invalid")
		}
		secret, err := r.resolve(ctx, scope, credentialID, isolationCredentialPurpose)
		if err != nil {
			return nil, err
		}
		result[name] = string(secret)
		clear(secret)
	}
	return result, nil
}

func (r *attemptCredentials) resolve(ctx context.Context, scope executor.AttemptScope, credentialID, purpose string) ([]byte, error) {
	secret, err := r.service.ResolveForAttempt(ctx, platformops.ResolveCredentialRequest{
		TenantID: string(scope.TenantID), RunID: scope.RunID, AttemptID: scope.AttemptID,
		CredentialID: credentialID, Purpose: purpose, FencingToken: scope.FencingToken, Now: time.Now().UTC(),
	})
	if err != nil {
		return nil, errors.New("attempt credential is unavailable")
	}
	return secret, nil
}

type nodeCredentialGrant struct {
	reference string
	purpose   string
}

// AuthorizeCredentials grants only references extracted from the verified plan node.
func (r *attemptCredentials) AuthorizeCredentials(ctx context.Context, request workflowruntime.ExecutionRequest, node workflow.PlanNode) error {
	grants, err := credentialGrantsForNode(node, r.adapterCredentials)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, grant := range grants {
		if err = r.service.GrantForAttempt(ctx, platformops.GrantCredentialRequest{
			TenantID: string(request.TenantID), RunID: request.RunID, AttemptID: request.AttemptID,
			CredentialID: grant.reference, Purpose: grant.purpose, FencingToken: request.FencingToken, Now: now,
		}); err != nil {
			return errors.New("attempt credential grant was rejected")
		}
	}
	return nil
}

func credentialGrantsForNode(node workflow.PlanNode, adapters map[string]string) ([]nodeCredentialGrant, error) {
	unique := make(map[nodeCredentialGrant]struct{})
	switch node.Executor.Kind {
	case "http.request":
		var config struct {
			CredentialRef string `json:"credentialRef"`
		}
		if err := json.Unmarshal(node.Config, &config); err != nil {
			return nil, errors.New("invalid HTTP credential configuration")
		}
		if config.CredentialRef != "" {
			unique[nodeCredentialGrant{reference: config.CredentialRef, purpose: httpCredentialPurpose}] = struct{}{}
		}
	case "eino.llm", "eino.agent":
		var config struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(node.Config, &config); err != nil {
			return nil, errors.New("invalid provider credential configuration")
		}
		if reference := adapters[config.Name]; reference != "" {
			unique[nodeCredentialGrant{reference: reference, purpose: providerCredentialPurpose}] = struct{}{}
		}
	case "isolated.shell", "isolated.python":
		var config struct {
			Env []string `json:"env"`
		}
		if err := json.Unmarshal(node.Config, &config); err != nil {
			return nil, errors.New("invalid isolation credential configuration")
		}
		for _, binding := range config.Env {
			_, reference, ok := strings.Cut(binding, "=")
			if !ok || strings.TrimSpace(reference) == "" {
				return nil, errors.New("invalid isolation credential binding")
			}
			unique[nodeCredentialGrant{reference: reference, purpose: isolationCredentialPurpose}] = struct{}{}
		}
	}
	grants := make([]nodeCredentialGrant, 0, len(unique))
	for grant := range unique {
		grants = append(grants, grant)
	}
	sort.Slice(grants, func(i, j int) bool {
		if grants[i].purpose != grants[j].purpose {
			return grants[i].purpose < grants[j].purpose
		}
		return grants[i].reference < grants[j].reference
	})
	return grants, nil
}

func parseModelConfigs(value string) ([]modelConfig, error) {
	if value == "" {
		return nil, nil
	}
	var configs []modelConfig
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&configs); err != nil {
		return nil, errors.New("SUPAFLOW_EINO_MODELS_JSON is invalid")
	}
	for _, cfg := range configs {
		if cfg.Name == "" || cfg.CredentialRef == "" || cfg.Model == "" || cfg.TimeoutMS < 0 {
			return nil, errors.New("SUPAFLOW_EINO_MODELS_JSON is invalid")
		}
	}
	return configs, nil
}

func parseToolConfigs(value string) ([]toolConfig, error) {
	if value == "" {
		return nil, nil
	}
	var configs []toolConfig
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&configs) != nil {
		return nil, errors.New("SUPAFLOW_EINO_TOOLS_JSON is invalid")
	}
	return configs, nil
}

func parseAgentConfigs(value string) ([]agentConfig, error) {
	if value == "" {
		return nil, nil
	}
	var configs []agentConfig
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&configs) != nil {
		return nil, errors.New("SUPAFLOW_EINO_AGENTS_JSON is invalid")
	}
	return configs, nil
}

func decodeWorkerKey(value string) ([]byte, error) {
	decoded, err := hex.DecodeString(value)
	if err == nil && len(decoded) == 32 {
		return decoded, nil
	}
	decoded, err = base64.StdEncoding.DecodeString(value)
	if err != nil || len(decoded) != 32 {
		return nil, errors.New("invalid key")
	}
	return decoded, nil
}

func validEnvironmentName(value string) bool {
	if value == "" || !((value[0] >= 'A' && value[0] <= 'Z') || value[0] == '_') {
		return false
	}
	for _, char := range value[1:] {
		if !((char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '_') {
			return false
		}
	}
	return true
}

var envValue = os.Getenv
