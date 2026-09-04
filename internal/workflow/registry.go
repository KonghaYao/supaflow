package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
)

// Capability is an executor behavior that a workflow may explicitly request.
type Capability string

const (
	// CapabilityStream permits incremental output delivery.
	CapabilityStream Capability = "stream"
	// CapabilityCancel permits active executor cancellation.
	CapabilityCancel Capability = "cancel"
	// CapabilityInterrupt permits durable suspension and resume.
	CapabilityInterrupt Capability = "interrupt"
	// CapabilityIdempotent indicates duplicate calls with the same key are safe.
	CapabilityIdempotent Capability = "idempotent"
)

// CapabilitySet describes the behavior guaranteed by a pinned node executor.
type CapabilitySet struct {
	Streaming     bool `json:"streaming"`
	Cancellable   bool `json:"cancellable"`
	Interruptible bool `json:"interruptible"`
	Idempotent    bool `json:"idempotent"`
	// RecoverySafe permits re-execution after an expired running lease. False
	// fails closed because the previous worker may have completed a side effect
	// without persisting its result.
	RecoverySafe bool `json:"recoverySafe"`
	// RetrySafeErrors is the closed set of executor error codes for which a new
	// physical attempt may repeat this logical execution. Empty fails closed.
	RetrySafeErrors []string `json:"retrySafeErrors,omitempty"`
}

// AllowsRetry reports whether the immutable node contract permits retrying code.
func (s CapabilitySet) AllowsRetry(code string) bool {
	for _, allowed := range s.RetrySafeErrors {
		if allowed == code {
			return true
		}
	}
	return false
}

// Has reports whether the capability is present.
func (s CapabilitySet) Has(capability Capability) bool {
	switch capability {
	case CapabilityStream:
		return s.Streaming
	case CapabilityCancel:
		return s.Cancellable
	case CapabilityInterrupt:
		return s.Interruptible
	case CapabilityIdempotent:
		return s.Idempotent
	default:
		return false
	}
}

// Allows reports whether all requested capabilities are allowed.
func (s CapabilitySet) Allows(requested CapabilitySet) bool {
	return (!requested.Streaming || s.Streaming) &&
		(!requested.Cancellable || s.Cancellable) &&
		(!requested.Interruptible || s.Interruptible) &&
		(!requested.Idempotent || s.Idempotent)
}

// PermissionSet lists external resources an executor requires.
type PermissionSet struct {
	Network     bool `json:"network"`
	Credentials bool `json:"credentials"`
	Filesystem  bool `json:"filesystem"`
	Compute     bool `json:"compute"`
}

// Allows reports whether all requested permissions are allowed.
func (s PermissionSet) Allows(requested PermissionSet) bool {
	return (!requested.Network || s.Network) &&
		(!requested.Credentials || s.Credentials) &&
		(!requested.Filesystem || s.Filesystem) &&
		(!requested.Compute || s.Compute)
}

// ExecutorBinding fixes the executor implementation used by a node version.
type ExecutorBinding struct {
	Kind                 string `json:"kind"`
	MinimumRunnerVersion string `json:"minimumRunnerVersion"`
}

// NodeMigrationMetadata identifies one deterministic config upgrade adapter.
// The executable function remains in the process adapter cache; this metadata
// is persisted so the supported upgrade graph is discoverable and auditable.
type NodeMigrationMetadata struct {
	FromVersion string `json:"fromVersion"`
	ToVersion   string `json:"toVersion"`
	Binding     string `json:"binding"`
}

// PortDefinition describes one named typed node port.
type PortDefinition struct {
	Type     ValueType `json:"type"`
	Required bool      `json:"required"`
}

// ConfigValidator performs node-specific semantic config validation.
type ConfigValidator func(config json.RawMessage) error

// NodeTypeVersion is an immutable registry entry for one node type version.
type NodeTypeVersion struct {
	Type               string                    `json:"type"`
	Version            string                    `json:"version"`
	Capabilities       CapabilitySet             `json:"capabilities"`
	DefaultTimeoutMS   int64                     `json:"defaultTimeoutMs"`
	MaximumOutputBytes int64                     `json:"maximumOutputBytes"`
	Permissions        PermissionSet             `json:"permissions"`
	Executor           ExecutorBinding           `json:"executor"`
	Inputs             map[string]PortDefinition `json:"inputs"`
	Outputs            map[string]PortDefinition `json:"outputs"`
	ConfigSchema       json.RawMessage           `json:"configSchema"`
	// SensitiveFields contains JSON Pointer paths rooted at the node DTO.
	// Studio and logging adapters use this immutable contract to avoid
	// rendering or recording protected values.
	SensitiveFields []string                `json:"sensitiveFields"`
	Migrations      []NodeMigrationMetadata `json:"migrations"`
	ValidateConfig  ConfigValidator         `json:"-"`
}

// Registry resolves node types to explicitly pinned versions.
type Registry struct {
	mu         sync.RWMutex
	entries    map[string]NodeTypeVersion
	migrations map[string]NodeMigration
}

// NewRegistry builds a registry and rejects duplicate or malformed entries.
func NewRegistry(entries ...NodeTypeVersion) (*Registry, error) {
	r := &Registry{entries: make(map[string]NodeTypeVersion, len(entries)), migrations: make(map[string]NodeMigration)}
	for _, entry := range entries {
		if err := r.Register(entry); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// Register adds one immutable node type version.
func (r *Registry) Register(entry NodeTypeVersion) error {
	if r == nil {
		return fmt.Errorf("workflow: nil registry")
	}
	if !nodeTypePattern.MatchString(entry.Type) {
		return fmt.Errorf("workflow: invalid node type %q", entry.Type)
	}
	if !versionPattern.MatchString(entry.Version) {
		return fmt.Errorf("workflow: invalid node version %q", entry.Version)
	}
	if !nodeTypePattern.MatchString(entry.Executor.Kind) || !versionPattern.MatchString(entry.Executor.MinimumRunnerVersion) {
		return fmt.Errorf("workflow: node %s@%s has incomplete executor binding", entry.Type, entry.Version)
	}
	if entry.DefaultTimeoutMS <= 0 || entry.MaximumOutputBytes <= 0 {
		return fmt.Errorf("workflow: node %s@%s has invalid limits", entry.Type, entry.Version)
	}
	if entry.ValidateConfig == nil {
		return fmt.Errorf("workflow: node %s@%s has no config validator", entry.Type, entry.Version)
	}
	for name, port := range entry.Inputs {
		if !idPattern.MatchString(name) || !validValueType(port.Type) {
			return fmt.Errorf("workflow: node %s@%s has invalid input port %q", entry.Type, entry.Version, name)
		}
	}
	for name, port := range entry.Outputs {
		if !idPattern.MatchString(name) || !validValueType(port.Type) {
			return fmt.Errorf("workflow: node %s@%s has invalid output port %q", entry.Type, entry.Version, name)
		}
	}
	seenRetryErrors := make(map[string]struct{}, len(entry.Capabilities.RetrySafeErrors))
	for _, code := range entry.Capabilities.RetrySafeErrors {
		if !idPattern.MatchString(code) {
			return fmt.Errorf("workflow: node %s@%s has invalid retry-safe error %q", entry.Type, entry.Version, code)
		}
		if _, duplicate := seenRetryErrors[code]; duplicate {
			return fmt.Errorf("workflow: node %s@%s has duplicate retry-safe error %q", entry.Type, entry.Version, code)
		}
		seenRetryErrors[code] = struct{}{}
	}
	sort.Strings(entry.Capabilities.RetrySafeErrors)
	canonicalSchema, err := canonicalizeJSON(entry.ConfigSchema)
	if err != nil {
		return fmt.Errorf("workflow: node %s@%s has invalid config schema: %w", entry.Type, entry.Version, err)
	}
	entry.ConfigSchema = canonicalSchema
	seenSensitiveFields := make(map[string]struct{}, len(entry.SensitiveFields))
	for _, path := range entry.SensitiveFields {
		if len(path) < 2 || path[0] != '/' {
			return fmt.Errorf("workflow: node %s@%s has invalid sensitive field %q", entry.Type, entry.Version, path)
		}
		if _, duplicate := seenSensitiveFields[path]; duplicate {
			return fmt.Errorf("workflow: node %s@%s has duplicate sensitive field %q", entry.Type, entry.Version, path)
		}
		seenSensitiveFields[path] = struct{}{}
	}
	for _, migration := range entry.Migrations {
		if !versionPattern.MatchString(migration.FromVersion) || !versionPattern.MatchString(migration.ToVersion) ||
			migration.FromVersion == migration.ToVersion || migration.Binding == "" {
			return fmt.Errorf("workflow: node %s@%s has invalid migration metadata", entry.Type, entry.Version)
		}
	}
	if entry.SensitiveFields == nil {
		entry.SensitiveFields = []string{}
	}
	if entry.Migrations == nil {
		entry.Migrations = []NodeMigrationMetadata{}
	}
	if entry.Inputs == nil {
		entry.Inputs = map[string]PortDefinition{}
	}
	if entry.Outputs == nil {
		entry.Outputs = map[string]PortDefinition{}
	}
	entry = cloneNodeTypeVersion(entry)
	key := registryKey(entry.Type, entry.Version)
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.entries[key]; exists {
		return fmt.Errorf("workflow: duplicate node registration %s@%s", entry.Type, entry.Version)
	}
	r.entries[key] = entry
	return nil
}

func (r *Registry) hasType(nodeType string) bool {
	if r == nil {
		return false
	}
	prefix := nodeType + "\x00"
	r.mu.RLock()
	defer r.mu.RUnlock()
	for key := range r.entries {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			return true
		}
	}
	return false
}

// Resolve returns a defensive copy of a pinned node type version.
func (r *Registry) Resolve(nodeType, version string) (NodeTypeVersion, bool) {
	if r == nil {
		return NodeTypeVersion{}, false
	}
	r.mu.RLock()
	entry, ok := r.entries[registryKey(nodeType, version)]
	r.mu.RUnlock()
	if !ok {
		return NodeTypeVersion{}, false
	}
	return cloneNodeTypeVersion(entry), true
}

// Entries returns all registered versions in deterministic type/version order.
func (r *Registry) Entries() []NodeTypeVersion {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	entries := make([]NodeTypeVersion, 0, len(r.entries))
	for _, entry := range r.entries {
		entries = append(entries, cloneNodeTypeVersion(entry))
	}
	r.mu.RUnlock()
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Type != entries[j].Type {
			return entries[i].Type < entries[j].Type
		}
		return entries[i].Version < entries[j].Version
	})
	return entries
}

// ListNodeTypes exposes the in-process registry as a read-only catalog adapter.
// Production discovery uses the PostgreSQL implementation; this adapter keeps
// protocol and local tests lightweight without making memory the authority.
func (r *Registry) ListNodeTypes(context.Context) ([]NodeTypeVersion, error) {
	return r.Entries(), nil
}

// VerifyProjection ensures a durable registry contains exactly the immutable
// contracts backed by this process adapter cache. It is intended for startup
// checks before a compiler or worker accepts traffic.
func (r *Registry) VerifyProjection(entries []NodeTypeVersion) error {
	if r == nil || len(entries) == 0 {
		return fmt.Errorf("workflow: durable registry projection is required")
	}
	seen := make(map[string]struct{}, len(entries))
	for _, persisted := range entries {
		key := registryKey(persisted.Type, persisted.Version)
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("workflow: duplicate durable node version %s@%s", persisted.Type, persisted.Version)
		}
		seen[key] = struct{}{}
		adapter, found := r.Resolve(persisted.Type, persisted.Version)
		if !found {
			return fmt.Errorf("workflow: durable node version %s@%s has no adapter", persisted.Type, persisted.Version)
		}
		persisted.ValidateConfig = nil
		adapter.ValidateConfig = nil
		persistedJSON, persistedErr := json.Marshal(persisted)
		adapterJSON, adapterErr := json.Marshal(adapter)
		canonicalPersisted, persistedCanonicalErr := canonicalizeJSON(persistedJSON)
		canonicalAdapter, adapterCanonicalErr := canonicalizeJSON(adapterJSON)
		if persistedErr != nil || adapterErr != nil || persistedCanonicalErr != nil || adapterCanonicalErr != nil ||
			!bytes.Equal(canonicalPersisted, canonicalAdapter) {
			return fmt.Errorf("workflow: durable node version %s@%s differs from its adapter", persisted.Type, persisted.Version)
		}
	}
	for _, adapter := range r.Entries() {
		if _, found := seen[registryKey(adapter.Type, adapter.Version)]; !found {
			return fmt.Errorf("workflow: adapter %s@%s is absent from the durable registry", adapter.Type, adapter.Version)
		}
	}
	return nil
}

func registryKey(nodeType, version string) string { return nodeType + "\x00" + version }

func cloneNodeTypeVersion(entry NodeTypeVersion) NodeTypeVersion {
	entry.Capabilities.RetrySafeErrors = append([]string(nil), entry.Capabilities.RetrySafeErrors...)
	entry.Inputs = cloneMap(entry.Inputs)
	entry.Outputs = cloneMap(entry.Outputs)
	entry.ConfigSchema = cloneRaw(entry.ConfigSchema)
	if entry.SensitiveFields != nil {
		fields := make([]string, len(entry.SensitiveFields))
		copy(fields, entry.SensitiveFields)
		entry.SensitiveFields = fields
	}
	if entry.Migrations != nil {
		migrations := make([]NodeMigrationMetadata, len(entry.Migrations))
		copy(migrations, entry.Migrations)
		entry.Migrations = migrations
	}
	return entry
}

func cloneMap[K comparable, V any](source map[K]V) map[K]V {
	if source == nil {
		return nil
	}
	result := make(map[K]V, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
