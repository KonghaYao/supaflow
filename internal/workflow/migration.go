package workflow

import (
	"encoding/json"
	"fmt"
	"sort"
)

// ConfigMigration converts one immutable node config without changing its identity.
type ConfigMigration func(json.RawMessage) (json.RawMessage, error)

// NodeMigration is one explicit directed node-version upgrade step.
type NodeMigration struct {
	Type        string
	FromVersion string
	ToVersion   string
	Migrate     ConfigMigration
}

// RegisterMigration adds an immutable upgrade edge between registered versions.
func (r *Registry) RegisterMigration(migration NodeMigration) error {
	if r == nil {
		return fmt.Errorf("workflow: nil registry")
	}
	if migration.Type == "" || migration.FromVersion == migration.ToVersion || migration.Migrate == nil {
		return fmt.Errorf("workflow: invalid node migration")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.entries[registryKey(migration.Type, migration.FromVersion)]; !ok {
		return fmt.Errorf("workflow: migration source %s@%s is not registered", migration.Type, migration.FromVersion)
	}
	if _, ok := r.entries[registryKey(migration.Type, migration.ToVersion)]; !ok {
		return fmt.Errorf("workflow: migration target %s@%s is not registered", migration.Type, migration.ToVersion)
	}
	key := migrationKey(migration.Type, migration.FromVersion, migration.ToVersion)
	if _, exists := r.migrations[key]; exists {
		return fmt.Errorf("workflow: duplicate node migration %s@%s->%s", migration.Type, migration.FromVersion, migration.ToVersion)
	}
	r.migrations[key] = migration
	entryKey := registryKey(migration.Type, migration.FromVersion)
	entry := r.entries[entryKey]
	entry.Migrations = append(entry.Migrations, NodeMigrationMetadata{
		FromVersion: migration.FromVersion, ToVersion: migration.ToVersion,
		Binding: migration.Type + ":" + migration.FromVersion + "->" + migration.ToVersion,
	})
	sort.Slice(entry.Migrations, func(i, j int) bool {
		if entry.Migrations[i].FromVersion != entry.Migrations[j].FromVersion {
			return entry.Migrations[i].FromVersion < entry.Migrations[j].FromVersion
		}
		return entry.Migrations[i].ToVersion < entry.Migrations[j].ToVersion
	})
	r.entries[entryKey] = entry
	return nil
}

// MigrateNode applies a deterministic registered path and validates the target config.
func (r *Registry) MigrateNode(node NodeDefinition, targetVersion string) (NodeDefinition, error) {
	if r == nil || node.Type == "" || node.Version == "" || targetVersion == "" {
		return NodeDefinition{}, fmt.Errorf("workflow: node type and versions are required")
	}
	if node.Version == targetVersion {
		return cloneNodeDefinition(node), nil
	}
	path, target, err := r.migrationPath(node.Type, node.Version, targetVersion)
	if err != nil {
		return NodeDefinition{}, err
	}
	result := cloneNodeDefinition(node)
	for _, migration := range path {
		result.Config, err = migration.Migrate(cloneRaw(result.Config))
		if err != nil {
			return NodeDefinition{}, fmt.Errorf("workflow: migrate %s@%s->%s: %w", node.Type, migration.FromVersion, migration.ToVersion, err)
		}
		if !json.Valid(result.Config) {
			return NodeDefinition{}, fmt.Errorf("workflow: migration %s@%s->%s returned invalid JSON", node.Type, migration.FromVersion, migration.ToVersion)
		}
		result.Version = migration.ToVersion
	}
	if err := target.ValidateConfig(result.Config); err != nil {
		return NodeDefinition{}, fmt.Errorf("workflow: migrated config is invalid for %s@%s: %w", node.Type, targetVersion, err)
	}
	return result, nil
}

func (r *Registry) migrationPath(nodeType, from, target string) ([]NodeMigration, NodeTypeVersion, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	targetEntry, ok := r.entries[registryKey(nodeType, target)]
	if !ok {
		return nil, NodeTypeVersion{}, fmt.Errorf("workflow: migration target %s@%s is not registered", nodeType, target)
	}
	type route struct {
		version string
		path    []NodeMigration
	}
	queue := []route{{version: from}}
	visited := map[string]bool{from: true}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		outgoing := make([]NodeMigration, 0)
		for _, migration := range r.migrations {
			if migration.Type == nodeType && migration.FromVersion == current.version {
				outgoing = append(outgoing, migration)
			}
		}
		sort.Slice(outgoing, func(i, j int) bool { return outgoing[i].ToVersion < outgoing[j].ToVersion })
		for _, migration := range outgoing {
			path := append(append([]NodeMigration(nil), current.path...), migration)
			if migration.ToVersion == target {
				return path, cloneNodeTypeVersion(targetEntry), nil
			}
			if !visited[migration.ToVersion] {
				visited[migration.ToVersion] = true
				queue = append(queue, route{version: migration.ToVersion, path: path})
			}
		}
	}
	return nil, NodeTypeVersion{}, fmt.Errorf("workflow: no migration path for %s@%s->%s", nodeType, from, target)
}

func migrationKey(nodeType, from, target string) string {
	return nodeType + "\x00" + from + "\x00" + target
}

func cloneNodeDefinition(node NodeDefinition) NodeDefinition {
	node.Config = cloneRaw(node.Config)
	return node
}
