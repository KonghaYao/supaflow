package workflow

import (
	"encoding/json"
	"testing"
)

// 验证节点配置只沿显式注册的版本路径升级，并在目标版本重新校验。
func TestRegistryMigratesNodeThroughExplicitPath(t *testing.T) {
	v1 := migrationFixture("1.0.0")
	v2 := migrationFixture("2.0.0")
	v3 := migrationFixture("3.0.0")
	registry, err := NewRegistry(v1, v2, v3)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range []NodeMigration{
		{Type: v1.Type, FromVersion: "1.0.0", ToVersion: "2.0.0", Migrate: addMigrationField("middle")},
		{Type: v1.Type, FromVersion: "2.0.0", ToVersion: "3.0.0", Migrate: addMigrationField("final")},
	} {
		if err = registry.RegisterMigration(step); err != nil {
			t.Fatal(err)
		}
	}
	migrated, err := registry.MigrateNode(NodeDefinition{ID: "node", Type: v1.Type, Version: "1.0.0", Config: json.RawMessage(`{}`)}, "3.0.0")
	if err != nil {
		t.Fatal(err)
	}
	if migrated.Version != "3.0.0" || string(migrated.Config) != `{"final":true,"middle":true}` {
		t.Fatalf("migrated = %#v", migrated)
	}
}

// 验证没有显式迁移路径时拒绝升级，避免运行时猜测配置兼容性。
func TestRegistryRejectsImplicitNodeMigration(t *testing.T) {
	registry, err := NewRegistry(migrationFixture("1.0.0"), migrationFixture("2.0.0"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = registry.MigrateNode(NodeDefinition{ID: "node", Type: "test.node", Version: "1.0.0", Config: json.RawMessage(`{}`)}, "2.0.0"); err == nil {
		t.Fatal("MigrateNode() error = nil")
	}
}

func migrationFixture(version string) NodeTypeVersion {
	return NodeTypeVersion{
		Type: "test.node", Version: version, DefaultTimeoutMS: 1000, MaximumOutputBytes: 1024,
		Executor:     ExecutorBinding{Kind: "test.node", MinimumRunnerVersion: "1.0.0"},
		ConfigSchema: json.RawMessage(`{"type":"object"}`), ValidateConfig: func(config json.RawMessage) error { return nil },
	}
}

func addMigrationField(name string) ConfigMigration {
	return func(config json.RawMessage) (json.RawMessage, error) {
		var value map[string]bool
		if err := json.Unmarshal(config, &value); err != nil {
			return nil, err
		}
		value[name] = true
		return json.Marshal(value)
	}
}
