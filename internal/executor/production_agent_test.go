package executor

import (
	"context"
	"encoding/json"
	"testing"
)

type scriptedAgentModel struct {
	outputs []json.RawMessage
	calls   int
}

func (m *scriptedAgentModel) Generate(context.Context, AttemptScope, string, string, json.RawMessage) (json.RawMessage, error) {
	output := m.outputs[m.calls]
	m.calls++
	return output, nil
}

// 验证生产 Agent 只能调用启动时配置的真实工具，并在工具结果后结束模型循环。
func TestBoundedAgentUsesConfiguredTrustedTool(t *testing.T) {
	tools, err := NewTrustedToolRegistry([]TrustedToolConfig{{Name: "pick", Kind: "json.project"}})
	if err != nil {
		t.Fatal(err)
	}
	models := &scriptedAgentModel{outputs: []json.RawMessage{
		json.RawMessage(`{"content":"lookup","toolCalls":[{"id":"call-1","function":{"name":"pick","arguments":"{\"document\":{\"safe\":42},\"field\":\"safe\"}"}}]}`),
		json.RawMessage(`{"content":"42","toolCalls":[]}`),
	}}
	agents, err := NewBoundedAgentRegistry(models, tools, []AgentConfig{{Name: "analyst", Model: "chat", Tools: []string{"pick"}, System: "Only return verified values.", MaxSteps: 2}}, func(name string) bool { return name == "chat" })
	if err != nil {
		t.Fatal(err)
	}
	output, err := agents.Invoke(context.Background(), AttemptScope{TenantID: "tenant", AttemptID: "attempt"}, "analyst", json.RawMessage(`{"question":"value"}`))
	if err != nil || models.calls != 2 || string(output) != `{"content":"42","toolCalls":[]}` {
		t.Fatalf("output=%s calls=%d err=%v", output, models.calls, err)
	}
}

// 验证达到固定 maxSteps 后 Agent 必须失败，不能形成无边界工具调用循环。
func TestBoundedAgentRejectsUnboundedToolLoop(t *testing.T) {
	tools, _ := NewTrustedToolRegistry([]TrustedToolConfig{{Name: "merge", Kind: "json.merge"}})
	models := &scriptedAgentModel{outputs: []json.RawMessage{json.RawMessage(`{"toolCalls":[{"id":"1","function":{"name":"merge","arguments":"{\"values\":[{\"a\":1}]}"}}]}`)}}
	agents, err := NewBoundedAgentRegistry(models, tools, []AgentConfig{{Name: "one", Model: "chat", Tools: []string{"merge"}, System: "finite", MaxSteps: 1}}, func(string) bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	if _, err = agents.Invoke(context.Background(), AttemptScope{TenantID: "tenant", AttemptID: "attempt"}, "one", json.RawMessage(`{}`)); err == nil {
		t.Fatal("expected maxSteps failure")
	}
}
