package executor

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

// 验证真实 Eino graph 能在 checkpoint 后 interrupt 并从同一 checkpoint 恢复。
func TestEinoCheckpointInterruptProof(t *testing.T) {
	store := &proofCheckpointStore{values: make(map[string][]byte)}
	graph := compose.NewGraph[string, string]()
	if err := graph.AddLambdaNode("step", compose.InvokableLambda(func(_ context.Context, input string) (string, error) {
		return input + "-done", nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := graph.AddLambdaNode("finish", compose.InvokableLambda(func(_ context.Context, input string) (string, error) {
		return input, nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := graph.AddEdge(compose.START, "step"); err != nil {
		t.Fatal(err)
	}
	if err := graph.AddEdge("step", "finish"); err != nil {
		t.Fatal(err)
	}
	if err := graph.AddEdge("finish", compose.END); err != nil {
		t.Fatal(err)
	}
	runnable, err := graph.Compile(context.Background(), compose.WithCheckPointStore(store), compose.WithInterruptAfterNodes([]string{"step"}), compose.WithGraphName("proof"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = runnable.Invoke(context.Background(), "input", compose.WithCheckPointID("checkpoint"))
	interrupt, ok := compose.ExtractInterruptInfo(err)
	if !ok || interrupt == nil || len(interrupt.InterruptContexts) != 1 || store.sets == 0 {
		t.Fatalf("interrupt = %+v, sets = %d, err = %v", interrupt, store.sets, err)
	}
	resume := compose.ResumeWithData(context.Background(), interrupt.InterruptContexts[0].ID, nil)
	output, err := runnable.Invoke(resume, "input", compose.WithCheckPointID("checkpoint"))
	if err != nil || output != "input-done" {
		t.Fatalf("resumed output = %q, err = %v", output, err)
	}
}

// 验证 Supaflow 的模型/工具 registry 直接消费 Eino 官方接口且保留结构化输出。
func TestEinoRegistryContract(t *testing.T) {
	registry := NewEinoRegistry()
	chatModel := &proofChatModel{}
	if err := registry.RegisterModel("chat", chatModel); err != nil {
		t.Fatal(err)
	}
	output, err := registry.Generate(context.Background(), AttemptScope{OrganizationID: "organization", RunID: "run", AttemptID: "attempt", FencingToken: 1}, "chat", "system", json.RawMessage(`{"prompt":"hi"}`))
	if err != nil || len(chatModel.input) != 2 || chatModel.input[0].Role != schema.System || !json.Valid(output) {
		t.Fatalf("model output=%s input=%+v err=%v", output, chatModel.input, err)
	}
	if err = registry.RegisterTool("echo", proofTool{}); err != nil {
		t.Fatal(err)
	}
	output, err = registry.Invoke(context.Background(), AttemptScope{OrganizationID: "organization", RunID: "run", AttemptID: "attempt", FencingToken: 1}, "echo", json.RawMessage(`{"value":1}`))
	if err != nil || string(output) != `{"value":1}` {
		t.Fatalf("tool output=%s err=%v", output, err)
	}
}

// 验证生产 provider 模板按 organization/attempt grant 动态取密钥，且取消时不伪造模型成功。
func TestEinoProviderUsesAttemptScopedCredential(t *testing.T) {
	credentials := &proofProviderCredentials{}
	registry, err := NewCredentialEinoRegistry(credentials)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.RegisterOpenAIProvider(OpenAIModelConfig{
		Name: "chat", CredentialRef: "credential", BaseURL: "https://example.invalid", Model: "model", Timeout: time.Second,
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = registry.Generate(ctx, AttemptScope{OrganizationID: "organization", RunID: "run", AttemptID: "attempt", FencingToken: 1}, "chat", "", json.RawMessage(`"hello"`))
	if err == nil || credentials.organization != "organization" || credentials.attempt != "attempt" || credentials.reference != "credential" {
		t.Fatalf("credential scope = %+v, err = %v", credentials, err)
	}
}

// 验证携带 provider credential 的模型端点不得使用明文 HTTP，即使是 loopback 也不例外。
func TestEinoProviderRejectsPlaintextCredentialEndpoint(t *testing.T) {
	registry, err := NewCredentialEinoRegistry(&proofProviderCredentials{})
	if err != nil {
		t.Fatal(err)
	}
	for _, endpoint := range []string{"http://models.example.test/v1", "http://127.0.0.1:8080/v1"} {
		err = registry.RegisterOpenAIProvider(OpenAIModelConfig{
			Name: "chat", CredentialRef: "credential", BaseURL: endpoint, Model: "model", Timeout: time.Second,
		})
		if err == nil {
			t.Fatalf("plaintext provider endpoint %q was accepted", endpoint)
		}
	}
}

// 验证 provider 专用 HTTP client 禁止所有重定向，防止 credential 被带到降级或替换端点。
func TestEinoProviderHTTPClientRejectsRedirect(t *testing.T) {
	client := credentialProviderHTTPClient(time.Second)
	original, err := http.NewRequest(http.MethodPost, "https://models.example.test/v1/chat", nil)
	if err != nil {
		t.Fatal(err)
	}
	redirect, err := http.NewRequest(http.MethodGet, "https://models.example.test/redirected", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = client.CheckRedirect(redirect, []*http.Request{original}); err == nil {
		t.Fatal("credential-bearing provider redirect was accepted")
	}
}

// 验证模型端点即使使用 chunked 响应也不能绕过 worker 的响应字节上限。
func TestEinoProviderHTTPClientBoundsResponseBytes(t *testing.T) {
	client := credentialProviderHTTPClient(time.Second)
	client.Transport = newBoundedResponseTransport(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("12345")),
			Header:     make(http.Header),
		}, nil
	}), 4)
	request, err := http.NewRequest(http.MethodPost, "https://models.example.test/v1/chat", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if _, err = io.ReadAll(response.Body); !errors.Is(err, errProviderResponseTooLarge) {
		t.Fatalf("oversized provider response error = %v", err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

type proofCheckpointStore struct {
	mu     sync.Mutex
	values map[string][]byte
	sets   int
}

type proofChatModel struct{ input []*schema.Message }

func (m *proofChatModel) Generate(_ context.Context, input []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.input = input
	return &schema.Message{Role: schema.Assistant, Content: "ok"}, nil
}
func (*proofChatModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}

type proofTool struct{}

type proofProviderCredentials struct {
	organization workflowruntime.OrganizationID
	attempt      string
	reference    string
}

func (r *proofProviderCredentials) ResolveProviderCredential(_ context.Context, scope AttemptScope, reference string) ([]byte, error) {
	r.organization, r.attempt, r.reference = scope.OrganizationID, scope.AttemptID, reference
	return []byte("provider-key"), nil
}

func (proofTool) Info(context.Context) (*schema.ToolInfo, error) {
	return &schema.ToolInfo{Name: "echo"}, nil
}
func (proofTool) InvokableRun(_ context.Context, input string, _ ...tool.Option) (string, error) {
	return input, nil
}

func (s *proofCheckpointStore) Get(_ context.Context, id string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.values[id]
	return append([]byte(nil), value...), ok, nil
}
func (s *proofCheckpointStore) Set(_ context.Context, id string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[id] = append([]byte(nil), value...)
	s.sets++
	return nil
}
