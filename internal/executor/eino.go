package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	openai "github.com/cloudwego/eino-ext/components/model/openai"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/schema"
)

// EinoRegistry adapts official Eino model and tool contracts to the executor
// ports. Registration happens at trusted process assembly, never from a plan.
type EinoRegistry struct {
	mu          sync.RWMutex
	models      map[string]model.BaseChatModel
	providers   map[string]OpenAIModelConfig
	tools       map[string]tool.InvokableTool
	credentials ProviderCredentialResolver
}

// NewEinoRegistry creates an empty provider registry.
func NewEinoRegistry() *EinoRegistry {
	return &EinoRegistry{models: make(map[string]model.BaseChatModel), providers: make(map[string]OpenAIModelConfig), tools: make(map[string]tool.InvokableTool)}
}

// ProviderCredentialResolver resolves a provider key for one granted attempt.
// Implementations must return a fresh byte slice and avoid secret-bearing errors.
type ProviderCredentialResolver interface {
	ResolveProviderCredential(context.Context, AttemptScope, string) ([]byte, error)
}

// NewCredentialEinoRegistry creates a registry that can construct provider
// adapters from tenant-scoped, attempt-granted credential references.
func NewCredentialEinoRegistry(credentials ProviderCredentialResolver) (*EinoRegistry, error) {
	if credentials == nil {
		return nil, fmt.Errorf("executor: provider credential resolver is required")
	}
	registry := NewEinoRegistry()
	registry.credentials = credentials
	return registry, nil
}

// RegisterModel registers a trusted Eino model under a plan-safe name.
func (r *EinoRegistry) RegisterModel(name string, chatModel model.BaseChatModel) error {
	if name == "" || chatModel == nil {
		return fmt.Errorf("executor: model name and adapter are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.models[name]; exists {
		return fmt.Errorf("executor: model %q already registered", name)
	}
	if _, exists := r.providers[name]; exists {
		return fmt.Errorf("executor: model %q already registered", name)
	}
	r.models[name] = chatModel
	return nil
}

// HasModel reports whether a trusted assembly-time model is configured.
func (r *EinoRegistry) HasModel(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, modelExists := r.models[name]
	_, providerExists := r.providers[name]
	return modelExists || providerExists
}

// RegisterTool registers a trusted Eino invokable tool.
func (r *EinoRegistry) RegisterTool(name string, invokable tool.InvokableTool) error {
	if name == "" || invokable == nil {
		return fmt.Errorf("executor: tool name and adapter are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.tools[name]; exists {
		return fmt.Errorf("executor: tool %q already registered", name)
	}
	r.tools[name] = invokable
	return nil
}

// Generate invokes a registered Eino model without accepting credentials from input.
func (r *EinoRegistry) Generate(ctx context.Context, scope AttemptScope, name, system string, input json.RawMessage) (json.RawMessage, error) {
	r.mu.RLock()
	chatModel := r.models[name]
	provider, hasProvider := r.providers[name]
	r.mu.RUnlock()
	if chatModel == nil && hasProvider {
		var err error
		chatModel, err = r.providerModel(ctx, scope, provider)
		if err != nil {
			return nil, err
		}
	}
	if chatModel == nil {
		return nil, fmt.Errorf("model is not registered")
	}
	messages := make([]*schema.Message, 0, 2)
	if system != "" {
		messages = append(messages, &schema.Message{Role: schema.System, Content: system})
	}
	messages = append(messages, &schema.Message{Role: schema.User, Content: jsonText(input)})
	message, err := chatModel.Generate(ctx, messages)
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{"role": message.Role, "content": message.Content, "toolCalls": message.ToolCalls})
}

// GenerateStream invokes Eino's streaming contract, emits JSON deltas, and
// returns the canonical concatenated message used for the final node output.
func (r *EinoRegistry) GenerateStream(ctx context.Context, scope AttemptScope, name, system string, input json.RawMessage, emit func(json.RawMessage) error) (json.RawMessage, error) {
	chatModel, err := r.resolveModel(ctx, scope, name)
	if err != nil {
		return nil, err
	}
	messages := modelMessages(system, input)
	stream, err := chatModel.Stream(ctx, messages)
	if err != nil {
		return nil, err
	}
	defer stream.Close()
	chunks := make([]*schema.Message, 0, 16)
	for {
		chunk, receiveErr := stream.Recv()
		if receiveErr == io.EOF {
			break
		}
		if receiveErr != nil {
			return nil, receiveErr
		}
		encoded, encodeErr := encodeMessage(chunk)
		if encodeErr != nil {
			return nil, encodeErr
		}
		if emit != nil {
			if emitErr := emit(encoded); emitErr != nil {
				return nil, emitErr
			}
		}
		chunks = append(chunks, chunk)
	}
	if len(chunks) == 0 {
		return nil, fmt.Errorf("model stream returned no message")
	}
	message, err := schema.ConcatMessages(chunks)
	if err != nil {
		return nil, err
	}
	return encodeMessage(message)
}

// Invoke runs a registered Eino tool. It also satisfies AgentRegistry by using
// a registered model as a single-turn agent when no tool has that name.
func (r *EinoRegistry) Invoke(ctx context.Context, scope AttemptScope, name string, input json.RawMessage) (json.RawMessage, error) {
	r.mu.RLock()
	invokable := r.tools[name]
	_, modelExists := r.models[name]
	_, providerExists := r.providers[name]
	r.mu.RUnlock()
	if invokable != nil {
		result, err := invokable.InvokableRun(ctx, string(input))
		if err != nil {
			return nil, err
		}
		if json.Valid([]byte(result)) {
			return json.RawMessage(result), nil
		}
		return json.Marshal(result)
	}
	if modelExists || providerExists {
		return r.Generate(ctx, scope, name, "Act as a bounded workflow agent and return the final result.", input)
	}
	return nil, fmt.Errorf("adapter is not registered")
}

// InvokeStream streams a registered model acting as a bounded agent. Tools do
// not advertise streaming and therefore use the non-streaming Invoke contract.
func (r *EinoRegistry) InvokeStream(ctx context.Context, scope AttemptScope, name string, input json.RawMessage, emit func(json.RawMessage) error) (json.RawMessage, error) {
	r.mu.RLock()
	_, toolExists := r.tools[name]
	_, modelExists := r.models[name]
	_, providerExists := r.providers[name]
	r.mu.RUnlock()
	if toolExists || (!modelExists && !providerExists) {
		return nil, fmt.Errorf("streaming agent is not registered")
	}
	return r.GenerateStream(ctx, scope, name, "Act as a bounded workflow agent and return the final result.", input, emit)
}

func (r *EinoRegistry) resolveModel(ctx context.Context, scope AttemptScope, name string) (model.BaseChatModel, error) {
	r.mu.RLock()
	chatModel := r.models[name]
	provider, hasProvider := r.providers[name]
	r.mu.RUnlock()
	if chatModel == nil && hasProvider {
		var err error
		chatModel, err = r.providerModel(ctx, scope, provider)
		if err != nil {
			return nil, err
		}
	}
	if chatModel == nil {
		return nil, fmt.Errorf("model is not registered")
	}
	return chatModel, nil
}

func modelMessages(system string, input json.RawMessage) []*schema.Message {
	messages := make([]*schema.Message, 0, 2)
	if system != "" {
		messages = append(messages, &schema.Message{Role: schema.System, Content: system})
	}
	return append(messages, &schema.Message{Role: schema.User, Content: jsonText(input)})
}

func encodeMessage(message *schema.Message) (json.RawMessage, error) {
	return json.Marshal(map[string]any{"role": message.Role, "content": message.Content, "toolCalls": message.ToolCalls})
}

// OpenAIModelConfig contains trusted assembly-time EinoExt provider settings.
// APIKey must come from a credential resolver and is never persisted in a plan.
type OpenAIModelConfig struct {
	Name          string
	APIKey        string
	CredentialRef string
	BaseURL       string
	Model         string
	Timeout       time.Duration
}

// RegisterOpenAIProvider registers an official EinoExt OpenAI template whose
// API key is resolved per attempt, so rotation and grant revocation take effect.
func (r *EinoRegistry) RegisterOpenAIProvider(cfg OpenAIModelConfig) error {
	if r.credentials == nil || cfg.Name == "" || cfg.CredentialRef == "" || cfg.Model == "" || cfg.APIKey != "" || validateCredentialProviderURL(cfg.BaseURL) != nil {
		return fmt.Errorf("executor: OpenAI name, credential reference, and model are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.models[cfg.Name]; exists {
		return fmt.Errorf("executor: model %q already registered", cfg.Name)
	}
	if _, exists := r.providers[cfg.Name]; exists {
		return fmt.Errorf("executor: model %q already registered", cfg.Name)
	}
	r.providers[cfg.Name] = cfg
	return nil
}

func (r *EinoRegistry) providerModel(ctx context.Context, scope AttemptScope, cfg OpenAIModelConfig) (model.BaseChatModel, error) {
	secret, err := r.credentials.ResolveProviderCredential(ctx, scope, cfg.CredentialRef)
	if err != nil {
		return nil, fmt.Errorf("provider credential unavailable")
	}
	defer clear(secret)
	chatModel, err := openai.NewChatModel(ctx, &openai.ChatModelConfig{
		APIKey: string(secret), BaseURL: cfg.BaseURL, Model: cfg.Model,
		HTTPClient: credentialProviderHTTPClient(cfg.Timeout),
	})
	if err != nil {
		return nil, fmt.Errorf("create EinoExt OpenAI model")
	}
	return chatModel, nil
}

// RegisterOpenAI constructs the official EinoExt OpenAI model adapter.
func (r *EinoRegistry) RegisterOpenAI(ctx context.Context, cfg OpenAIModelConfig) error {
	if cfg.Name == "" || cfg.APIKey == "" || cfg.Model == "" || validateCredentialProviderURL(cfg.BaseURL) != nil {
		return fmt.Errorf("executor: OpenAI name, key, and model are required")
	}
	chatModel, err := openai.NewChatModel(ctx, &openai.ChatModelConfig{
		APIKey: cfg.APIKey, BaseURL: cfg.BaseURL, Model: cfg.Model,
		HTTPClient: credentialProviderHTTPClient(cfg.Timeout),
	})
	if err != nil {
		return fmt.Errorf("executor: create EinoExt OpenAI model: %w", err)
	}
	return r.RegisterModel(cfg.Name, chatModel)
}

func validateCredentialProviderURL(raw string) error {
	if raw == "" {
		return nil // Eino's built-in OpenAI endpoint is HTTPS.
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" || parsed.User != nil || parsed.Fragment != "" {
		return fmt.Errorf("credential provider URL must use HTTPS")
	}
	return nil
}

func credentialProviderHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout:   timeout,
		Transport: newBoundedResponseTransport(http.DefaultTransport, maximumProviderResponseBytes),
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return fmt.Errorf("credential provider redirects are disabled")
		},
	}
}

func jsonText(input json.RawMessage) string {
	var text string
	if json.Unmarshal(input, &text) == nil {
		return text
	}
	return string(input)
}

var _ ModelRegistry = (*EinoRegistry)(nil)
var _ StreamingModelRegistry = (*EinoRegistry)(nil)
var _ AgentRegistry = (*EinoRegistry)(nil)
var _ StreamingAgentRegistry = (*EinoRegistry)(nil)
var _ ToolRegistry = (*EinoRegistry)(nil)
