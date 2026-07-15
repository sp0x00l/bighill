package generation

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"inference_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

const (
	servingProtocolOllamaGenerate        = "OLLAMA_GENERATE"
	servingProtocolOpenAIChatCompletions = "OPENAI_CHAT_COMPLETIONS"
	httpHeaderContentType                = "Content-Type"
	jsonContentType                      = "application/json"
	ollamaGeneratePath                   = "/api/generate"
	openAIChatCompletionsPath            = "/v1/chat/completions"
	openAIUserRole                       = "user"
	httpErrorBodyLimitBytes              = 4096
)

type HTTPGenerator struct {
	protocol        string
	client          *http.Client
	maxOutputTokens int
}

func NewHTTPGenerator(protocol string, timeout time.Duration, maxOutputTokens int) *HTTPGenerator {
	log.Trace("NewHTTPGenerator")

	return &HTTPGenerator{
		protocol:        strings.ToUpper(strings.TrimSpace(protocol)),
		maxOutputTokens: maxOutputTokens,
		client: &http.Client{
			Timeout:   timeout,
			Transport: otelhttp.NewTransport(http.DefaultTransport),
		},
	}
}

func NewOpenAIChatCompletionsGenerator(timeout time.Duration, maxOutputTokens int) *HTTPGenerator {
	log.Trace("NewOpenAIChatCompletionsGenerator")

	return NewHTTPGenerator(servingProtocolOpenAIChatCompletions, timeout, maxOutputTokens)
}

func NewOllamaGenerateGenerator(timeout time.Duration, maxOutputTokens int) *HTTPGenerator {
	log.Trace("NewOllamaGenerateGenerator")

	return NewHTTPGenerator(servingProtocolOllamaGenerate, timeout, maxOutputTokens)
}

func (g *HTTPGenerator) Generate(ctx context.Context, request model.GenerationRequest) (model.GenerationResult, error) {
	log.Trace("HTTPGenerator Generate")

	switch g.protocol {
	case servingProtocolOllamaGenerate:
		return g.generateWithOllama(ctx, request)
	case servingProtocolOpenAIChatCompletions:
		return g.generateWithOpenAIChatCompletions(ctx, request)
	default:
		return model.GenerationResult{}, fmt.Errorf("unsupported serving protocol %q", g.protocol)
	}
}

func (g *HTTPGenerator) generateWithOllama(ctx context.Context, request model.GenerationRequest) (model.GenerationResult, error) {
	log.Trace("HTTPGenerator generateWithOllama")

	options := g.effectiveOptions(request)
	prompt := strings.TrimSpace(request.Prompt)
	if prompt == "" {
		return model.GenerationResult{}, fmt.Errorf("prompt is required")
	}
	modelName, endpoint, err := g.servingTarget(request)
	if err != nil {
		return model.GenerationResult{}, err
	}

	body, err := json.Marshal(ollamaGenerateRequest{
		Model:  modelName,
		Prompt: prompt,
		Stream: false,
		Options: ollamaGenerateOptions{
			NumPredict:  options.MaxOutputTokens,
			Temperature: options.Temperature,
			TopP:        options.TopP,
			Seed:        options.Seed,
		},
	})
	if err != nil {
		return model.GenerationResult{}, fmt.Errorf("marshal ollama request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+ollamaGeneratePath, bytes.NewReader(body))
	if err != nil {
		return model.GenerationResult{}, fmt.Errorf("build ollama request: %w", err)
	}
	httpReq.Header.Set(httpHeaderContentType, jsonContentType)

	resp, err := g.client.Do(httpReq)
	if err != nil {
		return model.GenerationResult{}, fmt.Errorf("ollama generate request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, httpErrorBodyLimitBytes))
		return model.GenerationResult{}, fmt.Errorf("ollama generate failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	var parsed ollamaGenerateResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return model.GenerationResult{}, fmt.Errorf("decode ollama response: %w", err)
	}
	answer := strings.TrimSpace(parsed.Response)
	if answer == "" {
		return model.GenerationResult{}, fmt.Errorf("ollama returned an empty response")
	}
	return model.GenerationResult{
		Content:      answer,
		FinishReason: model.GenerationFinishReasonStop,
		Options:      options,
	}, nil
}

func (g *HTTPGenerator) generateWithOpenAIChatCompletions(ctx context.Context, request model.GenerationRequest) (model.GenerationResult, error) {
	log.Trace("HTTPGenerator generateWithOpenAIChatCompletions")

	options := g.effectiveOptions(request)
	prompt := strings.TrimSpace(request.Prompt)
	messages := openAIChatMessages(request)
	if prompt == "" && len(messages) == 0 {
		return model.GenerationResult{}, fmt.Errorf("prompt is required")
	}
	modelName, endpoint, err := g.servingTarget(request)
	if err != nil {
		return model.GenerationResult{}, err
	}
	if len(messages) == 0 {
		messages = []openAIChatMessage{{
			Role:    openAIUserRole,
			Content: prompt,
		}}
	}
	body, err := json.Marshal(openAIChatCompletionRequest{
		Model:       modelName,
		Messages:    messages,
		Tools:       openAITools(request.Tools),
		ToolChoice:  openAIToolChoice(request.ToolChoice),
		Temperature: options.Temperature,
		TopP:        options.TopP,
		Seed:        options.Seed,
		MaxTokens:   options.MaxOutputTokens,
	})
	if err != nil {
		return model.GenerationResult{}, fmt.Errorf("marshal chat completions request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+openAIChatCompletionsPath, bytes.NewReader(body))
	if err != nil {
		return model.GenerationResult{}, fmt.Errorf("build chat completions request: %w", err)
	}
	httpReq.Header.Set(httpHeaderContentType, jsonContentType)

	resp, err := g.client.Do(httpReq)
	if err != nil {
		return model.GenerationResult{}, fmt.Errorf("chat completions request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, httpErrorBodyLimitBytes))
		return model.GenerationResult{}, fmt.Errorf("chat completions failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	var parsed openAIChatCompletionResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return model.GenerationResult{}, fmt.Errorf("decode chat completions response: %w", err)
	}
	if len(parsed.Choices) == 0 {
		return model.GenerationResult{}, fmt.Errorf("chat completions returned no choices")
	}
	choice := parsed.Choices[0]
	answer := strings.TrimSpace(choice.Message.Content)
	if answer == "" {
		answer = strings.TrimSpace(choice.Text)
	}
	toolCalls := openAIToolCalls(choice.Message.ToolCalls)
	if answer == "" && len(toolCalls) == 0 {
		return model.GenerationResult{}, fmt.Errorf("chat completions returned an empty response")
	}
	return model.GenerationResult{
		Content:      answer,
		ToolCalls:    toolCalls,
		FinishReason: openAIFinishReason(choice.FinishReason, toolCalls),
		Usage: model.TokenUsage{
			PromptTokens:     parsed.Usage.PromptTokens,
			CompletionTokens: parsed.Usage.CompletionTokens,
			TotalTokens:      parsed.Usage.TotalTokens,
		},
		Options: options,
	}, nil
}

func (g *HTTPGenerator) effectiveOptions(request model.GenerationRequest) model.GenerationOptions {
	log.Trace("HTTPGenerator effectiveOptions")

	options := request.Options
	if options.MaxOutputTokens <= 0 {
		options.MaxOutputTokens = g.maxOutputTokens
	}
	return options
}

func openAIChatMessages(request model.GenerationRequest) []openAIChatMessage {
	log.Trace("openAIChatMessages")

	if len(request.Messages) == 0 {
		return nil
	}
	messages := make([]openAIChatMessage, 0, len(request.Messages))
	for _, message := range request.Messages {
		messages = append(messages, openAIChatMessage{
			Role:       string(message.Role),
			Content:    message.Content,
			ToolCallID: message.ToolCallID,
			Name:       message.Name,
			ToolCalls:  openAIOutboundToolCalls(message.ToolCalls),
		})
	}
	return messages
}

func openAITools(specs []model.ToolSpec) []openAITool {
	log.Trace("openAITools")

	if len(specs) == 0 {
		return nil
	}
	tools := make([]openAITool, 0, len(specs))
	for _, spec := range specs {
		tools = append(tools, openAITool{
			Type: "function",
			Function: openAIFunction{
				Name:        spec.Name,
				Description: spec.Description,
				Parameters:  spec.Parameters,
			},
		})
	}
	return tools
}

func openAIToolChoice(choice string) any {
	log.Trace("openAIToolChoice")

	choice = strings.TrimSpace(choice)
	if choice == "" {
		return nil
	}
	return choice
}

func openAIToolCalls(calls []openAIToolCall) []model.ToolCall {
	log.Trace("openAIToolCalls")

	if len(calls) == 0 {
		return nil
	}
	out := make([]model.ToolCall, 0, len(calls))
	for _, call := range calls {
		if call.Type != "" && call.Type != "function" {
			continue
		}
		out = append(out, model.ToolCall{
			ID:        call.ID,
			Name:      call.Function.Name,
			Arguments: json.RawMessage(call.Function.Arguments),
		})
	}
	return out
}

func openAIOutboundToolCalls(calls []model.ToolCall) []openAIToolCall {
	log.Trace("openAIOutboundToolCalls")

	if len(calls) == 0 {
		return nil
	}
	out := make([]openAIToolCall, 0, len(calls))
	for _, call := range calls {
		out = append(out, openAIToolCall{
			ID:   call.ID,
			Type: "function",
			Function: openAIFunction{
				Name:      call.Name,
				Arguments: string(call.Arguments),
			},
		})
	}
	return out
}

func openAIFinishReason(value string, toolCalls []model.ToolCall) model.GenerationFinishReason {
	log.Trace("openAIFinishReason")

	value = strings.TrimSpace(value)
	if value != "" {
		return model.GenerationFinishReason(value)
	}
	if len(toolCalls) > 0 {
		return model.GenerationFinishReasonToolCalls
	}
	return model.GenerationFinishReasonStop
}

func (g *HTTPGenerator) servingTarget(request model.GenerationRequest) (string, string, error) {
	log.Trace("HTTPGenerator servingTarget")

	if request.Model == nil {
		return "", "", fmt.Errorf("served model record is required")
	}
	modelName := strings.TrimSpace(request.Model.ServingModel)
	endpoint := strings.TrimRight(strings.TrimSpace(request.Model.ServingTarget), "/")
	if modelName == "" {
		return "", "", fmt.Errorf("served model name is required")
	}
	if endpoint == "" {
		return "", "", fmt.Errorf("generation endpoint is required")
	}
	return modelName, endpoint, nil
}

type ollamaGenerateRequest struct {
	Model   string                `json:"model"`
	Prompt  string                `json:"prompt"`
	Stream  bool                  `json:"stream"`
	Options ollamaGenerateOptions `json:"options"`
}

type ollamaGenerateOptions struct {
	NumPredict  int     `json:"num_predict"`
	Temperature float64 `json:"temperature"`
	TopP        float64 `json:"top_p,omitempty"`
	Seed        int64   `json:"seed,omitempty"`
}

type ollamaGenerateResponse struct {
	Response string `json:"response"`
}

type openAIChatCompletionRequest struct {
	Model       string              `json:"model"`
	Messages    []openAIChatMessage `json:"messages"`
	Tools       []openAITool        `json:"tools,omitempty"`
	ToolChoice  any                 `json:"tool_choice,omitempty"`
	Temperature float64             `json:"temperature"`
	TopP        float64             `json:"top_p,omitempty"`
	Seed        int64               `json:"seed,omitempty"`
	MaxTokens   int                 `json:"max_tokens"`
}

type openAIChatMessage struct {
	Role       string           `json:"role"`
	Content    string           `json:"content"`
	ToolCallID string           `json:"tool_call_id,omitempty"`
	Name       string           `json:"name,omitempty"`
	ToolCalls  []openAIToolCall `json:"tool_calls,omitempty"`
}

type openAITool struct {
	Type     string         `json:"type"`
	Function openAIFunction `json:"function"`
}

type openAIFunction struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
	Arguments   string          `json:"arguments,omitempty"`
}

type openAIToolCall struct {
	ID       string         `json:"id"`
	Type     string         `json:"type"`
	Function openAIFunction `json:"function"`
}

type openAIChatCompletionResponse struct {
	Choices []openAIChoice `json:"choices"`
	Usage   openAIUsage    `json:"usage"`
}

type openAIChoice struct {
	Message      openAIChatMessage `json:"message"`
	Text         string            `json:"text"`
	FinishReason string            `json:"finish_reason"`
}

type openAIUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}
