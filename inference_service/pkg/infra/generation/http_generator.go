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

func (g *HTTPGenerator) Generate(ctx context.Context, request model.GenerationRequest) (string, error) {
	log.Trace("HTTPGenerator Generate")

	switch g.protocol {
	case servingProtocolOllamaGenerate:
		return g.generateWithOllama(ctx, request)
	case servingProtocolOpenAIChatCompletions:
		return g.generateWithOpenAIChatCompletions(ctx, request)
	default:
		return "", fmt.Errorf("unsupported serving protocol %q", g.protocol)
	}
}

func (g *HTTPGenerator) generateWithOllama(ctx context.Context, request model.GenerationRequest) (string, error) {
	log.Trace("HTTPGenerator generateWithOllama")

	prompt := strings.TrimSpace(request.Prompt)
	if prompt == "" {
		return "", fmt.Errorf("prompt is required")
	}
	modelName, endpoint, err := g.servingTarget(request)
	if err != nil {
		return "", err
	}

	body, err := json.Marshal(ollamaGenerateRequest{
		Model:   modelName,
		Prompt:  prompt,
		Stream:  false,
		Options: ollamaGenerateOptions{NumPredict: g.maxOutputTokens},
	})
	if err != nil {
		return "", fmt.Errorf("marshal ollama request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+ollamaGeneratePath, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("build ollama request: %w", err)
	}
	httpReq.Header.Set(httpHeaderContentType, jsonContentType)

	resp, err := g.client.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("ollama generate request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, httpErrorBodyLimitBytes))
		return "", fmt.Errorf("ollama generate failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	var parsed ollamaGenerateResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return "", fmt.Errorf("decode ollama response: %w", err)
	}
	answer := strings.TrimSpace(parsed.Response)
	if answer == "" {
		return "", fmt.Errorf("ollama returned an empty response")
	}
	return answer, nil
}

func (g *HTTPGenerator) generateWithOpenAIChatCompletions(ctx context.Context, request model.GenerationRequest) (string, error) {
	log.Trace("HTTPGenerator generateWithOpenAIChatCompletions")

	prompt := strings.TrimSpace(request.Prompt)
	if prompt == "" {
		return "", fmt.Errorf("prompt is required")
	}
	modelName, endpoint, err := g.servingTarget(request)
	if err != nil {
		return "", err
	}
	body, err := json.Marshal(openAIChatCompletionRequest{
		Model: modelName,
		Messages: []openAIChatMessage{{
			Role:    openAIUserRole,
			Content: prompt,
		}},
		Temperature: 0,
		MaxTokens:   g.maxOutputTokens,
	})
	if err != nil {
		return "", fmt.Errorf("marshal chat completions request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+openAIChatCompletionsPath, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("build chat completions request: %w", err)
	}
	httpReq.Header.Set(httpHeaderContentType, jsonContentType)

	resp, err := g.client.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("chat completions request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, httpErrorBodyLimitBytes))
		return "", fmt.Errorf("chat completions failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	var parsed openAIChatCompletionResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return "", fmt.Errorf("decode chat completions response: %w", err)
	}
	if len(parsed.Choices) == 0 {
		return "", fmt.Errorf("chat completions returned no choices")
	}
	answer := strings.TrimSpace(parsed.Choices[0].Message.Content)
	if answer == "" {
		answer = strings.TrimSpace(parsed.Choices[0].Text)
	}
	if answer == "" {
		return "", fmt.Errorf("chat completions returned an empty response")
	}
	return answer, nil
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
	NumPredict int `json:"num_predict"`
}

type ollamaGenerateResponse struct {
	Response string `json:"response"`
}

type openAIChatCompletionRequest struct {
	Model       string              `json:"model"`
	Messages    []openAIChatMessage `json:"messages"`
	Temperature float64             `json:"temperature"`
	MaxTokens   int                 `json:"max_tokens"`
}

type openAIChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIChatCompletionResponse struct {
	Choices []openAIChoice `json:"choices"`
}

type openAIChoice struct {
	Message openAIChatMessage `json:"message"`
	Text    string            `json:"text"`
}
