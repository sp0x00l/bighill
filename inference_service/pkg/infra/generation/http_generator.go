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

const DefaultHTTPGenerationTimeout = 60 * time.Second

type HTTPGenerator struct {
	provider string
	endpoint string
	model    string
	client   *http.Client
}

func NewHTTPGenerator(provider, endpoint, modelName string, timeout time.Duration) (*HTTPGenerator, error) {
	log.Trace("NewHTTPGenerator")

	return newHTTPGenerator(provider, endpoint, modelName, timeout, nil)
}

func NewHTTPGeneratorWithClient(provider, endpoint, modelName string, timeout time.Duration, client *http.Client) (*HTTPGenerator, error) {
	log.Trace("NewHTTPGeneratorWithClient")

	return newHTTPGenerator(provider, endpoint, modelName, timeout, client)
}

func newHTTPGenerator(provider, endpoint, modelName string, timeout time.Duration, client *http.Client) (*HTTPGenerator, error) {
	log.Trace("newHTTPGenerator")

	provider = strings.ToLower(strings.TrimSpace(provider))
	endpoint = strings.TrimRight(strings.TrimSpace(endpoint), "/")
	modelName = strings.TrimSpace(modelName)
	if provider == "" {
		return nil, fmt.Errorf("generation provider is required")
	}
	if endpoint == "" {
		return nil, fmt.Errorf("generation endpoint is required")
	}
	if modelName == "" {
		return nil, fmt.Errorf("generation model is required")
	}
	if timeout <= 0 {
		timeout = DefaultHTTPGenerationTimeout
	}
	if client == nil {
		client = &http.Client{
			Timeout:   timeout,
			Transport: otelhttp.NewTransport(http.DefaultTransport),
		}
	}
	return &HTTPGenerator{
		provider: provider,
		endpoint: endpoint,
		model:    modelName,
		client:   client,
	}, nil
}

func (g *HTTPGenerator) Generate(ctx context.Context, request model.GenerationRequest) (string, error) {
	log.Trace("HTTPGenerator Generate")

	switch g.provider {
	case "ollama":
		return g.generateWithOllama(ctx, request)
	case "vllm":
		return g.generateWithVLLM(ctx, request)
	default:
		return "", fmt.Errorf("unsupported generation provider %q", g.provider)
	}
}

func (g *HTTPGenerator) Provider() string {
	log.Trace("HTTPGenerator Provider")

	return g.provider
}

func (g *HTTPGenerator) Model() string {
	log.Trace("HTTPGenerator Model")

	return g.model
}

func (g *HTTPGenerator) generateWithOllama(ctx context.Context, request model.GenerationRequest) (string, error) {
	log.Trace("HTTPGenerator generateWithOllama")

	prompt := strings.TrimSpace(request.Prompt)
	if prompt == "" {
		return "", fmt.Errorf("prompt is required")
	}

	body, err := json.Marshal(ollamaGenerateRequest{
		Model:  g.model,
		Prompt: prompt,
		Stream: false,
	})
	if err != nil {
		return "", fmt.Errorf("marshal ollama request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, g.endpoint+"/api/generate", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("build ollama request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := g.client.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("ollama generate request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
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

func (g *HTTPGenerator) generateWithVLLM(ctx context.Context, request model.GenerationRequest) (string, error) {
	log.Trace("HTTPGenerator generateWithVLLM")

	prompt := strings.TrimSpace(request.Prompt)
	if prompt == "" {
		return "", fmt.Errorf("prompt is required")
	}
	modelName := g.model
	endpoint := g.endpoint
	if request.Model != nil && strings.TrimSpace(request.Model.ServingModel) != "" {
		modelName = strings.TrimSpace(request.Model.ServingModel)
	}
	if request.Model != nil && strings.TrimSpace(request.Model.ServingTarget) != "" {
		endpoint = strings.TrimRight(strings.TrimSpace(request.Model.ServingTarget), "/")
	}
	body, err := json.Marshal(vllmChatCompletionRequest{
		Model: modelName,
		Messages: []vllmChatMessage{{
			Role:    "user",
			Content: prompt,
		}},
		Temperature: 0,
	})
	if err != nil {
		return "", fmt.Errorf("marshal vllm request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("build vllm request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := g.client.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("vllm generate request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("vllm generate failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	var parsed vllmChatCompletionResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return "", fmt.Errorf("decode vllm response: %w", err)
	}
	if len(parsed.Choices) == 0 {
		return "", fmt.Errorf("vllm returned no choices")
	}
	answer := strings.TrimSpace(parsed.Choices[0].Message.Content)
	if answer == "" {
		answer = strings.TrimSpace(parsed.Choices[0].Text)
	}
	if answer == "" {
		return "", fmt.Errorf("vllm returned an empty response")
	}
	return answer, nil
}

type ollamaGenerateRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
	Stream bool   `json:"stream"`
}

type ollamaGenerateResponse struct {
	Response string `json:"response"`
}

type vllmChatCompletionRequest struct {
	Model       string            `json:"model"`
	Messages    []vllmChatMessage `json:"messages"`
	Temperature float64           `json:"temperature"`
}

type vllmChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type vllmChatCompletionResponse struct {
	Choices []vllmChoice `json:"choices"`
}

type vllmChoice struct {
	Message vllmChatMessage `json:"message"`
	Text    string          `json:"text"`
}
