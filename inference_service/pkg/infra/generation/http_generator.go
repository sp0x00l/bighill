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
			Timeout: timeout,
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
		prompt = fallbackPrompt(request)
	}
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

func fallbackPrompt(request model.GenerationRequest) string {
	log.Trace("fallbackPrompt")

	query := strings.TrimSpace(request.Query)
	if query == "" {
		return ""
	}
	var b strings.Builder
	b.WriteString("Answer using only the retrieved context.\n\n")
	for i, retrieved := range request.Contexts {
		sourceText := strings.TrimSpace(retrieved.SourceText)
		if sourceText == "" {
			continue
		}
		b.WriteString(fmt.Sprintf("[context:%d]\n%s\n\n", i+1, sourceText))
	}
	b.WriteString("Question:\n")
	b.WriteString(query)
	b.WriteString("\n\nAnswer:")
	return b.String()
}

type ollamaGenerateRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
	Stream bool   `json:"stream"`
}

type ollamaGenerateResponse struct {
	Response string `json:"response"`
}
