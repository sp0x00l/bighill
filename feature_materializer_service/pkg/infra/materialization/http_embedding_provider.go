package materialization

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"feature_materializer_service/pkg/domain"

	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

const (
	httpHeaderContentType    = "Content-Type"
	jsonContentType          = "application/json"
	embeddingProviderTEI     = "tei"
	embeddingProviderOllama  = "ollama"
	teiEmbeddingPath         = "/embed"
	ollamaEmbeddingPath      = "/api/embed"
	embeddingRequestInputs   = "inputs"
	embeddingRequestModel    = "model"
	embeddingRequestInput    = "input"
	embeddingResponseVectors = "embeddings"
)

type HTTPEmbeddingProvider struct {
	client     *http.Client
	provider   string
	endpoint   string
	model      string
	dimensions int
}

func NewHTTPEmbeddingProvider(provider, endpoint, model string, dimensions int, timeout time.Duration) *HTTPEmbeddingProvider {
	log.Trace("NewHTTPEmbeddingProvider")

	if timeout <= 0 {
		log.Fatalf("NewHTTPEmbeddingProvider: timeout must be greater than zero")
	}
	return NewHTTPEmbeddingProviderWithClient(provider, endpoint, model, dimensions, newTracedHTTPClient(timeout))
}

func NewHTTPEmbeddingProviderWithClient(provider, endpoint, model string, dimensions int, client *http.Client) *HTTPEmbeddingProvider {
	log.Trace("NewHTTPEmbeddingProviderWithClient")

	if client == nil {
		log.Fatalf("NewHTTPEmbeddingProviderWithClient: client is required")
	}
	return &HTTPEmbeddingProvider{
		client:     client,
		provider:   strings.ToLower(strings.TrimSpace(provider)),
		endpoint:   strings.TrimRight(strings.TrimSpace(endpoint), "/"),
		model:      strings.TrimSpace(model),
		dimensions: dimensions,
	}
}

func newTracedHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout:   timeout,
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}
}

func (p *HTTPEmbeddingProvider) Dimensions() int {
	log.Trace("HTTPEmbeddingProvider Dimensions")

	return p.dimensions
}

func (p *HTTPEmbeddingProvider) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	log.Trace("HTTPEmbeddingProvider Embed")

	if len(texts) == 0 {
		return [][]float32{}, nil
	}

	switch p.provider {
	case embeddingProviderTEI:
		return p.embedTEI(ctx, texts)
	case embeddingProviderOllama:
		return p.embedOllama(ctx, texts)
	default:
		return nil, domain.ErrEmbeddingMaterialize.Extend("unsupported embedding provider")
	}
}

func (p *HTTPEmbeddingProvider) embedTEI(ctx context.Context, texts []string) ([][]float32, error) {
	log.Trace("HTTPEmbeddingProvider embedTEI")

	body, err := json.Marshal(map[string]any{embeddingRequestInputs: texts})
	if err != nil {
		return nil, err
	}
	return p.postEmbeddings(ctx, p.endpoint+teiEmbeddingPath, body)
}

func (p *HTTPEmbeddingProvider) embedOllama(ctx context.Context, texts []string) ([][]float32, error) {
	log.Trace("HTTPEmbeddingProvider embedOllama")

	body, err := json.Marshal(map[string]any{
		embeddingRequestModel: p.model,
		embeddingRequestInput: texts,
	})
	if err != nil {
		return nil, err
	}
	return p.postEmbeddings(ctx, p.endpoint+ollamaEmbeddingPath, body)
}

func (p *HTTPEmbeddingProvider) postEmbeddings(ctx context.Context, url string, body []byte) ([][]float32, error) {
	log.Trace("HTTPEmbeddingProvider postEmbeddings")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set(httpHeaderContentType, jsonContentType)

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: embedding request failed: %w", domain.ErrEmbeddingMaterialize, err)
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%w: embedding service returned status %d: %s", domain.ErrEmbeddingMaterialize, resp.StatusCode, string(responseBody))
	}
	vectors, err := decodeEmbeddingResponse(responseBody)
	if err != nil {
		return nil, fmt.Errorf("%w: decode embedding response: %w", domain.ErrEmbeddingMaterialize, err)
	}
	for _, vector := range vectors {
		if p.dimensions > 0 && len(vector) != p.dimensions {
			return nil, fmt.Errorf("%w: embedding dimension mismatch: expected %d got %d", domain.ErrEmbeddingMaterialize, p.dimensions, len(vector))
		}
	}
	return vectors, nil
}

func decodeEmbeddingResponse(body []byte) ([][]float32, error) {
	log.Trace("decodeEmbeddingResponse")

	var direct [][]float32
	if err := json.Unmarshal(body, &direct); err == nil && direct != nil {
		return direct, nil
	}

	var wrapped map[string][][]float32
	if err := json.Unmarshal(body, &wrapped); err != nil {
		return nil, err
	}
	vectors := wrapped[embeddingResponseVectors]
	if vectors == nil {
		return nil, fmt.Errorf("embeddings field is required")
	}
	return vectors, nil
}
