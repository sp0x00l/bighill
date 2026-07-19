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
	"feature_materializer_service/pkg/domain/model"
	contractprompts "lib/data_contracts_lib/prompts"
	contractschemas "lib/data_contracts_lib/schemas"

	"github.com/santhosh-tekuri/jsonschema/v6"
	log "github.com/sirupsen/logrus"
)

const (
	graphExtractionDefaultMaxResponseBytes = 1024 * 1024
	graphExtractionDefaultMaxOutputTokens  = 512
	graphExtractionDefaultMaxRetries       = 2
	graphExtractionPromptV1Version         = "graph_extraction_prompt_v1"
	graphChatResponseFormatJSONSchema      = "json_schema"
	graphExtractionResponseSchemaName      = "graph_extraction_v1"
)

type graphExtractionHTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

type ModelServingGraphExtractorConfig struct {
	Endpoint         string
	AuthToken        string
	Timeout          time.Duration
	MaxResponseBytes int64
	MaxOutputTokens  int
	MaxRetries       int
}

type ModelServingGraphExtractor struct {
	client           graphExtractionHTTPClient
	endpoint         string
	authToken        string
	maxResponseBytes int64
	maxOutputTokens  int
	maxRetries       int
	schema           *jsonschema.Schema
	schemaDocument   any
}

type graphChatCompletionRequestDTO struct {
	Model          string                     `json:"model"`
	Messages       []graphChatMessageDTO      `json:"messages"`
	Temperature    float64                    `json:"temperature"`
	Stream         bool                       `json:"stream"`
	MaxTokens      int                        `json:"max_tokens"`
	ResponseFormat graphChatResponseFormatDTO `json:"response_format"`
}

type graphChatMessageDTO struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type graphChatResponseFormatDTO struct {
	Type       string                 `json:"type"`
	JSONSchema graphChatJSONSchemaDTO `json:"json_schema"`
}

type graphChatJSONSchemaDTO struct {
	Name   string `json:"name"`
	Strict bool   `json:"strict"`
	Schema any    `json:"schema"`
}

type graphChatCompletionResponseDTO struct {
	Choices []graphChatChoiceDTO `json:"choices"`
	Error   *graphChatErrorDTO   `json:"error,omitempty"`
}

type graphChatChoiceDTO struct {
	Message graphChatMessageDTO `json:"message"`
}

type graphChatErrorDTO struct {
	Message string `json:"message"`
}

func NewModelServingGraphExtractor(config ModelServingGraphExtractorConfig) (*ModelServingGraphExtractor, error) {
	log.Trace("NewModelServingGraphExtractor")

	if config.Timeout <= 0 {
		return nil, domain.ErrValidationFailed.Extend("graph extraction timeout must be greater than zero")
	}
	return NewModelServingGraphExtractorWithClient(config, newTracedHTTPClient(config.Timeout))
}

func NewModelServingGraphExtractorWithClient(config ModelServingGraphExtractorConfig, client graphExtractionHTTPClient) (*ModelServingGraphExtractor, error) {
	log.Trace("NewModelServingGraphExtractorWithClient")

	if client == nil {
		return nil, domain.ErrValidationFailed.Extend("graph extraction HTTP client is required")
	}
	endpoint := strings.TrimSpace(config.Endpoint)
	if endpoint == "" {
		return nil, domain.ErrValidationFailed.Extend("graph extraction endpoint is required")
	}
	maxResponseBytes := config.MaxResponseBytes
	if maxResponseBytes <= 0 {
		maxResponseBytes = graphExtractionDefaultMaxResponseBytes
	}
	maxOutputTokens := config.MaxOutputTokens
	if maxOutputTokens <= 0 {
		maxOutputTokens = graphExtractionDefaultMaxOutputTokens
	}
	maxRetries := config.MaxRetries
	if maxRetries < 0 {
		maxRetries = graphExtractionDefaultMaxRetries
	}
	if !json.Valid(contractschemas.GraphExtractionV1Schema()) {
		return nil, domain.ErrValidationFailed.Extend("graph extraction schema is invalid JSON")
	}
	schema, schemaDocument, err := compileGraphExtractionSchema()
	if err != nil {
		return nil, domain.ErrValidationFailed.Extend("graph extraction schema is invalid")
	}
	return &ModelServingGraphExtractor{
		client:           client,
		endpoint:         endpoint,
		authToken:        strings.TrimSpace(config.AuthToken),
		maxResponseBytes: maxResponseBytes,
		maxOutputTokens:  maxOutputTokens,
		maxRetries:       maxRetries,
		schema:           schema,
		schemaDocument:   schemaDocument,
	}, nil
}

func (e *ModelServingGraphExtractor) ExtractGraph(ctx context.Context, chunks []model.GraphChunk, strategy model.GraphExtractionStrategy) (*model.GraphExtraction, error) {
	log.Trace("ModelServingGraphExtractor ExtractGraph")

	strategy = model.ApplyGraphExtractionStrategyDefaults(strategy)
	if strings.TrimSpace(strategy.ExtractionModel) == "" {
		return nil, domain.ErrGraphMaterialize.Extend("graph extraction model is required")
	}
	if strategy.ExtractionSchemaVersion != model.DefaultGraphExtractionSchemaVersion {
		return nil, domain.ErrGraphMaterialize.Extend("unsupported graph extraction schema version")
	}
	prompt, err := graphExtractionPrompt(strategy.ExtractionPromptVersion)
	if err != nil {
		return nil, err
	}

	out := &model.GraphExtraction{}
	for _, chunk := range chunks {
		document, err := e.extractChunkWithRetries(ctx, chunk, strategy.ExtractionModel, prompt)
		if err != nil {
			return nil, fmt.Errorf("extract graph chunk %d: %w", chunk.ChunkIndex, err)
		}
		appendGraphExtractionDocument(out, document, chunk.ChunkIndex)
	}
	return out, nil
}

func (e *ModelServingGraphExtractor) extractChunkWithRetries(ctx context.Context, chunk model.GraphChunk, extractionModel string, prompt string) (graphExtractionDocument, error) {
	log.Trace("ModelServingGraphExtractor extractChunkWithRetries")

	var lastErr error
	for attempt := 0; attempt <= e.maxRetries; attempt++ {
		document, err := e.extractChunk(ctx, chunk, extractionModel, prompt)
		if err == nil {
			return document, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return graphExtractionDocument{}, ctxErr
		}
		lastErr = err
	}
	return graphExtractionDocument{}, lastErr
}

func (e *ModelServingGraphExtractor) extractChunk(ctx context.Context, chunk model.GraphChunk, extractionModel string, prompt string) (graphExtractionDocument, error) {
	log.Trace("ModelServingGraphExtractor extractChunk")

	requestBody, err := json.Marshal(graphChatCompletionRequestDTO{
		Model: strings.TrimSpace(extractionModel),
		Messages: []graphChatMessageDTO{
			{Role: "system", Content: prompt},
			{Role: "user", Content: graphExtractionChunkPrompt(chunk)},
		},
		Temperature: 0,
		Stream:      false,
		MaxTokens:   e.maxOutputTokens,
		ResponseFormat: graphChatResponseFormatDTO{
			Type: graphChatResponseFormatJSONSchema,
			JSONSchema: graphChatJSONSchemaDTO{
				Name:   graphExtractionResponseSchemaName,
				Strict: true,
				Schema: e.schemaDocument,
			},
		},
	})
	if err != nil {
		return graphExtractionDocument{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.endpoint, bytes.NewReader(requestBody))
	if err != nil {
		return graphExtractionDocument{}, err
	}
	req.Header.Set(httpHeaderContentType, jsonContentType)
	if e.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+e.authToken)
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return graphExtractionDocument{}, fmt.Errorf("%w: graph extraction request failed: %w", domain.ErrGraphMaterialize, err)
	}
	defer resp.Body.Close()

	body, err := readGraphExtractionResponseBody(resp.Body, e.maxResponseBytes)
	if err != nil {
		return graphExtractionDocument{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return graphExtractionDocument{}, fmt.Errorf("%w: graph extraction service returned status %d: %s", domain.ErrGraphMaterialize, resp.StatusCode, string(body))
	}
	return e.decodeGraphExtractionChatResponse(body, chunk.SourceText)
}

func graphExtractionPrompt(promptVersion string) (string, error) {
	log.Trace("graphExtractionPrompt")

	switch strings.TrimSpace(promptVersion) {
	case graphExtractionPromptV1Version:
		return string(contractprompts.GraphExtractionPromptV1()), nil
	default:
		return "", domain.ErrGraphMaterialize.Extend("unsupported graph extraction prompt version")
	}
}

func graphExtractionChunkPrompt(chunk model.GraphChunk) string {
	log.Trace("graphExtractionChunkPrompt")

	return fmt.Sprintf("chunk_index: %d\nsource_text:\n%s", chunk.ChunkIndex, chunk.SourceText)
}

func readGraphExtractionResponseBody(body io.Reader, maxBytes int64) ([]byte, error) {
	log.Trace("readGraphExtractionResponseBody")

	limited := io.LimitReader(body, maxBytes+1)
	raw, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > maxBytes {
		return nil, domain.ErrGraphMaterialize.Extend("graph extraction response exceeded max bytes")
	}
	return raw, nil
}

func (e *ModelServingGraphExtractor) decodeGraphExtractionChatResponse(body []byte, sourceText string) (graphExtractionDocument, error) {
	log.Trace("ModelServingGraphExtractor decodeGraphExtractionChatResponse")

	var response graphChatCompletionResponseDTO
	if err := json.Unmarshal(body, &response); err != nil {
		return graphExtractionDocument{}, fmt.Errorf("%w: decode graph extraction response: %w", domain.ErrGraphMaterialize, err)
	}
	if response.Error != nil && strings.TrimSpace(response.Error.Message) != "" {
		return graphExtractionDocument{}, domain.ErrGraphMaterialize.Extend(response.Error.Message)
	}
	if len(response.Choices) == 0 {
		return graphExtractionDocument{}, domain.ErrGraphMaterialize.Extend("graph extraction response choices are required")
	}
	content := strings.TrimSpace(response.Choices[0].Message.Content)
	if content == "" {
		return graphExtractionDocument{}, domain.ErrGraphMaterialize.Extend("graph extraction response content is required")
	}
	documentJSON := []byte(stripJSONCodeFence(content))
	var instance any
	if err := json.Unmarshal(documentJSON, &instance); err != nil {
		return graphExtractionDocument{}, fmt.Errorf("%w: decode graph extraction document: %w", domain.ErrGraphExtractionInvalid, err)
	}
	if err := e.schema.Validate(instance); err != nil {
		return graphExtractionDocument{}, domain.ErrGraphExtractionInvalid.Extend("graph extraction document does not match graph_extraction_v1")
	}
	var document graphExtractionDocument
	if err := json.Unmarshal(documentJSON, &document); err != nil {
		return graphExtractionDocument{}, fmt.Errorf("%w: decode graph extraction document: %w", domain.ErrGraphExtractionInvalid, err)
	}
	document = canonicalizeGraphExtractionDocument(document, sourceText)
	if err := validateGraphExtractionDocument(document); err != nil {
		return graphExtractionDocument{}, err
	}
	return document, nil
}

func compileGraphExtractionSchema() (*jsonschema.Schema, any, error) {
	log.Trace("compileGraphExtractionSchema")

	var schemaDocument any
	if err := json.Unmarshal(contractschemas.GraphExtractionV1Schema(), &schemaDocument); err != nil {
		return nil, nil, err
	}
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource("graph_extraction_v1.schema.json", schemaDocument); err != nil {
		return nil, nil, err
	}
	schema, err := compiler.Compile("graph_extraction_v1.schema.json")
	if err != nil {
		return nil, nil, err
	}
	return schema, schemaDocument, nil
}

func stripJSONCodeFence(content string) string {
	log.Trace("stripJSONCodeFence")

	trimmed := strings.TrimSpace(content)
	if !strings.HasPrefix(trimmed, "```") {
		return trimmed
	}
	lines := strings.Split(trimmed, "\n")
	if len(lines) < 3 {
		return trimmed
	}
	if !strings.HasPrefix(strings.TrimSpace(lines[0]), "```") || !strings.HasPrefix(strings.TrimSpace(lines[len(lines)-1]), "```") {
		return trimmed
	}
	return strings.TrimSpace(strings.Join(lines[1:len(lines)-1], "\n"))
}

func appendGraphExtractionDocument(out *model.GraphExtraction, document graphExtractionDocument, chunkIndex int) {
	log.Trace("appendGraphExtractionDocument")

	for _, entity := range document.Entities {
		out.Entities = append(out.Entities, model.GraphExtractionEntity{
			ID:          strings.TrimSpace(entity.ID),
			Name:        strings.TrimSpace(entity.Name),
			Type:        strings.TrimSpace(entity.Type),
			Description: strings.TrimSpace(entity.Description),
			ChunkIndex:  chunkIndex,
		})
	}
	for _, relation := range document.Relations {
		out.Relations = append(out.Relations, model.GraphExtractionRelation{
			Source:      strings.TrimSpace(relation.Source),
			Target:      strings.TrimSpace(relation.Target),
			Type:        strings.TrimSpace(relation.Type),
			Description: strings.TrimSpace(relation.Description),
			Weight:      relation.Weight,
		})
	}
}
