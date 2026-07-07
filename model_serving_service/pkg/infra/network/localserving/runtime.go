package localserving

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"model_serving_service/pkg/domain"
	"model_serving_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
)

const (
	modelKindBase  = "BASE"
	ollamaTagsPath = "/api/tags"
)

type Runtime struct {
	namespace      string
	port           int32
	ollamaEndpoint string
	client         *http.Client
}

func NewRuntime(namespace string, port int32, ollamaEndpoint string) *Runtime {
	log.Trace("localserving NewRuntime")

	return &Runtime{
		namespace:      namespace,
		port:           port,
		ollamaEndpoint: strings.TrimRight(strings.TrimSpace(ollamaEndpoint), "/"),
		client:         &http.Client{Timeout: 5 * time.Second},
	}
}

func (r *Runtime) EnsureServedModel(ctx context.Context, servedModel *model.ServedModel) (*model.ServingRuntimeState, error) {
	log.Trace("localserving Runtime EnsureServedModel")

	if strings.TrimSpace(servedModel.BaseModel) == "" {
		return nil, domain.ErrValidationFailed.Extend("base model is required")
	}
	servingModel := strings.TrimSpace(servedModel.ServingModel)
	if servingModel == "" {
		if strings.EqualFold(strings.TrimSpace(servedModel.ModelKind), modelKindBase) {
			servingModel = strings.TrimSpace(servedModel.BaseModel)
		} else {
			return nil, domain.ErrValidationFailed.Extend("serving model is required for non-base local served models")
		}
	}
	servingTarget := strings.TrimSpace(servedModel.ServingTarget)
	if servingTarget == "" {
		servingTarget = r.ollamaEndpoint
	}
	if servingTarget == "" {
		return nil, domain.ErrValidationFailed.Extend("local serving target is required")
	}
	if strings.EqualFold(strings.TrimSpace(servedModel.ModelKind), modelKindBase) {
		if err := r.ensureOllamaTag(ctx, servingTarget, servingModel); err != nil {
			return nil, err
		}
	}
	return &model.ServingRuntimeState{
		Ready:           true,
		ServingTarget:   servingTarget,
		ServingModel:    servingModel,
		ServingProtocol: model.ServingProtocolOpenAIChatCompletions,
		ReadyReplicas:   1,
	}, nil
}

func (r *Runtime) ensureOllamaTag(ctx context.Context, endpoint string, tag string) error {
	log.Trace("localserving Runtime ensureOllamaTag")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(endpoint, "/")+ollamaTagsPath, nil)
	if err != nil {
		return fmt.Errorf("%w: build ollama tags request: %w", domain.ErrValidationFailed, err)
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: local ollama endpoint is not available: %w", domain.ErrValidationFailed, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return domain.ErrValidationFailed.Extend(fmt.Sprintf("local ollama endpoint returned status %d", resp.StatusCode))
	}
	var payload struct {
		Models []struct {
			Name string `json:"name"`
		} `json:"models"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return fmt.Errorf("%w: decode ollama tags response: %w", domain.ErrValidationFailed, err)
	}
	for _, candidate := range payload.Models {
		if normalizedOllamaTag(candidate.Name) == normalizedOllamaTag(tag) {
			return nil
		}
	}
	return domain.ErrValidationFailed.Extend(fmt.Sprintf("local ollama model %q is not available; run `ollama pull %s`", tag, tag))
}

func normalizedOllamaTag(tag string) string {
	trimmed := strings.TrimSpace(tag)
	if trimmed == "" || strings.Contains(trimmed, ":") {
		return trimmed
	}
	return trimmed + ":latest"
}
