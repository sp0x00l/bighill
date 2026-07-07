package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"training_service/pkg/domain"
	"training_service/pkg/domain/model"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type ModelResolver struct {
	baseURL string
	client  *http.Client
}

func NewModelResolver(baseURL string, client *http.Client) *ModelResolver {
	log.Trace("NewModelResolver")

	return &ModelResolver{
		baseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		client:  client,
	}
}

func (r *ModelResolver) ResolveTrainableModel(ctx context.Context, userID uuid.UUID, orgID uuid.UUID, modelID uuid.UUID) (model.SourceModelRef, error) {
	log.Trace("ModelResolver ResolveTrainableModel")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.baseURL+"/v1/models/"+url.PathEscape(modelID.String()), nil)
	if err != nil {
		return model.SourceModelRef{}, fmt.Errorf("%w: build model resolver request: %w", domain.ErrDependencyFailed, err)
	}
	req.Header.Set(userIDHeader, userID.String())
	req.Header.Set(orgIDHeader, orgID.String())
	resp, err := r.client.Do(req)
	if err != nil {
		return model.SourceModelRef{}, fmt.Errorf("%w: resolve source model: %w", domain.ErrDependencyFailed, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return model.SourceModelRef{}, domain.ErrValidationFailed.Extend("source model not found")
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return model.SourceModelRef{}, fmt.Errorf("%w: model resolver status %d: %s", domain.ErrDependencyFailed, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var dto modelDTO
	if err := json.NewDecoder(resp.Body).Decode(&dto); err != nil {
		return model.SourceModelRef{}, fmt.Errorf("%w: decode model resolver response: %w", domain.ErrDependencyFailed, err)
	}
	return model.SourceModelRef{
		ModelID:           dto.ID,
		UserID:            dto.UserID,
		OrgID:             dto.OrgID,
		ModelKind:         dto.ModelKind,
		Name:              dto.Name,
		ModelVersion:      dto.ModelVersion,
		BaseModel:         dto.BaseModel,
		ArtifactLocation:  dto.ArtifactLocation,
		ArtifactChecksum:  dto.ArtifactChecksum,
		AdapterURI:        dto.AdapterURI,
		ServingLoadStatus: dto.ServingLoadStatus,
		Status:            dto.Status,
	}, nil
}

type modelDTO struct {
	ID                string `json:"id"`
	UserID            string `json:"user_id"`
	OrgID             string `json:"org_id"`
	ModelKind         string `json:"model_kind"`
	Name              string `json:"name"`
	ModelVersion      int    `json:"model_version"`
	BaseModel         string `json:"base_model"`
	ArtifactLocation  string `json:"artifact_location"`
	ArtifactChecksum  string `json:"artifact_checksum"`
	AdapterURI        string `json:"adapter_uri"`
	ServingLoadStatus string `json:"serving_load_status"`
	Status            string `json:"status"`
}
