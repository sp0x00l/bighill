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

	sharedDomain "lib/shared_lib/domain"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

const modelStatusReady = "READY"

type modelDTO struct {
	ID                string `json:"id"            validate:"required,uuid"`
	UserID            string `json:"user_id"       validate:"required,uuid"`
	OrgID             string `json:"org_id"        validate:"required,uuid"`
	ModelKind         string `json:"model_kind"    validate:"required"`
	Name              string `json:"name"          validate:"required"`
	ModelVersion      int    `json:"model_version" validate:"gt=0"`
	BaseModel         string `json:"base_model"`
	ArtifactLocation  string `json:"artifact_location" validate:"required"`
	ArtifactChecksum  string `json:"artifact_checksum"`
	AdapterURI        string `json:"adapter_uri"`
	ServingLoadStatus string `json:"serving_load_status"`
	Status            string `json:"status" validate:"required"`
}

type ModelResolver struct {
	baseURL string
	client  *http.Client
	adapter *modelDTOAdapter
}

type modelDTOAdapter struct {
	validator *validator.Validate
}

func NewModelResolver(baseURL string, client *http.Client) *ModelResolver {
	log.Trace("NewModelResolver")

	return &ModelResolver{
		baseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		client:  client,
		adapter: newModelDTOAdapter(),
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
	return r.adapter.FromDTO(ctx, dto, modelID, orgID)
}

func newModelDTOAdapter() *modelDTOAdapter {
	log.Trace("newModelDTOAdapter")

	return &modelDTOAdapter{validator: validator.New()}
}

func (a *modelDTOAdapter) FromDTO(ctx context.Context, dto modelDTO, modelID uuid.UUID, orgID uuid.UUID) (model.SourceModelRef, error) {
	log.Trace("modelDTOAdapter FromDTO")

	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("modelDTO validation failed")
		return model.SourceModelRef{}, domain.ErrValidationFailed.Extend(err.Error())
	}

	if strings.TrimSpace(dto.ID) != modelID.String() {
		return model.SourceModelRef{}, domain.ErrValidationFailed.Extend("model resolver returned a different model")
	}
	if strings.TrimSpace(dto.OrgID) != orgID.String() {
		return model.SourceModelRef{}, domain.ErrValidationFailed.Extend("source model does not belong to active org")
	}
	if strings.TrimSpace(dto.Status) != modelStatusReady {
		return model.SourceModelRef{}, domain.ErrValidationFailed.Extend("source model is not ready")
	}
	kind := sharedDomain.ToModelKind(dto.ModelKind)
	if !sharedDomain.IsKnownModelKind(kind) {
		return model.SourceModelRef{}, domain.ErrValidationFailed.Extend("source model is not trainable")
	}
	if kind == sharedDomain.ModelKindFineTuned && strings.TrimSpace(dto.AdapterURI) == "" {
		return model.SourceModelRef{}, domain.ErrValidationFailed.Extend("fine tuned source model adapter uri is required")
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
