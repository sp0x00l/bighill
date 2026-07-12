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

type preferenceDatasetDTO struct {
	PreferenceDatasetID    string   `json:"preference_dataset_id"     validate:"required,uuid"`
	DatasetID              string   `json:"dataset_id"                validate:"omitempty,uuid"`
	DatasetIDs             []string `json:"dataset_ids"               validate:"omitempty,dive,uuid"`
	ModelID                string   `json:"model_id"                  validate:"required,uuid"`
	ParentModelKind        string   `json:"parent_model_kind"         validate:"required"`
	ParentArtifactURI      string   `json:"parent_artifact_uri"       validate:"required"`
	ParentArtifactChecksum string   `json:"parent_artifact_checksum"`
	ParentAdapterURI       string   `json:"parent_adapter_uri"`
	ParentBaseModel        string   `json:"parent_base_model"         validate:"required"`
	ParentModelName        string   `json:"parent_model_name"         validate:"required"`
	ParentLineageName      string   `json:"parent_lineage_name"`
	ParentModelVersion     int      `json:"parent_model_version"      validate:"gt=0"`
	OutputURI              string   `json:"output_uri"                validate:"required"`
	EvaluationOutputURI    string   `json:"evaluation_output_uri"`
	ExampleCount           int      `json:"example_count"             validate:"gt=0"`
	IntegrityKey           string   `json:"integrity_key"             validate:"required"`
}

type PreferenceDatasetResolver struct {
	baseURL string
	client  *http.Client
	adapter *preferenceDatasetDTOAdapter
}

type preferenceDatasetDTOAdapter struct {
	validator *validator.Validate
}

func NewPreferenceDatasetResolver(baseURL string, client *http.Client) *PreferenceDatasetResolver {
	log.Trace("NewPreferenceDatasetResolver")

	return &PreferenceDatasetResolver{
		baseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		client:  client,
		adapter: newPreferenceDatasetDTOAdapter(),
	}
}

func (r *PreferenceDatasetResolver) ResolvePreferenceDataset(ctx context.Context, userID uuid.UUID, orgID uuid.UUID, preferenceDatasetID uuid.UUID) (model.PreferenceDatasetRef, error) {
	log.Trace("PreferenceDatasetResolver ResolvePreferenceDataset")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.baseURL+"/v1/inference/preference-datasets/"+url.PathEscape(preferenceDatasetID.String()), nil)
	if err != nil {
		return model.PreferenceDatasetRef{}, fmt.Errorf("%w: build preference dataset resolver request: %w", domain.ErrDependencyFailed, err)
	}
	req.Header.Set(userIDHeader, userID.String())
	req.Header.Set(orgIDHeader, orgID.String())
	resp, err := r.client.Do(req)
	if err != nil {
		return model.PreferenceDatasetRef{}, fmt.Errorf("%w: resolve preference dataset: %w", domain.ErrDependencyFailed, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return model.PreferenceDatasetRef{}, domain.ErrValidationFailed.Extend("preference dataset not found")
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return model.PreferenceDatasetRef{}, fmt.Errorf("%w: preference dataset resolver status %d: %s", domain.ErrDependencyFailed, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var dtos []preferenceDatasetDTO
	if err := json.NewDecoder(resp.Body).Decode(&dtos); err != nil {
		return model.PreferenceDatasetRef{}, fmt.Errorf("%w: decode preference dataset resolver response: %w", domain.ErrDependencyFailed, err)
	}
	if len(dtos) != 1 {
		return model.PreferenceDatasetRef{}, domain.ErrValidationFailed.Extend("preference dataset resolver returned an invalid response")
	}
	return r.adapter.FromDTO(ctx, dtos[0], preferenceDatasetID)
}

func newPreferenceDatasetDTOAdapter() *preferenceDatasetDTOAdapter {
	log.Trace("newPreferenceDatasetDTOAdapter")

	return &preferenceDatasetDTOAdapter{validator: validator.New()}
}

func (a *preferenceDatasetDTOAdapter) FromDTO(ctx context.Context, dto preferenceDatasetDTO, preferenceDatasetID uuid.UUID) (model.PreferenceDatasetRef, error) {
	log.Trace("preferenceDatasetDTOAdapter FromDTO")

	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("preferenceDatasetDTO validation failed")
		return model.PreferenceDatasetRef{}, domain.ErrValidationFailed.Extend(err.Error())
	}
	if strings.TrimSpace(dto.PreferenceDatasetID) != preferenceDatasetID.String() {
		return model.PreferenceDatasetRef{}, domain.ErrValidationFailed.Extend("preference dataset resolver returned a different dataset")
	}
	kind := sharedDomain.ToModelKind(dto.ParentModelKind)
	if !sharedDomain.IsKnownModelKind(kind) {
		return model.PreferenceDatasetRef{}, domain.ErrValidationFailed.Extend("preference dataset parent model kind is invalid")
	}
	if kind == sharedDomain.ModelKindFineTuned && strings.TrimSpace(dto.ParentAdapterURI) == "" {
		return model.PreferenceDatasetRef{}, domain.ErrValidationFailed.Extend("fine-tuned preference dataset parent adapter uri is required")
	}
	datasetID := strings.TrimSpace(dto.DatasetID)
	if datasetID == "" && len(dto.DatasetIDs) > 0 {
		datasetID = strings.TrimSpace(dto.DatasetIDs[0])
	}
	return model.PreferenceDatasetRef{
		PreferenceDatasetID:    dto.PreferenceDatasetID,
		DatasetID:              datasetID,
		DatasetIDs:             append([]string(nil), dto.DatasetIDs...),
		ModelID:                dto.ModelID,
		ParentModelKind:        kind.String(),
		ParentArtifactURI:      dto.ParentArtifactURI,
		ParentArtifactChecksum: dto.ParentArtifactChecksum,
		ParentAdapterURI:       dto.ParentAdapterURI,
		ParentBaseModel:        dto.ParentBaseModel,
		ParentModelName:        dto.ParentModelName,
		ParentLineageName:      preferenceDatasetLineageName(dto),
		ParentModelVersion:     dto.ParentModelVersion,
		OutputURI:              dto.OutputURI,
		EvaluationOutputURI:    dto.EvaluationOutputURI,
		ExampleCount:           dto.ExampleCount,
		IntegrityKey:           dto.IntegrityKey,
	}, nil
}

func preferenceDatasetLineageName(dto preferenceDatasetDTO) string {
	log.Trace("preferenceDatasetLineageName")

	if lineageName := strings.TrimSpace(dto.ParentLineageName); lineageName != "" {
		return lineageName
	}
	return strings.TrimSpace(dto.ParentModelName)
}
