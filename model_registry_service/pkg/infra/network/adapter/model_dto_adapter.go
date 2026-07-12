package adapter

import (
	"context"

	"model_registry_service/pkg/domain/model"

	serializers "lib/shared_lib/serializer"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type ModelDTO struct {
	ID                 string        `json:"id"`
	UserID             string        `json:"user_id,omitempty"`
	OrgID              string        `json:"org_id,omitempty"`
	TrainingRunID      string        `json:"training_run_id,omitempty"`
	DatasetID          string        `json:"dataset_id,omitempty"`
	ModelKind          string        `json:"model_kind"`
	Source             string        `json:"source"`
	SourceURI          string        `json:"source_uri,omitempty"`
	Name               string        `json:"name"`
	LineageName        string        `json:"lineage_name"`
	ModelVersion       int           `json:"model_version"`
	BaseModel          string        `json:"base_model"`
	ArtifactLocation   string        `json:"artifact_location"`
	ArtifactFormat     string        `json:"artifact_format"`
	ArtifactChecksum   string        `json:"artifact_checksum,omitempty"`
	ArtifactSizeBytes  int64         `json:"artifact_size_bytes"`
	AdapterURI         string        `json:"adapter_uri,omitempty"`
	AdapterRank        int           `json:"adapter_rank,omitempty"`
	ServingTarget      string        `json:"serving_target,omitempty"`
	ServingModel       string        `json:"serving_model,omitempty"`
	ServingProtocol    string        `json:"serving_protocol,omitempty"`
	ServingLoadStatus  string        `json:"serving_load_status"`
	PromotionReportURI string        `json:"promotion_report_uri,omitempty"`
	PromotionDeltas    string        `json:"promotion_deltas,omitempty"`
	PromotionDecision  string        `json:"promotion_decision,omitempty"`
	PromotionReason    string        `json:"promotion_reason,omitempty"`
	Status             string        `json:"status"`
	FailureReason      string        `json:"failure_reason,omitempty"`
	Links              ResourceLinks `json:"links"`
}

type ResourceLinks struct {
	Self Self `json:"self"`
}

type Self struct {
	Href string `json:"href"`
}

type ModelDTOAdapter interface {
	ToDTO(ctx context.Context, modelRecord *model.Model, baseURL string) ([]byte, error)
	ToDTOs(ctx context.Context, models []*model.Model, baseURL string) []any
}

type modelDTOAdapter struct {
	encoder *serializers.Encoder
}

func NewModelDTOAdapter(encoder *serializers.Encoder) *modelDTOAdapter {
	log.Trace("NewModelDTOAdapter")

	return &modelDTOAdapter{encoder: encoder}
}

func (a *modelDTOAdapter) ToDTO(ctx context.Context, modelRecord *model.Model, baseURL string) ([]byte, error) {
	log.Trace("modelDTOAdapter ToDTO")

	encoded, err := a.encoder.EncodeDataToString(a.toDTO(modelRecord, baseURL))
	if err != nil {
		return nil, err
	}
	return []byte(encoded), nil
}

func (a *modelDTOAdapter) ToDTOs(ctx context.Context, models []*model.Model, baseURL string) []any {
	log.Trace("modelDTOAdapter ToDTOs")

	resources := make([]any, 0, len(models))
	for _, modelRecord := range models {
		resources = append(resources, a.toDTO(modelRecord, baseURL))
	}
	return resources
}

func (a *modelDTOAdapter) toDTO(modelRecord *model.Model, baseURL string) *ModelDTO {
	log.Trace("modelDTOAdapter toDTO")

	return &ModelDTO{
		ID:                 modelRecord.ModelID.String(),
		UserID:             optionalUUIDString(modelRecord.UserID),
		OrgID:              optionalUUIDString(modelRecord.OrgID),
		TrainingRunID:      optionalUUIDString(modelRecord.TrainingRunID),
		DatasetID:          optionalUUIDString(modelRecord.DatasetID),
		ModelKind:          modelRecord.ModelKind.String(),
		Source:             modelRecord.Source.String(),
		SourceURI:          modelRecord.SourceURI,
		Name:               modelRecord.Name,
		LineageName:        modelRecord.LineageName,
		ModelVersion:       modelRecord.ModelVersion,
		BaseModel:          modelRecord.BaseModel,
		ArtifactLocation:   modelRecord.ArtifactLocation,
		ArtifactFormat:     modelRecord.ArtifactFormat,
		ArtifactChecksum:   modelRecord.ArtifactChecksum,
		ArtifactSizeBytes:  modelRecord.ArtifactSizeBytes,
		AdapterURI:         modelRecord.AdapterURI,
		AdapterRank:        modelRecord.AdapterRank,
		ServingTarget:      modelRecord.ServingTarget,
		ServingModel:       modelRecord.ServingModel,
		ServingProtocol:    modelRecord.ServingProtocol.String(),
		ServingLoadStatus:  modelRecord.ServingLoadStatus.String(),
		PromotionReportURI: modelRecord.PromotionReportURI,
		PromotionDeltas:    modelRecord.PromotionDeltas,
		PromotionDecision:  modelRecord.PromotionDecision,
		PromotionReason:    modelRecord.PromotionReason,
		Status:             modelRecord.Status.String(),
		FailureReason:      modelRecord.FailureReason,
		Links: ResourceLinks{Self: Self{
			Href: baseURL + "/" + modelRecord.ModelID.String(),
		}},
	}
}

func optionalUUIDString(id uuid.UUID) string {
	log.Trace("optionalUUIDString")

	if id == uuid.Nil {
		return ""
	}
	return id.String()
}
