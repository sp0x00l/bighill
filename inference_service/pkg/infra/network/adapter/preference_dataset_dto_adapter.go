package adapter

import (
	"context"
	"strings"

	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"
	serializers "lib/shared_lib/serializer"

	"github.com/go-playground/validator/v10"
	log "github.com/sirupsen/logrus"
)

type PreferenceDatasetDTOAdapter interface {
	FromDTO(ctx context.Context, body []byte) (model.PreferenceDatasetBuildRequest, error)
	ToDTO(ctx context.Context, dataset *model.PreferenceDataset) ([]byte, error)
	ToDTOs(ctx context.Context, datasets []*model.PreferenceDataset) ([]byte, error)
}

type preferenceDatasetDTOAdapter struct {
	validator *validator.Validate
	encoder   *serializers.Encoder
}

type PreferenceDatasetBuildDTO struct {
	OutputURI   string `json:"output_uri"    validate:"required,max=2048"`
	MinExamples int    `json:"min_examples" validate:"omitempty,min=1,max=100000"`
	Limit       int    `json:"limit"        validate:"omitempty,min=1,max=100000"`
	MaxPerUser  int    `json:"max_per_user" validate:"omitempty,min=0,max=100000"`
}

type PreferenceDatasetDTO struct {
	PreferenceDatasetID    string   `json:"preference_dataset_id"`
	EndpointID             string   `json:"endpoint_id,omitempty"`
	DatasetID              string   `json:"dataset_id,omitempty"`
	DatasetIDs             []string `json:"dataset_ids"`
	ModelID                string   `json:"model_id"`
	ParentModelKind        string   `json:"parent_model_kind"`
	ParentArtifactURI      string   `json:"parent_artifact_uri"`
	ParentArtifactChecksum string   `json:"parent_artifact_checksum"`
	ParentAdapterURI       string   `json:"parent_adapter_uri"`
	ParentBaseModel        string   `json:"parent_base_model"`
	ParentModelName        string   `json:"parent_model_name"`
	ParentLineageName      string   `json:"parent_lineage_name"`
	ParentModelVersion     int      `json:"parent_model_version"`
	OutputURI              string   `json:"output_uri"`
	EvaluationOutputURI    string   `json:"evaluation_output_uri"`
	Format                 string   `json:"format"`
	EligibilityPolicy      string   `json:"eligibility_policy"`
	ExampleCount           int      `json:"example_count"`
	TrainingExampleCount   int      `json:"training_example_count"`
	EvaluationExampleCount int      `json:"evaluation_example_count"`
	MinExamples            int      `json:"min_examples"`
	Limit                  int      `json:"limit"`
	IntegrityKey           string   `json:"integrity_key"`
}

func NewPreferenceDatasetDTOAdapter(encoder *serializers.Encoder) *preferenceDatasetDTOAdapter {
	log.Trace("NewPreferenceDatasetDTOAdapter")

	return &preferenceDatasetDTOAdapter{
		validator: validator.New(),
		encoder:   encoder,
	}
}

func (a *preferenceDatasetDTOAdapter) FromDTO(ctx context.Context, body []byte) (model.PreferenceDatasetBuildRequest, error) {
	log.Trace("PreferenceDatasetDTOAdapter FromDTO")

	var dto PreferenceDatasetBuildDTO
	if err := a.encoder.DecodeStringToData(string(body), &dto); err != nil {
		return model.PreferenceDatasetBuildRequest{}, domain.ErrValidationFailed.Extend(err.Error())
	}
	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("PreferenceDatasetBuildDTO validation failed")
		return model.PreferenceDatasetBuildRequest{}, domain.ErrValidationFailed.Extend(err.Error())
	}
	if strings.Contains(dto.OutputURI, "{request_id}") {
		return model.PreferenceDatasetBuildRequest{}, domain.ErrValidationFailed.Extend("preference dataset output_uri cannot use request_id")
	}
	return model.PreferenceDatasetBuildRequest{
		OutputURI:   dto.OutputURI,
		MinExamples: dto.MinExamples,
		Limit:       dto.Limit,
		MaxPerUser:  dto.MaxPerUser,
	}, nil
}

func (a *preferenceDatasetDTOAdapter) ToDTO(ctx context.Context, dataset *model.PreferenceDataset) ([]byte, error) {
	log.Trace("PreferenceDatasetDTOAdapter ToDTO")

	dtos := []PreferenceDatasetDTO{}
	if dataset != nil {
		dtos = append(dtos, preferenceDatasetDTO(dataset))
	}
	encoded, err := a.encoder.EncodeDataToString(dtos)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("PreferenceDatasetDTO encoding failed")
		return nil, err
	}
	return []byte(encoded), nil
}

func (a *preferenceDatasetDTOAdapter) ToDTOs(ctx context.Context, datasets []*model.PreferenceDataset) ([]byte, error) {
	log.Trace("PreferenceDatasetDTOAdapter ToDTOs")

	dtos := make([]PreferenceDatasetDTO, 0, len(datasets))
	for _, dataset := range datasets {
		if dataset == nil {
			continue
		}
		dtos = append(dtos, preferenceDatasetDTO(dataset))
	}
	encoded, err := a.encoder.EncodeDataToString(dtos)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("PreferenceDatasetDTOs encoding failed")
		return nil, err
	}
	return []byte(encoded), nil
}

func preferenceDatasetDTO(dataset *model.PreferenceDataset) PreferenceDatasetDTO {
	log.Trace("preferenceDatasetDTO")

	return PreferenceDatasetDTO{
		PreferenceDatasetID:    dataset.PreferenceDatasetID.String(),
		EndpointID:             optionalUUIDString(dataset.EndpointID),
		DatasetID:              optionalUUIDString(dataset.DatasetID),
		DatasetIDs:             uuidStrings(dataset.DatasetIDs),
		ModelID:                dataset.ModelID.String(),
		ParentModelKind:        dataset.ParentModelKind.String(),
		ParentArtifactURI:      dataset.ParentArtifactURI,
		ParentArtifactChecksum: dataset.ParentArtifactChecksum,
		ParentAdapterURI:       dataset.ParentAdapterURI,
		ParentBaseModel:        dataset.ParentBaseModel,
		ParentModelName:        dataset.ParentModelName,
		ParentLineageName:      dataset.ParentLineageName,
		ParentModelVersion:     dataset.ParentModelVersion,
		OutputURI:              dataset.OutputURI,
		EvaluationOutputURI:    dataset.EvaluationOutputURI,
		Format:                 dataset.Format,
		EligibilityPolicy:      dataset.EligibilityPolicy,
		ExampleCount:           dataset.ExampleCount(),
		TrainingExampleCount:   dataset.TrainingExampleCount(),
		EvaluationExampleCount: dataset.EvaluationExampleCount(),
		MinExamples:            dataset.MinExamples,
		Limit:                  dataset.Limit,
		IntegrityKey:           dataset.IntegrityKey,
	}
}
