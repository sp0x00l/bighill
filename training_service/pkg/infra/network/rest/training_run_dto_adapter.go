package rest

import (
	"context"
	"fmt"
	"strings"

	"training_service/pkg/domain"
	"training_service/pkg/domain/model"

	serializers "lib/shared_lib/serializer"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

const profileVersionSeparator = "@"

type StartTrainingRunDTO struct {
	DatasetID         string `json:"dataset_id"         validate:"required,uuid,ne=00000000-0000-0000-0000-000000000000"`
	SourceModelID     string `json:"source_model_id"    validate:"required,uuid,ne=00000000-0000-0000-0000-000000000000"`
	TrainingProfile   string `json:"training_profile"`
	EvaluationProfile string `json:"evaluation_profile"`
}

type StartDPOTrainingRunDTO struct {
	PreferenceDatasetID string `json:"preference_dataset_id" validate:"required,uuid,ne=00000000-0000-0000-0000-000000000000"`
	TrainingProfile     string `json:"training_profile"`
	EvaluationProfile   string `json:"evaluation_profile"`
}

type StartTrainingRunResponseDTO struct {
	TrainingRunID string `json:"training_run_id"`
	StatusURL     string `json:"status_url"`
}

type TrainingRunStatusDTO struct {
	TrainingRunID string `json:"training_run_id"`
	Status        string `json:"status"`
}

type TrainingRunDTOAdapter interface {
	FromDTO(ctx context.Context, body []byte) (model.StartTrainingRunCommand, error)
	FromDPOTrainingRunDTO(ctx context.Context, body []byte) (model.StartDPOTrainingRunCommand, error)
	ToStartTrainingRunDTO(ctx context.Context, result *model.TrainingRunStartResult) ([]byte, error)
	ToTrainingRunStatusDTO(ctx context.Context, result *model.TrainingRunStatusResult) ([]byte, error)
}

type trainingRunDTOAdapter struct {
	validator *validator.Validate
	encoder   *serializers.Encoder
}

func NewTrainingRunDTOAdapter(encoder *serializers.Encoder) *trainingRunDTOAdapter {
	log.Trace("NewTrainingRunDTOAdapter")

	return &trainingRunDTOAdapter{
		validator: validator.New(),
		encoder:   encoder,
	}
}

func (a *trainingRunDTOAdapter) FromDTO(ctx context.Context, body []byte) (model.StartTrainingRunCommand, error) {
	log.Trace("TrainingRunDTOAdapter FromDTO")

	var dto StartTrainingRunDTO
	if err := a.encoder.DecodeStringToData(string(body), &dto); err != nil {
		return model.StartTrainingRunCommand{}, domain.ErrValidationFailed.Extend(err.Error())
	}
	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("StartTrainingRunDTO validation failed")
		return model.StartTrainingRunCommand{}, domain.ErrValidationFailed.Extend(err.Error())
	}
	datasetID, err := uuid.Parse(dto.DatasetID)
	if err != nil {
		return model.StartTrainingRunCommand{}, domain.ErrValidationFailed.Extend("dataset id is required")
	}
	sourceModelID, err := uuid.Parse(dto.SourceModelID)
	if err != nil {
		return model.StartTrainingRunCommand{}, domain.ErrValidationFailed.Extend("source model id is required")
	}
	if err := validatePinnedProfileName("training profile", dto.TrainingProfile); err != nil {
		return model.StartTrainingRunCommand{}, err
	}
	if err := validatePinnedProfileName("evaluation profile", dto.EvaluationProfile); err != nil {
		return model.StartTrainingRunCommand{}, err
	}
	return model.StartTrainingRunCommand{
		DatasetID:         datasetID,
		SourceModelID:     sourceModelID,
		TrainingProfile:   strings.TrimSpace(dto.TrainingProfile),
		EvaluationProfile: strings.TrimSpace(dto.EvaluationProfile),
	}, nil
}

func (a *trainingRunDTOAdapter) FromDPOTrainingRunDTO(ctx context.Context, body []byte) (model.StartDPOTrainingRunCommand, error) {
	log.Trace("TrainingRunDTOAdapter FromDPOTrainingRunDTO")

	var dto StartDPOTrainingRunDTO
	if err := a.encoder.DecodeStringToData(string(body), &dto); err != nil {
		return model.StartDPOTrainingRunCommand{}, domain.ErrValidationFailed.Extend(err.Error())
	}
	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("StartDPOTrainingRunDTO validation failed")
		return model.StartDPOTrainingRunCommand{}, domain.ErrValidationFailed.Extend(err.Error())
	}
	preferenceDatasetID, err := uuid.Parse(dto.PreferenceDatasetID)
	if err != nil {
		return model.StartDPOTrainingRunCommand{}, domain.ErrValidationFailed.Extend("preference dataset id is required")
	}
	if err := validatePinnedProfileName("training profile", dto.TrainingProfile); err != nil {
		return model.StartDPOTrainingRunCommand{}, err
	}
	if err := validatePinnedProfileName("evaluation profile", dto.EvaluationProfile); err != nil {
		return model.StartDPOTrainingRunCommand{}, err
	}
	return model.StartDPOTrainingRunCommand{
		PreferenceDatasetID: preferenceDatasetID,
		TrainingProfile:     strings.TrimSpace(dto.TrainingProfile),
		EvaluationProfile:   strings.TrimSpace(dto.EvaluationProfile),
	}, nil
}

func (a *trainingRunDTOAdapter) ToStartTrainingRunDTO(ctx context.Context, result *model.TrainingRunStartResult) ([]byte, error) {
	log.Trace("TrainingRunDTOAdapter ToStartTrainingRunDTO")

	encoded, err := a.encoder.EncodeDataToString(StartTrainingRunResponseDTO{
		TrainingRunID: result.TrainingRunID,
		StatusURL:     result.StatusURL,
	})
	if err != nil {
		return nil, err
	}
	return []byte(encoded), nil
}

func validatePinnedProfileName(kind string, name string) error {
	log.Trace("validatePinnedProfileName")

	name = strings.TrimSpace(name)
	if name == "" {
		return nil
	}
	base, version, ok := strings.Cut(name, profileVersionSeparator)
	if !ok || strings.TrimSpace(base) == "" || strings.TrimSpace(version) == "" {
		return domain.ErrValidationFailed.Extend(fmt.Sprintf("%s must be version pinned", kind))
	}
	return nil
}

func (a *trainingRunDTOAdapter) ToTrainingRunStatusDTO(ctx context.Context, result *model.TrainingRunStatusResult) ([]byte, error) {
	log.Trace("TrainingRunDTOAdapter ToTrainingRunStatusDTO")

	encoded, err := a.encoder.EncodeDataToString(TrainingRunStatusDTO{
		TrainingRunID: result.TrainingRunID,
		Status:        result.Status,
	})
	if err != nil {
		return nil, err
	}
	return []byte(encoded), nil
}
