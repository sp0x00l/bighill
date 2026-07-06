package adapter

import (
	"context"

	"training_service/pkg/domain"
	"training_service/pkg/domain/model"

	serializers "lib/shared_lib/serializer"

	"github.com/go-playground/validator/v10"
	log "github.com/sirupsen/logrus"
)

type StartTrainingRunDTO struct {
	DatasetID         string `json:"dataset_id"         validate:"required,uuid,ne=00000000-0000-0000-0000-000000000000"`
	SourceModelID     string `json:"source_model_id"    validate:"required,uuid,ne=00000000-0000-0000-0000-000000000000"`
	TrainingProfile   string `json:"training_profile"`
	EvaluationProfile string `json:"evaluation_profile"`
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
	FromStartTrainingRunDTO(ctx context.Context, body []byte) (model.StartTrainingRunCommand, error)
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

func (a *trainingRunDTOAdapter) FromStartTrainingRunDTO(ctx context.Context, body []byte) (model.StartTrainingRunCommand, error) {
	log.Trace("TrainingRunDTOAdapter FromStartTrainingRunDTO")

	var dto StartTrainingRunDTO
	if err := a.encoder.DecodeStringToData(string(body), &dto); err != nil {
		return model.StartTrainingRunCommand{}, domain.ErrValidationFailed.Extend(err.Error())
	}
	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("StartTrainingRunDTO validation failed")
		return model.StartTrainingRunCommand{}, domain.ErrValidationFailed.Extend(err.Error())
	}
	return model.StartTrainingRunCommand{
		DatasetID:         dto.DatasetID,
		SourceModelID:     dto.SourceModelID,
		TrainingProfile:   dto.TrainingProfile,
		EvaluationProfile: dto.EvaluationProfile,
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
