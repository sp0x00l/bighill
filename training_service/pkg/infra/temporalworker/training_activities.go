package temporalworker

import (
	"context"
	"strings"

	"training_service/pkg/domain"
	"training_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
)

type TrainingEventPublisher interface {
	PublishModelTrainingCompleted(ctx context.Context, result *model.TrainingRunResult) error
	PublishModelTrainingFailed(ctx context.Context, result *model.TrainingRunResult) error
}

type TrainingActivities struct {
	eventPublisher TrainingEventPublisher
}

func NewTrainingActivities(publisher ...TrainingEventPublisher) *TrainingActivities {
	log.Trace("NewTrainingActivities")

	activities := &TrainingActivities{}
	if len(publisher) > 0 {
		activities.eventPublisher = publisher[0]
	}
	return activities
}

func (a *TrainingActivities) PrepareTrainingDataset(_ context.Context, request model.TrainingRunRequest) (*model.PreparedTrainingDataset, error) {
	log.Trace("TrainingActivities PrepareTrainingDataset")

	if strings.TrimSpace(request.TrainingRunID) == "" {
		return nil, domain.ErrValidationFailed.Extend("training run id is required")
	}
	if strings.TrimSpace(request.FeatureSnapshotID) == "" {
		return nil, domain.ErrValidationFailed.Extend("feature snapshot id is required")
	}
	return &model.PreparedTrainingDataset{
		TrainingRunID:     request.TrainingRunID,
		FeatureSnapshotID: request.FeatureSnapshotID,
		DatasetURI:        "s3://local-dev-bucket/features/" + request.FeatureSnapshotID + ".parquet",
	}, nil
}

func (a *TrainingActivities) RunTrainingJob(_ context.Context, prepared model.PreparedTrainingDataset, request model.TrainingRunRequest) (*model.TrainedModelArtifact, error) {
	log.Trace("TrainingActivities RunTrainingJob")

	if strings.TrimSpace(prepared.DatasetURI) == "" {
		return nil, domain.ErrPrepareDataset.Extend("prepared dataset uri is required")
	}
	modelName := strings.TrimSpace(request.ModelName)
	if modelName == "" {
		return nil, domain.ErrTrainModel.Extend("model name is required")
	}
	modelVersion := strings.TrimSpace(request.ModelVersion)
	if modelVersion == "" {
		return nil, domain.ErrTrainModel.Extend("model version is required")
	}
	baseModel := strings.TrimSpace(request.BaseModel)
	if baseModel == "" {
		return nil, domain.ErrTrainModel.Extend("base model is required")
	}
	return &model.TrainedModelArtifact{
		TrainingRunID:     request.TrainingRunID,
		ModelURI:          "s3://local-dev-bucket/models/" + request.TrainingRunID,
		ModelName:         modelName,
		ModelVersion:      modelVersion,
		BaseModel:         baseModel,
		ArtifactFormat:    "HF_PEFT_ADAPTER",
		ArtifactChecksum:  "local-dev-" + request.TrainingRunID,
		ArtifactSizeBytes: 1,
	}, nil
}

func (a *TrainingActivities) EvaluateTrainedModel(_ context.Context, artifact model.TrainedModelArtifact, request model.TrainingRunRequest) (*model.EvaluationReport, error) {
	log.Trace("TrainingActivities EvaluateTrainedModel")

	if strings.TrimSpace(artifact.ModelURI) == "" {
		return nil, domain.ErrTrainModel.Extend("trained model uri is required")
	}
	return &model.EvaluationReport{
		TrainingRunID: request.TrainingRunID,
		ReportURI:     "s3://local-dev-bucket/evaluations/" + request.TrainingRunID + ".json",
		Passed:        true,
	}, nil
}

func (a *TrainingActivities) PublishModelTrainingCompleted(ctx context.Context, result model.TrainingRunResult) error {
	log.Trace("TrainingActivities PublishModelTrainingCompleted")

	if a.eventPublisher == nil {
		return domain.ErrTrainModel.Extend("training event publisher is required")
	}
	if strings.TrimSpace(result.TrainingRunID) == "" {
		return domain.ErrValidationFailed.Extend("training run id is required")
	}
	return a.eventPublisher.PublishModelTrainingCompleted(ctx, &result)
}

func (a *TrainingActivities) PublishModelTrainingFailed(ctx context.Context, result model.TrainingRunResult) error {
	log.Trace("TrainingActivities PublishModelTrainingFailed")

	if a.eventPublisher == nil {
		return domain.ErrTrainModel.Extend("training event publisher is required")
	}
	if strings.TrimSpace(result.TrainingRunID) == "" {
		return domain.ErrValidationFailed.Extend("training run id is required")
	}
	if strings.TrimSpace(result.FailureReason) == "" {
		return domain.ErrValidationFailed.Extend("failure reason is required")
	}
	return a.eventPublisher.PublishModelTrainingFailed(ctx, &result)
}
