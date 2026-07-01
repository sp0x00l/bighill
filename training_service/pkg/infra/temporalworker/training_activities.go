package temporalworker

import (
	"context"
	"strings"

	"training_service/pkg/domain"
	"training_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
)

type TrainingActivities struct{}

func NewTrainingActivities() *TrainingActivities {
	log.Trace("NewTrainingActivities")

	return &TrainingActivities{}
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
		modelName = "local-dev-model"
	}
	modelVersion := strings.TrimSpace(request.ModelVersion)
	if modelVersion == "" {
		modelVersion = "local-dev"
	}
	return &model.TrainedModelArtifact{
		TrainingRunID: request.TrainingRunID,
		ModelURI:      "s3://local-dev-bucket/models/" + request.TrainingRunID,
		ModelName:     modelName,
		ModelVersion:  modelVersion,
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
