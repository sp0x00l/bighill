package app

import (
	"context"

	"training_service/pkg/domain/model"
)

type TrainingExecutor interface {
	RunTrainingJob(ctx context.Context, spec model.TrainingJobSpec) (*model.TrainedModelArtifact, error)
	EvaluateModel(ctx context.Context, spec model.EvaluationJobSpec) (*model.EvaluationReport, error)
}
