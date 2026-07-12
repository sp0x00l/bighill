package app

import (
	"context"

	"training_service/pkg/domain/model"

	"github.com/google/uuid"
)

type TrainingExecutor interface {
	RunTrainingJob(ctx context.Context, spec model.TrainingJobSpec) (*model.TrainedModelArtifact, error)
	EvaluateModel(ctx context.Context, spec model.EvaluationJobSpec) (*model.EvaluationReport, error)
	RunPromotionReport(ctx context.Context, spec model.PromotionReportJobSpec) (*model.PromotionReport, error)
}

type ManifestReader interface {
	Read(ctx context.Context, location string) ([]byte, error)
	Stat(ctx context.Context, location string) (model.ObjectInfo, error)
}

type TrainingWorkflowStarter interface {
	StartTrainingWorkflow(ctx context.Context, request model.TrainingRunRequest) error
}

type TrainingWorkflowStatusReader interface {
	ReadTrainingWorkflowStatus(ctx context.Context, trainingRunID string) (*model.TrainingRunStatusResult, error)
}

type TrainingEventPublisher interface {
	PublishModelTrainingCompleted(ctx context.Context, result *model.TrainingRunResult) error
	PublishModelTrainingFailed(ctx context.Context, result *model.TrainingRunResult) error
	PublishPromotionReportReady(ctx context.Context, report *model.PromotionReport) error
}

type DatasetResolver interface {
	ResolveMaterializedDataset(ctx context.Context, userID uuid.UUID, orgID uuid.UUID, datasetID uuid.UUID) (model.MaterializedDatasetRef, error)
}

type ModelResolver interface {
	ResolveTrainableModel(ctx context.Context, userID uuid.UUID, orgID uuid.UUID, modelID uuid.UUID) (model.SourceModelRef, error)
}

type PreferenceDatasetResolver interface {
	ResolvePreferenceDataset(ctx context.Context, userID uuid.UUID, orgID uuid.UUID, preferenceDatasetID uuid.UUID) (model.PreferenceDatasetRef, error)
}
