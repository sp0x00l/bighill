package temporalworker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"training_service/pkg/app"
	"training_service/pkg/domain"
	"training_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
)

type TrainingEventPublisher interface {
	PublishModelTrainingCompleted(ctx context.Context, result *model.TrainingRunResult) error
	PublishModelTrainingFailed(ctx context.Context, result *model.TrainingRunResult) error
}

type TrainingActivities struct {
	eventPublisher      TrainingEventPublisher
	executor            app.TrainingExecutor
	modelURIPrefix      string
	evaluationURIPrefix string
}

type TrainingActivitiesOption func(*TrainingActivities)

func WithExecutor(executor app.TrainingExecutor) TrainingActivitiesOption {
	log.Trace("WithExecutor")

	return func(a *TrainingActivities) {
		a.executor = executor
	}
}

func WithModelURIPrefix(prefix string) TrainingActivitiesOption {
	log.Trace("WithModelURIPrefix")

	return func(a *TrainingActivities) {
		a.modelURIPrefix = strings.TrimRight(strings.TrimSpace(prefix), "/")
	}
}

func WithEvaluationURIPrefix(prefix string) TrainingActivitiesOption {
	log.Trace("WithEvaluationURIPrefix")

	return func(a *TrainingActivities) {
		a.evaluationURIPrefix = strings.TrimRight(strings.TrimSpace(prefix), "/")
	}
}

func NewTrainingActivities(publisher TrainingEventPublisher, opts ...TrainingActivitiesOption) *TrainingActivities {
	log.Trace("NewTrainingActivities")

	activities := &TrainingActivities{
		eventPublisher:      publisher,
		modelURIPrefix:      "s3://local-dev-bucket/models",
		evaluationURIPrefix: "s3://local-dev-bucket/evaluations",
	}
	for _, opt := range opts {
		if opt != nil {
			opt(activities)
		}
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

func (a *TrainingActivities) RunTrainingJob(ctx context.Context, prepared model.PreparedTrainingDataset, request model.TrainingRunRequest) (*model.TrainedModelArtifact, error) {
	log.Trace("TrainingActivities RunTrainingJob")

	if a.executor == nil {
		return nil, domain.ErrTrainModel.Extend("training executor is required")
	}
	spec, err := a.trainingJobSpec(prepared, request)
	if err != nil {
		return nil, err
	}
	return a.executor.RunTrainingJob(ctx, spec)
}

func (a *TrainingActivities) EvaluateTrainedModel(ctx context.Context, artifact model.TrainedModelArtifact, request model.TrainingRunRequest) (*model.EvaluationReport, error) {
	log.Trace("TrainingActivities EvaluateTrainedModel")

	if a.executor == nil {
		return nil, domain.ErrEvaluateModel.Extend("training executor is required")
	}
	spec, err := a.evaluationJobSpec(artifact, request)
	if err != nil {
		return nil, err
	}
	return a.executor.EvaluateModel(ctx, spec)
}

func (a *TrainingActivities) trainingJobSpec(prepared model.PreparedTrainingDataset, request model.TrainingRunRequest) (model.TrainingJobSpec, error) {
	log.Trace("TrainingActivities trainingJobSpec")

	trainingRunID := strings.TrimSpace(request.TrainingRunID)
	datasetURI := strings.TrimSpace(prepared.DatasetURI)
	modelName := strings.TrimSpace(request.ModelName)
	modelVersion := strings.TrimSpace(request.ModelVersion)
	baseModel := strings.TrimSpace(request.BaseModel)
	if datasetURI == "" {
		return model.TrainingJobSpec{}, domain.ErrPrepareDataset.Extend("prepared dataset uri is required")
	}
	if modelName == "" {
		return model.TrainingJobSpec{}, domain.ErrTrainModel.Extend("model name is required")
	}
	if modelVersion == "" {
		return model.TrainingJobSpec{}, domain.ErrTrainModel.Extend("model version is required")
	}
	if baseModel == "" {
		return model.TrainingJobSpec{}, domain.ErrTrainModel.Extend("base model is required")
	}
	modelURI := a.modelURIPrefix + "/" + trainingRunID
	recipe := axolotlRecipeYAML(model.TrainingJobSpec{
		TrainingRunID: trainingRunID,
		DatasetURI:    datasetURI,
		ModelName:     modelName,
		ModelVersion:  modelVersion,
		BaseModel:     baseModel,
		ModelURI:      modelURI,
	})
	hash := sha256.Sum256([]byte(recipe))
	recipeHash := hex.EncodeToString(hash[:])
	return model.TrainingJobSpec{
		TrainingRunID:       trainingRunID,
		DatasetURI:          datasetURI,
		ModelName:           modelName,
		ModelVersion:        modelVersion,
		BaseModel:           baseModel,
		ModelURI:            modelURI,
		ArtifactManifestURI: modelURI + "/artifact.json",
		RecipeYAML:          recipe,
		RecipeHash:          recipeHash,
		SubmissionID:        deterministicSubmissionID("train", trainingRunID, recipeHash),
	}, nil
}

func (a *TrainingActivities) evaluationJobSpec(artifact model.TrainedModelArtifact, request model.TrainingRunRequest) (model.EvaluationJobSpec, error) {
	log.Trace("TrainingActivities evaluationJobSpec")

	if strings.TrimSpace(artifact.ModelURI) == "" {
		return model.EvaluationJobSpec{}, domain.ErrTrainModel.Extend("trained model uri is required")
	}
	trainingRunID := strings.TrimSpace(request.TrainingRunID)
	reportURI := a.evaluationURIPrefix + "/" + trainingRunID + ".json"
	hash := sha256.Sum256([]byte(artifact.ModelURI + "|" + strings.TrimSpace(request.EvaluationProfile)))
	return model.EvaluationJobSpec{
		TrainingRunID:     trainingRunID,
		ModelURI:          artifact.ModelURI,
		EvaluationProfile: strings.TrimSpace(request.EvaluationProfile),
		ReportURI:         reportURI,
		ReportManifestURI: reportURI,
		SubmissionID:      deterministicSubmissionID("eval", trainingRunID, hex.EncodeToString(hash[:])),
	}, nil
}

func axolotlRecipeYAML(spec model.TrainingJobSpec) string {
	log.Trace("axolotlRecipeYAML")

	return fmt.Sprintf(`base_model: %s
model_name: %s
model_version: %s
datasets:
  - path: %s
output_dir: %s
adapter: lora
load_in_8bit: true
sequence_len: 2048
sample_packing: true
`, spec.BaseModel, spec.ModelName, spec.ModelVersion, spec.DatasetURI, spec.ModelURI)
}

func deterministicSubmissionID(prefix, trainingRunID, hash string) string {
	log.Trace("deterministicSubmissionID")

	shortHash := hash
	if len(shortHash) > 16 {
		shortHash = shortHash[:16]
	}
	return prefix + "-" + strings.ReplaceAll(trainingRunID, "_", "-") + "-" + shortHash
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
