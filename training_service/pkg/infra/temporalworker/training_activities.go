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
	eventPublisher       TrainingEventPublisher
	executor             app.TrainingExecutor
	modelURIPrefix       string
	evaluationURIPrefix  string
	servingTarget        string
	servingModel         string
	servingLoadStatus    string
	artifactBucketRegion string
	axolotlCommand       string
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

func WithServingConfig(target string, modelName string, loadStatus string) TrainingActivitiesOption {
	log.Trace("WithServingConfig")

	return func(a *TrainingActivities) {
		a.servingTarget = strings.TrimSpace(target)
		a.servingModel = strings.TrimSpace(modelName)
		a.servingLoadStatus = strings.TrimSpace(loadStatus)
	}
}

func WithArtifactBucketRegion(region string) TrainingActivitiesOption {
	log.Trace("WithArtifactBucketRegion")

	return func(a *TrainingActivities) {
		a.artifactBucketRegion = strings.TrimSpace(region)
	}
}

func WithAxolotlCommand(command string) TrainingActivitiesOption {
	log.Trace("WithAxolotlCommand")

	return func(a *TrainingActivities) {
		a.axolotlCommand = strings.TrimSpace(command)
	}
}

func NewTrainingActivities(publisher TrainingEventPublisher, opts ...TrainingActivitiesOption) *TrainingActivities {
	log.Trace("NewTrainingActivities")

	activities := &TrainingActivities{
		eventPublisher:       publisher,
		modelURIPrefix:       "s3://local-dev-bucket/models",
		evaluationURIPrefix:  "s3://local-dev-bucket/evaluations",
		servingLoadStatus:    "NOT_LOADED",
		artifactBucketRegion: "local-dev",
		axolotlCommand:       "axolotl train",
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
	artifact, err := a.executor.RunTrainingJob(ctx, spec)
	if err != nil {
		return nil, err
	}
	return applyServingMetadata(artifact, spec), nil
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
	servingModel := a.servingModel
	if servingModel == "" {
		servingModel = modelName
	}
	recipe := axolotlRecipeYAML(model.TrainingJobSpec{
		TrainingRunID:   trainingRunID,
		DatasetURI:      datasetURI,
		ModelName:       modelName,
		ModelVersion:    modelVersion,
		BaseModel:       baseModel,
		TrainingProfile: request.TrainingProfile,
		ModelURI:        modelURI,
		AdapterURI:      modelURI,
		ServingTarget:   a.servingTarget,
		ServingModel:    servingModel,
	})
	hash := sha256.Sum256([]byte(recipe))
	recipeHash := hex.EncodeToString(hash[:])
	return model.TrainingJobSpec{
		TrainingRunID:        trainingRunID,
		DatasetURI:           datasetURI,
		ModelName:            modelName,
		ModelVersion:         modelVersion,
		BaseModel:            baseModel,
		TrainingProfile:      request.TrainingProfile,
		ModelURI:             modelURI,
		AdapterURI:           modelURI,
		ServingTarget:        a.servingTarget,
		ServingModel:         servingModel,
		ServingLoadStatus:    a.servingLoadStatus,
		ArtifactManifestURI:  modelURI + "/artifact.json",
		ArtifactBucketRegion: a.artifactBucketRegion,
		AxolotlCommand:       a.axolotlCommand,
		RecipeYAML:           recipe,
		RecipeHash:           recipeHash,
		SubmissionID:         deterministicSubmissionID("train", trainingRunID, recipeHash),
	}, nil
}

func applyServingMetadata(artifact *model.TrainedModelArtifact, spec model.TrainingJobSpec) *model.TrainedModelArtifact {
	log.Trace("applyServingMetadata")

	if artifact == nil {
		return nil
	}
	if strings.TrimSpace(artifact.AdapterURI) == "" {
		artifact.AdapterURI = spec.AdapterURI
	}
	if strings.TrimSpace(artifact.ServingTarget) == "" {
		artifact.ServingTarget = spec.ServingTarget
	}
	if strings.TrimSpace(artifact.ServingModel) == "" {
		artifact.ServingModel = spec.ServingModel
	}
	if strings.TrimSpace(artifact.ServingLoadStatus) == "" {
		artifact.ServingLoadStatus = spec.ServingLoadStatus
	}
	return artifact
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
		TrainingRunID:        trainingRunID,
		ModelURI:             artifact.ModelURI,
		EvaluationProfile:    strings.TrimSpace(request.EvaluationProfile),
		ReportURI:            reportURI,
		ReportManifestURI:    reportURI,
		ArtifactBucketRegion: a.artifactBucketRegion,
		SubmissionID:         deterministicSubmissionID("eval", trainingRunID, hex.EncodeToString(hash[:])),
	}, nil
}

func axolotlRecipeYAML(spec model.TrainingJobSpec) string {
	log.Trace("axolotlRecipeYAML")

	profile := spec.TrainingProfile
	quantization := ""
	switch strings.ToLower(strings.TrimSpace(profile.Quantization)) {
	case "4bit":
		quantization = "load_in_4bit: true\n"
	case "8bit":
		quantization = "load_in_8bit: true\n"
	}
	trainer := strings.ToLower(strings.TrimSpace(profile.Trainer))
	if trainer == "" {
		trainer = "sft"
	}
	preferenceDataset := ""
	if strings.TrimSpace(profile.PreferenceDatasetURI) != "" {
		preferenceDataset = fmt.Sprintf("preference_dataset: %s\n", strings.TrimSpace(profile.PreferenceDatasetURI))
	}
	return fmt.Sprintf(`base_model: %s
model_name: %s
model_version: %s
training_profile: %s
trainer: %s
datasets:
  - path: %s
%s
output_dir: %s
adapter: %s
%ssequence_len: %d
sample_packing: %t
learning_rate: %.8g
num_epochs: %.8g
micro_batch_size: %d
gradient_accumulation_steps: %d
lora_r: %d
lora_alpha: %d
`, spec.BaseModel, spec.ModelName, spec.ModelVersion, profile.Name, trainer, spec.DatasetURI, preferenceDataset, spec.ModelURI, profile.Adapter, quantization, profile.SequenceLength, profile.SamplePacking, profile.LearningRate, profile.Epochs, profile.MicroBatchSize, profile.GradientAccumulationSteps, profile.LoRAR, profile.LoRAAlpha)
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
