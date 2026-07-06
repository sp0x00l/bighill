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
		artifactBucketRegion: "eu-west-1",
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
	if strings.EqualFold(strings.TrimSpace(request.TrainingProfile.Trainer), "dpo") {
		preferenceDatasetURI := strings.TrimSpace(request.PreferenceDatasetURI)
		if preferenceDatasetURI == "" {
			preferenceDatasetURI = strings.TrimSpace(request.TrainingProfile.PreferenceDatasetURI)
		}
		if preferenceDatasetURI == "" {
			return nil, domain.ErrValidationFailed.Extend("preference dataset uri is required")
		}
		return &model.PreparedTrainingDataset{
			TrainingRunID:     request.TrainingRunID,
			FeatureSnapshotID: strings.TrimSpace(request.FeatureSnapshotID),
			DatasetURI:        preferenceDatasetURI,
		}, nil
	}
	if strings.TrimSpace(request.FeatureSnapshotID) == "" {
		return nil, domain.ErrValidationFailed.Extend("feature snapshot id is required")
	}
	datasetURI := strings.TrimSpace(request.DatasetURI)
	if datasetURI == "" {
		datasetURI = "s3://local-dev-bucket/features/" + request.FeatureSnapshotID + ".parquet"
	}
	return &model.PreparedTrainingDataset{
		TrainingRunID:     request.TrainingRunID,
		FeatureSnapshotID: request.FeatureSnapshotID,
		DatasetURI:        datasetURI,
	}, nil
}

func (a *TrainingActivities) RunTrainingJob(ctx context.Context, prepared model.PreparedTrainingDataset, request model.TrainingRunRequest) (*model.TrainedModelArtifact, error) {
	log.Trace("TrainingActivities RunTrainingJob")

	if a.executor == nil {
		return nil, domain.ErrTrainModel.Extend("training executor is not configured")
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
		return nil, domain.ErrEvaluateModel.Extend("training executor is not configured")
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
	trainingProfile := request.TrainingProfile
	if strings.EqualFold(strings.TrimSpace(trainingProfile.Trainer), "dpo") && strings.TrimSpace(trainingProfile.PreferenceDatasetURI) == "" {
		trainingProfile.PreferenceDatasetURI = strings.TrimSpace(request.PreferenceDatasetURI)
	}
	if strings.EqualFold(strings.TrimSpace(trainingProfile.Trainer), "dpo") {
		if strings.TrimSpace(request.PreferenceDatasetID) == "" {
			return model.TrainingJobSpec{}, domain.ErrTrainModel.Extend("preference dataset id is required")
		}
		if strings.TrimSpace(request.ParentModelID) == "" {
			return model.TrainingJobSpec{}, domain.ErrTrainModel.Extend("parent model id is required")
		}
		if strings.TrimSpace(request.ParentModelVersion) == "" {
			return model.TrainingJobSpec{}, domain.ErrTrainModel.Extend("parent model version is required")
		}
		if strings.TrimSpace(request.ParentAdapterURI) == "" {
			return model.TrainingJobSpec{}, domain.ErrTrainModel.Extend("parent adapter uri is required")
		}
		if trainingProfile.DPOBeta <= 0 {
			return model.TrainingJobSpec{}, domain.ErrTrainModel.Extend("dpo beta must be greater than zero")
		}
	}
	modelURI := a.modelURIPrefix + "/" + trainingRunID
	servingModel := a.servingModel
	if servingModel == "" {
		servingModel = modelName
	}
	recipe := axolotlRecipeYAML(model.TrainingJobSpec{
		TrainingRunID:          trainingRunID,
		DatasetURI:             datasetURI,
		PreferenceDatasetID:    strings.TrimSpace(request.PreferenceDatasetID),
		ModelName:              modelName,
		ModelVersion:           modelVersion,
		BaseModel:              baseModel,
		SourceModelID:          strings.TrimSpace(request.SourceModelID),
		SourceArtifactURI:      strings.TrimSpace(request.SourceArtifactURI),
		SourceModelKind:        strings.TrimSpace(request.SourceModelKind),
		SourceArtifactChecksum: strings.TrimSpace(request.SourceArtifactChecksum),
		ParentModelID:          strings.TrimSpace(request.ParentModelID),
		ParentModelVersion:     strings.TrimSpace(request.ParentModelVersion),
		ParentAdapterURI:       strings.TrimSpace(request.ParentAdapterURI),
		TrainingProfile:        trainingProfile,
		ModelURI:               modelURI,
		AdapterURI:             modelURI,
		ServingTarget:          a.servingTarget,
		ServingModel:           servingModel,
	})
	hash := sha256.Sum256([]byte(recipe))
	recipeHash := hex.EncodeToString(hash[:])
	return model.TrainingJobSpec{
		TrainingRunID:          trainingRunID,
		DatasetURI:             datasetURI,
		PreferenceDatasetID:    strings.TrimSpace(request.PreferenceDatasetID),
		ModelName:              modelName,
		ModelVersion:           modelVersion,
		BaseModel:              baseModel,
		SourceModelID:          strings.TrimSpace(request.SourceModelID),
		SourceArtifactURI:      strings.TrimSpace(request.SourceArtifactURI),
		SourceModelKind:        strings.TrimSpace(request.SourceModelKind),
		SourceArtifactChecksum: strings.TrimSpace(request.SourceArtifactChecksum),
		ParentModelID:          strings.TrimSpace(request.ParentModelID),
		ParentModelVersion:     strings.TrimSpace(request.ParentModelVersion),
		ParentAdapterURI:       strings.TrimSpace(request.ParentAdapterURI),
		TrainingProfile:        trainingProfile,
		ModelURI:               modelURI,
		AdapterURI:             modelURI,
		ServingTarget:          a.servingTarget,
		ServingModel:           servingModel,
		ServingLoadStatus:      a.servingLoadStatus,
		ArtifactFormat:         "HF_PEFT_ADAPTER",
		ArtifactManifestURI:    modelURI + "/artifact.json",
		ArtifactBucketRegion:   a.artifactBucketRegion,
		AxolotlCommand:         a.axolotlCommand,
		RecipeYAML:             recipe,
		RecipeHash:             recipeHash,
		SubmissionID:           deterministicSubmissionID("train", trainingRunID, recipeHash),
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
	trainer := strings.ToLower(strings.TrimSpace(profile.Trainer))
	if trainer == "dpo" {
		return axolotlDPORecipeYAML(spec)
	}
	return axolotlSFTRecipeYAML(spec)
}

func axolotlSFTRecipeYAML(spec model.TrainingJobSpec) string {
	log.Trace("axolotlSFTRecipeYAML")

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
	return fmt.Sprintf(`base_model: %s
model_name: %s
model_version: %s
training_profile: %s
trainer: %s
datasets:
  - path: %s
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
`, spec.BaseModel, spec.ModelName, spec.ModelVersion, profile.Name, trainer, spec.DatasetURI, spec.ModelURI, profile.Adapter, quantization, profile.SequenceLength, profile.SamplePacking, profile.LearningRate, profile.Epochs, profile.MicroBatchSize, profile.GradientAccumulationSteps, profile.LoRAR, profile.LoRAAlpha)
}

func axolotlDPORecipeYAML(spec model.TrainingJobSpec) string {
	log.Trace("axolotlDPORecipeYAML")

	profile := spec.TrainingProfile
	parentAdapterURI := strings.TrimSpace(spec.ParentAdapterURI)
	preferenceDatasetURI := strings.TrimSpace(profile.PreferenceDatasetURI)
	if preferenceDatasetURI == "" {
		preferenceDatasetURI = strings.TrimSpace(spec.DatasetURI)
	}
	quantization := ""
	switch strings.ToLower(strings.TrimSpace(profile.Quantization)) {
	case "4bit":
		quantization = "load_in_4bit: true\n"
	case "8bit":
		quantization = "load_in_8bit: true\n"
	}
	return fmt.Sprintf(`base_model: %s
model_name: %s
model_version: %s
training_profile: %s
trainer: dpo
rl: dpo
dpo_beta: %.8g
rl_beta: %.8g
parent_model_id: %s
parent_model_version: %s
preference_dataset_id: %s
lora_model_dir: %s
datasets:
  - path: %s
    type: chat_template.default
    field_messages: prompt
    field_chosen: chosen
    field_rejected: rejected
output_dir: %s
adapter: %s
%ssequence_len: %d
learning_rate: %.8g
num_epochs: %.8g
micro_batch_size: %d
gradient_accumulation_steps: %d
lora_r: %d
lora_alpha: %d
`, spec.BaseModel, spec.ModelName, spec.ModelVersion, profile.Name, profile.DPOBeta, profile.DPOBeta, spec.ParentModelID, spec.ParentModelVersion, spec.PreferenceDatasetID, parentAdapterURI, preferenceDatasetURI, spec.ModelURI, profile.Adapter, quantization, profile.SequenceLength, profile.LearningRate, profile.Epochs, profile.MicroBatchSize, profile.GradientAccumulationSteps, profile.LoRAR, profile.LoRAAlpha)
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

	if strings.TrimSpace(result.TrainingRunID) == "" {
		return domain.ErrValidationFailed.Extend("training run id is required")
	}
	return a.eventPublisher.PublishModelTrainingCompleted(ctx, &result)
}

func (a *TrainingActivities) PublishModelTrainingFailed(ctx context.Context, result model.TrainingRunResult) error {
	log.Trace("TrainingActivities PublishModelTrainingFailed")

	if strings.TrimSpace(result.TrainingRunID) == "" {
		return domain.ErrValidationFailed.Extend("training run id is required")
	}
	if strings.TrimSpace(result.FailureReason) == "" {
		return domain.ErrValidationFailed.Extend("failure reason is required")
	}
	return a.eventPublisher.PublishModelTrainingFailed(ctx, &result)
}
