package temporalworker_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"training_service/pkg/domain"
	"training_service/pkg/domain/model"
	"training_service/pkg/infra/temporalworker"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"gopkg.in/yaml.v3"
)

func TestTemporalWorker(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Training service Temporal worker unit test suite")
}

type recordingTrainingEventPublisher struct {
	completedResult *model.TrainingRunResult
	failedResult    *model.TrainingRunResult
	err             error
}

func (p *recordingTrainingEventPublisher) PublishModelTrainingCompleted(_ context.Context, result *model.TrainingRunResult) error {
	p.completedResult = result
	return p.err
}

func (p *recordingTrainingEventPublisher) PublishModelTrainingFailed(_ context.Context, result *model.TrainingRunResult) error {
	p.failedResult = result
	return p.err
}

type recordingTrainingExecutor struct {
	trainingSpec   model.TrainingJobSpec
	evaluationSpec model.EvaluationJobSpec
	artifact       *model.TrainedModelArtifact
	report         *model.EvaluationReport
	err            error
}

func (e *recordingTrainingExecutor) RunTrainingJob(_ context.Context, spec model.TrainingJobSpec) (*model.TrainedModelArtifact, error) {
	e.trainingSpec = spec
	if e.err != nil {
		return nil, e.err
	}
	return e.artifact, nil
}

func (e *recordingTrainingExecutor) EvaluateModel(_ context.Context, spec model.EvaluationJobSpec) (*model.EvaluationReport, error) {
	e.evaluationSpec = spec
	if e.err != nil {
		return nil, e.err
	}
	return e.report, nil
}

var _ = Describe("TrainingActivities", func() {
	It("prepares dataset metadata for a feature snapshot", func() {
		activities := temporalworker.NewTrainingActivities(nil)

		prepared, err := activities.PrepareTrainingDataset(context.Background(), model.TrainingRunRequest{
			TrainingRunID:     "training-run-1",
			FeatureSnapshotID: "feature-snapshot-1",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(prepared.DatasetURI).To(Equal("s3://local-dev-bucket/features/feature-snapshot-1.parquet"))
	})

	It("prepares DPO dataset metadata from a preference dataset artifact", func() {
		activities := temporalworker.NewTrainingActivities(nil)

		prepared, err := activities.PrepareTrainingDataset(context.Background(), model.TrainingRunRequest{
			TrainingRunID:        "training-run-1",
			PreferenceDatasetID:  uuid.NewString(),
			PreferenceDatasetURI: "s3://local-dev-bucket/preferences/dpo.jsonl",
			TrainingProfile: model.TrainingProfile{
				Trainer: "dpo",
			},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(prepared.DatasetURI).To(Equal("s3://local-dev-bucket/preferences/dpo.jsonl"))
	})

	It("rejects invalid preparation requests", func() {
		activities := temporalworker.NewTrainingActivities(nil)

		prepared, err := activities.PrepareTrainingDataset(context.Background(), model.TrainingRunRequest{})

		Expect(prepared).To(BeNil())
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
	})

	It("builds a training job spec and delegates execution", func() {
		executor := &recordingTrainingExecutor{artifact: &model.TrainedModelArtifact{
			TrainingRunID:     "training-run-1",
			ModelURI:          "s3://models/training-run-1",
			ModelName:         "model",
			ModelVersion:      "v1",
			BaseModel:         "mistral-7b",
			ArtifactFormat:    "HF_PEFT_ADAPTER",
			ArtifactChecksum:  "sha256:abc",
			ArtifactSizeBytes: 128,
		}}
		activities := temporalworker.NewTrainingActivities(
			nil,
			temporalworker.WithExecutor(executor),
			temporalworker.WithModelURIPrefix("s3://models"),
			temporalworker.WithServingConfig("vllm-local", "movie-ranker-v1", "LOADED"),
			temporalworker.WithArtifactBucketRegion("eu-west-1"),
			temporalworker.WithAxolotlCommand("axolotl train"),
		)

		artifact, err := activities.RunTrainingJob(context.Background(), model.PreparedTrainingDataset{
			TrainingRunID: "training-run-1",
			DatasetURI:    "s3://local-dev-bucket/features/feature-snapshot-1.parquet",
		}, model.TrainingRunRequest{
			TrainingRunID:   "training-run-1",
			ModelName:       "model",
			ModelVersion:    "v1",
			BaseModel:       "mistral-7b",
			TrainingProfile: trainingProfile(),
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(artifact).To(Equal(executor.artifact))
		Expect(executor.trainingSpec.TrainingRunID).To(Equal("training-run-1"))
		Expect(executor.trainingSpec.DatasetURI).To(Equal("s3://local-dev-bucket/features/feature-snapshot-1.parquet"))
		Expect(executor.trainingSpec.ModelURI).To(Equal("s3://models/training-run-1"))
		Expect(executor.trainingSpec.AdapterURI).To(Equal("s3://models/training-run-1"))
		Expect(executor.trainingSpec.ServingTarget).To(Equal("vllm-local"))
		Expect(executor.trainingSpec.ServingModel).To(Equal("movie-ranker-v1"))
		Expect(executor.trainingSpec.ServingLoadStatus).To(Equal("LOADED"))
		Expect(executor.trainingSpec.ArtifactManifestURI).To(Equal("s3://models/training-run-1/artifact.json"))
		Expect(executor.trainingSpec.ArtifactBucketRegion).To(Equal("eu-west-1"))
		Expect(executor.trainingSpec.AxolotlCommand).To(Equal("axolotl train"))
		Expect(executor.trainingSpec.RecipeYAML).To(ContainSubstring("base_model: mistral-7b"))
		Expect(executor.trainingSpec.RecipeYAML).To(ContainSubstring("training_profile: profile-v1"))
		Expect(executor.trainingSpec.RecipeYAML).To(ContainSubstring("trainer: sft"))
		Expect(executor.trainingSpec.RecipeYAML).NotTo(ContainSubstring("rl: dpo"))
		Expect(executor.trainingSpec.RecipeYAML).NotTo(ContainSubstring("preference_dataset"))
		Expect(executor.trainingSpec.RecipeYAML).To(ContainSubstring("adapter: qlora"))
		Expect(executor.trainingSpec.RecipeYAML).To(ContainSubstring("load_in_4bit: true"))
		Expect(executor.trainingSpec.RecipeYAML).To(ContainSubstring("learning_rate: 0.0002"))
		Expect(executor.trainingSpec.TrainingProfile).To(Equal(trainingProfile()))
		Expect(executor.trainingSpec.RecipeYAML).To(ContainSubstring("path: s3://local-dev-bucket/features/feature-snapshot-1.parquet"))
		Expect(executor.trainingSpec.RecipeHash).NotTo(BeEmpty())
		Expect(executor.trainingSpec.SubmissionID).To(HavePrefix("train-training-run-1-"))
		Expect(artifact.ModelName).To(Equal("model"))
		Expect(artifact.ModelVersion).To(Equal("v1"))
		Expect(artifact.BaseModel).To(Equal("mistral-7b"))
		Expect(artifact.ArtifactFormat).To(Equal("HF_PEFT_ADAPTER"))
		Expect(artifact.ArtifactChecksum).To(Equal("sha256:abc"))
		Expect(artifact.ArtifactSizeBytes).To(BeNumerically(">", 0))
		Expect(artifact.AdapterURI).To(Equal("s3://models/training-run-1"))
		Expect(artifact.ServingTarget).To(Equal("vllm-local"))
		Expect(artifact.ServingModel).To(Equal("movie-ranker-v1"))
		Expect(artifact.ServingLoadStatus).To(Equal("LOADED"))
	})

	It("builds a real DPO recipe from the parent adapter and preference dataset", func() {
		executor := &recordingTrainingExecutor{artifact: &model.TrainedModelArtifact{
			TrainingRunID:     "training-run-dpo",
			ModelURI:          "s3://models/training-run-dpo",
			ModelName:         "dpo-parent-model",
			ModelVersion:      "8",
			BaseModel:         "mistral-7b",
			ArtifactFormat:    "HF_PEFT_ADAPTER",
			ArtifactChecksum:  "sha256:dpo",
			ArtifactSizeBytes: 128,
		}}
		activities := temporalworker.NewTrainingActivities(nil, temporalworker.WithExecutor(executor), temporalworker.WithModelURIPrefix("s3://models"))

		_, err := activities.RunTrainingJob(context.Background(), model.PreparedTrainingDataset{
			TrainingRunID: "training-run-dpo",
			DatasetURI:    "s3://local-dev-bucket/preferences/snapshot.jsonl",
		}, model.TrainingRunRequest{
			TrainingRunID:        "training-run-dpo",
			DatasetID:            uuid.NewString(),
			DatasetVersion:       "8",
			PreferenceDatasetID:  uuid.NewString(),
			PreferenceDatasetURI: "s3://local-dev-bucket/preferences/snapshot.jsonl",
			ParentModelID:        uuid.NewString(),
			ParentModelVersion:   "7",
			ParentAdapterURI:     "s3://models/parent-adapter",
			ModelName:            "dpo-parent-model",
			ModelVersion:         "8",
			BaseModel:            "mistral-7b",
			TrainingProfile:      dpoTrainingProfile(),
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(executor.trainingSpec.DatasetURI).To(Equal("s3://local-dev-bucket/preferences/snapshot.jsonl"))
		Expect(executor.trainingSpec.ParentAdapterURI).To(Equal("s3://models/parent-adapter"))
		Expect(executor.trainingSpec.RecipeYAML).To(ContainSubstring("trainer: dpo"))
		Expect(executor.trainingSpec.RecipeYAML).To(ContainSubstring("rl: dpo"))
		Expect(executor.trainingSpec.RecipeYAML).To(ContainSubstring("dpo_beta: 0.1"))
		Expect(executor.trainingSpec.RecipeYAML).To(ContainSubstring("rl_beta: 0.1"))
		Expect(executor.trainingSpec.RecipeYAML).To(ContainSubstring("preference_dataset_id: "))
		Expect(executor.trainingSpec.RecipeYAML).NotTo(ContainSubstring("reference_model:"))
		Expect(executor.trainingSpec.RecipeYAML).To(ContainSubstring("lora_model_dir: s3://models/parent-adapter"))
		Expect(executor.trainingSpec.RecipeYAML).To(ContainSubstring("path: s3://local-dev-bucket/preferences/snapshot.jsonl"))
		Expect(executor.trainingSpec.RecipeYAML).To(ContainSubstring("field_chosen: chosen"))
		Expect(executor.trainingSpec.RecipeYAML).To(ContainSubstring("field_rejected: rejected"))
		Expect(executor.trainingSpec.RecipeYAML).NotTo(ContainSubstring("\t"))
		Expect(executor.trainingSpec.RecipeYAML).NotTo(ContainSubstring("sample_packing:"))
		expectValidYAML(executor.trainingSpec.RecipeYAML)
	})

	It("builds an evaluation job spec and delegates execution", func() {
		executor := &recordingTrainingExecutor{report: &model.EvaluationReport{
			TrainingRunID: "training-run-1",
			ReportURI:     "s3://evaluations/training-run-1.json",
			Passed:        true,
		}}
		activities := temporalworker.NewTrainingActivities(
			nil,
			temporalworker.WithExecutor(executor),
			temporalworker.WithEvaluationURIPrefix("s3://evaluations"),
			temporalworker.WithArtifactBucketRegion("eu-west-1"),
		)

		evaluationProfile := `{"evaluator":"ragas","dataset_uri":"s3://evals/run-1.jsonl"}`
		report, err := activities.EvaluateTrainedModel(context.Background(), model.TrainedModelArtifact{
			TrainingRunID: "training-run-1",
			ModelURI:      "s3://local-dev-bucket/models/training-run-1",
		}, model.TrainingRunRequest{TrainingRunID: "training-run-1", EvaluationProfile: evaluationProfile})

		Expect(err).NotTo(HaveOccurred())
		Expect(report.Passed).To(BeTrue())
		Expect(report.ReportURI).To(Equal("s3://evaluations/training-run-1.json"))
		Expect(executor.evaluationSpec.TrainingRunID).To(Equal("training-run-1"))
		Expect(executor.evaluationSpec.ModelURI).To(Equal("s3://local-dev-bucket/models/training-run-1"))
		Expect(executor.evaluationSpec.EvaluationProfile).To(Equal(evaluationProfile))
		Expect(executor.evaluationSpec.ReportURI).To(Equal("s3://evaluations/training-run-1.json"))
		Expect(executor.evaluationSpec.ReportManifestURI).To(Equal("s3://evaluations/training-run-1.json"))
		Expect(executor.evaluationSpec.ArtifactBucketRegion).To(Equal("eu-west-1"))
		Expect(executor.evaluationSpec.SubmissionID).To(HavePrefix("eval-training-run-1-"))
	})

	It("publishes completed training facts", func() {
		publisher := &recordingTrainingEventPublisher{}
		activities := temporalworker.NewTrainingActivities(publisher)
		result := model.TrainingRunResult{
			TrainingRunID: "training-run-1",
			DatasetID:     uuid.NewString(),
			ModelID:       uuid.NewString(),
			Status:        model.TrainingRunStatusCompleted,
		}

		err := activities.PublishModelTrainingCompleted(context.Background(), result)

		Expect(err).NotTo(HaveOccurred())
		Expect(publisher.completedResult).To(Equal(&result))
	})

	It("publishes failed training facts", func() {
		publisher := &recordingTrainingEventPublisher{}
		activities := temporalworker.NewTrainingActivities(publisher)
		result := model.TrainingRunResult{
			TrainingRunID: "training-run-1",
			DatasetID:     uuid.NewString(),
			ModelID:       uuid.NewString(),
			Status:        model.TrainingRunStatusFailed,
			FailureReason: "model evaluation failed",
		}

		err := activities.PublishModelTrainingFailed(context.Background(), result)

		Expect(err).NotTo(HaveOccurred())
		Expect(publisher.failedResult).To(Equal(&result))
	})

	It("rejects completed training publish without a publisher", func() {
		activities := temporalworker.NewTrainingActivities(nil)

		err := activities.PublishModelTrainingCompleted(context.Background(), model.TrainingRunResult{TrainingRunID: "training-run-1"})

		Expect(errors.Is(err, domain.ErrTrainModel)).To(BeTrue())
	})

	It("rejects failed training publish without a failure reason", func() {
		activities := temporalworker.NewTrainingActivities(&recordingTrainingEventPublisher{})

		err := activities.PublishModelTrainingFailed(context.Background(), model.TrainingRunResult{TrainingRunID: "training-run-1"})

		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
	})
})

func trainingProfile() model.TrainingProfile {
	return model.TrainingProfile{
		Name:                      "profile-v1",
		Trainer:                   "sft",
		Adapter:                   "qlora",
		Quantization:              "4bit",
		SequenceLength:            2048,
		SamplePacking:             true,
		LearningRate:              0.0002,
		Epochs:                    3,
		MicroBatchSize:            1,
		GradientAccumulationSteps: 4,
		LoRAR:                     16,
		LoRAAlpha:                 32,
	}
}

func expectValidYAML(raw string) {
	var parsed map[string]any
	ExpectWithOffset(1, strings.TrimSpace(raw)).NotTo(BeEmpty())
	ExpectWithOffset(1, yaml.Unmarshal([]byte(raw), &parsed)).To(Succeed())
}

func dpoTrainingProfile() model.TrainingProfile {
	profile := trainingProfile()
	profile.Trainer = "dpo"
	profile.PreferenceDatasetURI = "s3://local-dev-bucket/preferences/snapshot.jsonl"
	profile.DPOBeta = 0.1
	return profile
}
