package temporalworker_test

import (
	"context"
	"errors"
	"testing"

	"training_service/pkg/domain"
	"training_service/pkg/domain/model"
	"training_service/pkg/infra/temporalworker"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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
		)

		artifact, err := activities.RunTrainingJob(context.Background(), model.PreparedTrainingDataset{
			TrainingRunID: "training-run-1",
			DatasetURI:    "s3://local-dev-bucket/features/feature-snapshot-1.parquet",
		}, model.TrainingRunRequest{
			TrainingRunID: "training-run-1",
			ModelName:     "model",
			ModelVersion:  "v1",
			BaseModel:     "mistral-7b",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(artifact).To(Equal(executor.artifact))
		Expect(executor.trainingSpec.TrainingRunID).To(Equal("training-run-1"))
		Expect(executor.trainingSpec.DatasetURI).To(Equal("s3://local-dev-bucket/features/feature-snapshot-1.parquet"))
		Expect(executor.trainingSpec.ModelURI).To(Equal("s3://models/training-run-1"))
		Expect(executor.trainingSpec.ArtifactManifestURI).To(Equal("s3://models/training-run-1/artifact.json"))
		Expect(executor.trainingSpec.RecipeYAML).To(ContainSubstring("base_model: mistral-7b"))
		Expect(executor.trainingSpec.RecipeYAML).To(ContainSubstring("path: s3://local-dev-bucket/features/feature-snapshot-1.parquet"))
		Expect(executor.trainingSpec.RecipeHash).NotTo(BeEmpty())
		Expect(executor.trainingSpec.SubmissionID).To(HavePrefix("train-training-run-1-"))
		Expect(artifact.ModelName).To(Equal("model"))
		Expect(artifact.ModelVersion).To(Equal("v1"))
		Expect(artifact.BaseModel).To(Equal("mistral-7b"))
		Expect(artifact.ArtifactFormat).To(Equal("HF_PEFT_ADAPTER"))
		Expect(artifact.ArtifactChecksum).To(Equal("sha256:abc"))
		Expect(artifact.ArtifactSizeBytes).To(BeNumerically(">", 0))
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
		)

		report, err := activities.EvaluateTrainedModel(context.Background(), model.TrainedModelArtifact{
			TrainingRunID: "training-run-1",
			ModelURI:      "s3://local-dev-bucket/models/training-run-1",
		}, model.TrainingRunRequest{TrainingRunID: "training-run-1", EvaluationProfile: "smoke"})

		Expect(err).NotTo(HaveOccurred())
		Expect(report.Passed).To(BeTrue())
		Expect(report.ReportURI).To(Equal("s3://evaluations/training-run-1.json"))
		Expect(executor.evaluationSpec.TrainingRunID).To(Equal("training-run-1"))
		Expect(executor.evaluationSpec.ModelURI).To(Equal("s3://local-dev-bucket/models/training-run-1"))
		Expect(executor.evaluationSpec.EvaluationProfile).To(Equal("smoke"))
		Expect(executor.evaluationSpec.ReportURI).To(Equal("s3://evaluations/training-run-1.json"))
		Expect(executor.evaluationSpec.ReportManifestURI).To(Equal("s3://evaluations/training-run-1.json"))
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
