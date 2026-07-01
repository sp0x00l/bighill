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

var _ = Describe("TrainingActivities", func() {
	It("prepares dataset metadata for a feature snapshot", func() {
		activities := temporalworker.NewTrainingActivities()

		prepared, err := activities.PrepareTrainingDataset(context.Background(), model.TrainingRunRequest{
			TrainingRunID:     "training-run-1",
			FeatureSnapshotID: "feature-snapshot-1",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(prepared.DatasetURI).To(Equal("s3://local-dev-bucket/features/feature-snapshot-1.parquet"))
	})

	It("rejects invalid preparation requests", func() {
		activities := temporalworker.NewTrainingActivities()

		prepared, err := activities.PrepareTrainingDataset(context.Background(), model.TrainingRunRequest{})

		Expect(prepared).To(BeNil())
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
	})

	It("creates model artifact metadata", func() {
		activities := temporalworker.NewTrainingActivities()

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
		Expect(artifact.ModelURI).To(Equal("s3://local-dev-bucket/models/training-run-1"))
		Expect(artifact.ModelName).To(Equal("model"))
		Expect(artifact.ModelVersion).To(Equal("v1"))
		Expect(artifact.BaseModel).To(Equal("mistral-7b"))
		Expect(artifact.ArtifactFormat).To(Equal("HF_PEFT_ADAPTER"))
		Expect(artifact.ArtifactChecksum).To(Equal("local-dev-training-run-1"))
		Expect(artifact.ArtifactSizeBytes).To(BeNumerically(">", 0))
	})

	It("creates evaluation report metadata", func() {
		activities := temporalworker.NewTrainingActivities()

		report, err := activities.EvaluateTrainedModel(context.Background(), model.TrainedModelArtifact{
			TrainingRunID: "training-run-1",
			ModelURI:      "s3://local-dev-bucket/models/training-run-1",
		}, model.TrainingRunRequest{TrainingRunID: "training-run-1"})

		Expect(err).NotTo(HaveOccurred())
		Expect(report.Passed).To(BeTrue())
		Expect(report.ReportURI).To(Equal("s3://local-dev-bucket/evaluations/training-run-1.json"))
	})

	It("publishes completed training facts", func() {
		publisher := &recordingTrainingEventPublisher{}
		activities := temporalworker.NewTrainingActivities(publisher)
		result := model.TrainingRunResult{
			TrainingRunID: "training-run-1",
			DatasetID:     uuid.NewString(),
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
			Status:        model.TrainingRunStatusFailed,
			FailureReason: "model evaluation failed",
		}

		err := activities.PublishModelTrainingFailed(context.Background(), result)

		Expect(err).NotTo(HaveOccurred())
		Expect(publisher.failedResult).To(Equal(&result))
	})

	It("rejects completed training publish without a publisher", func() {
		activities := temporalworker.NewTrainingActivities()

		err := activities.PublishModelTrainingCompleted(context.Background(), model.TrainingRunResult{TrainingRunID: "training-run-1"})

		Expect(errors.Is(err, domain.ErrTrainModel)).To(BeTrue())
	})

	It("rejects failed training publish without a failure reason", func() {
		activities := temporalworker.NewTrainingActivities(&recordingTrainingEventPublisher{})

		err := activities.PublishModelTrainingFailed(context.Background(), model.TrainingRunResult{TrainingRunID: "training-run-1"})

		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
	})
})
