package temporalworker_test

import (
	"context"
	"errors"
	"testing"

	"training_service/pkg/domain"
	"training_service/pkg/domain/model"
	"training_service/pkg/infra/temporalworker"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestTemporalWorker(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Training service Temporal worker unit test suite")
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
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(artifact.ModelURI).To(Equal("s3://local-dev-bucket/models/training-run-1"))
		Expect(artifact.ModelName).To(Equal("model"))
		Expect(artifact.ModelVersion).To(Equal("v1"))
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
})
