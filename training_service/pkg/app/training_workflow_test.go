package app_test

import (
	"testing"

	"training_service/pkg/app"
	"training_service/pkg/domain/model"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestApp(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Training service app unit test suite")
}

var _ = Describe("TrainModelWorkflow", func() {
	var suite testsuite.WorkflowTestSuite

	It("runs the training workflow through all activities", func() {
		env := suite.NewTestWorkflowEnvironment()
		request := model.TrainingRunRequest{
			TrainingRunID:     "training-run-1",
			DatasetID:         "dataset-1",
			DatasetVersion:    "3",
			FeatureSnapshotID: "feature-snapshot-1",
			ModelName:         "sentence-transformer",
			ModelVersion:      "local-dev",
			BaseModel:         "mistral-7b",
			EvaluationProfile: "smoke",
		}
		prepared := model.PreparedTrainingDataset{
			TrainingRunID:     request.TrainingRunID,
			FeatureSnapshotID: request.FeatureSnapshotID,
			DatasetURI:        "s3://local-dev-bucket/features/feature-snapshot-1.parquet",
		}
		artifact := model.TrainedModelArtifact{
			TrainingRunID:     request.TrainingRunID,
			ModelURI:          "s3://local-dev-bucket/models/training-run-1",
			ModelName:         request.ModelName,
			ModelVersion:      request.ModelVersion,
			BaseModel:         request.BaseModel,
			ArtifactFormat:    "HF_PEFT_ADAPTER",
			ArtifactChecksum:  "sha256:abc",
			ArtifactSizeBytes: 128,
		}
		report := model.EvaluationReport{
			TrainingRunID: request.TrainingRunID,
			ReportURI:     "s3://local-dev-bucket/evaluations/training-run-1.json",
			Passed:        true,
		}

		env.RegisterActivityWithOptions(func(model.TrainingRunRequest) (*model.PreparedTrainingDataset, error) {
			return nil, nil
		}, activity.RegisterOptions{Name: app.PrepareTrainingDatasetActivity})
		env.RegisterActivityWithOptions(func(model.PreparedTrainingDataset, model.TrainingRunRequest) (*model.TrainedModelArtifact, error) {
			return nil, nil
		}, activity.RegisterOptions{Name: app.RunTrainingJobActivity})
		env.RegisterActivityWithOptions(func(model.TrainedModelArtifact, model.TrainingRunRequest) (*model.EvaluationReport, error) {
			return nil, nil
		}, activity.RegisterOptions{Name: app.EvaluateTrainedModelActivity})
		env.RegisterActivityWithOptions(func(model.TrainingRunResult) error {
			return nil
		}, activity.RegisterOptions{Name: app.PublishModelTrainingCompletedActivity})
		env.RegisterActivityWithOptions(func(model.TrainingRunResult) error {
			return nil
		}, activity.RegisterOptions{Name: app.PublishModelTrainingFailedActivity})

		env.OnActivity(app.PrepareTrainingDatasetActivity, request).Return(&prepared, nil)
		env.OnActivity(app.RunTrainingJobActivity, prepared, request).Return(&artifact, nil)
		env.OnActivity(app.EvaluateTrainedModelActivity, artifact, request).Return(&report, nil)
		env.OnActivity(app.PublishModelTrainingCompletedActivity, mock.Anything).Return(nil)

		env.ExecuteWorkflow(app.TrainModelWorkflow, request)

		Expect(env.IsWorkflowCompleted()).To(BeTrue())
		Expect(env.GetWorkflowError()).NotTo(HaveOccurred())

		var result model.TrainingRunResult
		Expect(env.GetWorkflowResult(&result)).To(Succeed())
		Expect(result.Status).To(Equal(model.TrainingRunStatusCompleted))
		Expect(result.DatasetVersion).To(Equal("3"))
		Expect(result.ModelURI).To(Equal(artifact.ModelURI))
		Expect(result.ReportURI).To(Equal(report.ReportURI))
	})

	It("publishes a failed training fact when evaluation does not pass", func() {
		env := suite.NewTestWorkflowEnvironment()
		request := model.TrainingRunRequest{
			TrainingRunID:     "training-run-2",
			DatasetID:         "dataset-2",
			DatasetVersion:    "5",
			FeatureSnapshotID: "feature-snapshot-2",
			ModelName:         "sentence-transformer",
			ModelVersion:      "local-dev",
			BaseModel:         "mistral-7b",
		}
		prepared := model.PreparedTrainingDataset{
			TrainingRunID:     request.TrainingRunID,
			FeatureSnapshotID: request.FeatureSnapshotID,
			DatasetURI:        "s3://local-dev-bucket/features/feature-snapshot-2.parquet",
		}
		artifact := model.TrainedModelArtifact{
			TrainingRunID:     request.TrainingRunID,
			ModelURI:          "s3://local-dev-bucket/models/training-run-2",
			ModelName:         request.ModelName,
			ModelVersion:      request.ModelVersion,
			BaseModel:         request.BaseModel,
			ArtifactFormat:    "HF_PEFT_ADAPTER",
			ArtifactChecksum:  "sha256:def",
			ArtifactSizeBytes: 128,
		}
		report := model.EvaluationReport{
			TrainingRunID: request.TrainingRunID,
			ReportURI:     "s3://local-dev-bucket/evaluations/training-run-2.json",
			Passed:        false,
		}

		env.RegisterActivityWithOptions(func(model.TrainingRunRequest) (*model.PreparedTrainingDataset, error) {
			return nil, nil
		}, activity.RegisterOptions{Name: app.PrepareTrainingDatasetActivity})
		env.RegisterActivityWithOptions(func(model.PreparedTrainingDataset, model.TrainingRunRequest) (*model.TrainedModelArtifact, error) {
			return nil, nil
		}, activity.RegisterOptions{Name: app.RunTrainingJobActivity})
		env.RegisterActivityWithOptions(func(model.TrainedModelArtifact, model.TrainingRunRequest) (*model.EvaluationReport, error) {
			return nil, nil
		}, activity.RegisterOptions{Name: app.EvaluateTrainedModelActivity})
		env.RegisterActivityWithOptions(func(model.TrainingRunResult) error {
			return nil
		}, activity.RegisterOptions{Name: app.PublishModelTrainingCompletedActivity})
		env.RegisterActivityWithOptions(func(model.TrainingRunResult) error {
			return nil
		}, activity.RegisterOptions{Name: app.PublishModelTrainingFailedActivity})

		env.OnActivity(app.PrepareTrainingDatasetActivity, request).Return(&prepared, nil)
		env.OnActivity(app.RunTrainingJobActivity, prepared, request).Return(&artifact, nil)
		env.OnActivity(app.EvaluateTrainedModelActivity, artifact, request).Return(&report, nil)
		env.OnActivity(app.PublishModelTrainingFailedActivity, mock.Anything).Return(nil)

		env.ExecuteWorkflow(app.TrainModelWorkflow, request)

		Expect(env.IsWorkflowCompleted()).To(BeTrue())
		Expect(env.GetWorkflowError()).NotTo(HaveOccurred())

		var result model.TrainingRunResult
		Expect(env.GetWorkflowResult(&result)).To(Succeed())
		Expect(result.Status).To(Equal(model.TrainingRunStatusFailed))
		Expect(result.FailureReason).To(Equal("model evaluation failed"))
		Expect(result.MetricsMetadata).To(Equal(`{"passed":false}`))
	})
})
