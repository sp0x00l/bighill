package app_test

import (
	usecase "feature_materializer_service/pkg/app"
	"feature_materializer_service/pkg/domain/model"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

var _ = Describe("MaterializeWorkflow", func() {
	var suite testsuite.WorkflowTestSuite

	It("runs raw, feature, and embedding materialization activities in order", func() {
		env := suite.NewTestWorkflowEnvironment()
		datasetFile := validDatasetFile()
		rawSnapshot := validRawSnapshot()
		rawSnapshot.DatasetID = datasetFile.DatasetID
		rawSnapshot.UserID = datasetFile.UserID
		featureSnapshot := validFeatureSnapshot(rawSnapshot.RawSnapshotID)
		featureSnapshot.DatasetID = datasetFile.DatasetID
		featureSnapshot.UserID = datasetFile.UserID
		embeddingSnapshot := validEmbeddingSnapshot(featureSnapshot.FeatureSnapshotID)
		embeddingSnapshot.DatasetID = datasetFile.DatasetID
		embeddingSnapshot.UserID = datasetFile.UserID
		rawIdempotencyKey := uuid.New()

		env.RegisterActivityWithOptions(func(usecase.MaterializeRawSnapshotActivityInput) (*model.RawSnapshot, error) {
			return nil, nil
		}, activity.RegisterOptions{Name: usecase.MaterializeRawSnapshotActivityName})
		env.RegisterActivityWithOptions(func(usecase.BuildFeatureSnapshotActivityInput) (*model.FeatureSnapshot, error) {
			return nil, nil
		}, activity.RegisterOptions{Name: usecase.BuildFeatureSnapshotActivityName})
		env.RegisterActivityWithOptions(func(usecase.MaterializeEmbeddingsActivityInput) (*model.EmbeddingSnapshot, error) {
			return nil, nil
		}, activity.RegisterOptions{Name: usecase.MaterializeEmbeddingsActivityName})

		env.OnActivity(usecase.MaterializeRawSnapshotActivityName, usecase.MaterializeRawSnapshotActivityInput{
			DatasetFile:    *datasetFile,
			IdempotencyKey: rawIdempotencyKey,
		}).Return(rawSnapshot, nil)
		env.OnActivity(usecase.BuildFeatureSnapshotActivityName, usecase.BuildFeatureSnapshotActivityInput{
			RawSnapshotID:  rawSnapshot.RawSnapshotID,
			IdempotencyKey: usecase.FeatureSnapshotIdempotencyKey(rawSnapshot.RawSnapshotID),
		}).Return(featureSnapshot, nil)
		env.OnActivity(usecase.MaterializeEmbeddingsActivityName, usecase.MaterializeEmbeddingsActivityInput{
			FeatureSnapshotID: featureSnapshot.FeatureSnapshotID,
			IdempotencyKey:    usecase.EmbeddingSnapshotIdempotencyKey(featureSnapshot.FeatureSnapshotID),
		}).Return(embeddingSnapshot, nil)

		env.ExecuteWorkflow(usecase.MaterializeWorkflow, usecase.MaterializeWorkflowInput{
			DatasetFile:       *datasetFile,
			RawIdempotencyKey: rawIdempotencyKey,
		})

		Expect(env.IsWorkflowCompleted()).To(BeTrue())
		Expect(env.GetWorkflowError()).NotTo(HaveOccurred())

		var result usecase.MaterializeWorkflowResult
		Expect(env.GetWorkflowResult(&result)).To(Succeed())
		Expect(result.RawSnapshotID).To(Equal(rawSnapshot.RawSnapshotID))
		Expect(result.FeatureSnapshotID).To(Equal(featureSnapshot.FeatureSnapshotID))
		Expect(result.EmbeddingSnapshotID).To(Equal(embeddingSnapshot.EmbeddingSnapshotID))
	})
})
