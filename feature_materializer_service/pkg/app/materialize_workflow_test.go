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

	It("runs raw, feature, embedding, and graph materialization activities when graph extraction is enabled", func() {
		env := suite.NewTestWorkflowEnvironment()
		datasetFile := validDatasetFile()
		datasetFile.ProcessingProfile = model.ProcessingProfileTextRAG
		rawSnapshot := validRawSnapshot()
		rawSnapshot.DatasetID = datasetFile.DatasetID
		rawSnapshot.UserID = datasetFile.UserID
		rawSnapshot.OrgID = datasetFile.OrgID
		rawSnapshot.ProcessingProfile = model.ProcessingProfileTextRAG
		featureSnapshot := validFeatureSnapshot(rawSnapshot.RawSnapshotID)
		featureSnapshot.DatasetID = datasetFile.DatasetID
		featureSnapshot.UserID = datasetFile.UserID
		featureSnapshot.OrgID = datasetFile.OrgID
		featureSnapshot.ProcessingProfile = model.ProcessingProfileTextRAG
		embeddingSnapshot := validEmbeddingSnapshot(featureSnapshot.FeatureSnapshotID)
		embeddingSnapshot.DatasetID = datasetFile.DatasetID
		embeddingSnapshot.UserID = datasetFile.UserID
		embeddingSnapshot.OrgID = datasetFile.OrgID
		graphSnapshotID := uuid.New()
		graphSnapshot := &model.GraphSnapshot{
			GraphSnapshotID:     graphSnapshotID,
			FeatureSnapshotID:   featureSnapshot.FeatureSnapshotID,
			EmbeddingSnapshotID: embeddingSnapshot.EmbeddingSnapshotID,
			DatasetID:           datasetFile.DatasetID,
			UserID:              datasetFile.UserID,
			OrgID:               datasetFile.OrgID,
			Status:              model.SnapshotStatusReady,
		}
		rawIdempotencyKey := uuid.New()
		strategy := model.ApplyEmbeddingStrategyDefaults(model.EmbeddingStrategy{
			StrategyVersion:     "rag-v1",
			ChunkerName:         "go-token-window",
			ChunkerVersion:      "v1",
			ChunkSize:           128,
			ChunkOverlap:        16,
			EmbeddingProvider:   "ollama",
			EmbeddingModel:      "bge-small-en-v1.5",
			EmbeddingDimensions: 384,
		})

		env.RegisterActivityWithOptions(func(usecase.MaterializeRawSnapshotActivityInput) (*model.RawSnapshot, error) {
			return nil, nil
		}, activity.RegisterOptions{Name: usecase.MaterializeRawSnapshotActivityName})
		env.RegisterActivityWithOptions(func(usecase.BuildFeatureSnapshotActivityInput) (*model.FeatureSnapshot, error) {
			return nil, nil
		}, activity.RegisterOptions{Name: usecase.BuildFeatureSnapshotActivityName})
		env.RegisterActivityWithOptions(func(usecase.MaterializeEmbeddingsActivityInput) (*model.EmbeddingSnapshot, error) {
			return nil, nil
		}, activity.RegisterOptions{Name: usecase.MaterializeEmbeddingsActivityName})
		env.RegisterActivityWithOptions(func(usecase.MaterializeGraphActivityInput) (*model.GraphSnapshot, error) {
			return nil, nil
		}, activity.RegisterOptions{Name: usecase.MaterializeGraphActivityName})

		env.OnActivity(usecase.MaterializeRawSnapshotActivityName, usecase.MaterializeRawSnapshotActivityInput{
			DatasetFile:    *datasetFile,
			IdempotencyKey: rawIdempotencyKey,
		}).Return(rawSnapshot, nil)
		env.OnActivity(usecase.BuildFeatureSnapshotActivityName, usecase.BuildFeatureSnapshotActivityInput{
			RawSnapshotID:  rawSnapshot.RawSnapshotID,
			UserID:         rawSnapshot.UserID,
			OrgID:          rawSnapshot.OrgID,
			IdempotencyKey: usecase.FeatureSnapshotIdempotencyKey(rawSnapshot.RawSnapshotID),
		}).Return(featureSnapshot, nil)
		env.OnActivity(usecase.MaterializeEmbeddingsActivityName, usecase.MaterializeEmbeddingsActivityInput{
			FeatureSnapshotID: featureSnapshot.FeatureSnapshotID,
			UserID:            featureSnapshot.UserID,
			OrgID:             featureSnapshot.OrgID,
			IdempotencyKey:    usecase.EmbeddingSnapshotIdempotencyKey(featureSnapshot.FeatureSnapshotID, strategy),
			Strategy:          strategy,
		}).Return(embeddingSnapshot, nil)
		graphStrategy := model.ApplyGraphExtractionStrategyDefaults(model.GraphExtractionStrategy{})
		env.OnActivity(usecase.MaterializeGraphActivityName, usecase.MaterializeGraphActivityInput{
			EmbeddingSnapshotID: embeddingSnapshot.EmbeddingSnapshotID,
			UserID:              embeddingSnapshot.UserID,
			OrgID:               embeddingSnapshot.OrgID,
			IdempotencyKey:      usecase.GraphSnapshotIdempotencyKey(embeddingSnapshot.EmbeddingSnapshotID, graphStrategy),
			Strategy:            graphStrategy,
		}).Return(graphSnapshot, nil)

		env.ExecuteWorkflow(usecase.MaterializeWorkflow, usecase.MaterializeWorkflowInput{
			DatasetFile:             *datasetFile,
			RawIdempotencyKey:       rawIdempotencyKey,
			EmbeddingStrategy:       strategy,
			GraphEnabled:            true,
			GraphExtractionStrategy: graphStrategy,
		})

		Expect(env.IsWorkflowCompleted()).To(BeTrue())
		Expect(env.GetWorkflowError()).NotTo(HaveOccurred())

		var result usecase.MaterializeWorkflowResult
		Expect(env.GetWorkflowResult(&result)).To(Succeed())
		Expect(result.RawSnapshotID).To(Equal(rawSnapshot.RawSnapshotID))
		Expect(result.FeatureSnapshotID).To(Equal(featureSnapshot.FeatureSnapshotID))
		Expect(result.EmbeddingSnapshotID).To(Equal(embeddingSnapshot.EmbeddingSnapshotID))
		Expect(result.GraphSnapshotID).To(Equal(graphSnapshotID))
	})

	It("does not run graph materialization unless graph extraction is enabled", func() {
		env := suite.NewTestWorkflowEnvironment()
		datasetFile := validDatasetFile()
		datasetFile.ProcessingProfile = model.ProcessingProfileTextRAG
		rawSnapshot := validRawSnapshot()
		rawSnapshot.DatasetID = datasetFile.DatasetID
		rawSnapshot.UserID = datasetFile.UserID
		rawSnapshot.OrgID = datasetFile.OrgID
		rawSnapshot.ProcessingProfile = model.ProcessingProfileTextRAG
		featureSnapshot := validFeatureSnapshot(rawSnapshot.RawSnapshotID)
		featureSnapshot.DatasetID = datasetFile.DatasetID
		featureSnapshot.UserID = datasetFile.UserID
		featureSnapshot.OrgID = datasetFile.OrgID
		featureSnapshot.ProcessingProfile = model.ProcessingProfileTextRAG
		embeddingSnapshot := validEmbeddingSnapshot(featureSnapshot.FeatureSnapshotID)
		embeddingSnapshot.DatasetID = datasetFile.DatasetID
		embeddingSnapshot.UserID = datasetFile.UserID
		embeddingSnapshot.OrgID = datasetFile.OrgID
		rawIdempotencyKey := uuid.New()
		strategy := model.ApplyEmbeddingStrategyDefaults(model.EmbeddingStrategy{
			StrategyVersion:     "rag-v1",
			ChunkerName:         "go-token-window",
			ChunkerVersion:      "v1",
			ChunkSize:           128,
			ChunkOverlap:        16,
			EmbeddingProvider:   "ollama",
			EmbeddingModel:      "bge-small-en-v1.5",
			EmbeddingDimensions: 384,
		})

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
			UserID:         rawSnapshot.UserID,
			OrgID:          rawSnapshot.OrgID,
			IdempotencyKey: usecase.FeatureSnapshotIdempotencyKey(rawSnapshot.RawSnapshotID),
		}).Return(featureSnapshot, nil)
		env.OnActivity(usecase.MaterializeEmbeddingsActivityName, usecase.MaterializeEmbeddingsActivityInput{
			FeatureSnapshotID: featureSnapshot.FeatureSnapshotID,
			UserID:            featureSnapshot.UserID,
			OrgID:             featureSnapshot.OrgID,
			IdempotencyKey:    usecase.EmbeddingSnapshotIdempotencyKey(featureSnapshot.FeatureSnapshotID, strategy),
			Strategy:          strategy,
		}).Return(embeddingSnapshot, nil)

		env.ExecuteWorkflow(usecase.MaterializeWorkflow, usecase.MaterializeWorkflowInput{
			DatasetFile:       *datasetFile,
			RawIdempotencyKey: rawIdempotencyKey,
			EmbeddingStrategy: strategy,
			GraphEnabled:      false,
		})

		Expect(env.IsWorkflowCompleted()).To(BeTrue())
		Expect(env.GetWorkflowError()).NotTo(HaveOccurred())

		var result usecase.MaterializeWorkflowResult
		Expect(env.GetWorkflowResult(&result)).To(Succeed())
		Expect(result.EmbeddingSnapshotID).To(Equal(embeddingSnapshot.EmbeddingSnapshotID))
		Expect(result.GraphSnapshotID).To(Equal(uuid.Nil))
	})

	It("derives distinct embedding idempotency keys when the strategy changes", func() {
		featureSnapshotID := uuid.New()
		first := model.ApplyEmbeddingStrategyDefaults(model.EmbeddingStrategy{EmbeddingProvider: "tei", EmbeddingModel: "bge-small-en-v1.5", ChunkSize: 512})
		second := model.ApplyEmbeddingStrategyDefaults(model.EmbeddingStrategy{EmbeddingProvider: "tei", EmbeddingModel: "bge-m3", ChunkSize: 512})

		Expect(usecase.EmbeddingSnapshotIdempotencyKey(featureSnapshotID, first)).NotTo(Equal(usecase.EmbeddingSnapshotIdempotencyKey(featureSnapshotID, second)))
	})

	It("skips embedding materialization for generic parquet datasets", func() {
		env := suite.NewTestWorkflowEnvironment()
		datasetFile := validDatasetFile()
		rawSnapshot := validRawSnapshot()
		rawSnapshot.DatasetID = datasetFile.DatasetID
		rawSnapshot.UserID = datasetFile.UserID
		rawSnapshot.OrgID = datasetFile.OrgID
		rawSnapshot.ProcessingProfile = model.ProcessingProfileGenericParquet
		featureSnapshot := validFeatureSnapshot(rawSnapshot.RawSnapshotID)
		featureSnapshot.DatasetID = datasetFile.DatasetID
		featureSnapshot.UserID = datasetFile.UserID
		featureSnapshot.OrgID = datasetFile.OrgID
		featureSnapshot.ProcessingProfile = model.ProcessingProfileGenericParquet
		rawIdempotencyKey := uuid.New()

		env.RegisterActivityWithOptions(func(usecase.MaterializeRawSnapshotActivityInput) (*model.RawSnapshot, error) {
			return nil, nil
		}, activity.RegisterOptions{Name: usecase.MaterializeRawSnapshotActivityName})
		env.RegisterActivityWithOptions(func(usecase.BuildFeatureSnapshotActivityInput) (*model.FeatureSnapshot, error) {
			return nil, nil
		}, activity.RegisterOptions{Name: usecase.BuildFeatureSnapshotActivityName})

		env.OnActivity(usecase.MaterializeRawSnapshotActivityName, usecase.MaterializeRawSnapshotActivityInput{
			DatasetFile:    *datasetFile,
			IdempotencyKey: rawIdempotencyKey,
		}).Return(rawSnapshot, nil)
		env.OnActivity(usecase.BuildFeatureSnapshotActivityName, usecase.BuildFeatureSnapshotActivityInput{
			RawSnapshotID:  rawSnapshot.RawSnapshotID,
			UserID:         rawSnapshot.UserID,
			OrgID:          rawSnapshot.OrgID,
			IdempotencyKey: usecase.FeatureSnapshotIdempotencyKey(rawSnapshot.RawSnapshotID),
		}).Return(featureSnapshot, nil)

		env.ExecuteWorkflow(usecase.MaterializeWorkflow, usecase.MaterializeWorkflowInput{
			DatasetFile:       *datasetFile,
			RawIdempotencyKey: rawIdempotencyKey,
			EmbeddingStrategy: model.EmbeddingStrategy{},
		})

		Expect(env.IsWorkflowCompleted()).To(BeTrue())
		Expect(env.GetWorkflowError()).NotTo(HaveOccurred())

		var result usecase.MaterializeWorkflowResult
		Expect(env.GetWorkflowResult(&result)).To(Succeed())
		Expect(result.RawSnapshotID).To(Equal(rawSnapshot.RawSnapshotID))
		Expect(result.FeatureSnapshotID).To(Equal(featureSnapshot.FeatureSnapshotID))
		Expect(result.EmbeddingSnapshotID).To(Equal(uuid.Nil))
	})
})
