package app

import (
	"fmt"
	"time"

	"feature_materializer_service/pkg/domain/model"

	"github.com/google/uuid"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	MaterializeWorkflowName             = "feature_materializer.materialize_dataset"
	DefaultMaterializeWorkflowTaskQueue = "feature-materializer-service"

	MaterializeRawSnapshotActivityName = "feature_materializer.materialize_raw_snapshot"
	BuildFeatureSnapshotActivityName   = "feature_materializer.build_feature_snapshot"
	MaterializeEmbeddingsActivityName  = "feature_materializer.materialize_embeddings"
)

type MaterializeWorkflowInput struct {
	DatasetFile       model.DatasetFile
	RawIdempotencyKey uuid.UUID
	EmbeddingStrategy model.EmbeddingStrategy
}

type MaterializeWorkflowResult struct {
	RawSnapshotID       uuid.UUID
	FeatureSnapshotID   uuid.UUID
	EmbeddingSnapshotID uuid.UUID
}

type MaterializeRawSnapshotActivityInput struct {
	DatasetFile    model.DatasetFile
	IdempotencyKey uuid.UUID
}

type BuildFeatureSnapshotActivityInput struct {
	RawSnapshotID  uuid.UUID
	UserID         uuid.UUID
	OrgID          uuid.UUID
	IdempotencyKey uuid.UUID
}

type MaterializeEmbeddingsActivityInput struct {
	FeatureSnapshotID uuid.UUID
	UserID            uuid.UUID
	OrgID             uuid.UUID
	IdempotencyKey    uuid.UUID
	Strategy          model.EmbeddingStrategy
}

func MaterializeWorkflowID(datasetID uuid.UUID, rawIdempotencyKey uuid.UUID) string {
	return fmt.Sprintf("feature-materializer:%s:%s", datasetID, rawIdempotencyKey)
}

func FeatureSnapshotIdempotencyKey(rawSnapshotID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("feature_snapshot:"+rawSnapshotID.String()))
}

func EmbeddingSnapshotIdempotencyKey(featureSnapshotID uuid.UUID, strategy model.EmbeddingStrategy) uuid.UUID {
	strategy = model.NormalizeEmbeddingStrategy(strategy)
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("embedding_snapshot:"+featureSnapshotID.String()+":"+strategy.CanonicalKey()))
}

func MaterializeWorkflow(ctx workflow.Context, input MaterializeWorkflowInput) (*MaterializeWorkflowResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("MaterializeWorkflow started", "dataset_id", input.DatasetFile.DatasetID.String())
	embeddingStrategy := model.NormalizeEmbeddingStrategy(input.EmbeddingStrategy)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    5,
		},
	})

	var rawSnapshot model.RawSnapshot
	if err := workflow.ExecuteActivity(ctx, MaterializeRawSnapshotActivityName, MaterializeRawSnapshotActivityInput{
		DatasetFile:    input.DatasetFile,
		IdempotencyKey: input.RawIdempotencyKey,
	}).Get(ctx, &rawSnapshot); err != nil {
		return nil, err
	}

	var featureSnapshot model.FeatureSnapshot
	if err := workflow.ExecuteActivity(ctx, BuildFeatureSnapshotActivityName, BuildFeatureSnapshotActivityInput{
		RawSnapshotID:  rawSnapshot.RawSnapshotID,
		UserID:         rawSnapshot.UserID,
		OrgID:          rawSnapshot.OrgID,
		IdempotencyKey: FeatureSnapshotIdempotencyKey(rawSnapshot.RawSnapshotID),
	}).Get(ctx, &featureSnapshot); err != nil {
		return nil, err
	}

	var embeddingSnapshotID uuid.UUID
	if featureSnapshot.ProcessingProfile.RequiresEmbeddings() {
		var embeddingSnapshot model.EmbeddingSnapshot
		if err := workflow.ExecuteActivity(ctx, MaterializeEmbeddingsActivityName, MaterializeEmbeddingsActivityInput{
			FeatureSnapshotID: featureSnapshot.FeatureSnapshotID,
			UserID:            featureSnapshot.UserID,
			OrgID:             featureSnapshot.OrgID,
			IdempotencyKey:    EmbeddingSnapshotIdempotencyKey(featureSnapshot.FeatureSnapshotID, embeddingStrategy),
			Strategy:          embeddingStrategy,
		}).Get(ctx, &embeddingSnapshot); err != nil {
			return nil, err
		}
		embeddingSnapshotID = embeddingSnapshot.EmbeddingSnapshotID
	}

	result := &MaterializeWorkflowResult{
		RawSnapshotID:       rawSnapshot.RawSnapshotID,
		FeatureSnapshotID:   featureSnapshot.FeatureSnapshotID,
		EmbeddingSnapshotID: embeddingSnapshotID,
	}
	logger.Info("MaterializeWorkflow completed", "dataset_id", input.DatasetFile.DatasetID.String())
	return result, nil
}
