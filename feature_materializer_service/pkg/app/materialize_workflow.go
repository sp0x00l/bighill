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
	IdempotencyKey uuid.UUID
}

type MaterializeEmbeddingsActivityInput struct {
	FeatureSnapshotID uuid.UUID
	IdempotencyKey    uuid.UUID
}

func MaterializeWorkflowID(datasetID uuid.UUID, rawIdempotencyKey uuid.UUID) string {
	return fmt.Sprintf("feature-materializer:%s:%s", datasetID, rawIdempotencyKey)
}

func FeatureSnapshotIdempotencyKey(rawSnapshotID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("feature_snapshot:"+rawSnapshotID.String()))
}

func EmbeddingSnapshotIdempotencyKey(featureSnapshotID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("embedding_snapshot:"+featureSnapshotID.String()))
}

func MaterializeWorkflow(ctx workflow.Context, input MaterializeWorkflowInput) (*MaterializeWorkflowResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("MaterializeWorkflow started", "dataset_id", input.DatasetFile.DatasetID.String())

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
		IdempotencyKey: FeatureSnapshotIdempotencyKey(rawSnapshot.RawSnapshotID),
	}).Get(ctx, &featureSnapshot); err != nil {
		return nil, err
	}

	var embeddingSnapshot model.EmbeddingSnapshot
	if err := workflow.ExecuteActivity(ctx, MaterializeEmbeddingsActivityName, MaterializeEmbeddingsActivityInput{
		FeatureSnapshotID: featureSnapshot.FeatureSnapshotID,
		IdempotencyKey:    EmbeddingSnapshotIdempotencyKey(featureSnapshot.FeatureSnapshotID),
	}).Get(ctx, &embeddingSnapshot); err != nil {
		return nil, err
	}

	result := &MaterializeWorkflowResult{
		RawSnapshotID:       rawSnapshot.RawSnapshotID,
		FeatureSnapshotID:   featureSnapshot.FeatureSnapshotID,
		EmbeddingSnapshotID: embeddingSnapshot.EmbeddingSnapshotID,
	}
	logger.Info("MaterializeWorkflow completed", "dataset_id", input.DatasetFile.DatasetID.String())
	return result, nil
}
