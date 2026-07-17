package temporalworker

import (
	"context"
	"errors"

	usecase "feature_materializer_service/pkg/app"
	"feature_materializer_service/pkg/domain/model"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

type MaterializationWorkflowStarter struct {
	temporalClient client.Client
	taskQueue      string
	strategy       model.EmbeddingStrategy
	graphConfig    usecase.GraphWorkflowConfig
}

func NewMaterializationWorkflowStarter(temporalClient client.Client, taskQueue string, strategy model.EmbeddingStrategy, graphConfig usecase.GraphWorkflowConfig) *MaterializationWorkflowStarter {
	log.Trace("NewMaterializationWorkflowStarter")

	return &MaterializationWorkflowStarter{
		temporalClient: temporalClient,
		taskQueue:      taskQueue,
		strategy:       model.NormalizeEmbeddingStrategy(strategy),
		graphConfig:    graphConfig,
	}
}

func (s *MaterializationWorkflowStarter) StartMaterializationWorkflow(ctx context.Context, datasetFile *model.DatasetFile, rawIdempotencyKey uuid.UUID) error {
	log.Trace("MaterializationWorkflowStarter StartMaterializationWorkflow")

	if datasetFile == nil {
		return nil
	}

	workflowID := usecase.MaterializeWorkflowID(datasetFile.DatasetID, rawIdempotencyKey)
	_, err := s.temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             s.taskQueue,
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
	}, usecase.MaterializeWorkflowName, usecase.MaterializeWorkflowInput{
		DatasetFile:             *datasetFile,
		RawIdempotencyKey:       rawIdempotencyKey,
		EmbeddingStrategy:       s.strategy,
		GraphEnabled:            s.graphConfig.Enabled,
		GraphExtractionStrategy: s.graphConfig.Strategy,
	})
	if err == nil {
		return nil
	}

	var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
	if errors.As(err, &alreadyStarted) {
		return nil
	}
	return err
}
