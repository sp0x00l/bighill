package messaging

import (
	"context"
	"fmt"
	"strings"

	"training_service/pkg/domain/model"

	datasetpb "lib/data_contracts_lib/dataset"
	msgConn "lib/shared_lib/messaging"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type TrainingWorkflowStarter interface {
	StartTrainingWorkflow(ctx context.Context, request model.TrainingRunRequest) error
}

type DatasetUpdatedSubscriber interface {
	Start(ctx context.Context) error
}

type TrainingTopics struct {
	DataRegistry string
	Training     string
}

type datasetUpdatedSubscriber struct {
	subscriber msgConn.Subscriber
	starter    TrainingWorkflowStarter
	topics     TrainingTopics
	baseModel  string
}

func NewDatasetUpdatedSubscriber(subscriber msgConn.Subscriber, starter TrainingWorkflowStarter, topics TrainingTopics, baseModel string) DatasetUpdatedSubscriber {
	log.Trace("NewDatasetUpdatedSubscriber")

	return &datasetUpdatedSubscriber{
		subscriber: subscriber,
		starter:    starter,
		topics:     topics,
		baseModel:  baseModel,
	}
}

func (s *datasetUpdatedSubscriber) Start(ctx context.Context) error {
	log.Trace("datasetUpdatedSubscriber Start")

	msgConn.AddListener(s.subscriber, NewDatasetUpdatedEventListener(s.starter, s.baseModel))
	return s.subscriber.Subscribe(ctx, []string{s.topics.DataRegistry})
}

type datasetUpdatedEventListener struct {
	starter   TrainingWorkflowStarter
	baseModel string
}

func NewDatasetUpdatedEventListener(starter TrainingWorkflowStarter, baseModel string) *datasetUpdatedEventListener {
	log.Trace("NewDatasetUpdatedEventListener")

	return &datasetUpdatedEventListener{
		starter:   starter,
		baseModel: baseModel,
	}
}

func (l *datasetUpdatedEventListener) MsgType() msgConn.MsgType {
	log.Trace("datasetUpdatedEventListener MsgType")

	return msgConn.MsgTypeDatasetUpdated
}

func (l *datasetUpdatedEventListener) NewMessage() *datasetpb.DatasetUpdatedEvent {
	log.Trace("datasetUpdatedEventListener NewMessage")

	return &datasetpb.DatasetUpdatedEvent{}
}

func (l *datasetUpdatedEventListener) Handle(ctx context.Context, resourceKey uuid.UUID, payload *datasetpb.DatasetUpdatedEvent) error {
	log.Trace("datasetUpdatedEventListener Handle")

	if l.starter == nil {
		return msgConn.NonRetryable(fmt.Errorf("training workflow starter is nil"))
	}
	request, shouldStart, err := datasetUpdatedToTrainingRunRequest(resourceKey, payload, l.baseModel)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	if !shouldStart {
		return nil
	}
	return l.starter.StartTrainingWorkflow(ctx, request)
}

func datasetUpdatedToTrainingRunRequest(resourceKey uuid.UUID, payload *datasetpb.DatasetUpdatedEvent, baseModel string) (model.TrainingRunRequest, bool, error) {
	log.Trace("datasetUpdatedToTrainingRunRequest")

	if resourceKey == uuid.Nil {
		return model.TrainingRunRequest{}, false, fmt.Errorf("resource key is required")
	}
	if payload == nil {
		return model.TrainingRunRequest{}, false, fmt.Errorf("dataset updated payload is required")
	}
	datasetID, err := msgConn.ParseUUID("dataset_id", payload.GetDatasetId())
	if err != nil {
		return model.TrainingRunRequest{}, false, err
	}
	if datasetID != resourceKey {
		return model.TrainingRunRequest{}, false, fmt.Errorf("dataset id %s does not match resource key %s", datasetID, resourceKey)
	}

	state := strings.TrimSpace(payload.GetProcessingState())
	if state != "FEATURE_MATERIALIZED" && state != "EMBEDDINGS_MATERIALIZED" {
		return model.TrainingRunRequest{}, false, nil
	}
	if strings.TrimSpace(payload.GetTableFormat()) != "PARQUET" {
		return model.TrainingRunRequest{}, false, fmt.Errorf("training requires PARQUET dataset updates")
	}
	featureSnapshotID, err := msgConn.ParseUUID("feature_snapshot_id", payload.GetFeatureSnapshotId())
	if err != nil {
		return model.TrainingRunRequest{}, false, err
	}
	trainingRunID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(fmt.Sprintf("training:%s:%s:%d", datasetID, featureSnapshotID, payload.GetDatasetVersion())))
	modelName := strings.TrimSpace(payload.GetTableName())
	if modelName == "" {
		return model.TrainingRunRequest{}, false, fmt.Errorf("table name is required")
	}
	if strings.TrimSpace(baseModel) == "" {
		return model.TrainingRunRequest{}, false, fmt.Errorf("base model is required")
	}
	return model.TrainingRunRequest{
		TrainingRunID:     trainingRunID.String(),
		DatasetID:         datasetID.String(),
		DatasetVersion:    fmt.Sprintf("%d", payload.GetDatasetVersion()),
		FeatureSnapshotID: featureSnapshotID.String(),
		ModelName:         modelName,
		ModelVersion:      fmt.Sprintf("%d", payload.GetDatasetVersion()),
		BaseModel:         baseModel,
		EvaluationProfile: "smoke",
	}, true, nil
}
