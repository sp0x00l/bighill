package messaging

import (
	"encoding/json"
	"fmt"

	"training_service/pkg/domain/model"

	trainingpb "lib/data_contracts_lib/training"
	msgConn "lib/shared_lib/messaging"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type TrainingEventBuilder struct {
	topic string
}

func NewTrainingEventBuilder(topic string) *TrainingEventBuilder {
	log.Trace("NewTrainingEventBuilder")

	return &TrainingEventBuilder{topic: topic}
}

func (b *TrainingEventBuilder) ModelTrainingCompletedMessage(result *model.TrainingRunResult) (msgConn.OutboundMessage, error) {
	log.Trace("TrainingEventBuilder ModelTrainingCompletedMessage")

	datasetID, modelID, userID, orgID, err := parseTrainingResultIDs(result)
	if err != nil {
		return msgConn.OutboundMessage{}, err
	}
	payload := marshalTrainingEvent(&trainingpb.ModelTrainingCompletedEvent{
		TrainingRunId:     result.TrainingRunID,
		DatasetId:         result.DatasetID,
		DatasetVersion:    result.DatasetVersion,
		FeatureSnapshotId: result.FeatureSnapshotID,
		ModelId:           modelID.String(),
		ModelName:         result.ModelName,
		LineageName:       trainingResultLineageName(result),
		ModelVersion:      result.ModelVersion,
		BaseModel:         result.BaseModel,
		ArtifactLocation:  result.ModelURI,
		ArtifactFormat:    result.ArtifactFormat,
		ArtifactChecksum:  result.ArtifactChecksum,
		ArtifactSizeBytes: result.ArtifactSizeBytes,
		MetricsMetadata:   result.MetricsMetadata,
		ReportLocation:    result.ReportURI,
		AdapterUri:        result.AdapterURI,
		ServingTarget:     result.ServingTarget,
		ServingModel:      result.ServingModel,
		ServingLoadStatus: result.ServingLoadStatus,
		UserId:            userID.String(),
		OrgId:             orgID.String(),
		AdapterRank:       int32(result.AdapterRank),
	})
	return msgConn.OutboundMessage{
		Topic: b.topic,
		Message: msgConn.Message{
			ResourceKey: datasetID,
			MsgType:     msgConn.MsgTypeModelTrainingCompleted,
			Payload:     payload,
		},
		DispatchKey: "model_training_completed:" + result.TrainingRunID,
	}, nil
}

func (b *TrainingEventBuilder) ModelTrainingFailedMessage(result *model.TrainingRunResult) (msgConn.OutboundMessage, error) {
	log.Trace("TrainingEventBuilder ModelTrainingFailedMessage")

	datasetID, modelID, userID, orgID, err := parseTrainingResultIDs(result)
	if err != nil {
		return msgConn.OutboundMessage{}, err
	}
	payload := marshalTrainingEvent(&trainingpb.ModelTrainingFailedEvent{
		TrainingRunId:     result.TrainingRunID,
		DatasetId:         result.DatasetID,
		DatasetVersion:    result.DatasetVersion,
		FeatureSnapshotId: result.FeatureSnapshotID,
		ModelId:           modelID.String(),
		ModelName:         result.ModelName,
		LineageName:       trainingResultLineageName(result),
		ModelVersion:      result.ModelVersion,
		BaseModel:         result.BaseModel,
		FailureReason:     result.FailureReason,
		UserId:            userID.String(),
		OrgId:             orgID.String(),
	})
	return msgConn.OutboundMessage{
		Topic: b.topic,
		Message: msgConn.Message{
			ResourceKey: datasetID,
			MsgType:     msgConn.MsgTypeModelTrainingFailed,
			Payload:     payload,
		},
		DispatchKey: "model_training_failed:" + result.TrainingRunID,
	}, nil
}

func trainingResultLineageName(result *model.TrainingRunResult) string {
	log.Trace("trainingResultLineageName")

	if result == nil {
		return ""
	}
	if result.LineageName != "" {
		return result.LineageName
	}
	return result.ModelName
}

func (b *TrainingEventBuilder) PromotionReportReadyMessage(report *model.PromotionReport) (msgConn.OutboundMessage, error) {
	log.Trace("TrainingEventBuilder PromotionReportReadyMessage")

	if report == nil {
		return msgConn.OutboundMessage{}, fmt.Errorf("promotion report is required")
	}
	modelID, err := uuid.Parse(report.ModelID)
	if err != nil || modelID == uuid.Nil {
		return msgConn.OutboundMessage{}, fmt.Errorf("model id is invalid: %w", err)
	}
	userID, err := uuid.Parse(report.UserID)
	if err != nil || userID == uuid.Nil {
		return msgConn.OutboundMessage{}, fmt.Errorf("user id is invalid: %w", err)
	}
	orgID, err := uuid.Parse(report.OrgID)
	if err != nil || orgID == uuid.Nil {
		return msgConn.OutboundMessage{}, fmt.Errorf("org id is invalid: %w", err)
	}
	trainingRunID, err := uuid.Parse(report.TrainingRunID)
	if err != nil || trainingRunID == uuid.Nil {
		return msgConn.OutboundMessage{}, fmt.Errorf("training run id is invalid: %w", err)
	}
	deltas, err := marshalPromotionDeltas(report.Deltas)
	if err != nil {
		return msgConn.OutboundMessage{}, err
	}
	payload := marshalTrainingEvent(&trainingpb.PromotionReportReadyEvent{
		UserId:              userID.String(),
		OrgId:               orgID.String(),
		ModelId:             modelID.String(),
		TrainingRunId:       trainingRunID.String(),
		PromotionReportUri:  report.PromotionReportURI,
		DeepchecksPassed:    report.DeepchecksPassed,
		DeepchecksReportUri: report.DeepchecksReportURI,
		EvidentlyPassed:     report.EvidentlyPassed,
		EvidentlyReportUri:  report.EvidentlyReportURI,
		PromotionDeltas:     deltas,
		FailureReason:       report.FailureReason,
	})
	return msgConn.OutboundMessage{
		Topic: b.topic,
		Message: msgConn.Message{
			ResourceKey: modelID,
			MsgType:     msgConn.MsgTypePromotionReportReady,
			Payload:     payload,
		},
		DispatchKey: "promotion_report_ready:" + report.ModelID,
	}, nil
}

func parseTrainingResultIDs(result *model.TrainingRunResult) (uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID, error) {
	log.Trace("parseTrainingResultIDs")

	if result == nil {
		return uuid.Nil, uuid.Nil, uuid.Nil, uuid.Nil, fmt.Errorf("training result is required")
	}
	datasetID, err := uuid.Parse(result.DatasetID)
	if err != nil || datasetID == uuid.Nil {
		return uuid.Nil, uuid.Nil, uuid.Nil, uuid.Nil, fmt.Errorf("dataset id is invalid: %w", err)
	}
	modelID, err := uuid.Parse(result.ModelID)
	if err != nil || modelID == uuid.Nil {
		return uuid.Nil, uuid.Nil, uuid.Nil, uuid.Nil, fmt.Errorf("model id is invalid: %w", err)
	}
	userID, err := uuid.Parse(result.UserID)
	if err != nil || userID == uuid.Nil {
		return uuid.Nil, uuid.Nil, uuid.Nil, uuid.Nil, fmt.Errorf("user id is invalid: %w", err)
	}
	orgID, err := uuid.Parse(result.OrgID)
	if err != nil || orgID == uuid.Nil {
		return uuid.Nil, uuid.Nil, uuid.Nil, uuid.Nil, fmt.Errorf("org id is invalid: %w", err)
	}
	return datasetID, modelID, userID, orgID, nil
}

func marshalPromotionDeltas(deltas map[string]float64) (string, error) {
	log.Trace("marshalPromotionDeltas")

	if len(deltas) == 0 {
		return "{}", nil
	}
	raw, err := json.Marshal(deltas)
	if err != nil {
		return "", fmt.Errorf("marshal promotion deltas: %w", err)
	}
	return string(raw), nil
}

func marshalTrainingEvent(payload proto.Message) []byte {
	log.Trace("marshalTrainingEvent")

	out, err := proto.Marshal(payload)
	if err != nil {
		log.Fatalf("marshal training event: %v", err)
	}
	return out
}
