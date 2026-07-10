package messaging

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"model_registry_service/pkg/domain/model"

	modelregistrypb "lib/data_contracts_lib/model_registry"
	msgConn "lib/shared_lib/messaging"
	"lib/shared_lib/uuidutil"

	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type ModelEventBuilder struct {
	topic string
}

func NewModelEventBuilder(topic string) *ModelEventBuilder {
	log.Trace("NewModelEventBuilder")

	return &ModelEventBuilder{topic: topic}
}

func (b *ModelEventBuilder) ModelUpdatedMessage(modelRecord *model.Model) msgConn.OutboundMessage {
	log.Trace("ModelEventBuilder ModelUpdatedMessage")

	payload := mustMarshal(&modelregistrypb.ModelUpdatedEvent{
		ModelId:            modelRecord.ModelID.String(),
		OrgId:              uuidutil.StringOrEmpty(modelRecord.OrgID),
		TrainingRunId:      uuidutil.StringOrEmpty(modelRecord.TrainingRunID),
		DatasetId:          uuidutil.StringOrEmpty(modelRecord.DatasetID),
		ModelKind:          modelRecord.ModelKind.String(),
		Source:             modelRecord.Source.String(),
		SourceUri:          modelRecord.SourceURI,
		SourceMetadata:     withDefaultJSON(modelRecord.SourceMetadata),
		PromotionReportUri: modelRecord.PromotionReportURI,
		PromotionDeltas:    withDefaultJSON(modelRecord.PromotionDeltas),
		Name:               modelRecord.Name,
		ModelVersion:       int32(modelRecord.ModelVersion),
		BaseModel:          modelRecord.BaseModel,
		ArtifactLocation:   modelRecord.ArtifactLocation,
		ArtifactFormat:     modelRecord.ArtifactFormat,
		ArtifactChecksum:   modelRecord.ArtifactChecksum,
		ArtifactSizeBytes:  modelRecord.ArtifactSizeBytes,
		AdapterUri:         modelRecord.AdapterURI,
		ServingTarget:      modelRecord.ServingTarget,
		ServingModel:       modelRecord.ServingModel,
		ServingProtocol:    modelRecord.ServingProtocol.String(),
		ServingLoadStatus:  modelRecord.ServingLoadStatus.String(),
		MetricsMetadata:    modelRecord.MetricsMetadata,
		Status:             modelRecord.Status.String(),
		FailureReason:      modelRecord.FailureReason,
		UserId:             uuidutil.StringOrEmpty(modelRecord.UserID),
	})
	return msgConn.OutboundMessage{
		Topic: b.topic,
		Message: msgConn.Message{
			ResourceKey: modelRecord.ModelID,
			MsgType:     msgConn.MsgTypeModelUpdated,
			Payload:     payload,
		},
		DispatchKey: fmt.Sprintf("model_updated:%s:%s:%d:%s", modelRecord.ModelID, modelRecord.Status.String(), modelRecord.ModelVersion, payloadHash(payload)),
	}
}

func (b *ModelEventBuilder) PromotionRequestedMessage(candidate *model.Model, champion *model.Model) msgConn.OutboundMessage {
	log.Trace("ModelEventBuilder PromotionRequestedMessage")

	event := &modelregistrypb.PromotionRequestedEvent{
		UserId:                   uuidutil.StringOrEmpty(candidate.UserID),
		OrgId:                    uuidutil.StringOrEmpty(candidate.OrgID),
		ModelId:                  candidate.ModelID.String(),
		TrainingRunId:            uuidutil.StringOrEmpty(candidate.TrainingRunID),
		DatasetId:                uuidutil.StringOrEmpty(candidate.DatasetID),
		ModelName:                candidate.Name,
		ModelVersion:             int32(candidate.ModelVersion),
		CandidateReportUri:       candidateMetricsReportURI(candidate),
		CandidateMetricsMetadata: withDefaultJSON(candidate.MetricsMetadata),
	}
	if champion != nil {
		event.ChampionModelId = champion.ModelID.String()
		event.ChampionReportUri = candidateMetricsReportURI(champion)
		event.ChampionMetricsMetadata = withDefaultJSON(champion.MetricsMetadata)
	}
	payload := mustMarshal(event)
	return msgConn.OutboundMessage{
		Topic: b.topic,
		Message: msgConn.Message{
			ResourceKey: candidate.ModelID,
			MsgType:     msgConn.MsgTypePromotionRequested,
			Payload:     payload,
		},
		DispatchKey: fmt.Sprintf("promotion_requested:%s:%d", candidate.ModelID, candidate.ModelVersion),
	}
}

func candidateMetricsReportURI(modelRecord *model.Model) string {
	log.Trace("candidateMetricsReportURI")

	var decoded struct {
		ReportURI string `json:"report_uri"`
	}
	if err := json.Unmarshal([]byte(withDefaultJSON(modelRecord.MetricsMetadata)), &decoded); err != nil {
		return ""
	}
	return decoded.ReportURI
}

func withDefaultJSON(value string) string {
	log.Trace("withDefaultJSON")

	if value == "" {
		return "{}"
	}
	return value
}

func mustMarshal(payload proto.Message) []byte {
	log.Trace("mustMarshal")

	out, err := proto.Marshal(payload)
	if err != nil {
		log.Fatalf("marshal model event: %v", err)
	}
	return out
}

func payloadHash(payload []byte) string {
	log.Trace("payloadHash")

	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])[:16]
}
