package messaging

import (
	"strings"

	"inference_service/pkg/domain/model"

	inferencepb "lib/data_contracts_lib/inference"
	msgConn "lib/shared_lib/messaging"

	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type PreferenceDatasetEventBuilder struct {
	topic string
}

func NewPreferenceDatasetEventBuilder(topic string) *PreferenceDatasetEventBuilder {
	log.Trace("NewPreferenceDatasetEventBuilder")

	return &PreferenceDatasetEventBuilder{topic: topic}
}

func (b *PreferenceDatasetEventBuilder) PreferenceDatasetReadyMessage(dataset *model.PreferenceDataset, request model.PreferenceDatasetExportRequest) msgConn.OutboundMessage {
	log.Trace("PreferenceDatasetEventBuilder PreferenceDatasetReadyMessage")

	payload := mustMarshal(&inferencepb.PreferenceDatasetReadyEvent{
		PreferenceDatasetId: dataset.PreferenceDatasetID.String(),
		UserId:              dataset.UserID.String(),
		OrgId:               dataset.OrgID.String(),
		DatasetId:           dataset.DatasetID.String(),
		ModelId:             dataset.ModelID.String(),
		SourceRequestId:     dataset.RequestID.String(),
		OutputUri:           strings.TrimSpace(dataset.OutputURI),
		EvaluationOutputUri: strings.TrimSpace(dataset.EvaluationOutputURI),
		ExampleCount:        int32(dataset.ExampleCount()),
		Format:              strings.TrimSpace(dataset.Format),
		MinExamples:         int32(request.MinExamples),
		Limit:               int32(request.Limit),
		ParentAdapterUri:    strings.TrimSpace(dataset.ParentAdapterURI),
		ParentBaseModel:     strings.TrimSpace(dataset.ParentBaseModel),
		ParentModelVersion:  int32(dataset.ParentModelVersion),
	})
	return msgConn.OutboundMessage{
		Topic: b.topic,
		Message: msgConn.Message{
			ResourceKey: dataset.DatasetID,
			MsgType:     msgConn.MsgTypePreferenceDatasetReady,
			Payload:     payload,
		},
		DispatchKey: "preference_dataset_ready:" + dataset.PreferenceDatasetID.String(),
	}
}

func mustMarshal(payload proto.Message) []byte {
	log.Trace("mustMarshal")

	out, err := proto.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return out
}
