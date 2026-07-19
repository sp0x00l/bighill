package messaging

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type MsgType int

const (
	MsgTypeUnknown MsgType = iota
	MsgTypeUserCreated
	MsgTypeUserUpdated
	MsgTypeUserDeleted
	msgTypeEmailVerificationRequestedReserved
	MsgTypeDatasetFileUploaded
	MsgTypeRawSnapshotReady
	msgTypeFeatureSnapshotBuildRequestedDeprecated
	MsgTypeFeatureSnapshotReady
	msgTypeEmbeddingMaterializationRequestedDeprecated
	MsgTypeEmbeddingSnapshotReady
	MsgTypeDatasetCreated
	MsgTypeDatasetDeleted
	MsgTypeDatasetUpdated
	MsgTypeModelTrainingCompleted
	MsgTypeModelTrainingFailed
	MsgTypeModelUpdated
	msgTypePreferenceDatasetReadyDeprecated
	MsgTypeModelArtifactIngested
	MsgTypePromotionRequested
	MsgTypePromotionReportReady
	MsgTypeGraphSnapshotReady
	MsgTypeAgentChampionUpdated
	MsgTypeToolCapabilityUpdated
	MsgTypeToolGrantUpdated
	MsgTypeToolCredentialBindingUpdated
)

var msgType = map[MsgType]string{
	MsgTypeUserCreated:                  "user_created",
	MsgTypeUserUpdated:                  "user_updated",
	MsgTypeUserDeleted:                  "user_deleted",
	MsgTypeDatasetFileUploaded:          "dataset_file_uploaded",
	MsgTypeRawSnapshotReady:             "raw_snapshot_ready",
	MsgTypeFeatureSnapshotReady:         "feature_snapshot_ready",
	MsgTypeEmbeddingSnapshotReady:       "embedding_snapshot_ready",
	MsgTypeDatasetCreated:               "dataset_created",
	MsgTypeDatasetDeleted:               "dataset_deleted",
	MsgTypeDatasetUpdated:               "dataset_updated",
	MsgTypeModelTrainingCompleted:       "model_training_completed",
	MsgTypeModelTrainingFailed:          "model_training_failed",
	MsgTypeModelUpdated:                 "model_updated",
	MsgTypeModelArtifactIngested:        "model_artifact_ingested",
	MsgTypePromotionRequested:           "promotion_requested",
	MsgTypePromotionReportReady:         "promotion_report_ready",
	MsgTypeGraphSnapshotReady:           "graph_snapshot_ready",
	MsgTypeAgentChampionUpdated:         "agent_champion_updated",
	MsgTypeToolCapabilityUpdated:        "tool_capability_updated",
	MsgTypeToolGrantUpdated:             "tool_grant_updated",
	MsgTypeToolCredentialBindingUpdated: "tool_credential_binding_updated",
}

func (m MsgType) String() string {
	return msgType[m]
}

func MsgTypeFromString(s string) MsgType {
	msgType, err := MsgTypeFromStringChecked(s)
	if err != nil {
		log.Fatalf("resolve message type: %v", err)
	}
	return msgType
}

func MsgTypeFromStringChecked(s string) (MsgType, error) {
	for k, v := range msgType {
		if v == s {
			return k, nil
		}
	}
	return MsgTypeUnknown, fmt.Errorf("unknown message type: %s", s)
}

type Message struct {
	ResourceKey uuid.UUID `json:"resourceKey"`
	MsgType     MsgType   `json:"eventType"`
	Payload     []byte    `json:"payload"`
}

// ResourceKey is the message's ordering key. Account-scoped events should use
// accountID so all mutations for the same account serialize together; events
// with a narrower authority may use their own aggregate ID.

func (m *Message) Serialize(ctx context.Context, payload proto.Message) ([]byte, error) {
	if payload != nil {
		payloadBytes, err := proto.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("%w: marshal payload: %w", ErrEnvelopeInvalid, err)
		}
		m.Payload = payloadBytes
	}
	return m.SerializeEnvelope(ctx)
}

func (m Message) SerializeEnvelope(_ context.Context) ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(m)
}

func (m *Message) Deserialize(ctx context.Context, data []byte) error {
	if err := json.Unmarshal(data, m); err != nil {
		return fmt.Errorf("%w: %w", ErrEnvelopeInvalid, err)
	}
	return m.Validate()
}

func (m *Message) DeserializePayload(target proto.Message) error {
	return proto.Unmarshal(m.Payload, target)
}

func (m Message) Validate() error {
	if m.ResourceKey == uuid.Nil {
		return fmt.Errorf("%w: resource_key required", ErrEnvelopeInvalid)
	}
	if m.MsgType == MsgTypeUnknown {
		return fmt.Errorf("%w: msg_type required", ErrEnvelopeInvalid)
	}
	if len(m.Payload) == 0 {
		return fmt.Errorf("%w: payload required", ErrEnvelopeInvalid)
	}
	return nil
}
