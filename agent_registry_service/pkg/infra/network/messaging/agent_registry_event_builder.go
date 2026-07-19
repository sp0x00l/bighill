package messaging

import (
	"fmt"

	"agent_registry_service/pkg/domain/model"
	agentregistrypb "lib/data_contracts_lib/agent_registry"
	msgConn "lib/shared_lib/messaging"
	shareduow "lib/shared_lib/uow"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type AgentRegistryEventBuilder struct {
	topic string
}

func NewAgentRegistryEventBuilder(topic string) *AgentRegistryEventBuilder {
	log.Trace("NewAgentRegistryEventBuilder")

	return &AgentRegistryEventBuilder{topic: topic}
}

func (b *AgentRegistryEventBuilder) AgentChampionUpdatedMessage(state *model.AgentChampionState, binding *model.AgentEndpointBinding) shareduow.OutboundMessage {
	log.Trace("AgentRegistryEventBuilder AgentChampionUpdatedMessage")

	payload := mustMarshal(&agentregistrypb.AgentChampionUpdatedEvent{
		EventId:               state.DecisionID.String(),
		IdempotencyKey:        state.DecisionID.String(),
		OrgId:                 state.OrgID.String(),
		AgentLineage:          state.AgentLineage,
		EndpointId:            binding.EndpointID.String(),
		AgentSpecHash:         state.ChampionAgentSpecHash,
		PreviousAgentSpecHash: state.PreviousAgentSpecHash,
		DecisionId:            state.DecisionID.String(),
		DecidedAt:             state.DecidedAt.UTC().Format(timeRFC3339Nano),
		ChampionAdapterId:     optionalUUID(state.ChampionAdapterID),
		ServingModelId:        optionalUUID(state.ServingModelID),
	})
	return msgConn.OutboundMessage{
		Topic: b.topic,
		Message: msgConn.Message{
			ResourceKey: binding.EndpointID,
			MsgType:     msgConn.MsgTypeAgentChampionUpdated,
			Payload:     payload,
		},
		DispatchKey: fmt.Sprintf("agent_champion_updated:%s:%s", binding.EndpointID, state.DecisionID),
	}
}

func optionalUUID(value uuid.UUID) string {
	if value == uuid.Nil {
		return ""
	}
	return value.String()
}

const timeRFC3339Nano = "2006-01-02T15:04:05.999999999Z07:00"

func mustMarshal(payload proto.Message) []byte {
	log.Trace("mustMarshal")

	out, err := proto.Marshal(payload)
	if err != nil {
		log.Fatalf("marshal agent registry event: %v", err)
	}
	return out
}
