package messaging_test

import (
	"testing"
	"time"

	"agent_registry_service/pkg/domain/model"
	agentmessaging "agent_registry_service/pkg/infra/network/messaging"
	agentregistrypb "lib/data_contracts_lib/agent_registry"
	msgConn "lib/shared_lib/messaging"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
)

func TestAgentRegistryMessaging(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Agent registry messaging unit test suite")
}

var _ = Describe("AgentRegistryEventBuilder", func() {
	It("builds champion update events keyed by endpoint and decision", func() {
		orgID := uuid.New()
		endpointID := uuid.New()
		decisionID := uuid.New()
		decidedBy := uuid.New()
		adapterID := uuid.New()
		servingModelID := uuid.New()
		decidedAt := time.Date(2026, 7, 18, 11, 0, 0, 0, time.UTC)
		builder := agentmessaging.NewAgentRegistryEventBuilder("agent_registry")

		message := builder.AgentChampionUpdatedMessage(&model.AgentChampionState{
			OrgID:                 orgID,
			AgentLineage:          "support-agent",
			ChampionAgentSpecHash: "sha256-new",
			ChampionAdapterID:     adapterID,
			ServingModelID:        servingModelID,
			PreviousAgentSpecHash: "sha256-old",
			DecisionID:            decisionID,
			DecidedBy:             decidedBy,
			DecidedAt:             decidedAt,
		}, &model.AgentEndpointBinding{
			OrgID:        orgID,
			AgentLineage: "support-agent",
			EndpointID:   endpointID,
		})

		Expect(message.Topic).To(Equal("agent_registry"))
		Expect(message.DispatchKey).To(Equal("agent_champion_updated:" + endpointID.String() + ":" + decisionID.String()))
		Expect(message.Message.MsgType).To(Equal(msgConn.MsgTypeAgentChampionUpdated))
		Expect(message.Message.ResourceKey).To(Equal(endpointID))
		payload := &agentregistrypb.AgentChampionUpdatedEvent{}
		Expect(proto.Unmarshal(message.Message.Payload, payload)).To(Succeed())
		Expect(payload.GetOrgId()).To(Equal(orgID.String()))
		Expect(payload.GetEndpointId()).To(Equal(endpointID.String()))
		Expect(payload.GetAgentLineage()).To(Equal("support-agent"))
		Expect(payload.GetAgentSpecHash()).To(Equal("sha256-new"))
		Expect(payload.GetPreviousAgentSpecHash()).To(Equal("sha256-old"))
		Expect(payload.GetChampionAdapterId()).To(Equal(adapterID.String()))
		Expect(payload.GetServingModelId()).To(Equal(servingModelID.String()))
		Expect(payload.GetDecisionId()).To(Equal(decisionID.String()))
		Expect(payload.GetDecidedAt()).To(Equal(decidedAt.Format(time.RFC3339Nano)))
	})
})
