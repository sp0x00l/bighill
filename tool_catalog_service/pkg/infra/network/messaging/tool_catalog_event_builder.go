package messaging

import (
	"fmt"

	toolcatalogpb "lib/data_contracts_lib/tool_catalog"
	msgConn "lib/shared_lib/messaging"
	shareduow "lib/shared_lib/uow"
	"tool_catalog_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type ToolCatalogEventBuilder struct {
	topic string
}

func NewToolCatalogEventBuilder(topic string) *ToolCatalogEventBuilder {
	log.Trace("NewToolCatalogEventBuilder")

	return &ToolCatalogEventBuilder{topic: topic}
}

func (b *ToolCatalogEventBuilder) CapabilityUpdatedMessage(capability *model.ToolCapabilityVersion) shareduow.OutboundMessage {
	log.Trace("ToolCatalogEventBuilder CapabilityUpdatedMessage")

	payload := mustMarshal(&toolcatalogpb.ToolCapabilityUpdatedEvent{
		EventId:               capability.CapabilityVersionID.String(),
		IdempotencyKey:        fmt.Sprintf("tool_capability:%s:%s", capability.CapabilityID, capability.ContentHash),
		CapabilityVersionId:   capability.CapabilityVersionID.String(),
		CapabilityId:          capability.CapabilityID,
		Version:               capability.Version,
		ToolName:              capability.ToolName,
		Kind:                  capability.Kind.String(),
		McpServerEndpoint:     capability.MCPServerEndpoint,
		Description:           capability.Description,
		ParametersJson:        append([]byte(nil), capability.ParametersJSON...),
		ImplementationVersion: capability.ImplementationVersion,
		EgressHosts:           append([]string(nil), capability.EgressHosts...),
		TimeoutMs:             capability.TimeoutMs,
		MaxResponseBytes:      capability.MaxResponseBytes,
		CredentialName:        capability.CredentialName,
		CredentialRequired:    capability.CredentialRequired,
		LifecycleStatus:       capability.LifecycleStatus.String(),
		ContentHash:           capability.ContentHash,
	})
	return msgConn.OutboundMessage{
		Topic: b.topic,
		Message: msgConn.Message{
			ResourceKey: capability.CapabilityVersionID,
			MsgType:     msgConn.MsgTypeToolCapabilityUpdated,
			Payload:     payload,
		},
		DispatchKey: fmt.Sprintf("tool_capability_updated:%s:%s", capability.CapabilityVersionID, capability.ContentHash),
	}
}

func (b *ToolCatalogEventBuilder) GrantUpdatedMessage(grant *model.TenantCapabilityGrant) shareduow.OutboundMessage {
	log.Trace("ToolCatalogEventBuilder GrantUpdatedMessage")

	payload := mustMarshal(&toolcatalogpb.ToolGrantUpdatedEvent{
		EventId:             grant.GrantID.String(),
		IdempotencyKey:      fmt.Sprintf("tool_grant:%s:%s", grant.OrgID, grant.CapabilityVersionID),
		GrantId:             grant.GrantID.String(),
		OrgId:               grant.OrgID.String(),
		CapabilityVersionId: grant.CapabilityVersionID.String(),
		Status:              grant.Status.String(),
	})
	return msgConn.OutboundMessage{
		Topic: b.topic,
		Message: msgConn.Message{
			ResourceKey: grant.OrgID,
			MsgType:     msgConn.MsgTypeToolGrantUpdated,
			Payload:     payload,
		},
		DispatchKey: fmt.Sprintf("tool_grant_updated:%s:%s:%s", grant.OrgID, grant.CapabilityVersionID, grant.Status),
	}
}

func (b *ToolCatalogEventBuilder) CredentialBindingUpdatedMessage(binding *model.ToolCredentialBinding) shareduow.OutboundMessage {
	log.Trace("ToolCatalogEventBuilder CredentialBindingUpdatedMessage")

	payload := mustMarshal(&toolcatalogpb.ToolCredentialBindingUpdatedEvent{
		EventId:        binding.BindingID.String(),
		IdempotencyKey: fmt.Sprintf("tool_credential_binding:%s:%s", binding.OrgID, binding.CapabilityID),
		BindingId:      binding.BindingID.String(),
		OrgId:          binding.OrgID.String(),
		CapabilityId:   binding.CapabilityID,
		CredentialRef:  binding.CredentialRef,
	})
	return msgConn.OutboundMessage{
		Topic: b.topic,
		Message: msgConn.Message{
			ResourceKey: binding.OrgID,
			MsgType:     msgConn.MsgTypeToolCredentialBindingUpdated,
			Payload:     payload,
		},
		DispatchKey: fmt.Sprintf("tool_credential_binding_updated:%s:%s", binding.OrgID, binding.CapabilityID),
	}
}

func mustMarshal(payload proto.Message) []byte {
	log.Trace("mustMarshal")

	out, err := proto.Marshal(payload)
	if err != nil {
		log.Fatalf("marshal tool catalog event: %v", err)
	}
	return out
}
