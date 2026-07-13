package network

import (
	"context"

	"socket_service/pkg/domain"

	"lib/shared_lib/userevents"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("protocolDTOAdapter", func() {
	It("maps a valid hello DTO into a domain message", func() {
		adapter := NewProtocolDTOAdapter()

		message, err := adapter.FromDTO(context.Background(), []byte(`{
			"type":"hello",
			"last_cursors":{"mlops:user:u1:events":"1-0"},
			"filters":[{"resource_type":"model","resource_id":"m1"}]
		}`))

		Expect(err).NotTo(HaveOccurred())
		Expect(message.Type).To(Equal(domain.ClientMessageTypeHello))
		Expect(message.LastCursors).To(HaveKeyWithValue("mlops:user:u1:events", "1-0"))
		Expect(message.Filters).To(HaveLen(1))
		Expect(message.Filters[0].ResourceType).To(Equal("model"))
	})

	It("rejects an unknown client message type", func() {
		adapter := NewProtocolDTOAdapter()

		_, err := adapter.FromDTO(context.Background(), []byte(`{"type":"join"}`))

		Expect(err).To(MatchError(ContainSubstring("validation failed")))
	})

	It("maps a domain server message to a DTO payload", func() {
		adapter := NewProtocolDTOAdapter()

		payload, err := adapter.ToDTO(context.Background(), domain.ServerMessage{
			Type:     domain.ServerMessageTypeEvent,
			Stream:   "mlops:user:u1:events",
			StreamID: "1-0",
			Event: &userevents.Event{
				EventID:       "event-1",
				OccurredAt:    testTime(),
				SourceService: "model_registry_service",
				EventType:     userevents.EventTypeModelServingLoaded,
				Severity:      userevents.SeveritySuccess,
				UserID:        "u1",
				Resource:      userevents.Resource{Type: userevents.ResourceTypeModel, ID: "m1"},
				Title:         "Model ready",
				Message:       "The model is ready for inference.",
			},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(payload).To(MatchJSON(`{
			"type":"event",
			"stream":"mlops:user:u1:events",
			"stream_id":"1-0",
			"event":{
				"event_id":"event-1",
				"occurred_at":"2026-07-13T10:00:00Z",
				"source_service":"model_registry_service",
				"event_type":"model.serving.loaded",
				"severity":"SUCCESS",
				"user_id":"u1",
				"resource":{"type":"model","id":"m1"},
				"status":{},
				"title":"Model ready",
				"message":"The model is ready for inference."
			}
		}`))
	})

	It("rejects an unknown outbound server message code", func() {
		adapter := NewProtocolDTOAdapter()

		_, err := adapter.ToDTO(context.Background(), domain.ServerMessage{
			Type: domain.ServerMessageTypeError,
			Code: domain.ServerMessageCode("nope"),
		})

		Expect(err).To(MatchError(ContainSubstring("validation failed")))
	})
})
