package network

import (
	"context"
	"encoding/json"
	"time"

	"socket_service/pkg/domain"

	"github.com/go-playground/validator/v10"
	log "github.com/sirupsen/logrus"
	"lib/shared_lib/userevents"
)

type clientMessageDTO struct {
	Type        string            `json:"type" validate:"required,oneof=hello subscribe ack ping"`
	LastCursors map[string]string `json:"last_cursors,omitempty"`
	LastEventID string            `json:"last_event_id,omitempty"`
	Filters     []filterDTO       `json:"filters,omitempty" validate:"dive"`
	EventID     string            `json:"event_id,omitempty"`
}

type filterDTO struct {
	ResourceType string `json:"resource_type" validate:"omitempty,max=100"`
	ResourceID   string `json:"resource_id"   validate:"omitempty,max=250"`
}

type serverMessageDTO struct {
	Type         string            `json:"type" validate:"required,oneof=ready event replay_complete warning pong error"`
	ConnectionID string            `json:"connection_id,omitempty"`
	ServerTime   string            `json:"server_time,omitempty"`
	Stream       string            `json:"stream,omitempty"`
	StreamID     string            `json:"stream_id,omitempty"`
	Event        *userevents.Event `json:"event,omitempty"`
	Code         string            `json:"code,omitempty" validate:"omitempty,oneof=replay_truncated invalid_message unauthorized internal_error"`
	Message      string            `json:"message,omitempty"`
	Cursors      map[string]string `json:"cursors,omitempty"`
}

type socketTicketDTO struct {
	Token     string `json:"token" validate:"required"`
	ExpiresAt string `json:"expires_at" validate:"required"`
}

type protocolDTOAdapter struct {
	validator *validator.Validate
}

func NewProtocolDTOAdapter() *protocolDTOAdapter {
	log.Trace("NewProtocolDTOAdapter")

	return &protocolDTOAdapter{validator: validator.New()}
}

func (a *protocolDTOAdapter) FromDTO(ctx context.Context, payload []byte) (domain.ClientMessage, error) {
	log.Trace("protocolDTOAdapter FromDTO")

	var dto clientMessageDTO
	if err := json.Unmarshal(payload, &dto); err != nil {
		log.WithContext(ctx).WithError(err).Warn("clientMessageDTO decode failed")
		return domain.ClientMessage{}, domain.ErrValidationFailed.Extend("invalid client message")
	}
	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Warn("clientMessageDTO validation failed")
		return domain.ClientMessage{}, domain.ErrValidationFailed.Extend("invalid client message")
	}
	message := domain.ClientMessage{
		Type:        domain.ClientMessageType(dto.Type),
		LastCursors: dto.LastCursors,
		LastEventID: dto.LastEventID,
		Filters:     filtersFromDTO(dto.Filters),
		EventID:     dto.EventID,
	}
	if !message.Type.IsKnown() {
		return domain.ClientMessage{}, domain.ErrValidationFailed.Extend("unknown client message type")
	}
	return message, nil
}

func (a *protocolDTOAdapter) ToDTO(ctx context.Context, message domain.ServerMessage) ([]byte, error) {
	log.Trace("protocolDTOAdapter ToDTO")

	dto := serverMessageDTO{
		Type:         string(message.Type),
		ConnectionID: message.ConnectionID,
		ServerTime:   message.ServerTime,
		Stream:       message.Stream,
		StreamID:     message.StreamID,
		Event:        message.Event,
		Code:         string(message.Code),
		Message:      message.Message,
		Cursors:      message.Cursors,
	}
	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Warn("serverMessageDTO validation failed")
		return nil, domain.ErrValidationFailed.Extend("invalid server message")
	}
	if !message.Type.IsKnown() {
		return nil, domain.ErrValidationFailed.Extend("unknown server message type")
	}
	if message.Code != "" && !message.Code.IsKnown() {
		return nil, domain.ErrValidationFailed.Extend("unknown server message code")
	}
	payload, err := json.Marshal(dto)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

func (a *protocolDTOAdapter) ToSocketTicketDTO(ctx context.Context, ticket domain.SocketTicket) ([]byte, error) {
	log.Trace("protocolDTOAdapter ToSocketTicketDTO")

	dto := socketTicketDTO{
		Token:     ticket.Token,
		ExpiresAt: ticket.ExpiresAt.UTC().Format(time.RFC3339),
	}
	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Warn("socketTicketDTO validation failed")
		return nil, domain.ErrValidationFailed.Extend("invalid socket ticket")
	}
	payload, err := json.Marshal(dto)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("socketTicketDTO encode failed")
		return nil, domain.ErrValidationFailed.Extend("invalid socket ticket")
	}
	return payload, nil
}

func filtersFromDTO(filters []filterDTO) []domain.Filter {
	log.Trace("filtersFromDTO")

	out := make([]domain.Filter, 0, len(filters))
	for _, filter := range filters {
		out = append(out, domain.Filter{
			ResourceType: filter.ResourceType,
			ResourceID:   filter.ResourceID,
		})
	}
	return out
}
