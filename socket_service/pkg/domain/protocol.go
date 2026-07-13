package domain

import (
	"lib/shared_lib/userevents"

	log "github.com/sirupsen/logrus"
)

type ClientMessageType string

const (
	ClientMessageTypeHello     ClientMessageType = "hello"
	ClientMessageTypeSubscribe ClientMessageType = "subscribe"
	ClientMessageTypeAck       ClientMessageType = "ack"
	ClientMessageTypePing      ClientMessageType = "ping"
)

func (t ClientMessageType) IsKnown() bool {
	log.Trace("ClientMessageType IsKnown")

	switch t {
	case ClientMessageTypeHello,
		ClientMessageTypeSubscribe,
		ClientMessageTypeAck,
		ClientMessageTypePing:
		return true
	default:
		return false
	}
}

type ServerMessageType string

const (
	ServerMessageTypeReady          ServerMessageType = "ready"
	ServerMessageTypeEvent          ServerMessageType = "event"
	ServerMessageTypeReplayComplete ServerMessageType = "replay_complete"
	ServerMessageTypeWarning        ServerMessageType = "warning"
	ServerMessageTypePong           ServerMessageType = "pong"
	ServerMessageTypeError          ServerMessageType = "error"
)

func (t ServerMessageType) IsKnown() bool {
	log.Trace("ServerMessageType IsKnown")

	switch t {
	case ServerMessageTypeReady,
		ServerMessageTypeEvent,
		ServerMessageTypeReplayComplete,
		ServerMessageTypeWarning,
		ServerMessageTypePong,
		ServerMessageTypeError:
		return true
	default:
		return false
	}
}

type ServerMessageCode string

const (
	ServerMessageCodeReplayTruncated ServerMessageCode = "replay_truncated"
	ServerMessageCodeInvalidMessage  ServerMessageCode = "invalid_message"
	ServerMessageCodeUnauthorized    ServerMessageCode = "unauthorized"
	ServerMessageCodeInternalError   ServerMessageCode = "internal_error"
)

func (c ServerMessageCode) IsKnown() bool {
	log.Trace("ServerMessageCode IsKnown")

	switch c {
	case ServerMessageCodeReplayTruncated,
		ServerMessageCodeInvalidMessage,
		ServerMessageCodeUnauthorized,
		ServerMessageCodeInternalError:
		return true
	default:
		return false
	}
}

type ClientMessage struct {
	Type        ClientMessageType
	LastCursors map[string]string
	LastEventID string
	Filters     []Filter
	EventID     string
}

type Filter struct {
	ResourceType string
	ResourceID   string
}

type ServerMessage struct {
	Type         ServerMessageType
	ConnectionID string
	ServerTime   string
	Stream       string
	StreamID     string
	Event        *userevents.Event
	Code         ServerMessageCode
	Message      string
	Cursors      map[string]string
}
