package network

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"

	"socket_service/pkg/app"
	"socket_service/pkg/domain"

	"lib/shared_lib/authz"
	"lib/shared_lib/userevents"

	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
)

const (
	websocketWriteMessageType = websocket.TextMessage
)

type WebSocketConfig struct {
	ReplayLimit     int
	SendQueueSize   int
	TicketTTL       time.Duration
	AllowedOrigins  []string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	PingInterval    time.Duration
	PongTimeout     time.Duration
	MaxMessageBytes int64
}

type WebSocketHandler struct {
	usecase  app.SubscriptionUsecase
	adapter  *protocolDTOAdapter
	config   WebSocketConfig
	upgrader websocket.Upgrader
}

func NewWebSocketHandler(usecase app.SubscriptionUsecase, adapter *protocolDTOAdapter, config WebSocketConfig) *WebSocketHandler {
	log.Trace("NewWebSocketHandler")

	return &WebSocketHandler{
		usecase:  usecase,
		adapter:  adapter,
		config:   config,
		upgrader: websocket.Upgrader{CheckOrigin: checkOrigin(config.AllowedOrigins)},
	}
}

func (h *WebSocketHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	log.Trace("WebSocketHandler ServeHTTP")

	ctx := request.Context()
	subscription, err := h.usecase.Open(ctx, socketTicket(request))
	if err != nil {
		httpErr := mapSocketHTTPError(err)
		http.Error(response, httpErr.message, httpErr.statusCode)
		return
	}

	conn, err := h.upgrader.Upgrade(response, request, nil)
	if err != nil {
		log.WithContext(ctx).WithError(err).Warn("websocket upgrade failed")
		return
	}
	defer conn.Close()

	h.serveConnection(ctx, conn, subscription)
}

func (h *WebSocketHandler) ServeTokenHTTP(response http.ResponseWriter, request *http.Request) {
	log.Trace("WebSocketHandler ServeTokenHTTP")

	ctx := request.Context()
	if request.Method != http.MethodPost {
		http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	session, err := sessionFromTrustedHeaders(request, h.config.TicketTTL)
	if err != nil {
		httpErr := mapSocketHTTPError(err)
		http.Error(response, httpErr.message, httpErr.statusCode)
		return
	}
	ticket, err := h.usecase.IssueTicket(ctx, session, h.config.TicketTTL)
	if err != nil {
		httpErr := mapSocketHTTPError(err)
		http.Error(response, httpErr.message, httpErr.statusCode)
		return
	}
	payload, err := h.adapter.ToSocketTicketDTO(ctx, ticket)
	if err != nil {
		httpErr := mapSocketHTTPError(err)
		http.Error(response, httpErr.message, httpErr.statusCode)
		return
	}
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(http.StatusCreated)
	_, _ = response.Write(payload)
}

func (h *WebSocketHandler) serveConnection(ctx context.Context, conn *websocket.Conn, subscription domain.Subscription) {
	log.Trace("WebSocketHandler serveConnection")

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	h.closeAtSessionExpiry(ctx, subscription, cancel)

	conn.SetReadLimit(h.config.MaxMessageBytes)
	if h.config.PongTimeout > 0 {
		_ = conn.SetReadDeadline(time.Now().Add(h.config.PongTimeout))
		conn.SetPongHandler(func(string) error {
			log.Trace("WebSocketHandler pong")
			return conn.SetReadDeadline(time.Now().Add(h.config.PongTimeout))
		})
	}

	sendQueue := make(chan domain.ServerMessage, h.config.SendQueueSize)
	defer close(sendQueue)

	var filtersMu sync.RWMutex
	filters := []domain.Filter{}
	dedupe := app.NewDedupeStore(h.config.ReplayLimit * 2)
	cursors := emptyCursors(subscription)

	go h.writeLoop(ctx, conn, sendQueue, cancel)
	go h.readLoop(ctx, conn, sendQueue, &filtersMu, &filters, cancel)
	go h.pingLoop(ctx, conn, cancel)

	h.enqueue(ctx, sendQueue, domain.ServerMessage{
		Type:         domain.ServerMessageTypeReady,
		ConnectionID: subscription.ConnectionID,
		ServerTime:   time.Now().UTC().Format(time.RFC3339),
	})

	hello, err := h.readInitialHello(ctx, conn)
	if err != nil {
		h.enqueue(ctx, sendQueue, domain.ServerMessage{
			Type:    domain.ServerMessageTypeError,
			Code:    socketErrorCode(err),
			Message: socketErrorMessage(err),
		})
		return
	}
	cursors = mergeInitialCursors(subscription, hello)
	filtersMu.Lock()
	filters = hello.Filters
	filtersMu.Unlock()

	if err := h.replay(ctx, subscription, cursors, dedupe, sendQueue, &filtersMu, &filters); err != nil {
		h.enqueue(ctx, sendQueue, domain.ServerMessage{
			Type:    domain.ServerMessageTypeError,
			Code:    socketErrorCode(err),
			Message: socketErrorMessage(err),
		})
		return
	}
	h.enqueue(ctx, sendQueue, domain.ServerMessage{
		Type:    domain.ServerMessageTypeReplayComplete,
		Cursors: cursors,
	})

	h.liveLoop(ctx, subscription, cursors, dedupe, sendQueue, &filtersMu, &filters, cancel)
}

func (h *WebSocketHandler) closeAtSessionExpiry(ctx context.Context, subscription domain.Subscription, cancel context.CancelFunc) {
	log.Trace("WebSocketHandler closeAtSessionExpiry")

	if subscription.Session.ExpiresAt.IsZero() {
		cancel()
		return
	}
	untilExpiry := time.Until(subscription.Session.ExpiresAt)
	if untilExpiry <= 0 {
		cancel()
		return
	}
	go func() {
		timer := time.NewTimer(untilExpiry)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			cancel()
		}
	}()
}

func (h *WebSocketHandler) readInitialHello(ctx context.Context, conn *websocket.Conn) (domain.ClientMessage, error) {
	log.Trace("WebSocketHandler readInitialHello")

	if h.config.ReadTimeout > 0 {
		_ = conn.SetReadDeadline(time.Now().Add(h.config.ReadTimeout))
	}
	_, payload, err := conn.ReadMessage()
	if err != nil {
		return domain.ClientMessage{}, domain.ErrValidationFailed.Extend("initial hello read failed")
	}
	message, err := h.adapter.FromDTO(ctx, payload)
	if err != nil {
		return domain.ClientMessage{}, err
	}
	if message.Type != domain.ClientMessageTypeHello {
		return domain.ClientMessage{}, domain.ErrValidationFailed.Extend("hello message is required")
	}
	if h.config.PongTimeout > 0 {
		_ = conn.SetReadDeadline(time.Now().Add(h.config.PongTimeout))
	}
	return message, nil
}

func (h *WebSocketHandler) replay(ctx context.Context, subscription domain.Subscription, cursors map[string]string, dedupe *app.DedupeStore, sendQueue chan<- domain.ServerMessage, filtersMu *sync.RWMutex, filters *[]domain.Filter) error {
	log.Trace("WebSocketHandler replay")

	events, err := h.usecase.Replay(ctx, subscription, cursors, h.config.ReplayLimit)
	if err != nil {
		return err
	}
	for _, event := range events {
		cursors[event.Room.Key] = event.StreamID
		if dedupe.Seen(event.Event.EventID) ||
			!authorizedEvent(subscription.Session, event.Event) ||
			!matchesFilters(event.Event.Resource.Type, event.Event.Resource.ID, filtersMu, filters) {
			continue
		}
		if !h.enqueue(ctx, sendQueue, domain.ServerMessage{
			Type:     domain.ServerMessageTypeEvent,
			Stream:   event.Room.Key,
			StreamID: event.StreamID,
			Event:    &event.Event,
			Cursors:  copyCursors(cursors),
		}) {
			return domain.ErrBackpressure.Extend("websocket send queue is full")
		}
	}
	for key, cursor := range cursors {
		if cursor == "" {
			cursors[key] = "$"
		}
	}
	return nil
}

func (h *WebSocketHandler) liveLoop(ctx context.Context, subscription domain.Subscription, cursors map[string]string, dedupe *app.DedupeStore, sendQueue chan<- domain.ServerMessage, filtersMu *sync.RWMutex, filters *[]domain.Filter, cancel context.CancelFunc) {
	log.Trace("WebSocketHandler liveLoop")

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		events, err := h.usecase.ReadLive(ctx, subscription, cursors, h.config.ReplayLimit)
		if err != nil {
			h.enqueue(ctx, sendQueue, domain.ServerMessage{
				Type:    domain.ServerMessageTypeError,
				Code:    socketErrorCode(err),
				Message: socketErrorMessage(err),
			})
			cancel()
			return
		}
		for _, event := range events {
			cursors[event.Room.Key] = event.StreamID
			if dedupe.Seen(event.Event.EventID) ||
				!authorizedEvent(subscription.Session, event.Event) ||
				!matchesFilters(event.Event.Resource.Type, event.Event.Resource.ID, filtersMu, filters) {
				continue
			}
			if !h.enqueue(ctx, sendQueue, domain.ServerMessage{
				Type:     domain.ServerMessageTypeEvent,
				Stream:   event.Room.Key,
				StreamID: event.StreamID,
				Event:    &event.Event,
				Cursors:  copyCursors(cursors),
			}) {
				cancel()
				return
			}
		}
	}
}

func (h *WebSocketHandler) readLoop(ctx context.Context, conn *websocket.Conn, sendQueue chan<- domain.ServerMessage, filtersMu *sync.RWMutex, filters *[]domain.Filter, cancel context.CancelFunc) {
	log.Trace("WebSocketHandler readLoop")

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		_, payload, err := conn.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				log.WithContext(ctx).WithError(err).Debug("websocket read loop ended")
			}
			cancel()
			return
		}
		message, err := h.adapter.FromDTO(ctx, payload)
		if err != nil {
			h.enqueue(ctx, sendQueue, domain.ServerMessage{
				Type:    domain.ServerMessageTypeError,
				Code:    socketErrorCode(err),
				Message: socketErrorMessage(err),
			})
			continue
		}
		switch message.Type {
		case domain.ClientMessageTypePing:
			h.enqueue(ctx, sendQueue, domain.ServerMessage{Type: domain.ServerMessageTypePong})
		case domain.ClientMessageTypeSubscribe:
			filtersMu.Lock()
			*filters = message.Filters
			filtersMu.Unlock()
		case domain.ClientMessageTypeAck, domain.ClientMessageTypeHello:
			continue
		default:
			h.enqueue(ctx, sendQueue, domain.ServerMessage{
				Type:    domain.ServerMessageTypeError,
				Code:    domain.ServerMessageCodeInvalidMessage,
				Message: "unsupported message type",
			})
		}
	}
}

func (h *WebSocketHandler) writeLoop(ctx context.Context, conn *websocket.Conn, sendQueue <-chan domain.ServerMessage, cancel context.CancelFunc) {
	log.Trace("WebSocketHandler writeLoop")

	for {
		select {
		case <-ctx.Done():
			return
		case message, ok := <-sendQueue:
			if !ok {
				return
			}
			payload, err := h.adapter.ToDTO(ctx, message)
			if err != nil {
				log.WithContext(ctx).WithError(err).Error("failed to encode websocket message")
				cancel()
				return
			}
			if h.config.WriteTimeout > 0 {
				_ = conn.SetWriteDeadline(time.Now().Add(h.config.WriteTimeout))
			}
			if err := conn.WriteMessage(websocketWriteMessageType, payload); err != nil {
				log.WithContext(ctx).WithError(err).Debug("websocket write failed")
				cancel()
				return
			}
		}
	}
}

func (h *WebSocketHandler) pingLoop(ctx context.Context, conn *websocket.Conn, cancel context.CancelFunc) {
	log.Trace("WebSocketHandler pingLoop")

	if h.config.PingInterval <= 0 {
		return
	}
	ticker := time.NewTicker(h.config.PingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(h.config.WriteTimeout)); err != nil {
				log.WithContext(ctx).WithError(err).Debug("websocket ping failed")
				cancel()
				return
			}
		}
	}
}

func (h *WebSocketHandler) enqueue(ctx context.Context, sendQueue chan<- domain.ServerMessage, message domain.ServerMessage) bool {
	log.Trace("WebSocketHandler enqueue")

	select {
	case <-ctx.Done():
		return false
	case sendQueue <- message:
		return true
	default:
		if message.Event != nil && strings.EqualFold(message.Event.Severity, userevents.SeverityInfo) {
			return true
		}
		return false
	}
}

func emptyCursors(subscription domain.Subscription) map[string]string {
	log.Trace("emptyCursors")

	cursors := map[string]string{}
	for _, room := range subscription.Rooms {
		cursors[room.Key] = ""
	}
	return cursors
}

func mergeInitialCursors(subscription domain.Subscription, hello domain.ClientMessage) map[string]string {
	log.Trace("mergeInitialCursors")

	cursors := emptyCursors(subscription)
	for key, cursor := range hello.LastCursors {
		if _, ok := cursors[key]; ok {
			cursors[key] = cursor
		}
	}
	if hello.LastEventID != "" {
		for key, cursor := range cursors {
			if cursor == "" {
				cursors[key] = hello.LastEventID
			}
		}
	}
	return cursors
}

func matchesFilters(resourceType string, resourceID string, filtersMu *sync.RWMutex, filters *[]domain.Filter) bool {
	log.Trace("matchesFilters")

	filtersMu.RLock()
	defer filtersMu.RUnlock()
	if len(*filters) == 0 {
		return true
	}
	for _, filter := range *filters {
		if strings.TrimSpace(filter.ResourceType) != "" && filter.ResourceType != resourceType {
			continue
		}
		if strings.TrimSpace(filter.ResourceID) != "" && filter.ResourceID != resourceID {
			continue
		}
		return true
	}
	return false
}

func authorizedEvent(session domain.Session, event userevents.Event) bool {
	log.Trace("authorizedEvent")

	requiredPermission := strings.TrimSpace(event.RequiredPermission)
	if event.OrgID != "" {
		if event.OrgID != session.OrgID {
			return false
		}
		if requiredPermission == "" {
			return event.UserID == "" || event.UserID == session.UserID
		}
		return authz.HasPermission(session.Permissions, requiredPermission)
	}
	if event.UserID == "" || event.UserID != session.UserID {
		return false
	}
	if requiredPermission == "" {
		return true
	}
	return authz.HasPermission(session.Permissions, requiredPermission)
}

func checkOrigin(allowedOrigins []string) func(*http.Request) bool {
	log.Trace("checkOrigin")

	allowed := make(map[string]struct{}, len(allowedOrigins))
	allowAny := false
	for _, origin := range allowedOrigins {
		origin = strings.TrimSpace(origin)
		if origin == "" {
			continue
		}
		if origin == "*" {
			allowAny = true
			continue
		}
		allowed[origin] = struct{}{}
	}
	return func(request *http.Request) bool {
		origin := strings.TrimSpace(request.Header.Get("Origin"))
		if origin == "" {
			return true
		}
		if allowAny {
			return true
		}
		_, ok := allowed[origin]
		return ok
	}
}

func copyCursors(cursors map[string]string) map[string]string {
	log.Trace("copyCursors")

	out := make(map[string]string, len(cursors))
	for key, value := range cursors {
		out[key] = value
	}
	return out
}
