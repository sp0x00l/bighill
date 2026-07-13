package redis

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"socket_service/pkg/domain"

	"github.com/google/uuid"
	"github.com/redis/rueidis"
	log "github.com/sirupsen/logrus"
)

const (
	socketTicketKeyPrefix = "socket:ticket:"
)

type TicketStore struct {
	client rueidis.Client
	now    func() time.Time
}

type socketTicketRecord struct {
	UserID      string    `json:"user_id"`
	OrgID       string    `json:"org_id"`
	Roles       []string  `json:"roles,omitempty"`
	Permissions []string  `json:"permissions,omitempty"`
	SessionID   string    `json:"session_id"`
	ExpiresAt   time.Time `json:"expires_at"`
}

func NewTicketStore(client rueidis.Client) *TicketStore {
	log.Trace("NewTicketStore")

	return &TicketStore{
		client: client,
		now:    time.Now,
	}
}

func (s *TicketStore) Issue(ctx context.Context, session domain.Session, ttl time.Duration) (domain.SocketTicket, error) {
	log.Trace("TicketStore Issue")

	expiresAt := s.now().UTC().Add(ttl)
	if !session.ExpiresAt.IsZero() && session.ExpiresAt.Before(expiresAt) {
		expiresAt = session.ExpiresAt.UTC()
	}
	if !expiresAt.After(s.now()) {
		return domain.SocketTicket{}, domain.ErrUnauthorized.Extend("session is expired")
	}
	token := uuid.NewString()
	record := socketTicketRecord{
		UserID:      strings.TrimSpace(session.UserID),
		OrgID:       strings.TrimSpace(session.OrgID),
		Roles:       session.Roles,
		Permissions: session.Permissions,
		SessionID:   strings.TrimSpace(session.SessionID),
		ExpiresAt:   expiresAt,
	}
	payload, err := json.Marshal(record)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("socket ticket encode failed")
		return domain.SocketTicket{}, domain.ErrValidationFailed.Extend("invalid socket ticket session")
	}
	redisTTL := time.Until(expiresAt)
	if redisTTL <= 0 {
		return domain.SocketTicket{}, domain.ErrUnauthorized.Extend("session is expired")
	}
	cmd := s.client.B().Set().Key(socketTicketKeyPrefix + token).Value(string(payload)).Nx().Ex(redisTTL).Build()
	if err := s.client.Do(ctx, cmd).Error(); err != nil {
		log.WithContext(ctx).WithError(err).Warn("socket ticket write failed")
		return domain.SocketTicket{}, domain.ErrDependencyFailed.Extend("socket ticket write failed")
	}
	return domain.SocketTicket{Token: token, ExpiresAt: expiresAt}, nil
}

func (s *TicketStore) Consume(ctx context.Context, token string) (domain.Session, error) {
	log.Trace("TicketStore Consume")

	token = strings.TrimSpace(token)
	if token == "" {
		return domain.Session{}, domain.ErrUnauthorized.Extend("socket ticket is required")
	}
	result := s.client.Do(ctx, s.client.B().Getdel().Key(socketTicketKeyPrefix+token).Build())
	if err := result.Error(); err != nil {
		if errors.Is(err, rueidis.Nil) {
			return domain.Session{}, domain.ErrUnauthorized.Extend("socket ticket is invalid or expired")
		}
		log.WithContext(ctx).WithError(err).Warn("socket ticket read failed")
		return domain.Session{}, domain.ErrDependencyFailed.Extend("socket ticket read failed")
	}
	payload, err := result.ToString()
	if err != nil {
		log.WithContext(ctx).WithError(err).Warn("socket ticket payload read failed")
		return domain.Session{}, domain.ErrDependencyFailed.Extend("socket ticket read failed")
	}
	var record socketTicketRecord
	if err := json.Unmarshal([]byte(payload), &record); err != nil {
		log.WithContext(ctx).WithError(err).Warn("socket ticket decode failed")
		return domain.Session{}, domain.ErrUnauthorized.Extend("socket ticket is invalid")
	}
	session := domain.Session{
		UserID:      record.UserID,
		OrgID:       record.OrgID,
		Roles:       record.Roles,
		Permissions: record.Permissions,
		SessionID:   record.SessionID,
		ExpiresAt:   record.ExpiresAt,
	}
	if session.UserID == "" || session.OrgID == "" || session.SessionID == "" || session.IsExpired(s.now()) {
		return domain.Session{}, domain.ErrUnauthorized.Extend("socket ticket is invalid or expired")
	}
	return session, nil
}
