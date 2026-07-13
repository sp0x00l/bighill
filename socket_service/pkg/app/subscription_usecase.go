package app

import (
	"context"
	"errors"
	"time"

	"socket_service/pkg/domain"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type SubscriptionUsecase interface {
	IssueTicket(ctx context.Context, session domain.Session, ttl time.Duration) (domain.SocketTicket, error)
	Open(ctx context.Context, socketTicket string) (domain.Subscription, error)
	Replay(ctx context.Context, subscription domain.Subscription, cursors map[string]string, limit int) ([]domain.StreamEvent, error)
	ReadLive(ctx context.Context, subscription domain.Subscription, cursors map[string]string, limit int) ([]domain.StreamEvent, error)
}

type subscriptionUsecase struct {
	ticketStore  TicketStore
	roomResolver *RoomResolver
	streamReader EventStreamReader
}

func NewSubscriptionUsecase(ticketStore TicketStore, roomResolver *RoomResolver, streamReader EventStreamReader) *subscriptionUsecase {
	log.Trace("NewSubscriptionUsecase")

	return &subscriptionUsecase{
		ticketStore:  ticketStore,
		roomResolver: roomResolver,
		streamReader: streamReader,
	}
}

func (u *subscriptionUsecase) IssueTicket(ctx context.Context, session domain.Session, ttl time.Duration) (domain.SocketTicket, error) {
	log.Trace("subscriptionUsecase IssueTicket")

	if session.UserID == "" || session.OrgID == "" || session.SessionID == "" {
		return domain.SocketTicket{}, domain.ErrValidationFailed.Extend("socket ticket requires user, org, and session")
	}
	if ttl <= 0 {
		return domain.SocketTicket{}, domain.ErrValidationFailed.Extend("socket ticket ttl must be positive")
	}
	return u.ticketStore.Issue(ctx, session, ttl)
}

func (u *subscriptionUsecase) Open(ctx context.Context, socketTicket string) (domain.Subscription, error) {
	log.Trace("subscriptionUsecase Open")

	if socketTicket == "" {
		return domain.Subscription{}, domain.ErrUnauthorized.Extend("socket ticket is required")
	}
	session, err := u.ticketStore.Consume(ctx, socketTicket)
	if err != nil {
		if errors.Is(err, domain.ErrUnauthorized) || errors.Is(err, domain.ErrDependencyFailed) {
			return domain.Subscription{}, err
		}
		return domain.Subscription{}, domain.ErrUnauthorized.Extend("socket ticket rejected")
	}
	return domain.Subscription{
		ConnectionID: uuid.NewString(),
		Session:      session,
		Rooms:        u.roomResolver.Resolve(session),
	}, nil
}

func (u *subscriptionUsecase) Replay(ctx context.Context, subscription domain.Subscription, cursors map[string]string, limit int) ([]domain.StreamEvent, error) {
	log.Trace("subscriptionUsecase Replay")

	return u.streamReader.Replay(ctx, subscription, cursors, limit)
}

func (u *subscriptionUsecase) ReadLive(ctx context.Context, subscription domain.Subscription, cursors map[string]string, limit int) ([]domain.StreamEvent, error) {
	log.Trace("subscriptionUsecase ReadLive")

	return u.streamReader.ReadLive(ctx, subscription, cursors, limit)
}
