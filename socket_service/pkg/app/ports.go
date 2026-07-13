package app

import (
	"context"
	"time"

	"socket_service/pkg/domain"
)

type EventStreamReader interface {
	Replay(ctx context.Context, subscription domain.Subscription, cursors map[string]string, limit int) ([]domain.StreamEvent, error)
	ReadLive(ctx context.Context, subscription domain.Subscription, cursors map[string]string, limit int) ([]domain.StreamEvent, error)
}

type TicketStore interface {
	Issue(ctx context.Context, session domain.Session, ttl time.Duration) (domain.SocketTicket, error)
	Consume(ctx context.Context, token string) (domain.Session, error)
}
