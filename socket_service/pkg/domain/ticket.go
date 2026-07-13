package domain

import (
	"time"

	log "github.com/sirupsen/logrus"
)

type SocketTicket struct {
	Token     string
	ExpiresAt time.Time
}

func (t SocketTicket) IsExpired(now time.Time) bool {
	log.Trace("SocketTicket IsExpired")

	return !t.ExpiresAt.IsZero() && !t.ExpiresAt.After(now)
}
