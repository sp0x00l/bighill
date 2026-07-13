package domain

import (
	"time"

	log "github.com/sirupsen/logrus"
)

type Session struct {
	UserID      string
	OrgID       string
	Roles       []string
	Permissions []string
	SessionID   string
	ExpiresAt   time.Time
}

func (s Session) IsExpired(now time.Time) bool {
	log.Trace("Session IsExpired")

	return !s.ExpiresAt.IsZero() && !now.Before(s.ExpiresAt)
}
