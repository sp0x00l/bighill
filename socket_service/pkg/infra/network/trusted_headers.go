package network

import (
	"net/http"
	"time"

	"socket_service/pkg/domain"

	"lib/shared_lib/authz"

	log "github.com/sirupsen/logrus"
)

const (
	rolesHeader = "X-Roles"
)

func sessionFromTrustedHeaders(request *http.Request, ticketTTL time.Duration) (domain.Session, error) {
	log.Trace("sessionFromTrustedHeaders")

	ctx := request.Context()
	userID, err := authz.ReadUserIDHeader(ctx, request)
	if err != nil {
		return domain.Session{}, domain.ErrUnauthorized.Extend("user header is required")
	}
	orgID, err := authz.ReadOrgIDHeader(ctx, request)
	if err != nil {
		return domain.Session{}, domain.ErrUnauthorized.Extend("org header is required")
	}
	sessionID, err := authz.ReadSessionIDHeader(ctx, request)
	if err != nil {
		return domain.Session{}, domain.ErrUnauthorized.Extend("session header is required")
	}
	permissions, err := authz.ReadPermissionsHeader(ctx, request)
	if err != nil {
		return domain.Session{}, domain.ErrUnauthorized.Extend("permissions header is required")
	}
	roles, err := authz.DecodeStringSlice(request.Header.Get(rolesHeader))
	if err != nil {
		return domain.Session{}, domain.ErrValidationFailed.Extend("invalid roles header")
	}
	return domain.Session{
		UserID:      userID.String(),
		OrgID:       orgID.String(),
		Roles:       roles,
		Permissions: permissions,
		SessionID:   sessionID,
		ExpiresAt:   time.Now().UTC().Add(ticketTTL),
	}, nil
}
