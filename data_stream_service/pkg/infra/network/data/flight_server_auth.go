package data

import (
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type FlightServerAuth struct {
	token          string
	allowAnonymous bool
}

func NewFlightServerAuth(token string, allowAnonymous bool) *FlightServerAuth {
	log.Trace("NewFlightServerAuth")

	return &FlightServerAuth{
		token:          strings.TrimSpace(token),
		allowAnonymous: allowAnonymous,
	}
}

func (a *FlightServerAuth) Authenticate(outgoing flight.AuthConn) error {
	log.Trace("FlightServerAuth Authenticate")

	return status.Error(codes.Unimplemented, "data stream flight handshake auth is not supported; use auth-token-bin metadata")
}

func (sa *FlightServerAuth) IsValid(token string) (interface{}, error) {
	log.Trace("FlightServerAuth IsValid")

	token = strings.TrimSpace(token)
	if sa == nil {
		return nil, status.Error(codes.PermissionDenied, "data stream auth is not configured")
	}
	if sa.allowAnonymous && sa.token == "" && token == "" {
		return "anonymous-local", nil
	}
	if sa.token == "" {
		return nil, status.Error(codes.PermissionDenied, "data stream auth token is required")
	}
	if token != sa.token {
		return nil, status.Error(codes.PermissionDenied, "invalid data stream auth token")
	}
	return "data-stream-client", nil
}
