package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type AuthResult struct {
	UserID  uuid.UUID
	ExpUnix int64
}

type TokenValidator interface {
	Validate(ctx context.Context, authorizationToken string) (map[string]any, error)
}

type AuthSessionStore interface {
	SessionExists(ctx context.Context, sid string) (bool, error)
	GetUserRevokedAfter(ctx context.Context, userID string) (int64, error)
}

type AuthHandler struct {
	authProvider TokenValidator
	authStore    AuthSessionStore
}

func NewAuthHandler(authProvider TokenValidator, authStore AuthSessionStore) *AuthHandler {
	return &AuthHandler{
		authProvider: authProvider,
		authStore:    authStore,
	}
}

func (h *AuthHandler) Authenticate(ctx context.Context, r *http.Request) (AuthResult, error) {
	authorization := ""
	if r != nil {
		authorization = strings.TrimSpace(r.Header.Get("Authorization"))
	}
	if authorization == "" {
		log.WithContext(ctx).Warn("DataUploadHandlers auth: missing authorization header")
		return AuthResult{}, ErrUnauthorized().WithMessage("missing authorization header")
	}

	claims, err := h.authProvider.Validate(ctx, authorization)
	if err != nil {
		log.WithContext(ctx).WithError(err).Warn("DataUploadHandlers auth: token validation failed")
		return AuthResult{}, ErrUnauthorized().Wrap(err).WithMessage("invalid token")
	}

	uid, ok := claims["userId"].(string)
	if !ok || uid == "" {
		log.WithContext(ctx).Warn("DataUploadHandlers auth: validated token missing userId claim")
		return AuthResult{}, ErrUnauthorized().WithMessage("invalid token: no userId")
	}
	userID, err := uuid.Parse(uid)
	if err != nil || userID == uuid.Nil {
		log.WithContext(ctx).WithError(err).Warn("DataUploadHandlers auth: validated token has invalid userId claim")
		return AuthResult{}, ErrUnauthorized().Wrap(err).WithMessage("invalid token: invalid userId")
	}

	sVal, _ := claims["sid"].(string)
	if sVal == "" {
		log.WithContext(ctx).Warn("DataUploadHandlers auth: validated token missing sid claim")
		return AuthResult{}, ErrUnauthorized().WithMessage("invalid token: no sid")
	}

	iatUnix := toInt64(claims["iat"])
	if iatUnix == 0 {
		log.WithContext(ctx).Warn("DataUploadHandlers auth: validated token missing iat claim")
		return AuthResult{}, ErrUnauthorized().WithMessage("invalid token: no iat")
	}

	expUnix := toInt64(claims["expiresAt"])
	if expUnix == 0 {
		log.WithContext(ctx).Warn("DataUploadHandlers auth: validated token missing exp claim")
		return AuthResult{}, ErrUnauthorized().WithMessage("invalid token: no exp")
	}

	okSess, err := h.authStore.SessionExists(ctx, sVal)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("DataUploadHandlers auth: session lookup failed")
		return AuthResult{}, ErrInternalServer().Wrap(err).WithMessage("auth check failed")
	}
	if !okSess {
		log.WithContext(ctx).Warn("DataUploadHandlers auth: session not found or revoked")
		return AuthResult{}, ErrUnauthorized().WithMessage("session revoked")
	}

	revokedAfter, err := h.authStore.GetUserRevokedAfter(ctx, uid)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("DataUploadHandlers auth: revoked_after lookup failed")
		return AuthResult{}, ErrInternalServer().Wrap(err).WithMessage("auth check failed")
	}
	if revokedAfter > 0 && iatUnix <= revokedAfter {
		log.WithContext(ctx).Warn("DataUploadHandlers auth: token issued before revoked_after cutoff")
		return AuthResult{}, ErrUnauthorized().WithMessage("token revoked")
	}

	return AuthResult{
		UserID:  userID,
		ExpUnix: expUnix,
	}, nil
}

func toInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case float64:
		return int64(x)
	case json.Number:
		n, _ := x.Int64()
		return n
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	default:
		return 0
	}
}
