package rest

import (
	"context"
	"errors"
	sharedrpc "lib/shared_lib/rpc"
	"lib/shared_lib/transport"
	"net/http"
	app "tenant_service/pkg/app"
	"tenant_service/pkg/domain"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

type contextKey string

type HttpHandler struct {
	profilesUseCase    app.ProfilesUseCase
	profilesDTOAdapter ProfilesDTOAdapter
}

func NewHttpHandler(profilesUseCase app.ProfilesUseCase, profilesDTOAdapter ProfilesDTOAdapter) *HttpHandler {
	log.Trace("NewHttpHandler")

	handlers := &HttpHandler{
		profilesUseCase:    profilesUseCase,
		profilesDTOAdapter: profilesDTOAdapter,
	}
	return handlers
}

func (ph *HttpHandler) GetRoutes() []transport.Route {
	log.Trace("HttpHandler GetRoutes")

	routes := []transport.Route{
		{
			Path:     "/public/v1/profiles",
			Handler:  ph.CreateProfile,
			Method:   http.MethodPost,
			SpanName: "create-profile",
		},
		{
			Path:     "/private/v1/profiles",
			Handler:  ph.ReplaceProfile,
			Method:   http.MethodPut,
			SpanName: "replace-profile",
		},
		{
			Path:     "/private/v1/profiles",
			Handler:  ph.DeleteProfile,
			Method:   http.MethodDelete,
			SpanName: "delete-profile",
		},
		{
			Path:     "/private/v1/profiles",
			Handler:  ph.ReadProfile,
			Method:   http.MethodGet,
			SpanName: "read-profile",
		},
		{
			Path:     "/private/v1/profiles/password",
			Handler:  ph.ReplacePassword,
			Method:   http.MethodPut,
			SpanName: "replace-password",
		},
		{
			Path:     "/private/v1/profiles/huggingface-token",
			Handler:  ph.ReplaceHuggingFaceToken,
			Method:   http.MethodPut,
			SpanName: "replace-huggingface-token",
		},
		{
			Path:     "/public/v1/profiles/password/verify",
			Handler:  ph.VerifyPassword,
			Method:   http.MethodPost,
			SpanName: "verify-password",
		},
		{
			Path:     "/public/v1/profiles/email/verify",
			Handler:  ph.VerifyEmail,
			Method:   http.MethodPost,
			SpanName: "verify-email",
		},
		{
			Path:     "/public/v1/profiles/oauth/{provider}/authorizations",
			Handler:  ph.CreateOAuthAuthorization,
			Method:   http.MethodPost,
			SpanName: "create-oauth-authorization",
		},
		{
			Path:     "/public/v1/profiles/oauth/{provider}/sessions",
			Handler:  ph.CreateOAuthSession,
			Method:   http.MethodPost,
			SpanName: "create-oauth-session",
		},
		{
			Path:     "/private/v1/profiles/logout",
			Handler:  ph.Logout,
			Method:   http.MethodPost,
			SpanName: "logout",
		},
		{
			Path:     "/private/v1/orgs/current",
			Handler:  ph.ReadCurrentOrganization,
			Method:   http.MethodGet,
			SpanName: "read-current-org",
		},
		{
			Path:     "/private/v1/orgs/{orgId}/members",
			Handler:  ph.ListOrganizationMembers,
			Method:   http.MethodGet,
			SpanName: "list-org-members",
		},
		{
			Path:     "/private/v1/orgs/{orgId}/members",
			Handler:  ph.UpsertOrganizationMember,
			Method:   http.MethodPost,
			SpanName: "create-org-member",
		},
		{
			Path:     "/private/v1/orgs/{orgId}/members/{userId}",
			Handler:  ph.UpsertOrganizationMember,
			Method:   http.MethodPut,
			SpanName: "replace-org-member",
		},
		{
			Path:     "/private/v1/orgs/{orgId}/members/{userId}",
			Handler:  ph.DeleteOrganizationMember,
			Method:   http.MethodDelete,
			SpanName: "delete-org-member",
		},
	}
	return routes
}

func (ph *HttpHandler) ReplaceHuggingFaceToken(ctx context.Context, r *http.Request) (int, []byte, error) {
	log.Trace("HttpHandler ReplaceHuggingFaceToken")

	userID, err := transport.ReadUserIDHeader(ctx, r)
	if err != nil {
		log.WithContext(ctx).WithError(err).Warn("failed to read user ID from header")
		return http.StatusInternalServerError, nil, err
	}
	ctx = context.WithValue(ctx, contextKey("UserID"), userID.String())

	reqBody, err := transport.ReadReqBody(ctx, r)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}

	token, err := ph.profilesDTOAdapter.FromHuggingFaceTokenDTO(ctx, reqBody)
	if err != nil {
		log.WithContext(ctx).WithError(err).Warn("parsing hugging face token payload failed")
		return http.StatusBadRequest, nil, err
	}

	if err := ph.profilesUseCase.ReplaceHuggingFaceToken(ctx, userID, token); err != nil {
		log.WithContext(ctx).WithError(err).Error("replace hugging face token failed")
		return httpStatusFor(err), nil, err
	}

	return http.StatusNoContent, nil, nil
}

func (ph *HttpHandler) CreateProfile(ctx context.Context, r *http.Request) (int, []byte, error) {
	log.Trace("HttpHandler CreateProfile")

	idempotencyUUID, err := transport.ReadIdempotencyIDHeader(ctx, r)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}

	profileAccount, err := ph.requestToProfileAccount(ctx, r)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}

	ctx = context.WithValue(ctx, contextKey("ProfileIdempotencyKey"), idempotencyUUID.String())

	if err := ph.profilesUseCase.CreateProfile(ctx, profileAccount, idempotencyUUID); err != nil {
		if httpStatusFor(err) == http.StatusConflict {
			log.WithContext(ctx).WithError(err).Warnf("profile email %s already exists", profileAccount.Email)
		}
		return httpStatusFor(err), nil, err
	}

	profileBytes, err := ph.profilesDTOAdapter.ToProfileAccountDTO(ctx, profileAccount)
	if err != nil {
		return http.StatusInternalServerError, nil, err
	}

	return http.StatusCreated, profileBytes, nil // 201 instead of 200
}

func (ph *HttpHandler) ReplaceProfile(ctx context.Context, r *http.Request) (int, []byte, error) {
	log.Trace("HttpHandler ReplaceProfile")

	profile, err := ph.requestToProfile(ctx, r)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}

	userID, err := transport.ReadUserIDHeader(ctx, r)
	if err != nil {
		log.WithContext(ctx).WithError(err).Warn("failed to read user ID from header")
		return http.StatusInternalServerError, nil, err
	}
	ctx = context.WithValue(ctx, contextKey("UserID"), userID.String())
	profile.ProfileAccount.ID = userID

	updatedProfle, err := ph.profilesUseCase.ReplaceProfile(ctx, userID, profile)
	if err != nil {
		if httpStatusFor(err) == http.StatusNotFound {
			log.WithContext(ctx).WithError(err).Warnf("profile userID %s not found", userID)
		}
		if httpStatusFor(err) == http.StatusConflict {
			log.WithContext(ctx).WithError(err).Warnf("profile email %s or phone %s already exists", profile.Email, profile.PhoneNumber)
		}
		return httpStatusFor(err), nil, err
	}

	profileBytes, err := ph.profilesDTOAdapter.ToDTO(ctx, updatedProfle)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("serialize profile failed")
		return http.StatusInternalServerError, nil, err
	}

	return http.StatusOK, profileBytes, nil
}

func (ph *HttpHandler) DeleteProfile(ctx context.Context, r *http.Request) (int, []byte, error) {
	log.Trace("HttpHandler DeleteProfile")

	userID, err := transport.ReadUserIDHeader(ctx, r)
	if err != nil {
		log.WithContext(ctx).WithError(err).Warn("failed to read user ID from header")
		return http.StatusInternalServerError, nil, err
	}
	ctx = context.WithValue(ctx, contextKey("UserID"), userID.String())
	profileID := userID
	if err := ph.profilesUseCase.DeleteProfile(ctx, profileID); err != nil {
		log.WithContext(ctx).WithError(err).Error("delete profile failed")
		return httpStatusFor(err), nil, err
	}
	return http.StatusNoContent, nil, nil
}

func (ph *HttpHandler) ReadProfile(ctx context.Context, r *http.Request) (int, []byte, error) {
	log.Trace("HttpHandler ReadProfile")

	userID, err := transport.ReadUserIDHeader(ctx, r)
	if err != nil {
		log.WithContext(ctx).WithError(err).Warn("failed to read user ID from header. Invalid user from gateway.")
		return http.StatusInternalServerError, nil, err
	}
	ctx = context.WithValue(ctx, contextKey("UserID"), userID.String())
	profileID := userID

	profile, err := ph.profilesUseCase.ReadProfile(ctx, profileID)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("read profile failed")
		return httpStatusFor(err), nil, err
	}

	profileBytes, err := ph.profilesDTOAdapter.ToDTO(ctx, profile)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("serialize profile failed")
		return http.StatusInternalServerError, nil, err
	}

	return http.StatusOK, profileBytes, nil
}

func (ph *HttpHandler) ReplacePassword(ctx context.Context, r *http.Request) (int, []byte, error) {
	log.Trace("HttpHandler ReplacePassword")

	userID, err := transport.ReadUserIDHeader(ctx, r)
	if err != nil {
		log.WithContext(ctx).WithError(err).Warn("failed to read user ID from header")
		return http.StatusInternalServerError, nil, err

	}
	ctx = context.WithValue(ctx, contextKey("UserID"), userID.String())

	reqBody, err := transport.ReadReqBody(ctx, r)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}

	password, err := ph.profilesDTOAdapter.FromPasswordDTO(ctx, reqBody)
	if err != nil {
		log.WithContext(ctx).WithError(err).Warn("parsing payload failed")
		return http.StatusBadRequest, nil, err
	}

	if err := ph.profilesUseCase.ReplacePassword(ctx, userID, password); err != nil {
		log.WithContext(ctx).WithError(err).Error("replace password failed")
		return httpStatusFor(err), nil, err
	}

	return http.StatusNoContent, nil, nil
}

func (ph *HttpHandler) VerifyPassword(ctx context.Context, r *http.Request) (int, []byte, error) {
	log.Trace("HttpHandler VerifyPassword")

	reqBody, err := transport.ReadReqBody(ctx, r)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}

	verified := true
	var token string
	email, password, err := ph.profilesDTOAdapter.FromPasswordValidationDTO(ctx, reqBody)
	if err != nil {
		verified = false
	} else {
		token, err = ph.profilesUseCase.VerifyPassword(ctx, email, password)
		if err != nil {
			if errors.Is(err, domain.ErrEmailNotVerified) {
				return http.StatusUnauthorized, []byte(`{"message":"email not verified"}`), nil
			}
			verified = false
		}
	}

	resultBytes, err := ph.profilesDTOAdapter.ToPasswordResultDTO(ctx, verified, token)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("serialize password result failed")
		return http.StatusInternalServerError, nil, err
	}
	return http.StatusOK, resultBytes, nil
}

func (ph *HttpHandler) VerifyEmail(ctx context.Context, r *http.Request) (int, []byte, error) {
	log.Trace("HttpHandler VerifyEmail")

	reqBody, err := transport.ReadReqBody(ctx, r)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}

	token, err := ph.profilesDTOAdapter.FromEmailVerificationDTO(ctx, reqBody)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}

	if err := ph.profilesUseCase.VerifyEmail(ctx, token); err != nil {
		return httpStatusFor(err), nil, err
	}

	return http.StatusNoContent, nil, nil
}

func (ph *HttpHandler) Logout(ctx context.Context, r *http.Request) (int, []byte, error) {
	log.Trace("HttpHandler Logout")

	userID, err := transport.ReadUserIDHeader(ctx, r)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}
	ctx = context.WithValue(ctx, contextKey("UserID"), userID.String())

	sessionID, err := transport.ReadSessionIDHeader(ctx, r)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}
	ctx = context.WithValue(ctx, contextKey("SessionID"), sessionID)

	if err := ph.profilesUseCase.Logout(ctx, sessionID); err != nil {
		log.WithContext(ctx).WithError(err).Error("logout failed")
		return http.StatusInternalServerError, nil, err
	}

	return http.StatusNoContent, nil, nil
}

func (ph *HttpHandler) CreateOAuthAuthorization(ctx context.Context, r *http.Request) (int, []byte, error) {
	log.Trace("HttpHandler CreateOAuthAuthorization")

	reqBody, err := transport.ReadReqBody(ctx, r)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}

	payload, err := ph.profilesDTOAdapter.FromOAuthAuthorizeRequestDTO(ctx, reqBody)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}

	provider := mux.Vars(r)["provider"]
	result, err := ph.profilesUseCase.CreateOAuthAuthorization(ctx, provider, *payload)
	if err != nil {
		if errors.Is(err, app.ErrUnsupportedOAuthProvider) {
			return http.StatusBadRequest, nil, err
		}
		log.WithContext(ctx).WithError(err).Error("oauth authorization setup failed")
		return http.StatusInternalServerError, nil, err
	}

	respBytes, err := ph.profilesDTOAdapter.ToOAuthAuthorizeResultDTO(ctx, result)
	if err != nil {
		return http.StatusInternalServerError, nil, err
	}
	return http.StatusOK, respBytes, nil
}

func (ph *HttpHandler) CreateOAuthSession(ctx context.Context, r *http.Request) (int, []byte, error) {
	log.Trace("HttpHandler CreateOAuthSession")

	reqBody, err := transport.ReadReqBody(ctx, r)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}

	payload, err := ph.profilesDTOAdapter.FromOAuthSessionRequestDTO(ctx, reqBody)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}

	provider := mux.Vars(r)["provider"]
	result, err := ph.profilesUseCase.CreateOAuthSession(ctx, provider, *payload)
	if err != nil {
		switch {
		case errors.Is(err, app.ErrUnsupportedOAuthProvider):
			return http.StatusBadRequest, nil, err
		case errors.Is(err, app.ErrInvalidOAuthState),
			errors.Is(err, app.ErrInvalidOAuthCode),
			errors.Is(err, app.ErrOAuthEmailRequired),
			errors.Is(err, app.ErrOAuthEmailUnverified):
			return http.StatusUnauthorized, nil, err
		default:
			log.WithContext(ctx).WithError(err).Error("oauth session creation failed")
			return http.StatusInternalServerError, nil, err
		}
	}

	respBytes, err := ph.profilesDTOAdapter.ToOAuthSessionResultDTO(ctx, result)
	if err != nil {
		return http.StatusInternalServerError, nil, err
	}
	return http.StatusOK, respBytes, nil
}

func (ph *HttpHandler) ReadCurrentOrganization(ctx context.Context, r *http.Request) (int, []byte, error) {
	log.Trace("HttpHandler ReadCurrentOrganization")

	userID, orgID, err := readActorOrgHeaders(ctx, r)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}
	organization, membership, err := ph.profilesUseCase.ReadCurrentOrganization(ctx, userID, orgID)
	if err != nil {
		return httpStatusFor(err), nil, err
	}
	body, err := ph.profilesDTOAdapter.ToOrganizationDTO(ctx, organization, membership)
	if err != nil {
		return http.StatusInternalServerError, nil, err
	}
	return http.StatusOK, body, nil
}

func (ph *HttpHandler) ListOrganizationMembers(ctx context.Context, r *http.Request) (int, []byte, error) {
	log.Trace("HttpHandler ListOrganizationMembers")

	userID, _, err := readActorOrgHeaders(ctx, r)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}
	orgID, err := readUUIDRouteParam(r, "orgId")
	if err != nil {
		return http.StatusBadRequest, nil, err
	}
	members, err := ph.profilesUseCase.ListOrganizationMembers(ctx, userID, orgID)
	if err != nil {
		return httpStatusFor(err), nil, err
	}
	body, err := ph.profilesDTOAdapter.ToOrganizationMembersDTO(ctx, members)
	if err != nil {
		return http.StatusInternalServerError, nil, err
	}
	return http.StatusOK, body, nil
}

func (ph *HttpHandler) UpsertOrganizationMember(ctx context.Context, r *http.Request) (int, []byte, error) {
	log.Trace("HttpHandler UpsertOrganizationMember")

	userID, _, err := readActorOrgHeaders(ctx, r)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}
	orgID, err := readUUIDRouteParam(r, "orgId")
	if err != nil {
		return http.StatusBadRequest, nil, err
	}
	reqBody, err := transport.ReadReqBody(ctx, r)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}
	membership, err := ph.profilesDTOAdapter.FromOrganizationMemberDTO(ctx, orgID, reqBody)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}
	if routeUserIDRaw := mux.Vars(r)["userId"]; routeUserIDRaw != "" {
		routeUserID, err := readUUIDRouteParam(r, "userId")
		if err != nil {
			return http.StatusBadRequest, nil, err
		}
		if membership.UserID != routeUserID {
			return http.StatusBadRequest, nil, domain.ErrValidationFailed
		}
	}
	updated, err := ph.profilesUseCase.UpsertOrganizationMember(ctx, userID, membership)
	if err != nil {
		return httpStatusFor(err), nil, err
	}
	body, err := ph.profilesDTOAdapter.ToOrganizationMembersDTO(ctx, []*domain.OrganizationMembership{updated})
	if err != nil {
		return http.StatusInternalServerError, nil, err
	}
	return http.StatusOK, body, nil
}

func (ph *HttpHandler) DeleteOrganizationMember(ctx context.Context, r *http.Request) (int, []byte, error) {
	log.Trace("HttpHandler DeleteOrganizationMember")

	userID, _, err := readActorOrgHeaders(ctx, r)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}
	orgID, err := readUUIDRouteParam(r, "orgId")
	if err != nil {
		return http.StatusBadRequest, nil, err
	}
	memberUserID, err := readUUIDRouteParam(r, "userId")
	if err != nil {
		return http.StatusBadRequest, nil, err
	}
	if err := ph.profilesUseCase.DeleteOrganizationMember(ctx, userID, orgID, memberUserID); err != nil {
		return httpStatusFor(err), nil, err
	}
	return http.StatusNoContent, nil, nil
}

func readActorOrgHeaders(ctx context.Context, r *http.Request) (uuid.UUID, uuid.UUID, error) {
	log.Trace("readActorOrgHeaders")

	userID, err := transport.ReadUserIDHeader(ctx, r)
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	orgID, err := transport.ReadOrgIDHeader(ctx, r)
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	return userID, orgID, nil
}

func readUUIDRouteParam(r *http.Request, name string) (uuid.UUID, error) {
	log.Trace("readUUIDRouteParam")

	value := mux.Vars(r)[name]
	parsed, err := uuid.Parse(value)
	if err != nil || parsed == uuid.Nil {
		return uuid.Nil, domain.ErrValidationFailed
	}
	return parsed, nil
}

func (ph *HttpHandler) requestToProfile(ctx context.Context, r *http.Request) (*domain.Profile, error) {
	log.Trace("HttpHandler requestToProfile")

	reqBody, err := transport.ReadReqBody(ctx, r)
	if err != nil {
		return nil, err
	}

	profile, err := ph.profilesDTOAdapter.FromDTO(ctx, reqBody)
	if err != nil {
		return nil, err
	}

	return profile, nil
}

func (ph *HttpHandler) requestToProfileAccount(ctx context.Context, r *http.Request) (*domain.ProfileAccount, error) {
	log.Trace("HttpHandler requestToProfileAccount")

	reqBody, err := transport.ReadReqBody(ctx, r)
	if err != nil {
		return nil, err
	}

	profileAccount, err := ph.profilesDTOAdapter.FromProfileAccountDTO(ctx, reqBody)
	if err != nil {
		return nil, err
	}

	return profileAccount, nil
}

func httpStatusFor(err error) int {
	return sharedrpc.MapToHTTPStatus(err,
		sharedrpc.HTTPStatus(http.StatusNotFound,
			domain.ErrProfileNotFound,
			domain.ErrOAuthIdentityNotFound,
		),
		sharedrpc.HTTPStatus(http.StatusConflict,
			domain.ErrProfileAlreadyExists,
		),
		sharedrpc.HTTPStatus(http.StatusBadRequest,
			domain.ErrValidationFailed,
		),
		sharedrpc.HTTPStatus(http.StatusForbidden,
			domain.ErrUnauthorized,
		),
	)
}
