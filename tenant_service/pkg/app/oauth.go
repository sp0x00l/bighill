package usecase

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"lib/shared_lib/authz"
	shareduow "lib/shared_lib/uow"
	usecasetrace "lib/shared_lib/usecasetrace"
	"tenant_service/pkg/domain"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

var (
	ErrUnsupportedOAuthProvider = domain.ErrUnsupportedOAuthProvider
	ErrInvalidOAuthState        = domain.ErrInvalidOAuthState
	ErrInvalidOAuthCode         = domain.ErrInvalidOAuthCode
	ErrOAuthEmailRequired       = domain.ErrOAuthEmailRequired
	ErrOAuthEmailUnverified     = domain.ErrOAuthEmailUnverified
)

type OAuthAuthorizeRequest struct {
	RedirectURI   string
	CodeChallenge string
}

type OAuthAuthorizeResult struct {
	AuthorizationURL string
	State            string
}

type OAuthSessionRequest struct {
	Code         string
	State        string
	RedirectURI  string
	CodeVerifier string
}

type OAuthSessionResult struct {
	Token     string
	Provider  string
	IsNewUser bool
}

func (u *profilesUseCase) CreateOAuthAuthorization(ctx context.Context, provider string, req OAuthAuthorizeRequest) (result *OAuthAuthorizeResult, err error) {
	log.Trace("profilesUseCase CreateOAuthAuthorization")

	provider = normalizeOAuthProvider(provider)
	ctx, span := usecasetrace.StartSpan(ctx, "tenant_service/app", "profile.create_oauth_authorization",
		attribute.String("provider", provider),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	client, ok := u.oauthProviders[provider]
	if !ok {
		return nil, ErrUnsupportedOAuthProvider
	}

	state := domain.OAuthState{
		State:         generateSessionSecret(),
		Provider:      provider,
		RedirectURI:   strings.TrimSpace(req.RedirectURI),
		CodeChallenge: strings.TrimSpace(req.CodeChallenge),
	}

	if err := u.oauthStateStore.Save(ctx, state, u.oauthStateTTL); err != nil {
		return nil, fmt.Errorf("failed to persist oauth state: %w", err)
	}

	authURL, err := client.AuthorizationURL(state.State, state.RedirectURI, state.CodeChallenge)
	if err != nil {
		return nil, err
	}

	return &OAuthAuthorizeResult{
		AuthorizationURL: authURL,
		State:            state.State,
	}, nil
}

func (u *profilesUseCase) CreateOAuthSession(ctx context.Context, provider string, req OAuthSessionRequest) (result *OAuthSessionResult, err error) {
	log.Trace("profilesUseCase CreateOAuthSession")

	provider = normalizeOAuthProvider(provider)
	ctx, span := usecasetrace.StartSpan(ctx, "tenant_service/app", "profile.create_oauth_session",
		attribute.String("provider", provider),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	client, ok := u.oauthProviders[provider]
	if !ok {
		return nil, ErrUnsupportedOAuthProvider
	}

	state, err := u.oauthStateStore.Load(ctx, strings.TrimSpace(req.State))
	if err != nil {
		return nil, fmt.Errorf("failed to load oauth state: %w", err)
	}
	if state == nil || state.Provider != provider || state.RedirectURI != strings.TrimSpace(req.RedirectURI) {
		return nil, ErrInvalidOAuthState
	}
	if !matchesCodeChallenge(strings.TrimSpace(req.CodeVerifier), state.CodeChallenge) {
		return nil, ErrInvalidOAuthState
	}
	if err := u.oauthStateStore.Delete(ctx, state.State); err != nil {
		return nil, fmt.Errorf("failed to consume oauth state: %w", err)
	}

	identity, err := client.BigHillCode(ctx, strings.TrimSpace(req.Code), state.RedirectURI, strings.TrimSpace(req.CodeVerifier))
	if err != nil {
		return nil, err
	}
	if identity == nil {
		return nil, ErrInvalidOAuthCode
	}
	if strings.TrimSpace(identity.Email) == "" {
		return nil, ErrOAuthEmailRequired
	}
	if !identity.EmailVerified {
		return nil, ErrOAuthEmailUnverified
	}

	passwordHash, err := hashPassword(generateSessionSecret())
	if err != nil {
		return nil, fmt.Errorf("failed to create oauth profile secret: %w", err)
	}

	userID, err := u.profilesRepository.ReadOAuthProfileIDByProviderSubject(ctx, identity.Provider, identity.Subject)
	switch {
	case err == nil:
	case errors.Is(err, domain.ErrOAuthIdentityNotFound):
		var isNewUser bool
		err := u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, enqueue shareduow.EnqueueFunc) error {
			var upsertErr error
			userID, isNewUser, upsertErr = u.findOrCreateOAuthProfileTx(ctx, tx, *identity, passwordHash)
			if upsertErr != nil {
				return upsertErr
			}
			if err := u.profilesRepository.SaveOAuthIdentityTx(ctx, tx, userID, *identity); err != nil {
				return err
			}
			if !isNewUser {
				return nil
			}
			profile, err := u.profilesRepository.ReadTx(ctx, tx, userID)
			if err != nil {
				return err
			}
			return enqueue(u.eventBuilder.UserCreatedMessage(&profile.ProfileAccount))
		})
		if err != nil {
			return nil, err
		}
		return u.createOAuthSession(ctx, provider, userID, isNewUser)
	default:
		return nil, err
	}

	return u.createOAuthSession(ctx, provider, userID, false)
}

func (u *profilesUseCase) findOrCreateOAuthProfile(ctx context.Context, identity domain.OAuthIdentity, passwordHash string) (uuid.UUID, bool, error) {
	log.Trace("profilesUseCase findOrCreateOAuthProfile")

	userID, err := u.profilesRepository.ReadProfileIDByEmail(ctx, identity.Email)
	switch {
	case err == nil:
		return userID, false, nil
	case errors.Is(err, domain.ErrProfileNotFound):
		userID, err = u.profilesRepository.CreateOAuthProfile(ctx, identity, passwordHash)
		if err != nil {
			return uuid.Nil, false, err
		}
		return userID, true, nil
	default:
		return uuid.Nil, false, err
	}
}

func (u *profilesUseCase) findOrCreateOAuthProfileTx(ctx context.Context, tx pgx.Tx, identity domain.OAuthIdentity, passwordHash string) (uuid.UUID, bool, error) {
	log.Trace("profilesUseCase findOrCreateOAuthProfileTx")

	userID, err := u.profilesRepository.ReadProfileIDByEmail(ctx, identity.Email)
	switch {
	case err == nil:
		return userID, false, nil
	case errors.Is(err, domain.ErrProfileNotFound):
		userID, err = u.profilesRepository.CreateOAuthProfileTx(ctx, tx, identity, passwordHash)
		if err != nil {
			return uuid.Nil, false, err
		}
		return userID, true, nil
	default:
		return uuid.Nil, false, err
	}
}

func (u *profilesUseCase) createOAuthSession(ctx context.Context, provider string, userID uuid.UUID, isNewUser bool) (*OAuthSessionResult, error) {
	log.Trace("profilesUseCase createOAuthSession")

	membership, err := u.profilesRepository.ReadDefaultMembership(ctx, userID)
	if err != nil {
		return nil, err
	}
	authToken, sid, expiresAt, err := u.authProvider.CreateAccessToken(ctx, authz.TokenClaims{
		UserID:      userID.String(),
		OrgID:       membership.OrgID.String(),
		Roles:       []string{membership.Role},
		Permissions: authz.PermissionsForRole(membership.Role),
		TokenType:   "access",
	}, u.authExpirationInMinutes)
	if err != nil {
		return nil, err
	}
	if err := u.authStore.CreateSession(ctx, sid, expiresAt); err != nil {
		return nil, err
	}

	return &OAuthSessionResult{
		Token:     authToken,
		Provider:  provider,
		IsNewUser: isNewUser,
	}, nil
}

func normalizeOAuthProvider(provider string) string {
	log.Trace("normalizeOAuthProvider")

	return strings.ToLower(strings.TrimSpace(provider))
}

func matchesCodeChallenge(codeVerifier, expectedCodeChallenge string) bool {
	log.Trace("matchesCodeChallenge")

	if codeVerifier == "" || expectedCodeChallenge == "" {
		return false
	}
	sum := sha256.Sum256([]byte(codeVerifier))
	got := base64.RawURLEncoding.EncodeToString(sum[:])
	return got == expectedCodeChallenge
}

func generateSessionSecret() string {
	log.Trace("generateSessionSecret")

	// OAuth state/code secrets are bearer nonces, not database row IDs.
	return uuid.NewString() + uuid.NewString()
}
