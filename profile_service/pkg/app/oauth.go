package usecase

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	usecasetrace "lib/shared_lib/usecasetrace"
	"profile_service/pkg/domain"

	"github.com/google/uuid"
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
	provider = normalizeOAuthProvider(provider)
	ctx, span := usecasetrace.StartSpan(ctx, "profile_service/app", "profile.create_oauth_authorization",
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
	provider = normalizeOAuthProvider(provider)
	ctx, span := usecasetrace.StartSpan(ctx, "profile_service/app", "profile.create_oauth_session",
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
		userID, isNewUser, err := u.findOrCreateOAuthProfile(ctx, *identity, passwordHash)
		if err != nil {
			return nil, err
		}
		if err := u.profilesRepository.SaveOAuthIdentity(ctx, userID, *identity); err != nil {
			return nil, err
		}
		if isNewUser {
			profile, err := u.profilesRepository.Read(ctx, userID)
			if err != nil {
				return nil, err
			}
			if err := u.msgPublisher.PublishUserCreatedEvent(ctx, &profile.ProfileAccount); err != nil {
				return nil, err
			}
		}
		return u.createOAuthSession(ctx, provider, userID, isNewUser)
	default:
		return nil, err
	}

	return u.createOAuthSession(ctx, provider, userID, false)
}

func (u *profilesUseCase) findOrCreateOAuthProfile(ctx context.Context, identity domain.OAuthIdentity, passwordHash string) (uuid.UUID, bool, error) {
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

func (u *profilesUseCase) createOAuthSession(ctx context.Context, provider string, userID uuid.UUID, isNewUser bool) (*OAuthSessionResult, error) {
	authToken, sid, expiresAt, err := u.authProvider.CreateToken(ctx, userID, u.authExpirationInMinutes)
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
	return strings.ToLower(strings.TrimSpace(provider))
}

func matchesCodeChallenge(codeVerifier, expectedCodeChallenge string) bool {
	if codeVerifier == "" || expectedCodeChallenge == "" {
		return false
	}
	sum := sha256.Sum256([]byte(codeVerifier))
	got := base64.RawURLEncoding.EncodeToString(sum[:])
	return got == expectedCodeChallenge
}

func generateSessionSecret() string {
	// OAuth state/code secrets are bearer nonces, not database row IDs.
	return uuid.NewString() + uuid.NewString()
}
