package usecase

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	auth "lib/shared_lib/auth"
	sharedclock "lib/shared_lib/clock"
	usecasetrace "lib/shared_lib/usecasetrace"

	"github.com/alexedwards/argon2id"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"

	"profile_service/pkg/domain"
)

var params = &argon2id.Params{
	Memory:      64 * 1024, // 64 MB
	Iterations:  3,
	Parallelism: 1,
	SaltLength:  16,
	KeyLength:   32,
}

const stagingTestEmailVerifyToken = "staging-test-email-verify-token"

func hashPassword(plain string) (string, error) {
	return argon2id.CreateHash(plain, params)
}

func verifyPassword(plain, encoded string) (bool, error) {
	match, err := argon2id.ComparePasswordAndHash(plain, encoded)
	return match, err
}

type ProfilesUseCase interface {
	CreateProfile(ctx context.Context, profile *domain.ProfileAccount, idempotencyKey uuid.UUID) error
	ReplaceProfile(ctx context.Context, userID uuid.UUID, profile *domain.Profile) (*domain.Profile, error)
	ReadProfile(ctx context.Context, userID uuid.UUID) (*domain.Profile, error)
	DeleteProfile(ctx context.Context, userID uuid.UUID) error
	ReplacePassword(ctx context.Context, userID uuid.UUID, newPassword string) error
	VerifyPassword(ctx context.Context, email, newPassword string) (string, error)
	VerifyEmail(ctx context.Context, token string) error
	CreateOAuthAuthorization(ctx context.Context, provider string, req OAuthAuthorizeRequest) (*OAuthAuthorizeResult, error)
	CreateOAuthSession(ctx context.Context, provider string, req OAuthSessionRequest) (*OAuthSessionResult, error)
	Logout(ctx context.Context, sessionID string) error
}

type profilesUseCase struct {
	profilesRepository      ProfileDB
	msgPublisher            UserEventPublisher
	authProvider            auth.AuthProvider
	authStore               auth.RevocationStore
	authExpirationInMinutes int
	emailValidationTTL      time.Duration
	oauthProviders          map[string]OAuthProviderClient
	oauthStateStore         OAuthStateStore
	oauthStateTTL           time.Duration
	useStagingTestToken     bool
	clock                   sharedclock.Clock
}

type ProfilesUseCaseDeps struct {
	ProfilesRepository ProfileDB
	MsgPublisher       UserEventPublisher
	AuthStore          auth.RevocationStore
	AuthProvider       auth.AuthProvider
}

type ProfilesUseCaseConfig struct {
	AuthExpirationInMinutes int
	EmailValidationTTL      time.Duration
	OAuthProviders          map[string]OAuthProviderClient
	OAuthStateStore         OAuthStateStore
	OAuthStateTTL           time.Duration
	UseStagingTestToken     bool
	Clock                   sharedclock.Clock
}

type ProfilesUseCaseOption func(*ProfilesUseCaseConfig)

func WithProfileOAuth(providers map[string]OAuthProviderClient, stateStore OAuthStateStore, stateTTL time.Duration) ProfilesUseCaseOption {
	return func(cfg *ProfilesUseCaseConfig) {
		cfg.OAuthProviders = providers
		cfg.OAuthStateStore = stateStore
		cfg.OAuthStateTTL = stateTTL
	}
}

func WithStagingTestEmailToken(enabled bool) ProfilesUseCaseOption {
	return func(cfg *ProfilesUseCaseConfig) {
		cfg.UseStagingTestToken = enabled
	}
}

func WithProfileClock(clock sharedclock.Clock) ProfilesUseCaseOption {
	return func(cfg *ProfilesUseCaseConfig) {
		cfg.Clock = clock
	}
}

func NewProfilesUseCase(deps ProfilesUseCaseDeps, cfg ProfilesUseCaseConfig, opts ...ProfilesUseCaseOption) ProfilesUseCase {
	log.Trace("NewProfilesUseCase")
	for _, opt := range opts {
		opt(&cfg)
	}
	clock := cfg.Clock
	if clock == nil {
		log.Fatal("NewProfilesUseCase: clock is required")
	}
	return &profilesUseCase{
		profilesRepository:      deps.ProfilesRepository,
		msgPublisher:            deps.MsgPublisher,
		authProvider:            deps.AuthProvider,
		authStore:               deps.AuthStore,
		authExpirationInMinutes: cfg.AuthExpirationInMinutes,
		emailValidationTTL:      cfg.EmailValidationTTL,
		oauthProviders:          cfg.OAuthProviders,
		oauthStateStore:         cfg.OAuthStateStore,
		oauthStateTTL:           cfg.OAuthStateTTL,
		useStagingTestToken:     cfg.UseStagingTestToken,
		clock:                   clock,
	}
}

func (u *profilesUseCase) CreateProfile(ctx context.Context, profileAccount *domain.ProfileAccount, idempotencyKey uuid.UUID) (err error) {
	log.Trace("profilesUseCase CreateProfile")
	ctx, span := usecasetrace.StartSpan(ctx, "profile_service/app", "profile.create_profile",
		attribute.String("email", profileAccount.Email),
		attribute.String("idempotency_key", idempotencyKey.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	passwordHash, err := hashPassword(profileAccount.Password)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("Failed to hash password")
		return fmt.Errorf("failed to hash password: %w", err)
	}
	profileAccount.Password = passwordHash
	profileAccount.EmailVerifyToken, err = generateEmailVerifyToken(profileAccount.Email, u.useStagingTestToken)
	if err != nil {
		return fmt.Errorf("generate email verify token: %w", err)
	}
	profileAccount.EmailVerifyExpiresAt = u.clock.Now().Add(u.emailValidationTTL)

	if err = u.profilesRepository.Save(ctx, profileAccount, idempotencyKey); err != nil {
		return err
	}

	if err = u.msgPublisher.PublishUserCreatedEvent(ctx, profileAccount); err != nil {
		return err
	}
	if !profileAccount.EmailVerified {
		if err = u.msgPublisher.PublishEmailVerificationRequestedEvent(ctx, profileAccount); err != nil {
			return err
		}
	}

	return nil
}

func (u *profilesUseCase) ReplaceProfile(ctx context.Context, userID uuid.UUID, profile *domain.Profile) (updatedProfile *domain.Profile, err error) {
	log.Trace("profilesUseCase ReplaceProfile")
	ctx, span := usecasetrace.StartSpan(ctx, "profile_service/app", "profile.replace_profile",
		attribute.String("user_id", userID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	updatedProfile, err = u.profilesRepository.Update(ctx, userID, profile)
	if err != nil {
		return nil, err
	}

	if err := u.msgPublisher.PublishUserUpdatedEvent(ctx, updatedProfile); err != nil {
		return nil, err
	}

	return updatedProfile, nil
}

func (u *profilesUseCase) ReadProfile(ctx context.Context, userID uuid.UUID) (profile *domain.Profile, err error) {
	log.Trace("profilesUseCase ReadProfile")
	ctx, span := usecasetrace.StartSpan(ctx, "profile_service/app", "profile.read_profile",
		attribute.String("user_id", userID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	return u.profilesRepository.Read(ctx, userID)
}

func (u *profilesUseCase) DeleteProfile(ctx context.Context, userID uuid.UUID) (err error) {
	log.Trace("profilesUseCase DeleteProfile")
	ctx, span := usecasetrace.StartSpan(ctx, "profile_service/app", "profile.delete_profile",
		attribute.String("user_id", userID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	if err := u.profilesRepository.Delete(ctx, userID); err != nil {
		return err
	}

	return u.msgPublisher.PublishUserDeletedEvent(ctx, userID)
}

func (u *profilesUseCase) ReplacePassword(ctx context.Context, userID uuid.UUID, newPassword string) (err error) {
	log.Trace("profilesUseCase ReplacePassword")
	ctx, span := usecasetrace.StartSpan(ctx, "profile_service/app", "profile.replace_password",
		attribute.String("user_id", userID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	passwordHash, err := hashPassword(newPassword)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("Failed to hash password")
		return fmt.Errorf("failed to hash password: %w", err)
	}

	if err := u.profilesRepository.UpdatePassword(ctx, userID, passwordHash); err != nil {
		return err
	}

	now := u.clock.Now().Unix()
	if err := u.authStore.SetUserRevokedAfter(ctx, userID.String(), now); err != nil {
		log.WithContext(ctx).WithError(err).Error("Failed to revoke user sessions after password change")
		return fmt.Errorf("failed to revoke user sessions: %w", err)
	}

	return nil
}

func (u *profilesUseCase) VerifyPassword(ctx context.Context, email, password string) (authToken string, err error) {
	log.Trace("profilesUseCase VerifyPassword")
	ctx, span := usecasetrace.StartSpan(ctx, "profile_service/app", "profile.verify_password",
		attribute.String("email", email),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	userID, passwordHash, err := u.profilesRepository.ReadPasswordHash(ctx, email)
	if err != nil {
		return "", err
	}

	match, err := verifyPassword(password, passwordHash)
	if err != nil {
		return "", err
	}
	if !match {
		return "", fmt.Errorf("invalid password")
	}

	authToken, sid, expiresAt, err := u.authProvider.CreateToken(ctx, userID, u.authExpirationInMinutes)
	if err != nil {
		return "", err
	}

	if err := u.authStore.CreateSession(ctx, sid, expiresAt); err != nil {
		return "", err
	}

	return authToken, nil
}

func (u *profilesUseCase) VerifyEmail(ctx context.Context, token string) (err error) {
	log.Trace("profilesUseCase VerifyEmail")
	ctx, span := usecasetrace.StartSpan(ctx, "profile_service/app", "profile.verify_email")
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	profile, err := u.profilesRepository.VerifyEmail(ctx, token)
	if err != nil {
		return err
	}

	return u.msgPublisher.PublishUserUpdatedEvent(ctx, profile)
}

func (u *profilesUseCase) Logout(ctx context.Context, sessionID string) (err error) {
	log.Trace("profilesUseCase Logout")
	ctx, span := usecasetrace.StartSpan(ctx, "profile_service/app", "profile.logout")
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	if err := u.authStore.DeleteSession(ctx, sessionID); err != nil {
		log.WithContext(ctx).WithError(err).Error("Failed to delete session")
		return fmt.Errorf("failed to delete session: %w", err)
	}

	return nil
}

func generateEmailVerifyToken(email string, useStagingTestToken bool) (string, error) {
	if shouldUseStagingTestEmailToken(email, useStagingTestToken) {
		return stagingTestEmailVerifyToken, nil
	}

	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(tokenBytes), nil
}

func shouldUseStagingTestEmailToken(email string, useStagingTestToken bool) bool {
	if !useStagingTestToken {
		return false
	}
	parts := strings.Split(strings.TrimSpace(email), "@")
	if len(parts) != 2 {
		return false
	}
	return strings.EqualFold(parts[1], "test.com")
}
