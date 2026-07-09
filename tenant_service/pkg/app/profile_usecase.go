package usecase

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	auth "lib/shared_lib/auth"
	"lib/shared_lib/authz"
	sharedclock "lib/shared_lib/clock"
	shareduow "lib/shared_lib/uow"
	usecasetrace "lib/shared_lib/usecasetrace"

	"github.com/alexedwards/argon2id"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"

	"tenant_service/pkg/domain"
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
	ReplaceHuggingFaceToken(ctx context.Context, userID uuid.UUID, token string) error
	VerifyPassword(ctx context.Context, email, newPassword string) (string, error)
	VerifyEmail(ctx context.Context, token string) error
	CreateOAuthAuthorization(ctx context.Context, provider string, req OAuthAuthorizeRequest) (*OAuthAuthorizeResult, error)
	CreateOAuthSession(ctx context.Context, provider string, req OAuthSessionRequest) (*OAuthSessionResult, error)
	Logout(ctx context.Context, sessionID string) error
	ReadCurrentOrganization(ctx context.Context, actorUserID uuid.UUID, orgID uuid.UUID) (*domain.Organization, *domain.OrganizationMembership, error)
	ListOrganizationMembers(ctx context.Context, actorUserID uuid.UUID, orgID uuid.UUID) ([]*domain.OrganizationMembership, error)
	UpsertOrganizationMember(ctx context.Context, actorUserID uuid.UUID, membership *domain.OrganizationMembership) (*domain.OrganizationMembership, error)
	DeleteOrganizationMember(ctx context.Context, actorUserID uuid.UUID, orgID uuid.UUID, memberUserID uuid.UUID) error
}

type profilesUseCase struct {
	profilesRepository      ProfileDB
	unitOfWork              ProfileUnitOfWorkAdapter
	eventBuilder            UserEventBuilderAdapter
	authProvider            auth.AuthProvider
	authStore               auth.RevocationStore
	secretEncryptor         SecretEncryptor
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
	UnitOfWork         ProfileUnitOfWorkAdapter
	EventBuilder       UserEventBuilderAdapter
	AuthStore          auth.RevocationStore
	AuthProvider       auth.AuthProvider
	SecretEncryptor    SecretEncryptor
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
	return &profilesUseCase{
		profilesRepository:      deps.ProfilesRepository,
		unitOfWork:              deps.UnitOfWork,
		eventBuilder:            deps.EventBuilder,
		authProvider:            deps.AuthProvider,
		secretEncryptor:         deps.SecretEncryptor,
		authStore:               deps.AuthStore,
		authExpirationInMinutes: cfg.AuthExpirationInMinutes,
		emailValidationTTL:      cfg.EmailValidationTTL,
		oauthProviders:          cfg.OAuthProviders,
		oauthStateStore:         cfg.OAuthStateStore,
		oauthStateTTL:           cfg.OAuthStateTTL,
		useStagingTestToken:     cfg.UseStagingTestToken,
		clock:                   cfg.Clock,
	}
}

func (u *profilesUseCase) ReplaceHuggingFaceToken(ctx context.Context, userID uuid.UUID, token string) (err error) {
	log.Trace("profilesUseCase ReplaceHuggingFaceToken")
	ctx, span := usecasetrace.StartSpan(ctx, "tenant_service/app", "profile.replace_huggingface_token",
		attribute.String("user_id", userID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	ciphertext, err := u.secretEncryptor.Encrypt(ctx, token)
	if err != nil {
		return fmt.Errorf("encrypt hugging face token: %w", err)
	}
	return u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, enqueue shareduow.EnqueueFunc) error {
		updatedProfile, err := u.profilesRepository.UpdateHuggingFaceTokenTx(ctx, tx, userID, ciphertext)
		if err != nil {
			return err
		}
		return enqueue(u.eventBuilder.UserUpdatedMessage(updatedProfile))
	})
}

func (u *profilesUseCase) CreateProfile(ctx context.Context, profileAccount *domain.ProfileAccount, idempotencyKey uuid.UUID) (err error) {
	log.Trace("profilesUseCase CreateProfile")
	ctx, span := usecasetrace.StartSpan(ctx, "tenant_service/app", "profile.create_profile",
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

	return u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, enqueue shareduow.EnqueueFunc) error {
		if err := u.profilesRepository.SaveTx(ctx, tx, profileAccount, idempotencyKey); err != nil {
			return err
		}
		return enqueue(u.eventBuilder.UserCreatedMessage(profileAccount))
	})
}

func (u *profilesUseCase) ReplaceProfile(ctx context.Context, userID uuid.UUID, profile *domain.Profile) (updatedProfile *domain.Profile, err error) {
	log.Trace("profilesUseCase ReplaceProfile")
	ctx, span := usecasetrace.StartSpan(ctx, "tenant_service/app", "profile.replace_profile",
		attribute.String("user_id", userID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	err = u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, enqueue shareduow.EnqueueFunc) error {
		var updateErr error
		updatedProfile, updateErr = u.profilesRepository.UpdateTx(ctx, tx, userID, profile)
		if updateErr != nil {
			return updateErr
		}
		return enqueue(u.eventBuilder.UserUpdatedMessage(updatedProfile))
	})
	return updatedProfile, err
}

func (u *profilesUseCase) ReadProfile(ctx context.Context, userID uuid.UUID) (profile *domain.Profile, err error) {
	log.Trace("profilesUseCase ReadProfile")
	ctx, span := usecasetrace.StartSpan(ctx, "tenant_service/app", "profile.read_profile",
		attribute.String("user_id", userID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	return u.profilesRepository.Read(ctx, userID)
}

func (u *profilesUseCase) DeleteProfile(ctx context.Context, userID uuid.UUID) (err error) {
	log.Trace("profilesUseCase DeleteProfile")
	ctx, span := usecasetrace.StartSpan(ctx, "tenant_service/app", "profile.delete_profile",
		attribute.String("user_id", userID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	return u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, enqueue shareduow.EnqueueFunc) error {
		if err := u.profilesRepository.DeleteTx(ctx, tx, userID); err != nil {
			return err
		}
		return enqueue(u.eventBuilder.UserDeletedMessage(userID))
	})
}

func (u *profilesUseCase) ReplacePassword(ctx context.Context, userID uuid.UUID, newPassword string) (err error) {
	log.Trace("profilesUseCase ReplacePassword")
	ctx, span := usecasetrace.StartSpan(ctx, "tenant_service/app", "profile.replace_password",
		attribute.String("user_id", userID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	passwordHash, err := hashPassword(newPassword)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("Failed to hash password")
		return fmt.Errorf("failed to hash password: %w", err)
	}

	now := u.clock.Now().Unix()
	if err := u.authStore.SetUserRevokedAfter(ctx, userID.String(), now); err != nil {
		log.WithContext(ctx).WithError(err).Error("Failed to revoke user sessions before password change")
		return fmt.Errorf("failed to revoke user sessions: %w", err)
	}

	if err := u.profilesRepository.UpdatePassword(ctx, userID, passwordHash); err != nil {
		return err
	}

	return nil
}

func (u *profilesUseCase) VerifyPassword(ctx context.Context, email, password string) (authToken string, err error) {
	log.Trace("profilesUseCase VerifyPassword")
	ctx, span := usecasetrace.StartSpan(ctx, "tenant_service/app", "profile.verify_password",
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

	membership, err := u.profilesRepository.ReadDefaultMembership(ctx, userID)
	if err != nil {
		return "", err
	}
	authToken, sid, expiresAt, err := u.authProvider.CreateAccessToken(ctx, authz.TokenClaims{
		UserID:      userID.String(),
		OrgID:       membership.OrgID.String(),
		Roles:       []string{membership.Role},
		Permissions: authz.PermissionsForRole(membership.Role),
		TokenType:   "access",
	}, u.authExpirationInMinutes)
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
	ctx, span := usecasetrace.StartSpan(ctx, "tenant_service/app", "profile.verify_email")
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	return u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, enqueue shareduow.EnqueueFunc) error {
		profile, err := u.profilesRepository.VerifyEmailTx(ctx, tx, token)
		if err != nil {
			return err
		}
		return enqueue(u.eventBuilder.UserUpdatedMessage(profile))
	})
}

func (u *profilesUseCase) Logout(ctx context.Context, sessionID string) (err error) {
	log.Trace("profilesUseCase Logout")
	ctx, span := usecasetrace.StartSpan(ctx, "tenant_service/app", "profile.logout")
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	if err := u.authStore.DeleteSession(ctx, sessionID); err != nil {
		log.WithContext(ctx).WithError(err).Error("Failed to delete session")
		return fmt.Errorf("failed to delete session: %w", err)
	}

	return nil
}

func (u *profilesUseCase) ReadCurrentOrganization(ctx context.Context, actorUserID uuid.UUID, orgID uuid.UUID) (organization *domain.Organization, membership *domain.OrganizationMembership, err error) {
	log.Trace("profilesUseCase ReadCurrentOrganization")
	ctx, span := usecasetrace.StartSpan(ctx, "tenant_service/app", "org.read_current",
		attribute.String("user_id", actorUserID.String()),
		attribute.String("org_id", orgID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	membership, err = u.requireActiveMembership(ctx, orgID, actorUserID)
	if err != nil {
		return nil, nil, err
	}
	organization, err = u.profilesRepository.ReadOrganization(ctx, orgID)
	if err != nil {
		return nil, nil, err
	}
	return organization, membership, nil
}

func (u *profilesUseCase) ListOrganizationMembers(ctx context.Context, actorUserID uuid.UUID, orgID uuid.UUID) (memberships []*domain.OrganizationMembership, err error) {
	log.Trace("profilesUseCase ListOrganizationMembers")
	ctx, span := usecasetrace.StartSpan(ctx, "tenant_service/app", "org.list_members",
		attribute.String("user_id", actorUserID.String()),
		attribute.String("org_id", orgID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	if _, err := u.requireOrgAdmin(ctx, orgID, actorUserID); err != nil {
		return nil, err
	}
	return u.profilesRepository.ListMemberships(ctx, orgID)
}

func (u *profilesUseCase) UpsertOrganizationMember(ctx context.Context, actorUserID uuid.UUID, membership *domain.OrganizationMembership) (updated *domain.OrganizationMembership, err error) {
	log.Trace("profilesUseCase UpsertOrganizationMember")
	var attrs []attribute.KeyValue
	if membership != nil {
		attrs = append(attrs,
			attribute.String("org_id", membership.OrgID.String()),
			attribute.String("member_user_id", membership.UserID.String()),
			attribute.String("role", membership.Role),
		)
	}
	attrs = append(attrs, attribute.String("actor_user_id", actorUserID.String()))
	ctx, span := usecasetrace.StartSpan(ctx, "tenant_service/app", "org.upsert_member", attrs...)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	if _, err := u.requireOrgAdmin(ctx, membership.OrgID, actorUserID); err != nil {
		return nil, err
	}
	membership.CreatedByUserID = actorUserID
	if membership.Status == "" {
		membership.Status = domain.OrgMemberStatusActive
	}
	err = u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
		var writeErr error
		updated, writeErr = u.profilesRepository.UpsertMembership(ctx, tx, membership)
		return writeErr
	})
	if err != nil {
		return nil, err
	}
	if err := u.authStore.SetUserRevokedAfter(ctx, membership.UserID.String(), u.clock.Now().Unix()); err != nil {
		return nil, fmt.Errorf("failed to revoke changed member sessions: %w", err)
	}
	return updated, nil
}

func (u *profilesUseCase) DeleteOrganizationMember(ctx context.Context, actorUserID uuid.UUID, orgID uuid.UUID, memberUserID uuid.UUID) (err error) {
	log.Trace("profilesUseCase DeleteOrganizationMember")
	ctx, span := usecasetrace.StartSpan(ctx, "tenant_service/app", "org.delete_member",
		attribute.String("actor_user_id", actorUserID.String()),
		attribute.String("org_id", orgID.String()),
		attribute.String("member_user_id", memberUserID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	if _, err := u.requireOrgAdmin(ctx, orgID, actorUserID); err != nil {
		return err
	}
	err = u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
		return u.profilesRepository.DeleteMembership(ctx, tx, orgID, memberUserID)
	})
	if err != nil {
		return err
	}
	if err := u.authStore.SetUserRevokedAfter(ctx, memberUserID.String(), u.clock.Now().Unix()); err != nil {
		return fmt.Errorf("failed to revoke removed member sessions: %w", err)
	}
	return nil
}

func (u *profilesUseCase) requireOrgAdmin(ctx context.Context, orgID uuid.UUID, userID uuid.UUID) (*domain.OrganizationMembership, error) {
	log.Trace("profilesUseCase requireOrgAdmin")

	membership, err := u.requireActiveMembership(ctx, orgID, userID)
	if err != nil {
		return nil, err
	}
	if membership.Role != authz.RoleOrgAdmin {
		return nil, fmt.Errorf("%w: org_admin role is required", domain.ErrUnauthorized)
	}
	return membership, nil
}

func (u *profilesUseCase) requireActiveMembership(ctx context.Context, orgID uuid.UUID, userID uuid.UUID) (*domain.OrganizationMembership, error) {
	log.Trace("profilesUseCase requireActiveMembership")

	membership, err := u.profilesRepository.ReadMembership(ctx, orgID, userID)
	if err != nil {
		return nil, err
	}
	if membership.Status != domain.OrgMemberStatusActive {
		return nil, fmt.Errorf("%w: active organization membership is required", domain.ErrUnauthorized)
	}
	return membership, nil
}

func generateEmailVerifyToken(email string, useStagingTestToken bool) (string, error) {
	if shouldUseStagingTestEmailToken(email, useStagingTestToken) {
		emailHash := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(email))))
		return stagingTestEmailVerifyToken + "-" + hex.EncodeToString(emailHash[:8]), nil
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
