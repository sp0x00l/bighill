package usecase

import (
	"context"
	"time"

	"profile_service/pkg/domain"

	"github.com/google/uuid"
)

type ProfileDB interface {
	Save(ctx context.Context, profile *domain.ProfileAccount, idempotencyKey uuid.UUID) error
	Update(ctx context.Context, userID uuid.UUID, profile *domain.Profile) (*domain.Profile, error)
	UpdatePassword(ctx context.Context, userID uuid.UUID, newPassword string) error
	VerifyEmail(ctx context.Context, token string) (*domain.Profile, error)
	Read(ctx context.Context, userID uuid.UUID) (*domain.Profile, error)
	ReadByVerifyToken(ctx context.Context, token string) (*domain.Profile, error)
	ReadPasswordHash(ctx context.Context, email string) (uuid.UUID, string, error)
	ReadOAuthProfileIDByProviderSubject(ctx context.Context, provider, subject string) (uuid.UUID, error)
	ReadProfileIDByEmail(ctx context.Context, email string) (uuid.UUID, error)
	CreateOAuthProfile(ctx context.Context, identity domain.OAuthIdentity, passwordHash string) (uuid.UUID, error)
	SaveOAuthIdentity(ctx context.Context, userID uuid.UUID, identity domain.OAuthIdentity) error
	Delete(ctx context.Context, userID uuid.UUID) error
}

type UserEventPublisher interface {
	PublishUserCreatedEvent(ctx context.Context, profileAccount *domain.ProfileAccount) error
	PublishEmailVerificationRequestedEvent(ctx context.Context, profileAccount *domain.ProfileAccount) error
	PublishUserUpdatedEvent(ctx context.Context, profile *domain.Profile) error
	PublishUserDeletedEvent(ctx context.Context, userID uuid.UUID) error
}

type OAuthStateStore interface {
	Save(ctx context.Context, state domain.OAuthState, ttl time.Duration) error
	Load(ctx context.Context, state string) (*domain.OAuthState, error)
	Delete(ctx context.Context, state string) error
}

type OAuthProviderClient interface {
	AuthorizationURL(state, redirectURI, codeChallenge string) (string, error)
	ExchangeCode(ctx context.Context, code, redirectURI, codeVerifier string) (*domain.OAuthIdentity, error)
}
