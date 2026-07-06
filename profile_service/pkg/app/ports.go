package usecase

import (
	"context"
	"time"

	shareduow "lib/shared_lib/uow"
	"profile_service/pkg/domain"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type ProfileDB interface {
	Save(ctx context.Context, profile *domain.ProfileAccount, idempotencyKey uuid.UUID) error
	SaveTx(ctx context.Context, tx pgx.Tx, profile *domain.ProfileAccount, idempotencyKey uuid.UUID) error
	Update(ctx context.Context, userID uuid.UUID, profile *domain.Profile) (*domain.Profile, error)
	UpdateTx(ctx context.Context, tx pgx.Tx, userID uuid.UUID, profile *domain.Profile) (*domain.Profile, error)
	UpdateHuggingFaceToken(ctx context.Context, userID uuid.UUID, ciphertext string) (*domain.Profile, error)
	UpdateHuggingFaceTokenTx(ctx context.Context, tx pgx.Tx, userID uuid.UUID, ciphertext string) (*domain.Profile, error)
	UpdatePassword(ctx context.Context, userID uuid.UUID, newPassword string) error
	VerifyEmail(ctx context.Context, token string) (*domain.Profile, error)
	VerifyEmailTx(ctx context.Context, tx pgx.Tx, token string) (*domain.Profile, error)
	Read(ctx context.Context, userID uuid.UUID) (*domain.Profile, error)
	ReadTx(ctx context.Context, tx pgx.Tx, userID uuid.UUID) (*domain.Profile, error)
	ReadByVerifyToken(ctx context.Context, token string) (*domain.Profile, error)
	ReadPasswordHash(ctx context.Context, email string) (uuid.UUID, string, error)
	ReadOAuthProfileIDByProviderSubject(ctx context.Context, provider, subject string) (uuid.UUID, error)
	ReadProfileIDByEmail(ctx context.Context, email string) (uuid.UUID, error)
	CreateOAuthProfile(ctx context.Context, identity domain.OAuthIdentity, passwordHash string) (uuid.UUID, error)
	CreateOAuthProfileTx(ctx context.Context, tx pgx.Tx, identity domain.OAuthIdentity, passwordHash string) (uuid.UUID, error)
	SaveOAuthIdentity(ctx context.Context, userID uuid.UUID, identity domain.OAuthIdentity) error
	SaveOAuthIdentityTx(ctx context.Context, tx pgx.Tx, userID uuid.UUID, identity domain.OAuthIdentity) error
	Delete(ctx context.Context, userID uuid.UUID) error
	DeleteTx(ctx context.Context, tx pgx.Tx, userID uuid.UUID) error
}

type SecretEncryptor interface {
	Encrypt(context.Context, string) (string, error)
}

type ProfileUnitOfWorkAdapter interface {
	Do(context.Context, shareduow.TxFunc) error
}

type UserEventBuilderAdapter interface {
	UserCreatedMessage(*domain.ProfileAccount) shareduow.OutboundMessage
	UserUpdatedMessage(*domain.Profile) shareduow.OutboundMessage
	UserDeletedMessage(uuid.UUID) shareduow.OutboundMessage
}

type OAuthStateStore interface {
	Save(ctx context.Context, state domain.OAuthState, ttl time.Duration) error
	Load(ctx context.Context, state string) (*domain.OAuthState, error)
	Delete(ctx context.Context, state string) error
}

type OAuthProviderClient interface {
	AuthorizationURL(state, redirectURI, codeChallenge string) (string, error)
	BigHillCode(ctx context.Context, code, redirectURI, codeVerifier string) (*domain.OAuthIdentity, error)
}
