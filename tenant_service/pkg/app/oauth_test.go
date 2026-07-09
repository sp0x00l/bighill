package usecase_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	auth "lib/shared_lib/auth"
	"lib/shared_lib/authz"
	sharedclock "lib/shared_lib/clock"
	msgConn "lib/shared_lib/messaging"
	shareduow "lib/shared_lib/uow"
	"time"

	usecase "tenant_service/pkg/app"
	"tenant_service/pkg/domain"
	repo "tenant_service/pkg/infra/repo/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type profileDBStub struct {
	readOAuthProfileIDResult uuid.UUID
	readOAuthProfileIDError  error
	readProfileIDResult      uuid.UUID
	readProfileIDError       error
	readProfileResult        *domain.Profile
	readProfileError         error
	createOAuthProfileResult uuid.UUID
	createOAuthProfileError  error
	saveOAuthIdentityError   error
	saveOAuthIdentityCalled  bool
	defaultMembership        *domain.OrganizationMembership
	defaultMembershipError   error
}

func (s *profileDBStub) Save(context.Context, *domain.ProfileAccount, uuid.UUID) error {
	return nil
}
func (s *profileDBStub) SaveTx(context.Context, pgx.Tx, *domain.ProfileAccount, uuid.UUID) error {
	return nil
}
func (s *profileDBStub) Update(context.Context, uuid.UUID, *domain.Profile) (*domain.Profile, error) {
	return nil, nil
}
func (s *profileDBStub) UpdateTx(context.Context, pgx.Tx, uuid.UUID, *domain.Profile) (*domain.Profile, error) {
	return nil, nil
}
func (s *profileDBStub) UpdateHuggingFaceToken(context.Context, uuid.UUID, string) (*domain.Profile, error) {
	return nil, nil
}
func (s *profileDBStub) UpdateHuggingFaceTokenTx(context.Context, pgx.Tx, uuid.UUID, string) (*domain.Profile, error) {
	return nil, nil
}
func (s *profileDBStub) UpdatePassword(context.Context, uuid.UUID, string) error {
	return nil
}
func (s *profileDBStub) VerifyEmail(context.Context, string) (*domain.Profile, error) {
	return nil, nil
}
func (s *profileDBStub) VerifyEmailTx(context.Context, pgx.Tx, string) (*domain.Profile, error) {
	return nil, nil
}
func (s *profileDBStub) Read(context.Context, uuid.UUID) (*domain.Profile, error) {
	return s.readProfileResult, s.readProfileError
}
func (s *profileDBStub) ReadTx(context.Context, pgx.Tx, uuid.UUID) (*domain.Profile, error) {
	return s.readProfileResult, s.readProfileError
}
func (s *profileDBStub) ReadByVerifyToken(context.Context, string) (*domain.Profile, error) {
	return nil, nil
}
func (s *profileDBStub) ReadPasswordHash(context.Context, string) (uuid.UUID, string, error) {
	return uuid.Nil, "", nil
}
func (s *profileDBStub) ReadOAuthProfileIDByProviderSubject(context.Context, string, string) (uuid.UUID, error) {
	return s.readOAuthProfileIDResult, s.readOAuthProfileIDError
}
func (s *profileDBStub) ReadProfileIDByEmail(context.Context, string) (uuid.UUID, error) {
	return s.readProfileIDResult, s.readProfileIDError
}
func (s *profileDBStub) CreateOAuthProfile(context.Context, domain.OAuthIdentity, string) (uuid.UUID, error) {
	return s.createOAuthProfileResult, s.createOAuthProfileError
}
func (s *profileDBStub) CreateOAuthProfileTx(context.Context, pgx.Tx, domain.OAuthIdentity, string) (uuid.UUID, error) {
	return s.createOAuthProfileResult, s.createOAuthProfileError
}
func (s *profileDBStub) SaveOAuthIdentity(context.Context, uuid.UUID, domain.OAuthIdentity) error {
	return s.SaveOAuthIdentityTx(context.Background(), nil, uuid.Nil, domain.OAuthIdentity{})
}
func (s *profileDBStub) SaveOAuthIdentityTx(context.Context, pgx.Tx, uuid.UUID, domain.OAuthIdentity) error {
	s.saveOAuthIdentityCalled = true
	return s.saveOAuthIdentityError
}
func (s *profileDBStub) Delete(context.Context, uuid.UUID) error {
	return nil
}
func (s *profileDBStub) DeleteTx(context.Context, pgx.Tx, uuid.UUID) error {
	return nil
}
func (s *profileDBStub) ReadDefaultMembership(_ context.Context, userID uuid.UUID) (*domain.OrganizationMembership, error) {
	if s.defaultMembershipError != nil {
		return nil, s.defaultMembershipError
	}
	if s.defaultMembership != nil {
		return s.defaultMembership, nil
	}
	return &domain.OrganizationMembership{
		OrgID:  uuid.New(),
		UserID: userID,
		Role:   domain.OrgMemberRoleOrgAdmin,
		Status: domain.OrgMemberStatusActive,
	}, nil
}
func (s *profileDBStub) ReadMembership(context.Context, uuid.UUID, uuid.UUID) (*domain.OrganizationMembership, error) {
	return nil, domain.ErrProfileNotFound
}
func (s *profileDBStub) ReadOrganization(context.Context, uuid.UUID) (*domain.Organization, error) {
	return nil, domain.ErrProfileNotFound
}
func (s *profileDBStub) ListMemberships(context.Context, uuid.UUID) ([]*domain.OrganizationMembership, error) {
	return nil, nil
}
func (s *profileDBStub) UpsertMembership(context.Context, pgx.Tx, *domain.OrganizationMembership) (*domain.OrganizationMembership, error) {
	return nil, nil
}
func (s *profileDBStub) DeleteMembership(context.Context, pgx.Tx, uuid.UUID, uuid.UUID) error {
	return nil
}

type oauthUnitOfWorkStub struct {
	messages []shareduow.OutboundMessage
}

func (u *oauthUnitOfWorkStub) Do(ctx context.Context, fn shareduow.TxFunc) error {
	return fn(ctx, nil, func(message shareduow.OutboundMessage) error {
		u.messages = append(u.messages, message)
		return nil
	})
}

type userEventBuilderStub struct {
	createdCalled bool
}

func (s *userEventBuilderStub) UserCreatedMessage(profile *domain.ProfileAccount) shareduow.OutboundMessage {
	s.createdCalled = true
	return oauthOutboundMessage(msgConn.MsgTypeUserCreated, profile.ID)
}
func (s *userEventBuilderStub) UserUpdatedMessage(profile *domain.Profile) shareduow.OutboundMessage {
	return oauthOutboundMessage(msgConn.MsgTypeUserUpdated, profile.ID)
}
func (s *userEventBuilderStub) UserDeletedMessage(userID uuid.UUID) shareduow.OutboundMessage {
	return oauthOutboundMessage(msgConn.MsgTypeUserDeleted, userID)
}

func oauthOutboundMessage(msgType msgConn.MsgType, resourceKey uuid.UUID) shareduow.OutboundMessage {
	return shareduow.OutboundMessage{
		Topic: "profile",
		Message: msgConn.Message{
			ResourceKey: resourceKey,
			MsgType:     msgType,
			Payload:     []byte("payload"),
		},
		DispatchKey: msgType.String() + ":" + resourceKey.String(),
	}
}

type authProviderStub struct {
	token      string
	sid        string
	exp        int64
	lastClaims authz.TokenClaims
}

func (s *authProviderStub) CreateToken(context.Context, uuid.UUID, int) (string, string, int64, error) {
	return s.token, s.sid, s.exp, nil
}
func (s *authProviderStub) CreateAccessToken(_ context.Context, claims authz.TokenClaims, _ int) (string, string, int64, error) {
	s.lastClaims = claims
	return s.token, s.sid, s.exp, nil
}
func (s *authProviderStub) Validate(context.Context, string) (map[string]any, error) {
	return nil, nil
}

type authStoreStub struct {
	createSessionCalled bool
}

func (s *authStoreStub) RevokeToken(context.Context, string, int64) error           { return nil }
func (s *authStoreStub) IsRevoked(context.Context, string) (bool, error)            { return false, nil }
func (s *authStoreStub) SetUserRevokedAfter(context.Context, string, int64) error   { return nil }
func (s *authStoreStub) GetUserRevokedAfter(context.Context, string) (int64, error) { return 0, nil }
func (s *authStoreStub) ClearUserRevokedAfter(context.Context, string) error        { return nil }
func (s *authStoreStub) SessionExists(context.Context, string) (bool, error)        { return false, nil }
func (s *authStoreStub) DeleteSession(context.Context, string) error                { return nil }
func (s *authStoreStub) CreateSession(context.Context, string, int64) error {
	s.createSessionCalled = true
	return nil
}

type oauthProviderStub struct {
	authorizationURL string
	identity         *domain.OAuthIdentity
	err              error
}

func (s *oauthProviderStub) AuthorizationURL(string, string, string) (string, error) {
	if s.authorizationURL == "" {
		return "https://provider.example/auth", nil
	}
	return s.authorizationURL, nil
}
func (s *oauthProviderStub) BigHillCode(context.Context, string, string, string) (*domain.OAuthIdentity, error) {
	return s.identity, s.err
}

type oauthStateStoreStub struct {
	saveCalled   bool
	deleteCalled bool
	state        *domain.OAuthState
	saveErr      error
	loadErr      error
	deleteErr    error
}

func (s *oauthStateStoreStub) Save(context.Context, domain.OAuthState, time.Duration) error {
	s.saveCalled = true
	return s.saveErr
}
func (s *oauthStateStoreStub) Load(context.Context, string) (*domain.OAuthState, error) {
	return s.state, s.loadErr
}
func (s *oauthStateStoreStub) Delete(context.Context, string) error {
	s.deleteCalled = true
	return s.deleteErr
}

var _ = Describe("OAuth usecase", func() {
	var (
		dbStub         *profileDBStub
		unitOfWorkStub *oauthUnitOfWorkStub
		builderStub    *userEventBuilderStub
		authStore      *authStoreStub
		authProvider   *authProviderStub
		oauthProvider  *oauthProviderStub
		stateStore     *oauthStateStoreStub
		profiles       usecase.ProfilesUseCase
		codeVerifier   string
		codeChallenge  string
	)

	BeforeEach(func() {
		codeVerifier = "8xY8A6yJ4n6y6E6qz_H2bZB4C2T5I3g0mQ4hR-4KV4s"
		sum := sha256.Sum256([]byte(codeVerifier))
		codeChallenge = base64.RawURLEncoding.EncodeToString(sum[:])

		dbStub = &profileDBStub{}
		unitOfWorkStub = &oauthUnitOfWorkStub{}
		builderStub = &userEventBuilderStub{}
		authStore = &authStoreStub{}
		authProvider = &authProviderStub{
			token: "token-1",
			sid:   "sid-1",
			exp:   time.Now().Add(time.Hour).Unix(),
		}
		oauthProvider = &oauthProviderStub{
			identity: &domain.OAuthIdentity{
				Provider:      "google",
				Subject:       "provider-user",
				Email:         "user@example.com",
				EmailVerified: true,
				FirstName:     "Test",
			},
		}
		stateStore = &oauthStateStoreStub{
			state: &domain.OAuthState{
				State:         "state-1",
				Provider:      "google",
				RedirectURI:   "https://app.example/callback",
				CodeChallenge: codeChallenge,
			},
		}

		profiles = usecase.NewProfilesUseCase(
			usecase.ProfilesUseCaseDeps{
				ProfilesRepository: dbStub,
				UnitOfWork:         unitOfWorkStub,
				EventBuilder:       builderStub,
				AuthStore:          authStore,
				AuthProvider:       authProvider,
			},
			usecase.ProfilesUseCaseConfig{
				AuthExpirationInMinutes: 15,
				EmailValidationTTL:      60 * time.Minute,
			},
			usecase.WithProfileOAuth(
				map[string]usecase.OAuthProviderClient{"google": oauthProvider},
				stateStore,
				10*time.Minute,
			),
			usecase.WithProfileClock(sharedclock.System{}),
		)
	})

	It("creates a new profile and local session when oauth identity is new", func() {
		userID := uuid.New()
		dbStub.readOAuthProfileIDError = domain.ErrOAuthIdentityNotFound
		dbStub.readProfileIDError = domain.ErrProfileNotFound
		dbStub.createOAuthProfileResult = userID
		dbStub.readProfileResult = &domain.Profile{
			ProfileAccount: domain.ProfileAccount{
				ID:    userID,
				Email: "user@example.com",
			},
		}

		result, err := profiles.CreateOAuthSession(context.Background(), "google", usecase.OAuthSessionRequest{
			Code:         "oauth-code",
			State:        "state-1",
			RedirectURI:  "https://app.example/callback",
			CodeVerifier: codeVerifier,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(result.Token).To(Equal("token-1"))
		Expect(result.IsNewUser).To(BeTrue())
		Expect(authProvider.lastClaims.UserID).To(Equal(userID.String()))
		Expect(authProvider.lastClaims.Roles).To(Equal([]string{domain.OrgMemberRoleOrgAdmin}))
		Expect(authProvider.lastClaims.Permissions).To(ContainElement(authz.PermissionOrgMembersWrite))
		Expect(builderStub.createdCalled).To(BeTrue())
		Expect(authStore.createSessionCalled).To(BeTrue())
		Expect(dbStub.saveOAuthIdentityCalled).To(BeTrue())
		Expect(stateStore.deleteCalled).To(BeTrue())
	})

	It("rejects an invalid oauth state", func() {
		_, err := profiles.CreateOAuthSession(context.Background(), "google", usecase.OAuthSessionRequest{
			Code:         "oauth-code",
			State:        "state-1",
			RedirectURI:  "https://app.example/callback",
			CodeVerifier: "wrong-verifier",
		})

		Expect(err).To(MatchError(usecase.ErrInvalidOAuthState))
		Expect(stateStore.deleteCalled).To(BeFalse())
	})

	It("creates an authorization URL and persists state", func() {
		oauthProvider.authorizationURL = "https://accounts.example/authorize?state=generated"

		result, err := profiles.CreateOAuthAuthorization(context.Background(), "google", usecase.OAuthAuthorizeRequest{
			RedirectURI:   "https://app.example/callback",
			CodeChallenge: codeChallenge,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(result.AuthorizationURL).To(Equal("https://accounts.example/authorize?state=generated"))
		Expect(result.State).NotTo(BeEmpty())
		Expect(stateStore.saveCalled).To(BeTrue())
	})

	It("returns an existing profile session without publishing a new-user event", func() {
		userID := uuid.New()
		dbStub.readOAuthProfileIDResult = userID

		result, err := profiles.CreateOAuthSession(context.Background(), "google", usecase.OAuthSessionRequest{
			Code:         "oauth-code",
			State:        "state-1",
			RedirectURI:  "https://app.example/callback",
			CodeVerifier: codeVerifier,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(result.IsNewUser).To(BeFalse())
		Expect(builderStub.createdCalled).To(BeFalse())
		Expect(dbStub.saveOAuthIdentityCalled).To(BeFalse())
		Expect(authStore.createSessionCalled).To(BeTrue())
	})

	It("rejects an oauth identity without a verified email", func() {
		oauthProvider.identity = &domain.OAuthIdentity{
			Provider:      "google",
			Subject:       "provider-user",
			Email:         "user@example.com",
			EmailVerified: false,
		}

		_, err := profiles.CreateOAuthSession(context.Background(), "google", usecase.OAuthSessionRequest{
			Code:         "oauth-code",
			State:        "state-1",
			RedirectURI:  "https://app.example/callback",
			CodeVerifier: codeVerifier,
		})

		Expect(err).To(MatchError(usecase.ErrOAuthEmailUnverified))
		Expect(authStore.createSessionCalled).To(BeFalse())
	})

	It("rejects an unsupported oauth provider", func() {
		_, err := profiles.CreateOAuthSession(context.Background(), "discord", usecase.OAuthSessionRequest{})
		Expect(err).To(MatchError(usecase.ErrUnsupportedOAuthProvider))
	})
})

var (
	_ repo.ProfileDB              = (*profileDBStub)(nil)
	_ auth.AuthProvider           = (*authProviderStub)(nil)
	_ auth.RevocationStore        = (*authStoreStub)(nil)
	_ usecase.OAuthProviderClient = (*oauthProviderStub)(nil)
	_ usecase.OAuthStateStore     = (*oauthStateStoreStub)(nil)
)
