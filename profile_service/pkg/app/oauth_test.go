package usecase_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	auth "lib/shared_lib/auth"
	sharedclock "lib/shared_lib/clock"
	"time"

	usecase "profile_service/pkg/app"
	"profile_service/pkg/domain"
	"profile_service/pkg/infra/network/messaging"
	repo "profile_service/pkg/infra/repo/db"

	"github.com/google/uuid"
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
}

func (s *profileDBStub) Save(context.Context, *domain.ProfileAccount, uuid.UUID) error {
	return nil
}
func (s *profileDBStub) Update(context.Context, uuid.UUID, *domain.Profile) (*domain.Profile, error) {
	return nil, nil
}
func (s *profileDBStub) UpdateHuggingFaceToken(context.Context, uuid.UUID, string) (*domain.Profile, error) {
	return nil, nil
}
func (s *profileDBStub) UpdatePassword(context.Context, uuid.UUID, string) error {
	return nil
}
func (s *profileDBStub) VerifyEmail(context.Context, string) (*domain.Profile, error) {
	return nil, nil
}
func (s *profileDBStub) Read(context.Context, uuid.UUID) (*domain.Profile, error) {
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
func (s *profileDBStub) SaveOAuthIdentity(context.Context, uuid.UUID, domain.OAuthIdentity) error {
	s.saveOAuthIdentityCalled = true
	return s.saveOAuthIdentityError
}
func (s *profileDBStub) Delete(context.Context, uuid.UUID) error {
	return nil
}

type userPublisherStub struct {
	createdCalled bool
}

func (s *userPublisherStub) PublishUserCreatedEvent(context.Context, *domain.ProfileAccount) error {
	s.createdCalled = true
	return nil
}
func (s *userPublisherStub) PublishUserUpdatedEvent(context.Context, *domain.Profile) error {
	return nil
}
func (s *userPublisherStub) PublishUserDeletedEvent(context.Context, uuid.UUID) error {
	return nil
}

type authProviderStub struct {
	token string
	sid   string
	exp   int64
}

func (s *authProviderStub) CreateToken(context.Context, uuid.UUID, int) (string, string, int64, error) {
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
		dbStub        *profileDBStub
		publisherStub *userPublisherStub
		authStore     *authStoreStub
		authProvider  *authProviderStub
		oauthProvider *oauthProviderStub
		stateStore    *oauthStateStoreStub
		profiles      usecase.ProfilesUseCase
		codeVerifier  string
		codeChallenge string
	)

	BeforeEach(func() {
		codeVerifier = "8xY8A6yJ4n6y6E6qz_H2bZB4C2T5I3g0mQ4hR-4KV4s"
		sum := sha256.Sum256([]byte(codeVerifier))
		codeChallenge = base64.RawURLEncoding.EncodeToString(sum[:])

		dbStub = &profileDBStub{}
		publisherStub = &userPublisherStub{}
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
				MsgPublisher:       publisherStub,
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
		Expect(publisherStub.createdCalled).To(BeTrue())
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
		Expect(publisherStub.createdCalled).To(BeFalse())
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
	_ repo.ProfileDB               = (*profileDBStub)(nil)
	_ messaging.UserEventPublisher = (*userPublisherStub)(nil)
	_ auth.AuthProvider            = (*authProviderStub)(nil)
	_ auth.RevocationStore         = (*authStoreStub)(nil)
	_ usecase.OAuthProviderClient  = (*oauthProviderStub)(nil)
	_ usecase.OAuthStateStore      = (*oauthStateStoreStub)(nil)
)
