package usecase_test

import (
	"context"
	sharedclock "lib/shared_lib/clock"
	"time"

	usecase "profile_service/pkg/app"
	"profile_service/pkg/domain"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type passwordProfileDBStub struct {
	userID              uuid.UUID
	passwordHash        string
	readErr             error
	verifyTokenProfile  *domain.Profile
	verifyTokenErr      error
	deleteCalled        bool
	savedProfileAccount *domain.ProfileAccount
	savedIdempotencyKey uuid.UUID
}

func (s *passwordProfileDBStub) Save(_ context.Context, profile *domain.ProfileAccount, idempotencyKey uuid.UUID) error {
	copy := *profile
	s.savedProfileAccount = &copy
	s.savedIdempotencyKey = idempotencyKey
	return nil
}
func (s *passwordProfileDBStub) Update(context.Context, uuid.UUID, *domain.Profile) (*domain.Profile, error) {
	return nil, nil
}
func (s *passwordProfileDBStub) UpdatePassword(context.Context, uuid.UUID, string) error {
	return nil
}
func (s *passwordProfileDBStub) VerifyEmail(context.Context, string) (*domain.Profile, error) {
	if s.verifyTokenErr != nil {
		return nil, s.verifyTokenErr
	}
	profile := *s.verifyTokenProfile
	profile.EmailVerified = true
	return &profile, nil
}
func (s *passwordProfileDBStub) Read(context.Context, uuid.UUID) (*domain.Profile, error) {
	return nil, nil
}
func (s *passwordProfileDBStub) ReadByVerifyToken(context.Context, string) (*domain.Profile, error) {
	return s.verifyTokenProfile, s.verifyTokenErr
}
func (s *passwordProfileDBStub) ReadPasswordHash(context.Context, string) (uuid.UUID, string, error) {
	return s.userID, s.passwordHash, s.readErr
}
func (s *passwordProfileDBStub) ReadOAuthProfileIDByProviderSubject(context.Context, string, string) (uuid.UUID, error) {
	return uuid.Nil, domain.ErrOAuthIdentityNotFound
}
func (s *passwordProfileDBStub) ReadProfileIDByEmail(context.Context, string) (uuid.UUID, error) {
	return uuid.Nil, domain.ErrProfileNotFound
}
func (s *passwordProfileDBStub) CreateOAuthProfile(context.Context, domain.OAuthIdentity, string) (uuid.UUID, error) {
	return uuid.Nil, nil
}
func (s *passwordProfileDBStub) SaveOAuthIdentity(context.Context, uuid.UUID, domain.OAuthIdentity) error {
	return nil
}
func (s *passwordProfileDBStub) Delete(context.Context, uuid.UUID) error {
	s.deleteCalled = true
	return nil
}

type noopUserPublisher struct{}

type recordingUserPublisher struct {
	updatedProfile *domain.Profile
	deletedUserID  uuid.UUID
}

func (n *noopUserPublisher) PublishUserCreatedEvent(context.Context, *domain.ProfileAccount) error {
	return nil
}
func (n *noopUserPublisher) PublishEmailVerificationRequestedEvent(context.Context, *domain.ProfileAccount) error {
	return nil
}
func (n *noopUserPublisher) PublishUserUpdatedEvent(context.Context, *domain.Profile) error {
	return nil
}
func (n *noopUserPublisher) PublishUserDeletedEvent(context.Context, uuid.UUID) error {
	return nil
}
func (r *recordingUserPublisher) PublishUserCreatedEvent(context.Context, *domain.ProfileAccount) error {
	return nil
}
func (r *recordingUserPublisher) PublishEmailVerificationRequestedEvent(context.Context, *domain.ProfileAccount) error {
	return nil
}
func (r *recordingUserPublisher) PublishUserUpdatedEvent(_ context.Context, profile *domain.Profile) error {
	r.updatedProfile = profile
	return nil
}
func (r *recordingUserPublisher) PublishUserDeletedEvent(_ context.Context, userID uuid.UUID) error {
	r.deletedUserID = userID
	return nil
}

type passwordAuthProviderStub struct {
	createTokenCalled bool
}

func (s *passwordAuthProviderStub) CreateToken(context.Context, uuid.UUID, int) (string, string, int64, error) {
	s.createTokenCalled = true
	return "token-1", "sid-1", time.Now().Add(time.Hour).Unix(), nil
}
func (s *passwordAuthProviderStub) Validate(context.Context, string) (map[string]any, error) {
	return nil, nil
}

type passwordAuthStoreStub struct {
	createSessionCalled bool
}

func (s *passwordAuthStoreStub) RevokeToken(context.Context, string, int64) error         { return nil }
func (s *passwordAuthStoreStub) IsRevoked(context.Context, string) (bool, error)          { return false, nil }
func (s *passwordAuthStoreStub) SetUserRevokedAfter(context.Context, string, int64) error { return nil }
func (s *passwordAuthStoreStub) GetUserRevokedAfter(context.Context, string) (int64, error) {
	return 0, nil
}
func (s *passwordAuthStoreStub) ClearUserRevokedAfter(context.Context, string) error { return nil }
func (s *passwordAuthStoreStub) SessionExists(context.Context, string) (bool, error) {
	return false, nil
}
func (s *passwordAuthStoreStub) DeleteSession(context.Context, string) error { return nil }
func (s *passwordAuthStoreStub) CreateSession(context.Context, string, int64) error {
	s.createSessionCalled = true
	return nil
}

func newProfilesUseCaseForTest(repo usecase.ProfileDB, publisher usecase.UserEventPublisher, store *passwordAuthStoreStub, provider *passwordAuthProviderStub, opts ...usecase.ProfilesUseCaseOption) usecase.ProfilesUseCase {
	return usecase.NewProfilesUseCase(
		usecase.ProfilesUseCaseDeps{
			ProfilesRepository: repo,
			MsgPublisher:       publisher,
			AuthStore:          store,
			AuthProvider:       provider,
		},
		usecase.ProfilesUseCaseConfig{
			AuthExpirationInMinutes: 15,
			EmailValidationTTL:      60 * time.Minute,
		},
		append(opts, usecase.WithProfileClock(sharedclock.System{}))...,
	)
}

var _ = Describe("profilesUseCase VerifyPassword", func() {
	It("rejects a correct password when the email is not verified", func() {
		dbStub := &passwordProfileDBStub{
			readErr: domain.ErrEmailNotVerified,
		}
		authProvider := &passwordAuthProviderStub{}
		authStore := &passwordAuthStoreStub{}

		profiles := newProfilesUseCaseForTest(
			dbStub,
			&noopUserPublisher{},
			authStore,
			authProvider,
		)

		token, err := profiles.VerifyPassword(context.Background(), "user@example.com", "Password123!")
		Expect(err).To(MatchError(domain.ErrEmailNotVerified))
		Expect(token).To(BeEmpty())
		Expect(authProvider.createTokenCalled).To(BeFalse())
		Expect(authStore.createSessionCalled).To(BeFalse())
	})
})

var _ = Describe("profilesUseCase CreateProfile", func() {
	It("uses the fixed staging token for test.com addresses", func() {
		dbStub := &passwordProfileDBStub{}
		profiles := newProfilesUseCaseForTest(
			dbStub,
			&noopUserPublisher{},
			&passwordAuthStoreStub{},
			&passwordAuthProviderStub{},
			usecase.WithStagingTestEmailToken(true),
		)

		profile := &domain.ProfileAccount{
			Email:       "user@test.com",
			PhoneNumber: "123",
			CountryCode: "GB",
			Password:    "Password123!",
		}

		err := profiles.CreateProfile(context.Background(), profile, uuid.New())
		Expect(err).NotTo(HaveOccurred())
		Expect(dbStub.savedProfileAccount).NotTo(BeNil())
		Expect(dbStub.savedProfileAccount.EmailVerifyToken).To(Equal("staging-test-email-verify-token"))
		Expect(profile.EmailVerifyToken).To(Equal("staging-test-email-verify-token"))
		Expect(dbStub.savedProfileAccount.EmailVerifyExpiresAt.IsZero()).To(BeFalse())
	})

	It("uses a generated token for non-test addresses on staging", func() {
		dbStub := &passwordProfileDBStub{}
		profiles := newProfilesUseCaseForTest(
			dbStub,
			&noopUserPublisher{},
			&passwordAuthStoreStub{},
			&passwordAuthProviderStub{},
			usecase.WithStagingTestEmailToken(true),
		)

		profile := &domain.ProfileAccount{
			Email:       "user@example.com",
			PhoneNumber: "123",
			CountryCode: "GB",
			Password:    "Password123!",
		}

		err := profiles.CreateProfile(context.Background(), profile, uuid.New())
		Expect(err).NotTo(HaveOccurred())
		Expect(dbStub.savedProfileAccount).NotTo(BeNil())
		Expect(dbStub.savedProfileAccount.EmailVerifyToken).NotTo(BeEmpty())
		Expect(dbStub.savedProfileAccount.EmailVerifyToken).NotTo(Equal("staging-test-email-verify-token"))
	})
})

var _ = Describe("profilesUseCase VerifyEmail", func() {
	It("publishes a user updated event after successful verification", func() {
		userID := uuid.New()
		dbStub := &passwordProfileDBStub{
			verifyTokenProfile: &domain.Profile{
				ProfileAccount: domain.ProfileAccount{
					ID:            userID,
					Email:         "user@example.com",
					EmailVerified: false,
				},
			},
		}
		publisher := &recordingUserPublisher{}

		profiles := newProfilesUseCaseForTest(
			dbStub,
			publisher,
			&passwordAuthStoreStub{},
			&passwordAuthProviderStub{},
		)

		err := profiles.VerifyEmail(context.Background(), "token-1")
		Expect(err).NotTo(HaveOccurred())
		Expect(publisher.updatedProfile).NotTo(BeNil())
		Expect(publisher.updatedProfile.ID).To(Equal(userID))
		Expect(publisher.updatedProfile.EmailVerified).To(BeTrue())
	})
})

var _ = Describe("profilesUseCase DeleteProfile", func() {
	It("publishes a user deleted event after successful delete", func() {
		userID := uuid.New()
		dbStub := &passwordProfileDBStub{}
		publisher := &recordingUserPublisher{}

		profiles := newProfilesUseCaseForTest(
			dbStub,
			publisher,
			&passwordAuthStoreStub{},
			&passwordAuthProviderStub{},
		)

		err := profiles.DeleteProfile(context.Background(), userID)
		Expect(err).NotTo(HaveOccurred())
		Expect(dbStub.deleteCalled).To(BeTrue())
		Expect(publisher.deletedUserID).To(Equal(userID))
	})
})
