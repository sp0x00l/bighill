package usecase_test

import (
	"context"
	"errors"
	sharedclock "lib/shared_lib/clock"
	msgConn "lib/shared_lib/messaging"
	shareduow "lib/shared_lib/uow"
	"time"

	usecase "profile_service/pkg/app"
	"profile_service/pkg/domain"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type passwordProfileDBStub struct {
	userID                  uuid.UUID
	passwordHash            string
	readErr                 error
	verifyTokenProfile      *domain.Profile
	verifyTokenErr          error
	deleteCalled            bool
	huggingFaceTokenProfile *domain.Profile
	huggingFaceCiphertext   string
	savedProfileAccount     *domain.ProfileAccount
	savedIdempotencyKey     uuid.UUID
	updatedPasswordHash     string
	updatePasswordCalled    bool
	order                   *[]string
}

func (s *passwordProfileDBStub) Save(_ context.Context, profile *domain.ProfileAccount, idempotencyKey uuid.UUID) error {
	return s.SaveTx(context.Background(), nil, profile, idempotencyKey)
}
func (s *passwordProfileDBStub) SaveTx(_ context.Context, _ pgx.Tx, profile *domain.ProfileAccount, idempotencyKey uuid.UUID) error {
	copy := *profile
	s.savedProfileAccount = &copy
	s.savedIdempotencyKey = idempotencyKey
	return nil
}
func (s *passwordProfileDBStub) Update(context.Context, uuid.UUID, *domain.Profile) (*domain.Profile, error) {
	return nil, nil
}
func (s *passwordProfileDBStub) UpdateTx(context.Context, pgx.Tx, uuid.UUID, *domain.Profile) (*domain.Profile, error) {
	return nil, nil
}
func (s *passwordProfileDBStub) UpdateHuggingFaceToken(_ context.Context, _ uuid.UUID, ciphertext string) (*domain.Profile, error) {
	return s.UpdateHuggingFaceTokenTx(context.Background(), nil, uuid.Nil, ciphertext)
}
func (s *passwordProfileDBStub) UpdateHuggingFaceTokenTx(_ context.Context, _ pgx.Tx, _ uuid.UUID, ciphertext string) (*domain.Profile, error) {
	s.huggingFaceCiphertext = ciphertext
	return s.huggingFaceTokenProfile, nil
}
func (s *passwordProfileDBStub) UpdatePassword(_ context.Context, _ uuid.UUID, passwordHash string) error {
	s.updatePasswordCalled = true
	s.updatedPasswordHash = passwordHash
	if s.order != nil {
		*s.order = append(*s.order, "update-password")
	}
	return nil
}
func (s *passwordProfileDBStub) VerifyEmail(context.Context, string) (*domain.Profile, error) {
	return s.VerifyEmailTx(context.Background(), nil, "")
}
func (s *passwordProfileDBStub) VerifyEmailTx(context.Context, pgx.Tx, string) (*domain.Profile, error) {
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
func (s *passwordProfileDBStub) ReadTx(context.Context, pgx.Tx, uuid.UUID) (*domain.Profile, error) {
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
func (s *passwordProfileDBStub) CreateOAuthProfileTx(context.Context, pgx.Tx, domain.OAuthIdentity, string) (uuid.UUID, error) {
	return uuid.Nil, nil
}
func (s *passwordProfileDBStub) SaveOAuthIdentity(context.Context, uuid.UUID, domain.OAuthIdentity) error {
	return nil
}
func (s *passwordProfileDBStub) SaveOAuthIdentityTx(context.Context, pgx.Tx, uuid.UUID, domain.OAuthIdentity) error {
	return nil
}
func (s *passwordProfileDBStub) Delete(context.Context, uuid.UUID) error {
	return s.DeleteTx(context.Background(), nil, uuid.Nil)
}
func (s *passwordProfileDBStub) DeleteTx(context.Context, pgx.Tx, uuid.UUID) error {
	s.deleteCalled = true
	return nil
}

type recordingProfileUnitOfWork struct {
	messages []shareduow.OutboundMessage
	called   bool
}

func (u *recordingProfileUnitOfWork) Do(ctx context.Context, fn shareduow.TxFunc) error {
	u.called = true
	return fn(ctx, nil, func(message shareduow.OutboundMessage) error {
		u.messages = append(u.messages, message)
		return nil
	})
}

type noopUserEventBuilder struct{}

type recordingUserEventBuilder struct {
	createdProfile *domain.ProfileAccount
	updatedProfile *domain.Profile
	deletedUserID  uuid.UUID
}

type encryptorStub struct {
	nextCiphertext string
	nextErr        error
	lastPlaintext  string
}

func (s *encryptorStub) Encrypt(_ context.Context, plaintext string) (string, error) {
	s.lastPlaintext = plaintext
	return s.nextCiphertext, s.nextErr
}

func (n *noopUserEventBuilder) UserCreatedMessage(profile *domain.ProfileAccount) shareduow.OutboundMessage {
	return testOutboundMessage(msgConn.MsgTypeUserCreated, profile.ID)
}
func (n *noopUserEventBuilder) UserUpdatedMessage(profile *domain.Profile) shareduow.OutboundMessage {
	return testOutboundMessage(msgConn.MsgTypeUserUpdated, profile.ID)
}
func (n *noopUserEventBuilder) UserDeletedMessage(userID uuid.UUID) shareduow.OutboundMessage {
	return testOutboundMessage(msgConn.MsgTypeUserDeleted, userID)
}
func (r *recordingUserEventBuilder) UserCreatedMessage(profile *domain.ProfileAccount) shareduow.OutboundMessage {
	r.createdProfile = profile
	return testOutboundMessage(msgConn.MsgTypeUserCreated, profile.ID)
}
func (r *recordingUserEventBuilder) UserUpdatedMessage(profile *domain.Profile) shareduow.OutboundMessage {
	r.updatedProfile = profile
	return testOutboundMessage(msgConn.MsgTypeUserUpdated, profile.ID)
}
func (r *recordingUserEventBuilder) UserDeletedMessage(userID uuid.UUID) shareduow.OutboundMessage {
	r.deletedUserID = userID
	return testOutboundMessage(msgConn.MsgTypeUserDeleted, userID)
}

func testOutboundMessage(msgType msgConn.MsgType, resourceKey uuid.UUID) shareduow.OutboundMessage {
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
	revokeErr           error
	revokedUserID       string
	revokedAfter        int64
	order               *[]string
}

func (s *passwordAuthStoreStub) RevokeToken(context.Context, string, int64) error { return nil }
func (s *passwordAuthStoreStub) IsRevoked(context.Context, string) (bool, error)  { return false, nil }
func (s *passwordAuthStoreStub) SetUserRevokedAfter(_ context.Context, userID string, revokedAfter int64) error {
	s.revokedUserID = userID
	s.revokedAfter = revokedAfter
	if s.order != nil {
		*s.order = append(*s.order, "revoke")
	}
	return s.revokeErr
}
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

func newProfilesUseCaseForTest(repo usecase.ProfileDB, builder usecase.UserEventBuilderAdapter, store *passwordAuthStoreStub, provider *passwordAuthProviderStub, opts ...usecase.ProfilesUseCaseOption) usecase.ProfilesUseCase {
	options := append([]usecase.ProfilesUseCaseOption{usecase.WithProfileClock(sharedclock.System{})}, opts...)
	return usecase.NewProfilesUseCase(
		usecase.ProfilesUseCaseDeps{
			ProfilesRepository: repo,
			UnitOfWork:         &recordingProfileUnitOfWork{},
			EventBuilder:       builder,
			AuthStore:          store,
			AuthProvider:       provider,
		},
		usecase.ProfilesUseCaseConfig{
			AuthExpirationInMinutes: 15,
			EmailValidationTTL:      60 * time.Minute,
		},
		options...,
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
			&noopUserEventBuilder{},
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
			&noopUserEventBuilder{},
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
		Expect(dbStub.savedProfileAccount.EmailVerifyToken).To(HavePrefix("staging-test-email-verify-token-"))
		Expect(profile.EmailVerifyToken).To(Equal(dbStub.savedProfileAccount.EmailVerifyToken))
		Expect(dbStub.savedProfileAccount.EmailVerifyExpiresAt.IsZero()).To(BeFalse())
	})

	It("uses a generated token for non-test addresses on staging", func() {
		dbStub := &passwordProfileDBStub{}
		profiles := newProfilesUseCaseForTest(
			dbStub,
			&noopUserEventBuilder{},
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
		builder := &recordingUserEventBuilder{}

		profiles := newProfilesUseCaseForTest(
			dbStub,
			builder,
			&passwordAuthStoreStub{},
			&passwordAuthProviderStub{},
		)

		err := profiles.VerifyEmail(context.Background(), "token-1")
		Expect(err).NotTo(HaveOccurred())
		Expect(builder.updatedProfile).NotTo(BeNil())
		Expect(builder.updatedProfile.ID).To(Equal(userID))
		Expect(builder.updatedProfile.EmailVerified).To(BeTrue())
	})
})

var _ = Describe("profilesUseCase ReplacePassword", func() {
	It("revokes existing sessions before storing the new password hash", func() {
		userID := uuid.New()
		now := time.Unix(1710001234, 0).UTC()
		order := []string{}
		dbStub := &passwordProfileDBStub{order: &order}
		authStore := &passwordAuthStoreStub{order: &order}
		profiles := newProfilesUseCaseForTest(
			dbStub,
			&noopUserEventBuilder{},
			authStore,
			&passwordAuthProviderStub{},
			usecase.WithProfileClock(sharedclock.Func(func() time.Time { return now })),
		)

		err := profiles.ReplacePassword(context.Background(), userID, "NewPassword123!")

		Expect(err).NotTo(HaveOccurred())
		Expect(order).To(Equal([]string{"revoke", "update-password"}))
		Expect(authStore.revokedUserID).To(Equal(userID.String()))
		Expect(authStore.revokedAfter).To(Equal(now.Unix()))
		Expect(dbStub.updatePasswordCalled).To(BeTrue())
		Expect(dbStub.updatedPasswordHash).NotTo(BeEmpty())
		Expect(dbStub.updatedPasswordHash).NotTo(Equal("NewPassword123!"))
	})

	It("does not store a new password when session revocation fails", func() {
		userID := uuid.New()
		revokeErr := errors.New("redis unavailable")
		dbStub := &passwordProfileDBStub{}
		authStore := &passwordAuthStoreStub{revokeErr: revokeErr}
		profiles := newProfilesUseCaseForTest(
			dbStub,
			&noopUserEventBuilder{},
			authStore,
			&passwordAuthProviderStub{},
		)

		err := profiles.ReplacePassword(context.Background(), userID, "NewPassword123!")

		Expect(errors.Is(err, revokeErr)).To(BeTrue())
		Expect(dbStub.updatePasswordCalled).To(BeFalse())
	})
})

var _ = Describe("profilesUseCase DeleteProfile", func() {
	It("publishes a user deleted event after successful delete", func() {
		userID := uuid.New()
		dbStub := &passwordProfileDBStub{}
		builder := &recordingUserEventBuilder{}

		profiles := newProfilesUseCaseForTest(
			dbStub,
			builder,
			&passwordAuthStoreStub{},
			&passwordAuthProviderStub{},
		)

		err := profiles.DeleteProfile(context.Background(), userID)
		Expect(err).NotTo(HaveOccurred())
		Expect(dbStub.deleteCalled).To(BeTrue())
		Expect(builder.deletedUserID).To(Equal(userID))
	})
})

var _ = Describe("profilesUseCase ReplaceHuggingFaceToken", func() {
	It("encrypts the token, stores it, and publishes the updated profile", func() {
		userID := uuid.New()
		updated := &domain.Profile{
			ProfileAccount: domain.ProfileAccount{
				ID:                         userID,
				Email:                      "user@example.com",
				HuggingFaceTokenCiphertext: "ciphertext-1",
			},
		}
		dbStub := &passwordProfileDBStub{huggingFaceTokenProfile: updated}
		builder := &recordingUserEventBuilder{}
		encryptor := &encryptorStub{nextCiphertext: "ciphertext-1"}

		profiles := usecase.NewProfilesUseCase(
			usecase.ProfilesUseCaseDeps{
				ProfilesRepository: dbStub,
				UnitOfWork:         &recordingProfileUnitOfWork{},
				EventBuilder:       builder,
				AuthStore:          &passwordAuthStoreStub{},
				AuthProvider:       &passwordAuthProviderStub{},
				SecretEncryptor:    encryptor,
			},
			usecase.ProfilesUseCaseConfig{
				AuthExpirationInMinutes: 15,
				EmailValidationTTL:      60 * time.Minute,
			},
			usecase.WithProfileClock(sharedclock.System{}),
		)

		err := profiles.ReplaceHuggingFaceToken(context.Background(), userID, "hf-token")

		Expect(err).NotTo(HaveOccurred())
		Expect(encryptor.lastPlaintext).To(Equal("hf-token"))
		Expect(dbStub.huggingFaceCiphertext).To(Equal("ciphertext-1"))
		Expect(builder.updatedProfile).To(Equal(updated))
	})
})
