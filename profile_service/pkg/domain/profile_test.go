package domain

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDomain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Profile domain unit test suite")
}

var _ = Describe("Profile domain models", func() {
	It("carries account and profile attributes", func() {
		id := uuid.New()
		dob := time.Date(1990, 1, 2, 0, 0, 0, 0, time.UTC)
		profile := Profile{
			ProfileAccount: ProfileAccount{
				ID:                         id,
				Email:                      "user@example.com",
				EmailVerified:              true,
				HuggingFaceTokenCiphertext: "ciphertext",
			},
			FirstName:   "Ada",
			LastName:    "Lovelace",
			DateOfBirth: dob,
		}

		Expect(profile.ID).To(Equal(id))
		Expect(profile.EmailVerified).To(BeTrue())
		Expect(profile.HuggingFaceTokenCiphertext).To(Equal("ciphertext"))
		Expect(profile.DateOfBirth).To(Equal(dob))
	})

	It("carries OAuth identity and state attributes", func() {
		identity := OAuthIdentity{Provider: "google", Subject: "sub", Email: "user@example.com", EmailVerified: true}
		state := OAuthState{State: "state", Provider: "google", RedirectURI: "https://app/callback", CodeChallenge: "challenge"}

		Expect(identity.Provider).To(Equal("google"))
		Expect(identity.EmailVerified).To(BeTrue())
		Expect(state.CodeChallenge).To(Equal("challenge"))
	})
})

var _ = Describe("Profile errors", func() {
	It("matches exported sentinel errors", func() {
		Expect(errors.Is(ErrProfileNotFound, ErrProfileNotFound)).To(BeTrue())
		Expect(errors.Is(ErrProfileAlreadyExists, ErrProfileAlreadyExists)).To(BeTrue())
	})

	It("does not match unrelated errors", func() {
		Expect(errors.Is(errors.New("plain"), ErrInvalidOAuthCode)).To(BeFalse())
	})
})
