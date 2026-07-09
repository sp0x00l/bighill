package db_test

import (
	"tenant_service/pkg/domain"
	"tenant_service/pkg/infra/repo/db"
	"time"

	"github.com/google/uuid"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Profile DAO unit test", func() {
	Describe("ToDAO - converts a profile to a DAO", func() {
		When("the profile has a valid email and nickname", func() {
			It("should return the expected pgx.NamedArgs", func() {
				profile := &domain.Profile{
					ProfileAccount: domain.ProfileAccount{
						ID:          uuid.New(),
						Email:       "test@example.com",
						PhoneNumber: "1234567890",
						CountryCode: "IE",
					},
					FirstName:    "Test",
					LastName:     "User",
					DateOfBirth:  time.Date(1990, 1, 1, 0, 0, 0, 0, time.UTC),
					AddressLine1: "123 Test St",
					AddressLine2: "Apt 1",
					City:         "Test City",
					State:        "TS",
					PostalCode:   "12345",
					Country:      "Test Country",
				}

				expectedArgs := pgx.NamedArgs{
					"id":             pgtype.UUID{Bytes: profile.ID, Valid: true},
					"email":          pgtype.Text{String: profile.Email, Valid: true},
					"email_verified": pgtype.Bool{Bool: false, Valid: true},
					"first_name":     pgtype.Text{String: profile.FirstName, Valid: true},
					"last_name":      pgtype.Text{String: profile.LastName, Valid: true},
					"phone_number":   pgtype.Text{String: profile.PhoneNumber, Valid: true},
					"date_of_birth":  pgtype.Date{Time: profile.DateOfBirth, Valid: true},
					"country_code":   pgtype.Text{String: profile.CountryCode, Valid: true},
					"address_line_1": pgtype.Text{String: profile.AddressLine1, Valid: true},
					"address_line_2": pgtype.Text{String: profile.AddressLine2, Valid: true},
					"city":           pgtype.Text{String: profile.City, Valid: true},
					"state":          pgtype.Text{String: profile.State, Valid: true},
					"postal_code":    pgtype.Text{String: profile.PostalCode, Valid: true},
					"country":        pgtype.Text{String: profile.Country, Valid: true},
				}
				dao := db.ToDAO(profile, profile.ID)
				Expect(dao).To(Equal(expectedArgs))
			})
		})

		When("the profile has empty email and phone number", func() {
			It("should return the expected pgx.NamedArgs with nullable email and phone number", func() {
				profile := &domain.Profile{
					ProfileAccount: domain.ProfileAccount{
						ID:          uuid.New(),
						CountryCode: "IE",
					},
					FirstName:    "Test",
					LastName:     "User",
					DateOfBirth:  time.Date(1990, 1, 1, 0, 0, 0, 0, time.UTC),
					AddressLine1: "123 Test St",
					AddressLine2: "Apt 1",
					City:         "Test City",
					State:        "TS",
					PostalCode:   "12345",
					Country:      "Test Country",
				}

				expectedArgs := pgx.NamedArgs{
					"id":             pgtype.UUID{Bytes: profile.ID, Valid: true},
					"email_verified": pgtype.Bool{Bool: false, Valid: true},
					"first_name":     pgtype.Text{String: profile.FirstName, Valid: true},
					"last_name":      pgtype.Text{String: profile.LastName, Valid: true},
					"date_of_birth":  pgtype.Date{Time: profile.DateOfBirth, Valid: true},
					"country_code":   pgtype.Text{String: profile.CountryCode, Valid: true},
					"address_line_1": pgtype.Text{String: profile.AddressLine1, Valid: true},
					"address_line_2": pgtype.Text{String: profile.AddressLine2, Valid: true},
					"city":           pgtype.Text{String: profile.City, Valid: true},
					"state":          pgtype.Text{String: profile.State, Valid: true},
					"postal_code":    pgtype.Text{String: profile.PostalCode, Valid: true},
					"country":        pgtype.Text{String: profile.Country, Valid: true},
					"email":          pgtype.Text{Valid: false},
					"phone_number":   pgtype.Text{Valid: false},
				}
				dao := db.ToDAO(profile, profile.ID)
				Expect(dao).To(Equal(expectedArgs))
			})
		})
	})

	Describe("ToDAOOAuthIdentity", func() {
		It("returns the expected pgx.NamedArgs", func() {
			userID := uuid.New()
			identity := domain.OAuthIdentity{
				Provider:      "google",
				Subject:       "provider-subject",
				Email:         "user@example.com",
				EmailVerified: true,
			}

			expectedArgs := pgx.NamedArgs{
				"profile_id":       pgtype.UUID{Bytes: userID, Valid: true},
				"provider":         pgtype.Text{String: identity.Provider, Valid: true},
				"provider_subject": pgtype.Text{String: identity.Subject, Valid: true},
				"email":            pgtype.Text{String: identity.Email, Valid: true},
				"email_verified":   pgtype.Bool{Bool: identity.EmailVerified, Valid: true},
			}

			dao := db.ToDAOOAuthIdentity(userID, identity)
			Expect(dao).To(Equal(expectedArgs))
		})
	})

	Describe("ToDAOOAuthProfile", func() {
		It("returns the expected pgx.NamedArgs", func() {
			identity := domain.OAuthIdentity{
				Provider:  "google",
				Subject:   "oauth-subject",
				Email:     "user@example.com",
				FirstName: "Test",
				LastName:  "User",
			}

			dao := db.ToDAOOAuthProfile(identity, "password-hash")

			Expect(dao["idempotency_key"]).To(BeAssignableToTypeOf(pgtype.UUID{}))
			Expect(dao["email"]).To(Equal(pgtype.Text{String: identity.Email, Valid: true}))
			Expect(dao["phone_number"]).To(Equal(pgtype.Text{Valid: false}))
			Expect(dao["country_code"]).To(Equal(pgtype.Text{Valid: false}))
			Expect(dao["password_hash"]).To(Equal(pgtype.Text{String: "password-hash", Valid: true}))
			Expect(dao["first_name"]).To(Equal(pgtype.Text{String: identity.FirstName, Valid: true}))
			Expect(dao["last_name"]).To(Equal(pgtype.Text{String: identity.LastName, Valid: true}))
		})
	})

	Describe("ToDAOProfileAccount", func() {
		It("hashes the email verification token before persistence", func() {
			dao := db.ToDAOProfileAccount(&domain.ProfileAccount{
				Email:                "user@example.com",
				PhoneNumber:          "+447700900123",
				CountryCode:          "GB",
				Password:             "password-hash",
				EmailVerifyToken:     "token-1",
				EmailVerifyExpiresAt: time.Date(2026, 4, 19, 10, 0, 0, 0, time.UTC),
			})

			Expect(dao["email_verify_token_hash"]).To(BeAssignableToTypeOf(pgtype.Text{}))
			tokenHash := dao["email_verify_token_hash"].(pgtype.Text)
			Expect(tokenHash.Valid).To(BeTrue())
			Expect(tokenHash.String).To(HaveLen(64))
			Expect(tokenHash.String).NotTo(Equal("token-1"))
		})
	})

	Describe("FromDAOProfileID", func() {
		It("returns the UUID bytes", func() {
			id := uuid.New()
			dao := &db.ProfileIDDAO{
				ID: pgtype.UUID{Bytes: id, Valid: true},
			}

			Expect(db.FromDAOProfileID(dao)).To(Equal(id))
		})
	})

	Describe("FromDAOOAuthProfileID", func() {
		It("returns the UUID bytes", func() {
			id := uuid.New()
			dao := &db.OAuthProfileIDDAO{
				ProfileID: pgtype.UUID{Bytes: id, Valid: true},
			}

			Expect(db.FromDAOOAuthProfileID(dao)).To(Equal(id))
		})
	})
})
