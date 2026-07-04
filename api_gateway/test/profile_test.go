package test

import (
	"net/http"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Profile API", Ordered, func() {
	It("creates a profile and rejects duplicate email or phone details", func() {
		payload := map[string]any{
			"email":       "duplicate-" + uuid.NewString() + "@test.com",
			"phoneNumber": uniqueGBPhone(),
			"countryCode": "GB",
			"password":    "SecurePass123!",
		}

		status, body := doJSON(http.MethodPost, "/public/v1/profiles", payload, "", uuid.New())
		Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))

		status, body = doJSON(http.MethodPost, "/public/v1/profiles", payload, "", uuid.New())
		Expect(status).To(Equal(http.StatusConflict), "body: %s", string(body))

		status, body = doJSON(http.MethodPost, "/public/v1/profiles/email/verify", map[string]any{"token": testEmailVerificationToken(payload["email"].(string))}, "", uuid.Nil)
		Expect(status).To(Equal(http.StatusNoContent), "body: %s", string(body))
	})

	It("creates, verifies, authenticates, updates, reads, and logs out a profile", func() {
		user := createVerifiedProfileAndLogin()

		updatedProfile := map[string]any{
			"id":           user.ID.String(),
			"email":        user.Email,
			"firstName":    "Ada",
			"lastName":     "Lovelace",
			"phoneNumber":  user.Phone,
			"dateOfBirth":  "1990-05-02",
			"countryCode":  "GB",
			"addressLine1": "10 Downing Street",
			"city":         "London",
			"postalCode":   "SW1A 2AA",
			"country":      "United Kingdom",
		}

		status, body := doJSON(http.MethodPut, "/private/v1/profiles", updatedProfile, user.Token, uuid.Nil)
		Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))
		updated := decodeObject(body)
		Expect(updated["email"]).To(Equal(user.Email))
		Expect(updated["firstName"]).To(Equal("Ada"))
		Expect(updated["lastName"]).To(Equal("Lovelace"))

		status, body = doJSON(http.MethodGet, "/private/v1/profiles", nil, user.Token, uuid.Nil)
		Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))
		read := decodeObject(body)
		Expect(read["id"]).To(Equal(user.ID.String()))
		Expect(read["email"]).To(Equal(user.Email))
		Expect(read["firstName"]).To(Equal("Ada"))

		status, body = doJSON(http.MethodPost, "/private/v1/profiles/logout", nil, user.Token, uuid.Nil)
		Expect(status).To(Equal(http.StatusNoContent), "body: %s", string(body))
	})
})
