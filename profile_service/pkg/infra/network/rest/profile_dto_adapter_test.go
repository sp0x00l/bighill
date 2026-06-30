package rest_test

import (
	"context"
	"fmt"
	usecase "profile_service/pkg/app"
	"profile_service/pkg/domain"
	"profile_service/pkg/infra/network/rest"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"

	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("ProfilesDTO Adapter", func() {
	var (
		ctx                context.Context
		profilesDTOAdapter rest.ProfilesDTOAdapter
	)

	BeforeEach(func() {
		ctx = context.Background()
		profilesDTOAdapter = rest.NewProfilesDTOAdapter()
	})

	Describe("Profile DTO serialization success", func() {
		var (
			dob          time.Time
			profileModel *domain.Profile
		)

		BeforeEach(func() {
			dob = time.Date(2004, 10, 21, 0, 0, 0, 0, time.UTC)
			profileModel = &domain.Profile{
				ProfileAccount: domain.ProfileAccount{
					ID:          uuid.New(),
					Email:       "test@test.com",
					PhoneNumber: "+447078273441",
					CountryCode: "GB",
				},
				FirstName:    "Test",
				LastName:     "User",
				DateOfBirth:  dob,
				AddressLine1: "1 Test Street",
				AddressLine2: "Test Area",
				City:         "Test City",
				State:        "Test State",
				PostalCode:   "TE57 1NG",
				Country:      "Little Britain",
			}
		})

		When("converting a profile domain model to a serialized DTO (camelCase JSON keys)", func() {
			It("should contain all expected attributes with correct values", func() {
				profileBytes, err := profilesDTOAdapter.ToDTO(ctx, profileModel)
				Expect(err).ShouldNot(HaveOccurred())

				var profileDTO map[string]any
				err = json.Unmarshal(profileBytes, &profileDTO)
				Expect(err).ShouldNot(HaveOccurred())

				Expect(profileDTO["id"]).Should(Equal(profileModel.ID.String()))
				Expect(profileDTO["email"]).Should(Equal("test@test.com"))
				Expect(profileDTO["firstName"]).Should(Equal("Test"))
				Expect(profileDTO["lastName"]).Should(Equal("User"))
				Expect(profileDTO["phoneNumber"]).Should(Equal("+447078273441"))
				Expect(profileDTO["dateOfBirth"]).Should(Equal("2004-10-21"))
				Expect(profileDTO["countryCode"]).Should(Equal("GB"))
				Expect(profileDTO["addressLine1"]).Should(Equal("1 Test Street"))
				Expect(profileDTO["addressLine2"]).Should(Equal("Test Area"))
				Expect(profileDTO["city"]).Should(Equal("Test City"))
				Expect(profileDTO["state"]).Should(Equal("Test State"))
				Expect(profileDTO["postalCode"]).Should(Equal("TE57 1NG"))
				Expect(profileDTO["country"]).Should(Equal("Little Britain"))
			})
		})
	})

	Describe("Profile DTO deserialization success", func() {
		var (
			dob time.Time
		)

		BeforeEach(func() {
			dob = time.Date(2004, 10, 21, 0, 0, 0, 0, time.UTC)
		})

		When("deserialising profile DTO to a profile domain model", func() {
			It("should contain all domain model fields from the DTO attributes", func() {
				profileDTO := newProfileDTO() // dateOfBirth aligns with dob
				body, err := json.Marshal(profileDTO)
				Expect(err).ShouldNot(HaveOccurred())

				profileDTOBytes := []byte(body)
				profileModel, err := profilesDTOAdapter.FromDTO(ctx, profileDTOBytes)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(profileModel.Email).Should(Equal("test@test.com"))
				Expect(profileModel.FirstName).Should(Equal("test"))
				Expect(profileModel.LastName).Should(Equal("user"))
				Expect(profileModel.PhoneNumber).Should(Equal("+447078273441"))
				Expect(profileModel.DateOfBirth).Should(BeTemporally("==", dob))
				Expect(profileModel.CountryCode).Should(Equal("GB"))
				Expect(profileModel.AddressLine1).Should(Equal("1 Test Street"))
				Expect(profileModel.AddressLine2).Should(Equal("Test Area"))
				Expect(profileModel.City).Should(Equal("Test City"))
				Expect(profileModel.State).Should(Equal("Test State"))
				Expect(profileModel.PostalCode).Should(Equal("TE57 1NG"))
				Expect(profileModel.Country).Should(Equal("Little Britain"))
			})
		})
	})

	Describe("Profile deserialization fails with missing required fields", func() {
		When("converting a serialized profile DTO with each required field missing", func() {
			optional := map[string]struct{}{
				"addressLine2": {},
				"state":        {},
			}

			baseKeys := func(m map[string]any) []string {
				keys := make([]string, 0, len(m))
				for k := range m {
					if _, isOptional := optional[k]; !isOptional {
						keys = append(keys, k)
					}
				}
				sort.Strings(keys)
				return keys
			}

			for _, missing := range baseKeys(newProfileDTO()) {
				missing := missing
				It(fmt.Sprintf("should return an error when %q is missing", missing), func() {
					payload := cloneMap(newProfileDTO())
					delete(payload, missing)

					body, err := json.Marshal(payload)
					Expect(err).ShouldNot(HaveOccurred())

					profileDTOBytes := []byte(body)
					profileModel, err := profilesDTOAdapter.FromDTO(ctx, profileDTOBytes)
					Expect(err).Should(HaveOccurred())
					Expect(profileModel).Should(BeNil())

					Expect(err.Error()).Should(ContainSubstring("profileDTO." + jsonKeyToField(missing)))
					Expect(err.Error()).Should(ContainSubstring("required"))
				})
			}
		})

		When("converting a serialized profile DTO with optional fields missing", func() {
			optional := map[string]struct{}{
				"addressLine2": {},
				"state":        {},
			}

			for missing := range optional {
				missing := missing // capture
				It(fmt.Sprintf("should NOT return an error when optional %q is missing", missing), func() {
					payload := cloneMap(newProfileDTO())
					delete(payload, missing)

					body, err := json.Marshal(payload)
					Expect(err).ShouldNot(HaveOccurred())

					profileDTOBytes := []byte(body)
					profileModel, err := profilesDTOAdapter.FromDTO(ctx, profileDTOBytes)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(profileModel).ShouldNot(BeNil())
				})
			}
		})
	})

	Describe("Profile validation failure with invalid values", func() {
		When("deserialising an invalid profile", func() {
			It("should return an error", func() {
				profileDTO := map[string]any{"invalid": "invalid"}
				body, err := json.Marshal(profileDTO)
				Expect(err).ShouldNot(HaveOccurred())
				profileDTOBytes := []byte(body)

				profileModel, err := profilesDTOAdapter.FromDTO(ctx, profileDTOBytes)
				Expect(err).Should(HaveOccurred())
				Expect(profileModel).Should(BeNil())
				Expect(err.Error()).Should(ContainSubstring("profileDTO.Email"))
				Expect(err.Error()).Should(ContainSubstring("required"))
			})
		})

		When("deserialising profile DTO with an invalid phone number", func() {
			It("should return an error", func() {
				profileDTO := newProfileDTO()
				profileDTO["phoneNumber"] = "+4412345678901" // wrong format for GB

				body, err := json.Marshal(profileDTO)
				Expect(err).ShouldNot(HaveOccurred())
				profileDTOBytes := []byte(body)

				profileModel, err := profilesDTOAdapter.FromDTO(ctx, profileDTOBytes)
				Expect(err).Should(HaveOccurred())
				Expect(profileModel).Should(BeNil())
				Expect(err.Error()).Should(ContainSubstring("profileDTO.PhoneNumber"))
				Expect(err.Error()).Should(ContainSubstring("phone_by_cc"))
			})
		})

		When("deserialising profile DTO with invalid email", func() {
			It("should return an error", func() {
				invalidEmails := []string{" ", "email@example..com"}
				for _, invalidEmail := range invalidEmails {
					profileDTO := newProfileDTO()
					profileDTO["email"] = invalidEmail

					body, err := json.Marshal(profileDTO)
					Expect(err).ShouldNot(HaveOccurred())

					profileDTOBytes := []byte(body)
					_, err = profilesDTOAdapter.FromDTO(ctx, profileDTOBytes)
					Expect(err).Should(HaveOccurred())
					Expect(err.Error()).Should(ContainSubstring("profileDTO.Email"))
					Expect(err.Error()).Should(ContainSubstring("email"))
				}
			})
		})
	})

	Describe("Profile DTO deserialization errors due to max size limitations", func() {
		When("the profile DTO email address exceeds the maximum size", func() {
			It("should return an error", func() {
				profileDTO := newProfileDTO()
				name := strings.Repeat("a", 256)
				email := name + "@test.com"
				profileDTO["email"] = email

				body, err := json.Marshal(profileDTO)
				Expect(err).ShouldNot(HaveOccurred())

				profileDTOBytes := []byte(body)
				_, err = profilesDTOAdapter.FromDTO(ctx, profileDTOBytes)
				Expect(err).Should(HaveOccurred())
				Expect(err.Error()).Should(ContainSubstring("profileDTO.Email"))
				Expect(err.Error()).Should(ContainSubstring("max"))
			})
		})

		When("the profile DTO char fields exceed the maximum size", func() {
			It("should return an error", func() {
				fields := []string{
					"firstName",
					"lastName",
					"addressLine1",
					"addressLine2",
					"city",
					"state",
					"country",
				}
				for _, field := range fields {
					profileDTO := newProfileDTO()
					profileDTO[field] = strings.Repeat("a", 101)

					body, err := json.Marshal(profileDTO)
					Expect(err).ShouldNot(HaveOccurred())

					profileDTOBytes := []byte(body)
					_, err = profilesDTOAdapter.FromDTO(ctx, profileDTOBytes)
					Expect(err).Should(HaveOccurred())

					Expect(err.Error()).Should(ContainSubstring("profileDTO." + jsonKeyToField(field)))
					Expect(err.Error()).Should(ContainSubstring("max"))
				}
			})
		})
	})

	Describe("Email normalization to lowercase", func() {
		When("deserialising profile DTO with uppercase email", func() {
			It("should normalize email to lowercase", func() {
				profileDTO := newProfileDTO()
				profileDTO["email"] = "TEST@TEST.COM"

				body, err := json.Marshal(profileDTO)
				Expect(err).ShouldNot(HaveOccurred())

				profileDTOBytes := []byte(body)
				profileModel, err := profilesDTOAdapter.FromDTO(ctx, profileDTOBytes)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(profileModel.Email).Should(Equal("test@test.com"))
			})
		})

		When("deserialising profile DTO with mixed case email", func() {
			It("should normalize email to lowercase", func() {
				profileDTO := newProfileDTO()
				profileDTO["email"] = "TeSt@TeSt.CoM"

				body, err := json.Marshal(profileDTO)
				Expect(err).ShouldNot(HaveOccurred())

				profileDTOBytes := []byte(body)
				profileModel, err := profilesDTOAdapter.FromDTO(ctx, profileDTOBytes)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(profileModel.Email).Should(Equal("test@test.com"))
			})
		})

		When("deserialising profileAccount DTO with uppercase email", func() {
			It("should normalize email to lowercase", func() {
				profileAccountDTO := map[string]any{
					"email":       "TEST@TEST.COM",
					"phoneNumber": "+447078273441",
					"countryCode": "GB",
					"password":    "password123!",
				}

				body, err := json.Marshal(profileAccountDTO)
				Expect(err).ShouldNot(HaveOccurred())

				profileAccountDTOBytes := []byte(body)
				profile, err := profilesDTOAdapter.FromProfileAccountDTO(ctx, profileAccountDTOBytes)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(profile.Email).Should(Equal("test@test.com"))
			})
		})
	})

	Describe("ProfileAccount DTO serialization success", func() {
		When("converting a profileAccount domain model to a serialized DTO", func() {
			It("should succeed", func() {
				profileAccountModel := &domain.ProfileAccount{
					ID:          uuid.New(),
					Email:       "test@test.com",
					PhoneNumber: "+447078273441",
					CountryCode: "GB",
					Password:    "password123!",
				}

				profileAccountDTOBytes, err := profilesDTOAdapter.ToProfileAccountDTO(ctx, profileAccountModel)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(profileAccountDTOBytes).ShouldNot(BeNil())
			})
		})
	})

	Describe("ProfileAccount DTO deserialization success", func() {
		When("deserialising profileAccount DTO to a profileAccount domain model", func() {
			It("should succeed", func() {
				profileAccountDTO := map[string]any{
					"email":       "test@test.com",
					"phoneNumber": "+447078273441",
					"countryCode": "GB",
					"password":    "password123!",
				}

				body, err := json.Marshal(profileAccountDTO)
				Expect(err).ShouldNot(HaveOccurred())

				profileAccountDTOBytes := []byte(body)
				profile, err := profilesDTOAdapter.FromProfileAccountDTO(ctx, profileAccountDTOBytes)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(profile).ShouldNot(BeNil())
				Expect(profile.Email).Should(Equal("test@test.com"))
				Expect(profile.PhoneNumber).Should(Equal("+447078273441"))
				Expect(profile.CountryCode).Should(Equal("GB"))
				Expect(profile.Password).Should(Equal("password123!"))
			})
		})
	})

	Describe("ProfileAccount validation failure with invalid values", func() {
		var profileAccountDTO map[string]any

		BeforeEach(func() {
			profileAccountDTO = map[string]any{
				"email":       "test@test.com",
				"phoneNumber": "+447078273441",
				"countryCode": "GB",
				"password":    "password123!",
			}
		})

		When("deserialising an invalid profileAccount email", func() {
			It("should return an error", func() {
				payload := cloneMap(profileAccountDTO)
				payload["email"] = "invalid"

				body, err := json.Marshal(payload)
				Expect(err).ShouldNot(HaveOccurred())

				profileAccountDTOBytes := []byte(body)
				_, err = profilesDTOAdapter.FromProfileAccountDTO(ctx, profileAccountDTOBytes)
				Expect(err).Should(HaveOccurred())
				Expect(err.Error()).Should(ContainSubstring("profileAccountDTO.Email"))
				Expect(err.Error()).Should(ContainSubstring("email"))
			})
		})

		When("deserialising an invalid profileAccount password", func() {
			It("should return an error", func() {
				invalidPasswords := []string{
					"short",
					"alllowercase",
					"ALLUPPERCASE",
					"12345678",
					"NoSpecial1",
					"NoNumber!",
					"     ",
				}
				for _, invalidPassword := range invalidPasswords {
					payload := cloneMap(profileAccountDTO)
					payload["password"] = invalidPassword

					body, err := json.Marshal(payload)
					Expect(err).ShouldNot(HaveOccurred())

					profileAccountDTOBytes := []byte(body)
					_, err = profilesDTOAdapter.FromProfileAccountDTO(ctx, profileAccountDTOBytes)
					Expect(err).Should(HaveOccurred())
					Expect(err.Error()).Should(ContainSubstring("profileAccountDTO.Password"))
					Expect(err.Error()).Should(ContainSubstring("Password"))
				}
			})
		})

		When("deserialising an invalid profileAccount phone number", func() {
			It("should return an error for non-E164 format", func() {
				invalidPhoneNumbers := []string{
					"12345",
					"not-a-phone",
					"0044 7078 273441",
					"07078273441",
				}
				for _, invalidPhone := range invalidPhoneNumbers {
					payload := cloneMap(profileAccountDTO)
					payload["phoneNumber"] = invalidPhone

					body, err := json.Marshal(payload)
					Expect(err).ShouldNot(HaveOccurred())

					profileAccountDTOBytes := []byte(body)
					_, err = profilesDTOAdapter.FromProfileAccountDTO(ctx, profileAccountDTOBytes)
					Expect(err).Should(HaveOccurred(), "expected error for phone: %s", invalidPhone)
					Expect(err.Error()).Should(ContainSubstring("PhoneNumber"))
				}
			})
		})

		When("deserialising a numeric phone number that is invalid for the country", func() {
			It("should surface the phone_by_cc validation error", func() {
				payload := cloneMap(profileAccountDTO)
				payload["phoneNumber"] = "12345"

				body, err := json.Marshal(payload)
				Expect(err).ShouldNot(HaveOccurred())

				profileAccountDTOBytes := []byte(body)
				_, err = profilesDTOAdapter.FromProfileAccountDTO(ctx, profileAccountDTOBytes)
				Expect(err).Should(HaveOccurred())
				Expect(err.Error()).Should(ContainSubstring("profileAccountDTO.PhoneNumber"))
				Expect(err.Error()).Should(ContainSubstring("phone_by_cc"))
			})
		})
	})

	Describe("Password Result DTO serialization success", func() {
		When("converting from a boolean isValid to serialised password result DTO", func() {
			It("should contain isValid attribute", func() {
				passwordResultDTOBytes, err := profilesDTOAdapter.ToPasswordResultDTO(ctx, true, "test-token")
				Expect(err).ShouldNot(HaveOccurred())

				var passwordResultDTO map[string]any
				err = json.Unmarshal(passwordResultDTOBytes, &passwordResultDTO)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(passwordResultDTO["isValid"]).Should(Equal(true))
			})
		})
	})

	Describe("Password validation DTO deserialization success", func() {
		When("deserialising password validation DTO to email and password", func() {
			It("should contain email and password fields", func() {
				passwordValidationDTO := map[string]any{
					"email":    "test@test.com",
					"password": "password123!",
				}

				body, err := json.Marshal(passwordValidationDTO)
				Expect(err).ShouldNot(HaveOccurred())

				passwordValidationDTOBytes := []byte(body)
				email, password, err := profilesDTOAdapter.FromPasswordValidationDTO(ctx, passwordValidationDTOBytes)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(email).Should(Equal("test@test.com"))
				Expect(password).Should(Equal("password123!"))
			})
		})
	})

	Describe("Email verification DTO deserialization", func() {
		It("deserialises a verification token", func() {
			body, err := json.Marshal(map[string]any{
				"token": "token-1",
			})
			Expect(err).ShouldNot(HaveOccurred())

			token, err := profilesDTOAdapter.FromEmailVerificationDTO(ctx, body)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(token).Should(Equal("token-1"))
		})

		It("rejects a missing verification token", func() {
			body, err := json.Marshal(map[string]any{
				"token": "",
			})
			Expect(err).ShouldNot(HaveOccurred())

			_, err = profilesDTOAdapter.FromEmailVerificationDTO(ctx, body)
			Expect(err).Should(HaveOccurred())
			Expect(err.Error()).Should(ContainSubstring("emailVerificationDTO.Token"))
		})
	})

	Describe("OAuth authorize DTOs", func() {
		It("deserialises an oauth authorization request", func() {
			body, err := json.Marshal(map[string]any{
				"redirectUri":   "https://app.example/callback",
				"codeChallenge": strings.Repeat("a", 43),
			})
			Expect(err).ShouldNot(HaveOccurred())

			req, err := profilesDTOAdapter.FromOAuthAuthorizeRequestDTO(ctx, body)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(req.RedirectURI).Should(Equal("https://app.example/callback"))
			Expect(req.CodeChallenge).Should(Equal(strings.Repeat("a", 43)))
		})

		It("rejects an oauth authorization request with an invalid redirect URI", func() {
			body, err := json.Marshal(map[string]any{
				"redirectUri":   "not-a-url",
				"codeChallenge": strings.Repeat("a", 43),
			})
			Expect(err).ShouldNot(HaveOccurred())

			_, err = profilesDTOAdapter.FromOAuthAuthorizeRequestDTO(ctx, body)
			Expect(err).Should(HaveOccurred())
			Expect(err.Error()).Should(ContainSubstring("oauth authorize request"))
		})

		It("serialises an oauth authorization result", func() {
			payload, err := profilesDTOAdapter.ToOAuthAuthorizeResultDTO(ctx, &usecase.OAuthAuthorizeResult{
				AuthorizationURL: "https://provider.example/auth",
				State:            "state-1",
			})
			Expect(err).ShouldNot(HaveOccurred())
			Expect(payload).Should(MatchJSON(`{"authorizationUrl":"https://provider.example/auth","state":"state-1"}`))
		})
	})

	Describe("OAuth session DTOs", func() {
		It("deserialises an oauth session request", func() {
			body, err := json.Marshal(map[string]any{
				"code":         "oauth-code",
				"state":        "state-1",
				"redirectUri":  "https://app.example/callback",
				"codeVerifier": strings.Repeat("b", 43),
			})
			Expect(err).ShouldNot(HaveOccurred())

			req, err := profilesDTOAdapter.FromOAuthSessionRequestDTO(ctx, body)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(req.Code).Should(Equal("oauth-code"))
			Expect(req.State).Should(Equal("state-1"))
			Expect(req.RedirectURI).Should(Equal("https://app.example/callback"))
			Expect(req.CodeVerifier).Should(Equal(strings.Repeat("b", 43)))
		})

		It("rejects an oauth session request with a short code verifier", func() {
			body, err := json.Marshal(map[string]any{
				"code":         "oauth-code",
				"state":        "state-1",
				"redirectUri":  "https://app.example/callback",
				"codeVerifier": "short",
			})
			Expect(err).ShouldNot(HaveOccurred())

			_, err = profilesDTOAdapter.FromOAuthSessionRequestDTO(ctx, body)
			Expect(err).Should(HaveOccurred())
			Expect(err.Error()).Should(ContainSubstring("oauth session request"))
		})

		It("serialises an oauth session result", func() {
			payload, err := profilesDTOAdapter.ToOAuthSessionResultDTO(ctx, &usecase.OAuthSessionResult{
				Token:     "token-1",
				Provider:  "google",
				IsNewUser: true,
			})
			Expect(err).ShouldNot(HaveOccurred())
			Expect(payload).Should(MatchJSON(`{"verified":true,"token":"token-1","provider":"google","isNewUser":true}`))
		})
	})
})

func newProfileDTO() map[string]any {
	return map[string]any{
		"email":        "test@test.com",
		"firstName":    "test",
		"lastName":     "user",
		"phoneNumber":  "+447078273441",
		"dateOfBirth":  "2004-10-21",
		"countryCode":  "GB",
		"addressLine1": "1 Test Street",
		"addressLine2": "Test Area",
		"city":         "Test City",
		"state":        "Test State",
		"postalCode":   "TE57 1NG",
		"country":      "Little Britain",
	}
}

func cloneMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func jsonKeyToField(k string) string {
	switch k {
	case "email":
		return "Email"
	case "firstName":
		return "FirstName"
	case "lastName":
		return "LastName"
	case "phoneNumber":
		return "PhoneNumber"
	case "dateOfBirth":
		return "DateOfBirth"
	case "countryCode":
		return "CountryCode"
	case "addressLine1":
		return "AddressLine1"
	case "addressLine2":
		return "AddressLine2"
	case "city":
		return "City"
	case "state":
		return "State"
	case "postalCode":
		return "PostalCode"
	case "country":
		return "Country"
	default:
		if k == "" {
			return k
		}
		runes := []rune(k)
		runes[0] = unicode.ToUpper(runes[0])
		return string(runes)
	}
}
