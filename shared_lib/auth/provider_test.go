package provider_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"lib/shared_lib/authz"
	"strings"
	"testing"
	"time"

	auth "lib/shared_lib/auth"
	kms "lib/shared_lib/key_management"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestProvider(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "AuthProvider Suite")
}

var _ = Describe("AuthProvider", func() {
	var (
		ctx          context.Context
		kmsClient    kms.KMSClient
		authProvider auth.AuthProvider
		err          error
		userID       uuid.UUID
	)

	BeforeEach(func() {
		ctx = context.Background()

		kmsClient, err = kms.NewLocalKMS(ctx, "local-dev-kms-key")
		Expect(err).NotTo(HaveOccurred())
		Expect(kmsClient).NotTo(BeNil())
	})

	Describe("positive cases", func() {
		BeforeEach(func() {
			authProvider, err = auth.NewAuthProvider(ctx, kmsClient)
			Expect(err).NotTo(HaveOccurred())
			Expect(authProvider).NotTo(BeNil())

			userID = uuid.New()
		})

		It("should create a valid JWT token when credentials are correct", func() {
			token, _, _, err := authProvider.CreateToken(ctx, userID, 15)
			Expect(err).NotTo(HaveOccurred())
			Expect(token).NotTo(BeEmpty())
			parts := strings.Split(token, ".")
			Expect(parts).To(HaveLen(3))
			rawHeader, err := base64.RawURLEncoding.DecodeString(parts[0])
			Expect(err).NotTo(HaveOccurred())

			var header map[string]any
			Expect(json.Unmarshal(rawHeader, &header)).To(Succeed())
			Expect(header["kid"]).To(Equal(kmsClient.KeyID()))
			Expect(header["alg"]).To(Equal("RS256"))
		})

		It("should validate a Bearer token and return the userID claim", func() {
			token, _, _, err := authProvider.CreateToken(ctx, userID, 15)
			Expect(err).NotTo(HaveOccurred())

			claims, err := authProvider.Validate(ctx, "Bearer "+token)
			Expect(err).NotTo(HaveOccurred())
			Expect(claims).To(HaveKeyWithValue("userId", userID.String()))
		})

		It("should validate org, role, and permission claims for access tokens", func() {
			orgID := uuid.New()
			permissions := authz.PermissionsForRole(authz.RoleMLResearcher)
			token, _, _, err := authProvider.CreateAccessToken(ctx, authz.TokenClaims{
				UserID:      userID.String(),
				OrgID:       orgID.String(),
				Roles:       []string{authz.RoleMLResearcher},
				Permissions: permissions,
			}, 15)
			Expect(err).NotTo(HaveOccurred())

			claims, err := authProvider.Validate(ctx, "Bearer "+token)
			Expect(err).NotTo(HaveOccurred())
			Expect(claims).To(HaveKeyWithValue("userId", userID.String()))
			Expect(claims).To(HaveKeyWithValue("orgId", orgID.String()))
			Expect(claims["roles"]).To(Equal([]string{authz.RoleMLResearcher}))
			Expect(claims["permissions"]).To(Equal(permissions))
		})
	})

	Describe("negative cases", func() {
		Context("Validate with invalid/missing Authorization headers", func() {
			BeforeEach(func() {
				authProvider, err = auth.NewAuthProvider(ctx, kmsClient)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should fail when Authorization header is empty", func() {
				claims, err := authProvider.Validate(ctx, "")
				Expect(err).To(MatchError("missing authentication token"))
				Expect(claims).To(BeEmpty())
			})

			It("should fail when Authorization header format is invalid", func() {
				claims, err := authProvider.Validate(ctx, "Token something")
				Expect(err).To(MatchError("invalid authentication token format"))
				Expect(claims).To(BeEmpty())
			})

			It("should fail when token is expired", func() {
				token, _, _, err := authProvider.CreateToken(ctx, uuid.New(), 15)
				Expect(err).NotTo(HaveOccurred())

				parts := strings.Split(token, ".")
				Expect(parts).To(HaveLen(3))
				headerJSON, err := base64.RawURLEncoding.DecodeString(parts[0])
				Expect(err).NotTo(HaveOccurred())

				payloadJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
				Expect(err).NotTo(HaveOccurred())

				var payload map[string]any
				Expect(json.Unmarshal(payloadJSON, &payload)).To(Succeed())

				past := time.Now().Add(-1 * time.Hour).Unix()
				payload["exp"] = float64(past)
				payload["expiresAt"] = float64(past)
				newPayloadJSON, err := json.Marshal(payload)
				Expect(err).NotTo(HaveOccurred())

				newHeaderB64 := base64.RawURLEncoding.EncodeToString(headerJSON)
				newPayloadB64 := base64.RawURLEncoding.EncodeToString(newPayloadJSON)
				signingString := newHeaderB64 + "." + newPayloadB64
				sigBytes, err := kmsClient.SignJWT(ctx, signingString)
				Expect(err).NotTo(HaveOccurred())

				sigB64 := base64.RawURLEncoding.EncodeToString(sigBytes)
				expiredToken := signingString + "." + sigB64
				claims, err := authProvider.Validate(ctx, "Bearer "+expiredToken)
				Expect(err).To(MatchError("invalid JWT token"))
				Expect(claims).To(BeEmpty())
			})

			It("should fail when kid does not match current signing key", func() {
				token, _, _, err := authProvider.CreateToken(ctx, uuid.New(), 15)
				Expect(err).NotTo(HaveOccurred())

				parts := strings.Split(token, ".")
				Expect(parts).To(HaveLen(3))

				headerJSON, err := base64.RawURLEncoding.DecodeString(parts[0])
				Expect(err).NotTo(HaveOccurred())

				var header map[string]any
				Expect(json.Unmarshal(headerJSON, &header)).To(Succeed())
				header["kid"] = "some-other-key"
				newHeaderJSON, err := json.Marshal(header)
				Expect(err).NotTo(HaveOccurred())

				tamperedHeaderB64 := base64.RawURLEncoding.EncodeToString(newHeaderJSON)
				tamperedToken := tamperedHeaderB64 + "." + parts[1] + "." + parts[2]

				claims, err := authProvider.Validate(ctx, "Bearer "+tamperedToken)
				Expect(err).To(MatchError("invalid JWT token"))
				Expect(claims).To(BeEmpty())
			})
		})
	})
})
