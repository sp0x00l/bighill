package provider

import (
	"context"
	"errors"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type authProviderMock struct {
	token  string
	claims map[string]any
	err    error
}

func (m *authProviderMock) CreateToken(_ context.Context, _ uuid.UUID, _ int) (string, string, int64, error) {
	return "", "", 0, nil
}

func (m *authProviderMock) Validate(_ context.Context, authorizationToken string) (map[string]any, error) {
	m.token = authorizationToken
	return m.claims, m.err
}

var _ = Describe("AuthProvider", func() {
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
		authProvider = nil
	})

	AfterEach(func() {
		authProvider = nil
	})

	It("returns empty claims when the provider has not been initialized", func() {
		claims, err := AuthProvider(ctx, "Bearer token")

		Expect(err).NotTo(HaveOccurred())
		Expect(claims).To(BeEmpty())
	})

	It("delegates validation to the initialized shared auth provider", func() {
		provider := &authProviderMock{
			claims: map[string]any{
				"userId": "user-123",
				"sid":    "session-456",
			},
		}
		authProvider = provider

		claims, err := AuthProvider(ctx, "Bearer token")

		Expect(err).NotTo(HaveOccurred())
		Expect(provider.token).To(Equal("Bearer token"))
		Expect(claims).To(HaveKeyWithValue("userId", "user-123"))
		Expect(claims).To(HaveKeyWithValue("sid", "session-456"))
	})

	It("returns validation errors from the initialized shared auth provider", func() {
		provider := &authProviderMock{err: errors.New("invalid token")}
		authProvider = provider

		claims, err := AuthProvider(ctx, "Bearer token")

		Expect(err).To(MatchError("invalid token"))
		Expect(claims).To(BeNil())
	})
})
