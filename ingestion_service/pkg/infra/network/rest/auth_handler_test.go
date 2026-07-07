package rest_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"

	serviceRest "ingestion_service/pkg/infra/network/rest"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestRestHandlers(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Ingestion REST handler test suite")
}

type fakeTokenValidator struct {
	claims        map[string]any
	err           error
	receivedToken string
}

func (f *fakeTokenValidator) Validate(_ context.Context, authorizationToken string) (map[string]any, error) {
	f.receivedToken = authorizationToken
	return f.claims, f.err
}

type fakeAuthSessionStore struct {
	sessionExists  bool
	sessionErr     error
	revokedAfter   int64
	revokedErr     error
	receivedSID    string
	receivedUserID string
}

func (f *fakeAuthSessionStore) SessionExists(_ context.Context, sid string) (bool, error) {
	f.receivedSID = sid
	return f.sessionExists, f.sessionErr
}

func (f *fakeAuthSessionStore) GetUserRevokedAfter(_ context.Context, userID string) (int64, error) {
	f.receivedUserID = userID
	return f.revokedAfter, f.revokedErr
}

var _ = Describe("AuthHandler", func() {
	var (
		ctx       context.Context
		userID    uuid.UUID
		orgID     uuid.UUID
		validator *fakeTokenValidator
		store     *fakeAuthSessionStore
		handler   *serviceRest.AuthHandler
	)

	BeforeEach(func() {
		ctx = context.Background()
		userID = uuid.New()
		orgID = uuid.New()
		validator = &fakeTokenValidator{
			claims: map[string]any{
				"userId":    userID.String(),
				"orgId":     orgID.String(),
				"sid":       "session-id",
				"iat":       int64(100),
				"expiresAt": int64(200),
			},
		}
		store = &fakeAuthSessionStore{sessionExists: true}
		handler = serviceRest.NewAuthHandler(validator, store)
	})

	It("returns the token user when the session is active", func() {
		req := httptest.NewRequest("POST", "/v1/data/store/"+uuid.NewString(), nil)
		req.Header.Set("Authorization", "Bearer token")

		result, err := handler.Authenticate(ctx, req)

		Expect(err).NotTo(HaveOccurred())
		Expect(result.UserID).To(Equal(userID))
		Expect(result.OrgID).To(Equal(orgID))
		Expect(result.ExpUnix).To(Equal(int64(200)))
		Expect(validator.receivedToken).To(Equal("Bearer token"))
		Expect(store.receivedSID).To(Equal("session-id"))
		Expect(store.receivedUserID).To(Equal(userID.String()))
	})

	It("requires an authorization header", func() {
		req := httptest.NewRequest("POST", "/v1/data/store/"+uuid.NewString(), nil)

		_, err := handler.Authenticate(ctx, req)

		Expect(err).To(MatchError("missing authorization header"))
	})

	It("rejects invalid tokens", func() {
		validator.err = errors.New("bad token")
		req := httptest.NewRequest("POST", "/v1/data/store/"+uuid.NewString(), nil)
		req.Header.Set("Authorization", "Bearer token")

		_, err := handler.Authenticate(ctx, req)

		Expect(err).To(MatchError("invalid token"))
		Expect(store.receivedSID).To(BeEmpty())
	})

	It("rejects tokens without an active session", func() {
		store.sessionExists = false
		req := httptest.NewRequest("POST", "/v1/data/store/"+uuid.NewString(), nil)
		req.Header.Set("Authorization", "Bearer token")

		_, err := handler.Authenticate(ctx, req)

		Expect(err).To(MatchError("session revoked"))
	})

	It("rejects tokens issued before the user revocation cutoff", func() {
		store.revokedAfter = 100
		req := httptest.NewRequest("POST", "/v1/data/store/"+uuid.NewString(), nil)
		req.Header.Set("Authorization", "Bearer token")

		_, err := handler.Authenticate(ctx, req)

		Expect(err).To(MatchError("token revoked"))
	})

	It("returns an auth check failure when Redis session lookup fails", func() {
		store.sessionErr = errors.New("redis unavailable")
		req := httptest.NewRequest("POST", "/v1/data/store/"+uuid.NewString(), nil)
		req.Header.Set("Authorization", "Bearer token")

		_, err := handler.Authenticate(ctx, req)

		Expect(err).To(MatchError("auth check failed"))
	})
})
