package rest_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"

	usecase "tenant_service/pkg/app"
	"tenant_service/pkg/domain"
	"tenant_service/pkg/infra/network/rest"

	"github.com/google/uuid"
	"github.com/gorilla/mux"

	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	userIDHeader        = "X-User-ID"
	idempotencyIDHeader = "X-Request-ID"
)

type profilesDTOAdapterMock struct {
	ToDTOCalled                     bool
	FromDTOCalled                   bool
	ToProfileAccountDTOCalled       bool
	FromProfileAccountDTOCalled     bool
	FromPasswordDTOCalled           bool
	FromPasswordValidationDTOCalled bool
	FromEmailVerificationDTOCalled  bool
	FromHuggingFaceTokenDTOCalled   bool
	ToPasswordResultDTOCalled       bool
	FromOAuthAuthorizeCalled        bool
	ToOAuthAuthorizeCalled          bool
	FromOAuthSessionCalled          bool
	ToOAuthSessionCalled            bool
	FromOrganizationMemberCalled    bool
	ToOrganizationCalled            bool
	ToOrganizationMembersCalled     bool

	NextProfile             *domain.Profile
	NextProfileAccount      *domain.ProfileAccount
	NextOrganization        *domain.Organization
	NextMembership          *domain.OrganizationMembership
	NextMemberships         []*domain.OrganizationMembership
	NextProfileBytes        []byte
	NextProfileAccountBytes []byte
	NextOrganizationBytes   []byte
	NextMembershipsBytes    []byte
	NextEmail               string
	NextPassword            string
	NextHuggingFaceToken    string
	NextEmailVerifyToken    string
	NextPasswordBytes       []byte
	NextOAuthAuthorizeReq   *usecase.OAuthAuthorizeRequest
	NextOAuthAuthorizeRes   []byte
	NextOAuthSessionReq     *usecase.OAuthSessionRequest
	NextOAuthSessionRes     []byte
	NextError               error
	NextSerialisationError  error

	LastProfileModel        *domain.Profile
	LastProfileAccountModel *domain.ProfileAccount
	LastOrganizationModel   *domain.Organization
	LastMembershipModel     *domain.OrganizationMembership
	LastMembershipsModel    []*domain.OrganizationMembership
	LastProfileBytes        []byte
	LastProfileAccountBytes []byte
	LastPasswordBytes       []byte
	LastToken               string
}

func (m *profilesDTOAdapterMock) ToDTO(_ context.Context, profileModel *domain.Profile) ([]byte, error) {
	m.ToDTOCalled = true
	m.LastProfileModel = profileModel
	return m.NextProfileBytes, m.NextSerialisationError
}

func (m *profilesDTOAdapterMock) FromDTO(_ context.Context, profileBytes []byte) (*domain.Profile, error) {
	m.FromDTOCalled = true
	m.LastProfileBytes = profileBytes
	return m.NextProfile, m.NextError
}

func (m *profilesDTOAdapterMock) ToProfileAccountDTO(_ context.Context, profileAccountModel *domain.ProfileAccount) ([]byte, error) {
	m.ToProfileAccountDTOCalled = true
	m.LastProfileAccountModel = profileAccountModel
	return m.NextProfileAccountBytes, m.NextSerialisationError
}

func (m *profilesDTOAdapterMock) FromProfileAccountDTO(_ context.Context, profileAccountBytes []byte) (*domain.ProfileAccount, error) {
	m.FromProfileAccountDTOCalled = true
	m.LastProfileAccountBytes = profileAccountBytes
	return m.NextProfileAccount, m.NextError
}

func (m *profilesDTOAdapterMock) FromPasswordDTO(_ context.Context, passwordBytes []byte) (string, error) {
	m.FromPasswordDTOCalled = true
	m.LastPasswordBytes = passwordBytes
	return m.NextPassword, m.NextError
}

func (m *profilesDTOAdapterMock) FromPasswordValidationDTO(_ context.Context, passwordBytes []byte) (string, string, error) {
	m.FromPasswordValidationDTOCalled = true
	m.LastPasswordBytes = passwordBytes
	return m.NextEmail, m.NextPassword, m.NextError
}

func (m *profilesDTOAdapterMock) FromEmailVerificationDTO(_ context.Context, emailVerificationBytes []byte) (string, error) {
	m.FromEmailVerificationDTOCalled = true
	m.LastPasswordBytes = emailVerificationBytes
	return m.NextEmailVerifyToken, m.NextError
}

func (m *profilesDTOAdapterMock) FromHuggingFaceTokenDTO(_ context.Context, tokenBytes []byte) (string, error) {
	m.FromHuggingFaceTokenDTOCalled = true
	m.LastPasswordBytes = tokenBytes
	return m.NextHuggingFaceToken, m.NextError
}

func (m *profilesDTOAdapterMock) ToPasswordResultDTO(_ context.Context, isValid bool, token string) ([]byte, error) {
	m.ToPasswordResultDTOCalled = true
	m.LastToken = token
	return m.NextPasswordBytes, m.NextSerialisationError
}

func (m *profilesDTOAdapterMock) FromOAuthAuthorizeRequestDTO(_ context.Context, requestBytes []byte) (*usecase.OAuthAuthorizeRequest, error) {
	m.FromOAuthAuthorizeCalled = true
	return m.NextOAuthAuthorizeReq, m.NextError
}

func (m *profilesDTOAdapterMock) ToOAuthAuthorizeResultDTO(_ context.Context, _ *usecase.OAuthAuthorizeResult) ([]byte, error) {
	m.ToOAuthAuthorizeCalled = true
	return m.NextOAuthAuthorizeRes, m.NextSerialisationError
}

func (m *profilesDTOAdapterMock) FromOAuthSessionRequestDTO(_ context.Context, requestBytes []byte) (*usecase.OAuthSessionRequest, error) {
	m.FromOAuthSessionCalled = true
	return m.NextOAuthSessionReq, m.NextError
}

func (m *profilesDTOAdapterMock) ToOAuthSessionResultDTO(_ context.Context, _ *usecase.OAuthSessionResult) ([]byte, error) {
	m.ToOAuthSessionCalled = true
	return m.NextOAuthSessionRes, m.NextSerialisationError
}

func (m *profilesDTOAdapterMock) FromOrganizationMemberDTO(_ context.Context, _ uuid.UUID, requestBytes []byte) (*domain.OrganizationMembership, error) {
	m.FromOrganizationMemberCalled = true
	m.LastProfileBytes = requestBytes
	return m.NextMembership, m.NextError
}

func (m *profilesDTOAdapterMock) ToOrganizationDTO(_ context.Context, organization *domain.Organization, membership *domain.OrganizationMembership) ([]byte, error) {
	m.ToOrganizationCalled = true
	m.LastOrganizationModel = organization
	m.LastMembershipModel = membership
	return m.NextOrganizationBytes, m.NextSerialisationError
}

func (m *profilesDTOAdapterMock) ToOrganizationMembersDTO(_ context.Context, memberships []*domain.OrganizationMembership) ([]byte, error) {
	m.ToOrganizationMembersCalled = true
	m.LastMembershipsModel = memberships
	return m.NextMembershipsBytes, m.NextSerialisationError
}

type profileUseCaseMock struct {
	CreateProfileCalled            bool
	UpdateProfileCalled            bool
	ReadProfileCalled              bool
	DeleteProfileCalled            bool
	ReplacePasswordCalled          bool
	ReplaceHuggingFaceTokenCalled  bool
	VerifyPasswordCalled           bool
	VerifyEmailCalled              bool
	CreateOAuthAuthorizationCalled bool
	CreateOAuthSessionCalled       bool
	LogoutCalled                   bool
	ReadCurrentOrganizationCalled  bool
	ListOrganizationMembersCalled  bool
	UpsertOrganizationMemberCalled bool
	DeleteOrganizationMemberCalled bool

	NextProfile      *domain.Profile
	NextOrganization *domain.Organization
	NextMembership   *domain.OrganizationMembership
	NextMemberships  []*domain.OrganizationMembership
	NextToken        string
	NextOAuthAuth    *usecase.OAuthAuthorizeResult
	NextOAuthSession *usecase.OAuthSessionResult
	NextError        error

	LastProfile          *domain.Profile
	LastProfileAccount   *domain.ProfileAccount
	LastIdempotencyKey   uuid.UUID
	LastUserID           uuid.UUID
	LastOrgID            uuid.UUID
	LastMemberUserID     uuid.UUID
	LastMembership       *domain.OrganizationMembership
	LastEmail            string
	LastToken            string
	LastPassword         string
	LastHuggingFaceToken string
	LastSessionID        string
}

func (m *profileUseCaseMock) CreateProfile(_ context.Context, account *domain.ProfileAccount, idempotencyKey uuid.UUID) error {
	m.CreateProfileCalled = true
	m.LastProfileAccount = account
	m.LastIdempotencyKey = idempotencyKey
	return m.NextError
}

func (m *profileUseCaseMock) ReplaceProfile(_ context.Context, userID uuid.UUID, profile *domain.Profile) (*domain.Profile, error) {
	m.UpdateProfileCalled = true
	m.LastProfile = profile
	m.LastUserID = userID
	return m.NextProfile, m.NextError
}

func (m *profileUseCaseMock) ReadProfile(_ context.Context, id uuid.UUID) (*domain.Profile, error) {
	m.ReadProfileCalled = true
	m.LastUserID = id
	return m.NextProfile, m.NextError
}

func (m *profileUseCaseMock) DeleteProfile(_ context.Context, id uuid.UUID) error {
	m.DeleteProfileCalled = true
	m.LastUserID = id
	return m.NextError
}

func (m *profileUseCaseMock) ReplacePassword(_ context.Context, userID uuid.UUID, newPassword string) error {
	m.ReplacePasswordCalled = true
	m.LastUserID = userID
	m.LastPassword = newPassword
	return m.NextError
}

func (m *profileUseCaseMock) ReplaceHuggingFaceToken(_ context.Context, userID uuid.UUID, token string) error {
	m.ReplaceHuggingFaceTokenCalled = true
	m.LastUserID = userID
	m.LastHuggingFaceToken = token
	return m.NextError
}

func (m *profileUseCaseMock) VerifyPassword(_ context.Context, email, password string) (string, error) {
	m.VerifyPasswordCalled = true
	m.LastEmail = email
	m.LastPassword = password
	return m.NextToken, m.NextError
}

func (m *profileUseCaseMock) VerifyEmail(_ context.Context, token string) error {
	m.VerifyEmailCalled = true
	m.LastToken = token
	return m.NextError
}

func (m *profileUseCaseMock) CreateOAuthAuthorization(_ context.Context, _ string, _ usecase.OAuthAuthorizeRequest) (*usecase.OAuthAuthorizeResult, error) {
	m.CreateOAuthAuthorizationCalled = true
	return m.NextOAuthAuth, m.NextError
}

func (m *profileUseCaseMock) CreateOAuthSession(_ context.Context, _ string, _ usecase.OAuthSessionRequest) (*usecase.OAuthSessionResult, error) {
	m.CreateOAuthSessionCalled = true
	return m.NextOAuthSession, m.NextError
}

func (m *profileUseCaseMock) Logout(_ context.Context, sessionID string) error {
	m.LogoutCalled = true
	m.LastSessionID = sessionID
	return m.NextError
}

func (m *profileUseCaseMock) ReadCurrentOrganization(_ context.Context, actorUserID uuid.UUID, orgID uuid.UUID) (*domain.Organization, *domain.OrganizationMembership, error) {
	m.ReadCurrentOrganizationCalled = true
	m.LastUserID = actorUserID
	m.LastOrgID = orgID
	return m.NextOrganization, m.NextMembership, m.NextError
}

func (m *profileUseCaseMock) ListOrganizationMembers(_ context.Context, actorUserID uuid.UUID, orgID uuid.UUID) ([]*domain.OrganizationMembership, error) {
	m.ListOrganizationMembersCalled = true
	m.LastUserID = actorUserID
	m.LastOrgID = orgID
	return m.NextMemberships, m.NextError
}

func (m *profileUseCaseMock) UpsertOrganizationMember(_ context.Context, actorUserID uuid.UUID, membership *domain.OrganizationMembership) (*domain.OrganizationMembership, error) {
	m.UpsertOrganizationMemberCalled = true
	m.LastUserID = actorUserID
	m.LastMembership = membership
	return m.NextMembership, m.NextError
}

func (m *profileUseCaseMock) DeleteOrganizationMember(_ context.Context, actorUserID uuid.UUID, orgID uuid.UUID, memberUserID uuid.UUID) error {
	m.DeleteOrganizationMemberCalled = true
	m.LastUserID = actorUserID
	m.LastOrgID = orgID
	m.LastMemberUserID = memberUserID
	return m.NextError
}

func TestHTTPHandlers(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Profile HTTP handlers unit test suite")
}

var _ = Describe("Profiles HTTP handlers", func() {
	var (
		handlers          *rest.HttpHandler
		req               *http.Request
		dtoProfileAdapter *profilesDTOAdapterMock
		profileUsecase    *profileUseCaseMock
		ctx               = context.Background()
		userID            = uuid.New()
	)

	makeJSONReq := func(b []byte) *http.Request {
		r, _ := http.NewRequest(http.MethodPost, "/", bytes.NewReader(b))
		r.Header.Set("Content-Type", "application/json")
		return r
	}

	makeJSONReqWithProvider := func(b []byte, provider string) *http.Request {
		r := makeJSONReq(b)
		return mux.SetURLVars(r, map[string]string{"provider": provider})
	}

	BeforeEach(func() {
		profileUsecase = &profileUseCaseMock{}
		dtoProfileAdapter = &profilesDTOAdapterMock{}

		handlers = rest.NewHttpHandler(profileUsecase, dtoProfileAdapter)
	})

	Describe("CreateProfile - create user minimum profile", func() {
		Context("success", func() {
			When("the request is correct", func() {
				It("returns 200 with serialized profile with new User ID", func() {
					req = makeJSONReq([]byte(`{
						"email":"test@test.com"
					}`))
					req.Header.Set(idempotencyIDHeader, uuid.NewString())

					dtoProfileAdapter.NextProfileAccount = &domain.ProfileAccount{
						Email: "test@test.com",
					}
					nextProfileAccount := fmt.Sprintf(`{
						"email":"test@test.com",
						"id":"%s"
					}`, userID.String())
					profileUsecase.NextError = nil
					dtoProfileAdapter.NextProfileAccountBytes = []byte(nextProfileAccount)

					status, body, err := handlers.CreateProfile(ctx, req)

					Expect(err).NotTo(HaveOccurred())
					Expect(status).To(Equal(http.StatusCreated))
					Expect(body).To(MatchJSON(`{"email":"test@test.com","id":"` + userID.String() + `"}`))
					Expect(profileUsecase.CreateProfileCalled).To(BeTrue())
					Expect(dtoProfileAdapter.ToProfileAccountDTOCalled).To(BeTrue())
				})
			})
		})

		Context("failure", func() {
			When("Idempotency Header is missing", func() {
				It("returns 400", func() {
					req = makeJSONReq([]byte(`{}`))
					status, resBytes, err := handlers.CreateProfile(ctx, req)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(ContainSubstring("missing idempotency X-Request-ID header"))
					Expect(status).To(Equal(http.StatusBadRequest))
					Expect(resBytes).To(BeNil())
				})
			})

			When("payload can't be parsed to ProfileAccount", func() {
				It("returns 400", func() {
					req = makeJSONReq([]byte(`{invalid json`))
					req.Header.Set(idempotencyIDHeader, uuid.NewString())
					dtoProfileAdapter.NextError = errors.New("validation failed")
					status, respBytes, err := handlers.CreateProfile(ctx, req)
					Expect(err).To(HaveOccurred())
					Expect(status).To(Equal(http.StatusBadRequest))
					Expect(respBytes).To(BeNil())
				})
			})

			When("profile already exists", func() {
				It("returns 409", func() {
					req = makeJSONReq([]byte(`{"email":"test@test.com"}`))
					req.Header.Set(idempotencyIDHeader, uuid.NewString())
					dtoProfileAdapter.NextProfileAccount = &domain.ProfileAccount{
						Email: "test@test.com",
					}
					profileUsecase.NextError = domain.ErrProfileAlreadyExists

					status, _, err := handlers.CreateProfile(ctx, req)
					Expect(err).To(HaveOccurred())
					Expect(status).To(Equal(http.StatusConflict))
				})
			})

			When("profile creation fails with a logic error", func() {
				It("returns 500", func() {
					req = makeJSONReq([]byte(`{"email":"test@test.com"}`))
					req.Header.Set(idempotencyIDHeader, uuid.NewString())
					dtoProfileAdapter.NextProfileAccount = &domain.ProfileAccount{
						Email: "test@test.com",
					}
					profileUsecase.NextError = errors.New("internal error")

					status, _, err := handlers.CreateProfile(ctx, req)
					Expect(err).To(HaveOccurred())
					Expect(status).To(Equal(http.StatusInternalServerError))
				})
			})

			When("serialization fails", func() {
				It("returns 500", func() {
					req = makeJSONReq([]byte(`{"email":"test@test.com"}`))
					req.Header.Set(idempotencyIDHeader, uuid.NewString())
					dtoProfileAdapter.NextProfileAccount = &domain.ProfileAccount{Email: "test@test.com"}
					profileUsecase.NextError = nil
					dtoProfileAdapter.NextSerialisationError = errors.New("marshal error")

					status, _, err := handlers.CreateProfile(ctx, req)
					Expect(err).To(HaveOccurred())
					Expect(status).To(Equal(http.StatusInternalServerError))
				})
			})
		})
	})

	Describe("ReplaceProfile", func() {
		Context("success", func() {
			It("replaces and returns 200 with DTO", func() {
				req = makeJSONReq([]byte(`{"name":"Test"}`))
				req.Header.Set(userIDHeader, userID.String())

				dtoProfileAdapter.NextProfile = &domain.Profile{FirstName: "Test"}
				profileUsecase.NextProfile = &domain.Profile{
					ProfileAccount: domain.ProfileAccount{
						ID: userID,
					},
					FirstName: "Test",
				}
				dtoProfileAdapter.NextProfileBytes = []byte(`{"id":"` + userID.String() + `","firstName":"Test"}`)

				status, body, err := handlers.ReplaceProfile(ctx, req)
				Expect(err).NotTo(HaveOccurred())
				Expect(status).To(Equal(http.StatusOK))
				Expect(body).To(MatchJSON(`{"id":"` + userID.String() + `","firstName":"Test"}`))
				Expect(profileUsecase.UpdateProfileCalled).To(BeTrue())
			})
		})

		Context("failure", func() {
			When("user header missing", func() {
				It("returns 500 as it's set by the API Gateway", func() {
					req = makeJSONReq([]byte(`{}`))
					status, _, err := handlers.ReplaceProfile(ctx, req)
					Expect(err).To(HaveOccurred())
					Expect(status).To(Equal(http.StatusInternalServerError))
				})
			})

			When("payload is invalid", func() {
				It("returns 400", func() {
					req = makeJSONReq([]byte(`{bad json`))
					req.Header.Set(userIDHeader, userID.String())
					dtoProfileAdapter.NextError = errors.New("validation failed")

					status, _, err := handlers.ReplaceProfile(ctx, req)
					Expect(err).To(HaveOccurred())
					Expect(status).To(Equal(http.StatusBadRequest))
				})
			})

			When("profile is not found", func() {
				It("returns 404", func() {
					req = makeJSONReq([]byte(`{"firstName":"Test"}`))
					req.Header.Set(userIDHeader, userID.String())
					dtoProfileAdapter.NextProfile = &domain.Profile{FirstName: "Test"}
					profileUsecase.NextError = domain.ErrProfileNotFound

					status, _, err := handlers.ReplaceProfile(ctx, req)
					Expect(err).To(HaveOccurred())
					Expect(status).To(Equal(http.StatusNotFound))
				})
			})

			When("an internal error occurs", func() {
				It("returns 500", func() {
					req = makeJSONReq([]byte(`{"firstName":"Test"}`))
					req.Header.Set(userIDHeader, userID.String())
					dtoProfileAdapter.NextProfile = &domain.Profile{FirstName: "Test"}
					profileUsecase.NextError = errors.New("internal error")

					status, _, err := handlers.ReplaceProfile(ctx, req)
					Expect(err).To(HaveOccurred())
					Expect(status).To(Equal(http.StatusInternalServerError))
				})
			})

			When("serialization fails", func() {
				It("returns 500", func() {
					req = makeJSONReq([]byte(`{"firstName":"Test"}`))
					req.Header.Set(userIDHeader, userID.String())
					dtoProfileAdapter.NextProfile = &domain.Profile{FirstName: "Test"}
					profileUsecase.NextProfile = &domain.Profile{FirstName: "Test"}
					dtoProfileAdapter.NextSerialisationError = errors.New("marshal fail")

					status, _, err := handlers.ReplaceProfile(ctx, req)
					Expect(err).To(HaveOccurred())
					Expect(status).To(Equal(http.StatusInternalServerError))
				})
			})
		})
	})

	Describe("DeleteProfile", func() {
		Context("success", func() {
			It("deletes and returns 204", func() {
				req, _ = http.NewRequest(http.MethodDelete, "/", nil)
				req.Header.Set(userIDHeader, userID.String())
				profileUsecase.NextError = nil
				status, _, err := handlers.DeleteProfile(ctx, req)
				Expect(err).NotTo(HaveOccurred())
				Expect(status).To(Equal(http.StatusNoContent))
				Expect(profileUsecase.DeleteProfileCalled).To(BeTrue())
			})
		})
		Context("failure", func() {
			When("user header missing", func() {
				It("returns 500 as it's set by the API Gateway", func() {
					req, _ = http.NewRequest(http.MethodDelete, "/", nil)
					status, _, err := handlers.DeleteProfile(ctx, req)
					Expect(err).To(HaveOccurred())
					Expect(status).To(Equal(http.StatusInternalServerError))
				})
			})

			When("profile is not found", func() {
				It("returns 404", func() {
					req, _ = http.NewRequest(http.MethodDelete, "/", nil)
					req.Header.Set(userIDHeader, userID.String())
					profileUsecase.NextError = domain.ErrProfileNotFound
					status, _, err := handlers.DeleteProfile(ctx, req)
					Expect(err).To(HaveOccurred())
					Expect(status).To(Equal(http.StatusNotFound))
				})
			})

			When("an internal error occurs", func() {
				It("returns 500", func() {
					req, _ = http.NewRequest(http.MethodDelete, "/", nil)
					req.Header.Set(userIDHeader, userID.String())
					profileUsecase.NextError = errors.New("internal error")
					status, _, err := handlers.DeleteProfile(ctx, req)
					Expect(err).To(HaveOccurred())
					Expect(status).To(Equal(http.StatusInternalServerError))
				})
			})
		})
	})

	Describe("ReadProfile", func() {
		Context("success", func() {
			It("returns 200 with the profile", func() {
				req, _ = http.NewRequest(http.MethodGet, "/", nil)
				req.Header.Set(userIDHeader, userID.String())

				profileUsecase.NextProfile = &domain.Profile{
					ProfileAccount: domain.ProfileAccount{ID: userID},
					FirstName:      "Test",
				}

				dtoProfileAdapter.NextProfileBytes = []byte(`{"id":"` + userID.String() + `","firstName":"Test"}`)

				status, body, err := handlers.ReadProfile(ctx, req)
				Expect(err).NotTo(HaveOccurred())
				Expect(status).To(Equal(http.StatusOK))
				Expect(body).To(MatchJSON(`{"id":"` + userID.String() + `","firstName":"Test"}`))
			})
		})

		Context("failure", func() {
			When("the user header missing", func() {
				It("returns 500 if ", func() {
					req, _ = http.NewRequest(http.MethodGet, "/", nil)
					status, _, err := handlers.ReadProfile(ctx, req)
					Expect(err).To(HaveOccurred())
					Expect(status).To(Equal(http.StatusInternalServerError))
				})
			})

			When("the profile is not found", func() {
				It("returns 404", func() {
					req, _ = http.NewRequest(http.MethodGet, "/", nil)
					req.Header.Set(userIDHeader, userID.String())
					profileUsecase.NextError = domain.ErrProfileNotFound

					status, _, err := handlers.ReadProfile(ctx, req)
					Expect(err).To(HaveOccurred())
					Expect(status).To(Equal(http.StatusNotFound))
				})
			})

			When("a serialization error occurs", func() {
				It("returns 500", func() {
					req, _ = http.NewRequest(http.MethodGet, "/", nil)
					req.Header.Set(userIDHeader, userID.String())
					profileUsecase.NextProfile = &domain.Profile{
						ProfileAccount: domain.ProfileAccount{ID: userID},
						FirstName:      "Test",
					}
					dtoProfileAdapter.NextSerialisationError = errors.New("marshal fail")

					status, _, err := handlers.ReadProfile(ctx, req)
					Expect(err).To(HaveOccurred())
					Expect(status).To(Equal(http.StatusInternalServerError))
				})
			})
		})
	})

	Describe("ReplacePassword", func() {
		Context("success", func() {
			When("the request is valid", func() {
				It("returns 200", func() {
					req = makeJSONReq([]byte(`{"password":"Test"}`))
					req.Header.Set(userIDHeader, userID.String())
					dtoProfileAdapter.NextPassword = "Test"
					profileUsecase.NextError = nil

					status, _, err := handlers.ReplacePassword(ctx, req)
					Expect(err).NotTo(HaveOccurred())
					Expect(status).To(Equal(http.StatusNoContent))
					Expect(profileUsecase.ReplacePasswordCalled).To(BeTrue())
				})
			})
		})

		Context("failure", func() {
			When("the request user ID header is missing", func() {
				It("returns 500", func() {
					req = makeJSONReq([]byte(`{"password":"Test"}`))
					status, _, err := handlers.ReplacePassword(ctx, req)
					Expect(err).To(HaveOccurred())
					Expect(status).To(Equal(http.StatusInternalServerError))
				})
			})

			When("the payload is invalid json", func() {

				It("returns 400", func() {
					req = makeJSONReq([]byte(`{bad`))
					req.Header.Set(userIDHeader, userID.String())
					dtoProfileAdapter.NextError = errors.New("validation failed")

					status, _, err := handlers.ReplacePassword(ctx, req)
					Expect(err).To(HaveOccurred())
					Expect(status).To(Equal(http.StatusBadRequest))
				})
			})

			When("user not found", func() {
				It("returns 404", func() {
					req = makeJSONReq([]byte(`{"password":"Test"}`))
					req.Header.Set(userIDHeader, userID.String())
					dtoProfileAdapter.NextPassword = "Test"
					profileUsecase.NextError = domain.ErrProfileNotFound

					status, _, err := handlers.ReplacePassword(ctx, req)
					Expect(err).To(HaveOccurred())
					Expect(status).To(Equal(http.StatusNotFound))
				})
			})

			When("an internal error occurs", func() {

				It("returns 500", func() {
					req = makeJSONReq([]byte(`{"password":"Test"}`))
					req.Header.Set(userIDHeader, userID.String())
					dtoProfileAdapter.NextPassword = "Test"
					profileUsecase.NextError = errors.New("test error")

					status, _, err := handlers.ReplacePassword(ctx, req)
					Expect(err).To(HaveOccurred())
					Expect(status).To(Equal(http.StatusInternalServerError))
				})
			})
		})
	})

	Describe("ReplaceHuggingFaceToken", func() {
		Context("success", func() {
			It("stores the token for the authenticated user", func() {
				req = makeJSONReq([]byte(`{"token":"hf-token"}`))
				req.Header.Set(userIDHeader, userID.String())
				dtoProfileAdapter.NextHuggingFaceToken = "hf-token"

				status, body, err := handlers.ReplaceHuggingFaceToken(ctx, req)

				Expect(err).NotTo(HaveOccurred())
				Expect(status).To(Equal(http.StatusNoContent))
				Expect(body).To(BeNil())
				Expect(dtoProfileAdapter.FromHuggingFaceTokenDTOCalled).To(BeTrue())
				Expect(profileUsecase.ReplaceHuggingFaceTokenCalled).To(BeTrue())
				Expect(profileUsecase.LastUserID).To(Equal(userID))
				Expect(profileUsecase.LastHuggingFaceToken).To(Equal("hf-token"))
			})
		})

		Context("failure", func() {
			It("returns 500 when the user header is missing", func() {
				req = makeJSONReq([]byte(`{"token":"hf-token"}`))

				status, _, err := handlers.ReplaceHuggingFaceToken(ctx, req)

				Expect(err).To(HaveOccurred())
				Expect(status).To(Equal(http.StatusInternalServerError))
				Expect(profileUsecase.ReplaceHuggingFaceTokenCalled).To(BeFalse())
			})

			It("returns 400 when the token payload is invalid", func() {
				req = makeJSONReq([]byte(`{bad`))
				req.Header.Set(userIDHeader, userID.String())
				dtoProfileAdapter.NextError = errors.New("validation failed")

				status, _, err := handlers.ReplaceHuggingFaceToken(ctx, req)

				Expect(err).To(HaveOccurred())
				Expect(status).To(Equal(http.StatusBadRequest))
				Expect(profileUsecase.ReplaceHuggingFaceTokenCalled).To(BeFalse())
			})
		})
	})

	Describe("VerifyPassword", func() {
		Context("success", func() {
			It("returns 200 with verified=true", func() {
				req = makeJSONReq([]byte(`{"email":"test@example.com","password":"ok"}`))
				req.Header.Set(userIDHeader, userID.String())
				dtoProfileAdapter.NextPassword = "test"

				profileUsecase.NextError = nil
				dtoProfileAdapter.NextPasswordBytes = []byte(`{"verified":true}`)

				status, body, err := handlers.VerifyPassword(ctx, req)
				Expect(err).NotTo(HaveOccurred())
				Expect(status).To(Equal(http.StatusOK))
				Expect(body).To(MatchJSON(`{"verified":true}`))
			})

			It("returns 200 with verified=false when usecase returns error", func() {
				req = makeJSONReq([]byte(`{"email":"test@example.com","password":"test"}`))
				req.Header.Set(userIDHeader, userID.String())
				dtoProfileAdapter.NextPassword = "bad"
				profileUsecase.NextError = errors.New("test error")
				dtoProfileAdapter.NextPasswordBytes = []byte(`{"verified":false}`)

				status, body, err := handlers.VerifyPassword(ctx, req)
				Expect(err).NotTo(HaveOccurred())
				Expect(status).To(Equal(http.StatusOK))
				Expect(body).To(MatchJSON(`{"verified":false}`))
			})

			It("returns 401 when the email is not verified", func() {
				req = makeJSONReq([]byte(`{"email":"test@example.com","password":"test"}`))
				req.Header.Set(userIDHeader, userID.String())
				dtoProfileAdapter.NextEmail = "test@example.com"
				dtoProfileAdapter.NextPassword = "test"
				profileUsecase.NextError = domain.ErrEmailNotVerified

				status, body, err := handlers.VerifyPassword(ctx, req)
				Expect(err).NotTo(HaveOccurred())
				Expect(status).To(Equal(http.StatusUnauthorized))
				Expect(body).To(MatchJSON(`{"message":"email not verified"}`))
			})
		})

		Context("failure", func() {
			It("returns 200 if payload invalid", func() {
				req = makeJSONReq([]byte(`{bad`))
				req.Header.Set(userIDHeader, userID.String())
				dtoProfileAdapter.NextError = errors.New("validation failed")
				dtoProfileAdapter.NextPasswordBytes = []byte(`{"verified":false}`)

				status, resBytes, err := handlers.VerifyPassword(ctx, req)
				Expect(err).To(Not(HaveOccurred()))
				Expect(status).To(Equal(http.StatusOK))

				Expect(resBytes).To(MatchJSON(`{"verified":false}`))
			})

			It("returns 500 if result serialization fails", func() {
				req = makeJSONReq([]byte(`{"email":"test@example.com","password":"any"}`))
				req.Header.Set(userIDHeader, userID.String())
				dtoProfileAdapter.NextPassword = "test"
				profileUsecase.NextError = nil
				dtoProfileAdapter.NextSerialisationError = errors.New("marshal fail")

				status, _, err := handlers.VerifyPassword(ctx, req)
				Expect(err).To(HaveOccurred())
				Expect(status).To(Equal(http.StatusInternalServerError))
			})
		})
	})

	Describe("VerifyEmail", func() {
		It("returns 204 when email verification succeeds", func() {
			req = makeJSONReq([]byte(`{"token":"token-1"}`))
			dtoProfileAdapter.NextEmailVerifyToken = "token-1"

			status, body, err := handlers.VerifyEmail(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(status).To(Equal(http.StatusNoContent))
			Expect(body).To(BeNil())
			Expect(dtoProfileAdapter.FromEmailVerificationDTOCalled).To(BeTrue())
			Expect(profileUsecase.VerifyEmailCalled).To(BeTrue())
			Expect(profileUsecase.LastToken).To(Equal("token-1"))
		})

		It("returns 400 when the payload is invalid", func() {
			req = makeJSONReq([]byte(`{"token":""}`))
			dtoProfileAdapter.NextError = fmt.Errorf("validation error. token required")

			status, _, err := handlers.VerifyEmail(ctx, req)
			Expect(err).To(HaveOccurred())
			Expect(status).To(Equal(http.StatusBadRequest))
			Expect(profileUsecase.VerifyEmailCalled).To(BeFalse())
		})

		It("returns 404 when verification token is not found", func() {
			req = makeJSONReq([]byte(`{"token":"token-1"}`))
			dtoProfileAdapter.NextEmailVerifyToken = "token-1"
			profileUsecase.NextError = domain.ErrProfileNotFound

			status, _, err := handlers.VerifyEmail(ctx, req)
			Expect(err).To(HaveOccurred())
			Expect(status).To(Equal(http.StatusNotFound))
			Expect(profileUsecase.VerifyEmailCalled).To(BeTrue())
		})
	})

	Describe("Logout", func() {
		const sessionIDHeader = "X-Session-ID"
		var sessionID string

		BeforeEach(func() {
			sessionID = uuid.New().String()
		})

		Context("success", func() {
			It("returns 204 on successful logout", func() {
				req = makeJSONReq(nil)
				req.Header.Set(userIDHeader, userID.String())
				req.Header.Set(sessionIDHeader, sessionID)

				profileUsecase.NextError = nil

				status, body, err := handlers.Logout(ctx, req)
				Expect(err).NotTo(HaveOccurred())
				Expect(status).To(Equal(http.StatusNoContent))
				Expect(body).To(BeNil())
				Expect(profileUsecase.LogoutCalled).To(BeTrue())
				Expect(profileUsecase.LastSessionID).To(Equal(sessionID))
			})
		})

		Context("failure", func() {
			It("returns 400 when X-User-ID header is missing", func() {
				req = makeJSONReq(nil)
				req.Header.Set(sessionIDHeader, sessionID)

				status, _, err := handlers.Logout(ctx, req)
				Expect(err).To(HaveOccurred())
				Expect(status).To(Equal(http.StatusBadRequest))
				Expect(profileUsecase.LogoutCalled).To(BeFalse())
			})

			It("returns 400 when X-Session-ID header is missing", func() {
				req = makeJSONReq(nil)
				req.Header.Set(userIDHeader, userID.String())

				status, _, err := handlers.Logout(ctx, req)
				Expect(err).To(HaveOccurred())
				Expect(status).To(Equal(http.StatusBadRequest))
				Expect(profileUsecase.LogoutCalled).To(BeFalse())
			})

			It("returns 500 when usecase returns error", func() {
				req = makeJSONReq(nil)
				req.Header.Set(userIDHeader, userID.String())
				req.Header.Set(sessionIDHeader, sessionID)

				profileUsecase.NextError = errors.New("delete session failed")

				status, _, err := handlers.Logout(ctx, req)
				Expect(err).To(HaveOccurred())
				Expect(status).To(Equal(http.StatusInternalServerError))
				Expect(profileUsecase.LogoutCalled).To(BeTrue())
			})
		})
	})

	Describe("CreateOAuthAuthorization", func() {
		It("returns authorization payload", func() {
			req = makeJSONReqWithProvider([]byte(`{"redirectUri":"https://app.example/callback","codeChallenge":"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890-._"}`), "google")
			dtoProfileAdapter.NextOAuthAuthorizeReq = &usecase.OAuthAuthorizeRequest{
				RedirectURI:   "https://app.example/callback",
				CodeChallenge: "challenge",
			}
			profileUsecase.NextOAuthAuth = &usecase.OAuthAuthorizeResult{
				AuthorizationURL: "https://provider.example/auth",
				State:            "state-1",
			}
			dtoProfileAdapter.NextOAuthAuthorizeRes = []byte(`{"authorizationUrl":"https://provider.example/auth","state":"state-1"}`)

			status, body, err := handlers.CreateOAuthAuthorization(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(status).To(Equal(http.StatusOK))
			Expect(body).To(MatchJSON(`{"authorizationUrl":"https://provider.example/auth","state":"state-1"}`))
			Expect(profileUsecase.CreateOAuthAuthorizationCalled).To(BeTrue())
		})

		It("returns 400 for unsupported providers", func() {
			req = makeJSONReqWithProvider([]byte(`{"redirectUri":"https://app.example/callback","codeChallenge":"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890-._"}`), "unknown")
			dtoProfileAdapter.NextOAuthAuthorizeReq = &usecase.OAuthAuthorizeRequest{
				RedirectURI:   "https://app.example/callback",
				CodeChallenge: "challenge",
			}
			profileUsecase.NextError = usecase.ErrUnsupportedOAuthProvider

			status, _, err := handlers.CreateOAuthAuthorization(ctx, req)
			Expect(err).To(HaveOccurred())
			Expect(status).To(Equal(http.StatusBadRequest))
			Expect(profileUsecase.CreateOAuthAuthorizationCalled).To(BeTrue())
		})
	})

	Describe("CreateOAuthSession", func() {
		It("returns local token payload", func() {
			req = makeJSONReqWithProvider([]byte(`{"code":"oauth-code","state":"state-1","redirectUri":"https://app.example/callback","codeVerifier":"verifier"}`), "discord")
			dtoProfileAdapter.NextOAuthSessionReq = &usecase.OAuthSessionRequest{
				Code:         "oauth-code",
				State:        "state-1",
				RedirectURI:  "https://app.example/callback",
				CodeVerifier: "verifier",
			}
			profileUsecase.NextOAuthSession = &usecase.OAuthSessionResult{
				Token:     "token-1",
				Provider:  "discord",
				IsNewUser: true,
			}
			dtoProfileAdapter.NextOAuthSessionRes = []byte(`{"verified":true,"token":"token-1","provider":"discord","isNewUser":true}`)

			status, body, err := handlers.CreateOAuthSession(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(status).To(Equal(http.StatusOK))
			Expect(body).To(MatchJSON(`{"verified":true,"token":"token-1","provider":"discord","isNewUser":true}`))
			Expect(profileUsecase.CreateOAuthSessionCalled).To(BeTrue())
		})

		It("returns 400 when oauth state is invalid", func() {
			req = makeJSONReqWithProvider([]byte(`{"code":"oauth-code","state":"state-1","redirectUri":"https://app.example/callback","codeVerifier":"verifier"}`), "discord")
			dtoProfileAdapter.NextOAuthSessionReq = &usecase.OAuthSessionRequest{
				Code:         "oauth-code",
				State:        "state-1",
				RedirectURI:  "https://app.example/callback",
				CodeVerifier: "verifier",
			}
			profileUsecase.NextError = usecase.ErrInvalidOAuthState

			status, _, err := handlers.CreateOAuthSession(ctx, req)
			Expect(err).To(HaveOccurred())
			Expect(status).To(Equal(http.StatusUnauthorized))
			Expect(profileUsecase.CreateOAuthSessionCalled).To(BeTrue())
		})

		It("returns 400 for unsupported oauth providers", func() {
			req = makeJSONReqWithProvider([]byte(`{"code":"oauth-code","state":"state-1","redirectUri":"https://app.example/callback","codeVerifier":"verifier"}`), "unknown")
			dtoProfileAdapter.NextOAuthSessionReq = &usecase.OAuthSessionRequest{
				Code:         "oauth-code",
				State:        "state-1",
				RedirectURI:  "https://app.example/callback",
				CodeVerifier: "verifier",
			}
			profileUsecase.NextError = usecase.ErrUnsupportedOAuthProvider

			status, _, err := handlers.CreateOAuthSession(ctx, req)
			Expect(err).To(HaveOccurred())
			Expect(status).To(Equal(http.StatusBadRequest))
			Expect(profileUsecase.CreateOAuthSessionCalled).To(BeTrue())
		})
	})
})
