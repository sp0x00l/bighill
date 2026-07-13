package router_test

import (
	"api/pkg/router"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"lib/shared_lib/authz"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	testUserHeader        = "X-User-ID"
	testSessionHeader     = "X-Session-ID"
	testOrgHeader         = "X-Org-ID"
	testRolesHeader       = "X-Roles"
	testPermissionsHeader = "X-Permissions"
	testCORSAllowOrigin   = "*"
	testCORSAllowMethods  = "GET,POST,PUT,DELETE,PATCH,OPTIONS"
	testCORSAllowHeaders  = "Content-Type,Authorization,X-Request-ID,X-Amz-Date,X-Api-Key,X-Amz-Security-Token"
)

func TestRouteHandler(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "API Gateway route handler test suite")
}

type observedRequest struct {
	method  string
	url     string
	body    string
	headers http.Header
}

type routerHTTPClientMock struct {
	requests  []observedRequest
	responses []*http.Response
	errors    []error
}

func (m *routerHTTPClientMock) Do(req *http.Request) (*http.Response, error) {
	body := ""
	if req.Body != nil {
		bodyBytes, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		body = string(bodyBytes)
	}

	m.requests = append(m.requests, observedRequest{
		method:  req.Method,
		url:     req.URL.String(),
		body:    body,
		headers: req.Header.Clone(),
	})

	idx := len(m.requests) - 1
	if idx < len(m.errors) && m.errors[idx] != nil {
		return nil, m.errors[idx]
	}
	if idx < len(m.responses) && m.responses[idx] != nil {
		return m.responses[idx], nil
	}
	return responseWithBody(http.StatusOK, "ok"), nil
}

func responseWithBody(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Header:     http.Header{"X-Downstream": []string{"router-test"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func testRouter(client *routerHTTPClientMock) func(context.Context, events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	return router.NewRouter(client, http.NewRequest, router.Config{
		DataRegistryServiceRoute:  "http://data-registry.service",
		IngestionServiceRoute:     "http://ingestion.service",
		InferenceServiceRoute:     "http://inference.service",
		ModelRegistryServiceRoute: "http://model-registry.service",
		TenantServiceRoute:        "http://tenant.service",
		TrainingServiceRoute:      "http://training.service",
		SocketServiceRoute:        "http://socket.service",
	})
}

func authorizerContext(permissions ...string) events.APIGatewayProxyRequestContext {
	return events.APIGatewayProxyRequestContext{
		Authorizer: map[string]any{
			"userId":      "user-123",
			"sid":         "session-456",
			"orgId":       "org-789",
			"roles":       authz.EncodeStringSlice([]string{authz.RoleMLResearcher}),
			"permissions": authz.EncodeStringSlice(permissions),
		},
	}
}

var _ = Describe("NewRouter", func() {
	var (
		ctx    context.Context
		client *routerHTTPClientMock
	)

	BeforeEach(func() {
		ctx = context.Background()
		client = &routerHTTPClientMock{}
	})

	It("routes profile requests to the tenant service and preserves request details", func() {
		client.responses = []*http.Response{responseWithBody(http.StatusCreated, `{"id":"profile-1"}`)}
		handler := testRouter(client)

		resp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod: http.MethodPost,
			Path:       "/public/v1/profiles",
			Body:       `{"email":"user@example.com"}`,
			Headers: map[string]string{
				"Content-Type":   "application/json",
				"Content-Length": "999",
				"Origin":         "https://app.example",
			},
			QueryStringParameters: map[string]string{"invite": "alpha"},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusCreated))
		Expect(resp.Body).To(Equal(`{"id":"profile-1"}`))
		Expect(resp.Headers).To(HaveKeyWithValue("X-Downstream", "router-test"))
		Expect(resp.Headers).To(HaveKeyWithValue("Access-Control-Allow-Origin", testCORSAllowOrigin))
		Expect(resp.Headers).To(HaveKeyWithValue("Access-Control-Allow-Methods", testCORSAllowMethods))
		Expect(resp.Headers).To(HaveKeyWithValue("Access-Control-Allow-Headers", testCORSAllowHeaders))

		Expect(client.requests).To(HaveLen(1))
		req := client.requests[0]
		Expect(req.method).To(Equal(http.MethodPost))
		Expect(req.url).To(Equal("http://tenant.service/public/v1/profiles?invite=alpha"))
		Expect(req.body).To(Equal(`{"email":"user@example.com"}`))
		Expect(req.headers.Get("Content-Type")).To(Equal("application/json"))
		Expect(req.headers.Values("Content-Length")).To(BeEmpty())
	})

	It("routes data registry and ingestion requests to their service adapters", func() {
		client.responses = []*http.Response{
			responseWithBody(http.StatusOK, `{"datasets":[]}`),
			responseWithBody(http.StatusAccepted, `{"upload":"accepted"}`),
			responseWithBody(http.StatusCreated, `{"upload_id":"session-1"}`),
			responseWithBody(http.StatusCreated, `{"upload_id":"model-session-1"}`),
			responseWithBody(http.StatusCreated, `{"resource_id":"model-1"}`),
			responseWithBody(http.StatusOK, `{"resources":[]}`),
			responseWithBody(http.StatusOK, `{"id":"model-1"}`),
			responseWithBody(http.StatusAccepted, `{"training_run_id":"run-1"}`),
			responseWithBody(http.StatusAccepted, `{"training_run_id":"dpo-run-1"}`),
		}
		handler := testRouter(client)

		registryResp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod:     http.MethodGet,
			Path:           "/v1/private/data/registry",
			RequestContext: authorizerContext(authz.PermissionDataRead),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(registryResp.StatusCode).To(Equal(http.StatusOK))

		ingestionResp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod:      http.MethodPost,
			Path:            "/v1/private/data/store/2ef65f05-dc98-4be8-b952-ff73c84e10f1",
			Body:            base64.StdEncoding.EncodeToString([]byte("file-bytes")),
			IsBase64Encoded: true,
			Headers:         map[string]string{"Content-Type": "multipart/form-data; boundary=test"},
			RequestContext:  authorizerContext(authz.PermissionDataWrite),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(ingestionResp.StatusCode).To(Equal(http.StatusAccepted))

		sessionResp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod:     http.MethodPost,
			Path:           "/v1/private/data/uploads/2ef65f05-dc98-4be8-b952-ff73c84e10f1",
			Body:           `{"file_name":"dataset.csv"}`,
			Headers:        map[string]string{"Content-Type": "application/json"},
			RequestContext: authorizerContext(authz.PermissionDataWrite),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(sessionResp.StatusCode).To(Equal(http.StatusCreated))

		modelSessionResp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod:     http.MethodPost,
			Path:           "/v1/private/models/uploads",
			Body:           `{"file_name":"adapter.safetensors"}`,
			Headers:        map[string]string{"Content-Type": "application/json"},
			RequestContext: authorizerContext(authz.PermissionModelWrite),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(modelSessionResp.StatusCode).To(Equal(http.StatusCreated))

		hfOnboardResp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod:     http.MethodPost,
			Path:           "/v1/private/models/onboard/huggingface",
			Body:           `{"repo_id":"bigscience/bloom-560m"}`,
			Headers:        map[string]string{"Content-Type": "application/json"},
			RequestContext: authorizerContext(authz.PermissionModelWrite),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(hfOnboardResp.StatusCode).To(Equal(http.StatusCreated))

		modelListResp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod:            http.MethodGet,
			Path:                  "/v1/private/models",
			QueryStringParameters: map[string]string{"source": "HUGGING_FACE", "limit": "10"},
			RequestContext:        authorizerContext(authz.PermissionModelRead),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(modelListResp.StatusCode).To(Equal(http.StatusOK))

		modelReadResp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod:     http.MethodGet,
			Path:           "/v1/private/models/2ef65f05-dc98-4be8-b952-ff73c84e10f1",
			RequestContext: authorizerContext(authz.PermissionModelRead),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(modelReadResp.StatusCode).To(Equal(http.StatusOK))

		trainingResp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod:     http.MethodPost,
			Path:           "/v1/private/training-runs",
			Body:           `{"dataset_id":"dataset-1","source_model_id":"model-1"}`,
			Headers:        map[string]string{"Content-Type": "application/json", "X-Request-ID": "request-1"},
			RequestContext: authorizerContext(authz.PermissionTrainingStart),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(trainingResp.StatusCode).To(Equal(http.StatusAccepted))

		dpoTrainingResp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod:     http.MethodPost,
			Path:           "/v1/private/training-runs/dpo",
			Body:           `{"preference_dataset_id":"2ef65f05-dc98-4be8-b952-ff73c84e10f1"}`,
			Headers:        map[string]string{"Content-Type": "application/json", "X-Request-ID": "request-2"},
			RequestContext: authorizerContext(authz.PermissionTrainingStart),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(dpoTrainingResp.StatusCode).To(Equal(http.StatusAccepted))

		trainingReadResp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod:     http.MethodGet,
			Path:           "/v1/private/training-runs/2ef65f05-dc98-4be8-b952-ff73c84e10f1",
			RequestContext: authorizerContext(authz.PermissionTrainingRead),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(trainingReadResp.StatusCode).To(Equal(http.StatusOK))

		Expect(client.requests).To(HaveLen(10))
		Expect(client.requests[0].url).To(Equal("http://data-registry.service/v1/data/registry"))
		Expect(client.requests[1].url).To(Equal("http://ingestion.service/v1/data/store/2ef65f05-dc98-4be8-b952-ff73c84e10f1"))
		Expect(client.requests[1].body).To(Equal("file-bytes"))
		Expect(client.requests[2].url).To(Equal("http://ingestion.service/v1/data/uploads/2ef65f05-dc98-4be8-b952-ff73c84e10f1"))
		Expect(client.requests[2].body).To(Equal(`{"file_name":"dataset.csv"}`))
		Expect(client.requests[3].url).To(Equal("http://ingestion.service/v1/models/uploads"))
		Expect(client.requests[3].body).To(Equal(`{"file_name":"adapter.safetensors"}`))
		Expect(client.requests[4].url).To(Equal("http://ingestion.service/v1/models/onboard/huggingface"))
		Expect(client.requests[4].body).To(Equal(`{"repo_id":"bigscience/bloom-560m"}`))
		Expect(client.requests[5].url).To(Equal("http://model-registry.service/v1/models?limit=10&source=HUGGING_FACE"))
		Expect(client.requests[6].url).To(Equal("http://model-registry.service/v1/models/2ef65f05-dc98-4be8-b952-ff73c84e10f1"))
		Expect(client.requests[7].url).To(Equal("http://training.service/v1/training-runs"))
		Expect(client.requests[7].body).To(Equal(`{"dataset_id":"dataset-1","source_model_id":"model-1"}`))
		Expect(client.requests[7].headers.Get("X-Request-ID")).To(Equal("request-1"))
		Expect(client.requests[7].headers.Get(testUserHeader)).To(Equal("user-123"))
		Expect(client.requests[7].headers.Get(testOrgHeader)).To(Equal("org-789"))
		Expect(client.requests[7].headers.Get(testPermissionsHeader)).To(Equal(authz.EncodeStringSlice([]string{authz.PermissionTrainingStart})))
		Expect(client.requests[8].url).To(Equal("http://training.service/v1/training-runs/dpo"))
		Expect(client.requests[8].body).To(Equal(`{"preference_dataset_id":"2ef65f05-dc98-4be8-b952-ff73c84e10f1"}`))
		Expect(client.requests[8].headers.Get("X-Request-ID")).To(Equal("request-2"))
		Expect(client.requests[8].headers.Get(testUserHeader)).To(Equal("user-123"))
		Expect(client.requests[8].headers.Get(testOrgHeader)).To(Equal("org-789"))
		Expect(client.requests[9].url).To(Equal("http://training.service/v1/training-runs/2ef65f05-dc98-4be8-b952-ff73c84e10f1"))
		Expect(client.requests[9].headers.Get(testUserHeader)).To(Equal("user-123"))
		Expect(client.requests[9].headers.Get(testOrgHeader)).To(Equal("org-789"))
	})

	It("forwards authenticated user and session context to profile logout", func() {
		handler := testRouter(client)

		resp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod:     http.MethodPost,
			Path:           "/v1/private/profiles/logout",
			RequestContext: authorizerContext(),
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
		Expect(client.requests).To(HaveLen(1))
		Expect(client.requests[0].headers.Get(testUserHeader)).To(Equal("user-123"))
		Expect(client.requests[0].headers.Get(testSessionHeader)).To(Equal("session-456"))
	})

	It("forwards trusted auth context to private service routes", func() {
		handler := testRouter(client)

		resp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod:     http.MethodGet,
			Path:           "/v1/private/profiles",
			RequestContext: authorizerContext(),
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
		Expect(client.requests).To(HaveLen(1))
		Expect(client.requests[0].headers.Get(testUserHeader)).To(Equal("user-123"))
		Expect(client.requests[0].headers.Get(testSessionHeader)).To(Equal("session-456"))
		Expect(client.requests[0].headers.Get(testRolesHeader)).To(Equal(authz.EncodeStringSlice([]string{authz.RoleMLResearcher})))
	})

	It("routes socket ticket minting through the socket service", func() {
		handler := testRouter(client)

		resp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod:     http.MethodPost,
			Path:           "/v1/private/socket-token",
			RequestContext: authorizerContext(authz.PermissionInferenceEndpointsRead),
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
		Expect(client.requests).To(HaveLen(1))
		Expect(client.requests[0].url).To(Equal("http://socket.service/v1/socket-token"))
		Expect(client.requests[0].headers.Get(testUserHeader)).To(Equal("user-123"))
		Expect(client.requests[0].headers.Get(testOrgHeader)).To(Equal("org-789"))
		Expect(client.requests[0].headers.Get(testSessionHeader)).To(Equal("session-456"))
		Expect(client.requests[0].headers.Get(testPermissionsHeader)).To(Equal(authz.EncodeStringSlice([]string{authz.PermissionInferenceEndpointsRead})))
	})

	It("routes inference facade requests and enforces consumer-safe permissions", func() {
		client.responses = []*http.Response{
			responseWithBody(http.StatusOK, `{"endpoints":[]}`),
			responseWithBody(http.StatusAccepted, `{"request_id":"req-1"}`),
			responseWithBody(http.StatusOK, `{"feedback_id":"fb-1"}`),
			responseWithBody(http.StatusCreated, `{"preference_dataset_id":"pref-1"}`),
			responseWithBody(http.StatusOK, `{"resources":[]}`),
			responseWithBody(http.StatusOK, `{"preference_dataset_id":"pref-1"}`),
		}
		handler := testRouter(client)

		listResp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod:     http.MethodGet,
			Path:           "/v1/private/inference/endpoints",
			RequestContext: authorizerContext(authz.PermissionInferenceEndpointsRead),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(listResp.StatusCode).To(Equal(http.StatusOK))

		invokeResp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod: http.MethodPost,
			Path:       "/v1/private/inference/endpoints/2ef65f05-dc98-4be8-b952-ff73c84e10f1/generations",
			Body:       `{"query_text":"hello","top_k":5}`,
			Headers: map[string]string{
				"Content-Type": "application/json",
				"X-Request-ID": "request-2",
			},
			RequestContext: authorizerContext(authz.PermissionInferenceInvoke),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(invokeResp.StatusCode).To(Equal(http.StatusAccepted))

		feedbackResp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod: http.MethodPost,
			Path:       "/v1/private/inference/feedback",
			Body:       `{"request_id":"2ef65f05-dc98-4be8-b952-ff73c84e10f1","accepted":true}`,
			Headers: map[string]string{
				"Content-Type": "application/json",
				"X-Request-ID": "request-3",
			},
			RequestContext: authorizerContext(authz.PermissionInferenceFeedback),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(feedbackResp.StatusCode).To(Equal(http.StatusOK))

		preferenceBuildResp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod: http.MethodPost,
			Path:       "/v1/private/inference/endpoints/2ef65f05-dc98-4be8-b952-ff73c84e10f1/preference-datasets",
			Body:       `{"min_examples":1}`,
			Headers: map[string]string{
				"Content-Type": "application/json",
				"X-Request-ID": "request-4",
			},
			RequestContext: authorizerContext(authz.PermissionModelWrite),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(preferenceBuildResp.StatusCode).To(Equal(http.StatusCreated))

		preferenceListResp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod:            http.MethodGet,
			Path:                  "/v1/private/inference/preference-datasets",
			QueryStringParameters: map[string]string{"model_id": "2ef65f05-dc98-4be8-b952-ff73c84e10f1"},
			RequestContext:        authorizerContext(authz.PermissionInferenceEndpointsRead),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(preferenceListResp.StatusCode).To(Equal(http.StatusOK))

		preferenceReadResp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod:     http.MethodGet,
			Path:           "/v1/private/inference/preference-datasets/2ef65f05-dc98-4be8-b952-ff73c84e10f1",
			RequestContext: authorizerContext(authz.PermissionInferenceEndpointsRead),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(preferenceReadResp.StatusCode).To(Equal(http.StatusOK))

		deniedResp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod:     http.MethodPost,
			Path:           "/v1/private/training-runs",
			Body:           `{"dataset_id":"dataset-1","source_model_id":"model-1"}`,
			RequestContext: authorizerContext(authz.PermissionInferenceInvoke),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(deniedResp.StatusCode).To(Equal(http.StatusForbidden))

		Expect(client.requests).To(HaveLen(6))
		Expect(client.requests[0].url).To(Equal("http://inference.service/v1/inference/endpoints"))
		Expect(client.requests[0].headers.Get(testUserHeader)).To(Equal("user-123"))
		Expect(client.requests[0].headers.Get(testOrgHeader)).To(Equal("org-789"))
		Expect(client.requests[1].url).To(Equal("http://inference.service/v1/inference/endpoints/2ef65f05-dc98-4be8-b952-ff73c84e10f1/generations"))
		Expect(client.requests[1].body).To(Equal(`{"query_text":"hello","top_k":5}`))
		Expect(client.requests[1].headers.Get("X-Request-ID")).To(Equal("request-2"))
		Expect(client.requests[2].url).To(Equal("http://inference.service/v1/inference/feedback"))
		Expect(client.requests[2].body).To(Equal(`{"request_id":"2ef65f05-dc98-4be8-b952-ff73c84e10f1","accepted":true}`))
		Expect(client.requests[2].headers.Get("X-Request-ID")).To(Equal("request-3"))
		Expect(client.requests[3].url).To(Equal("http://inference.service/v1/inference/endpoints/2ef65f05-dc98-4be8-b952-ff73c84e10f1/preference-datasets"))
		Expect(client.requests[3].body).To(Equal(`{"min_examples":1}`))
		Expect(client.requests[3].headers.Get("X-Request-ID")).To(Equal("request-4"))
		Expect(client.requests[4].url).To(Equal("http://inference.service/v1/inference/preference-datasets?model_id=2ef65f05-dc98-4be8-b952-ff73c84e10f1"))
		Expect(client.requests[5].url).To(Equal("http://inference.service/v1/inference/preference-datasets/2ef65f05-dc98-4be8-b952-ff73c84e10f1"))
	})

	It("requires model write permission to build preference datasets", func() {
		handler := testRouter(client)

		resp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod:     http.MethodPost,
			Path:           "/v1/private/inference/endpoints/2ef65f05-dc98-4be8-b952-ff73c84e10f1/preference-datasets",
			Body:           `{"min_examples":1}`,
			RequestContext: authorizerContext(authz.PermissionInferenceFeedback),
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusForbidden))
		Expect(client.requests).To(BeEmpty())
	})

	It("denies unmatched private routes by default", func() {
		handler := testRouter(client)

		resp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod:     http.MethodGet,
			Path:           "/v1/private/inference/admin",
			RequestContext: authorizerContext(authz.PermissionDataRead, authz.PermissionModelRead, authz.PermissionTrainingRead),
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusForbidden))
		Expect(resp.Body).To(Equal("forbidden"))
		Expect(client.requests).To(BeEmpty())
	})

	DescribeTable("rejects spoofed gateway identity headers",
		func(headers map[string]string) {
			handler := testRouter(client)

			resp, err := handler(ctx, events.APIGatewayProxyRequest{
				HTTPMethod: http.MethodGet,
				Path:       "/v1/private/profiles",
				Headers:    headers,
			})

			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusBadRequest))
			Expect(resp.Body).To(Equal("Invalid request. Please check your headers"))
			Expect(client.requests).To(BeEmpty())
		},
		Entry("user id header", map[string]string{"X-User-ID": "spoofed-user"}),
		Entry("session id header", map[string]string{"x-session-id": "spoofed-session"}),
		Entry("org id header", map[string]string{"X-Org-ID": "spoofed-org"}),
		Entry("roles header", map[string]string{"X-Roles": `["org_admin"]`}),
		Entry("permissions header", map[string]string{"X-Permissions": `["model:write"]`}),
	)

	It("returns CORS headers for preflight requests", func() {
		handler := testRouter(client)

		resp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod: http.MethodOptions,
			Path:       "/v1/private/data/registry",
			Headers:    map[string]string{"Origin": "https://app.example"},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusNoContent))
		Expect(resp.Headers).To(HaveKeyWithValue("Access-Control-Allow-Origin", testCORSAllowOrigin))
		Expect(resp.Headers).To(HaveKeyWithValue("Access-Control-Allow-Methods", testCORSAllowMethods))
		Expect(resp.Headers).To(HaveKeyWithValue("Access-Control-Allow-Headers", testCORSAllowHeaders))
		Expect(client.requests).To(BeEmpty())
	})

	It("returns the downstream error body when routing fails after retries", func() {
		client.errors = []error{errors.New("dial failed")}
		handler := testRouter(client)

		resp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod:     http.MethodGet,
			Path:           "/v1/private/data/registry",
			RequestContext: authorizerContext(authz.PermissionDataRead),
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusBadGateway))
		Expect(resp.Body).To(Equal("no response. Bighill gateway routing error"))
		Expect(client.requests).To(HaveLen(1))
	})
})
