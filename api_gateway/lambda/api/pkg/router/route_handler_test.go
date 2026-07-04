package router

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

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
	return NewRouter(client, http.NewRequest, Config{
		DataRegistryServiceRoute: "http://data-registry.service",
		IngestionServiceRoute:    "http://ingestion.service",
		ProfileServiceRoute:      "http://profile.service",
	})
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

	It("routes profile requests to the profile service and preserves request details", func() {
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
		Expect(resp.Headers).To(HaveKeyWithValue("Access-Control-Allow-Origin", corsAllowOrigin))
		Expect(resp.Headers).To(HaveKeyWithValue("Access-Control-Allow-Methods", corsAllowMethods))
		Expect(resp.Headers).To(HaveKeyWithValue("Access-Control-Allow-Headers", corsAllowHeaders))

		Expect(client.requests).To(HaveLen(1))
		req := client.requests[0]
		Expect(req.method).To(Equal(http.MethodPost))
		Expect(req.url).To(Equal("http://profile.service/public/v1/profiles?invite=alpha"))
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
		}
		handler := testRouter(client)

		registryResp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod: http.MethodGet,
			Path:       "/v1/data/registry",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(registryResp.StatusCode).To(Equal(http.StatusOK))

		ingestionResp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod:      http.MethodPost,
			Path:            "/v1/data/store/2ef65f05-dc98-4be8-b952-ff73c84e10f1",
			Body:            base64.StdEncoding.EncodeToString([]byte("file-bytes")),
			IsBase64Encoded: true,
			Headers:         map[string]string{"Content-Type": "multipart/form-data; boundary=test"},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(ingestionResp.StatusCode).To(Equal(http.StatusAccepted))

		sessionResp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod: http.MethodPost,
			Path:       "/v1/data/uploads/2ef65f05-dc98-4be8-b952-ff73c84e10f1",
			Body:       `{"file_name":"dataset.csv"}`,
			Headers:    map[string]string{"Content-Type": "application/json"},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(sessionResp.StatusCode).To(Equal(http.StatusCreated))

		modelSessionResp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod: http.MethodPost,
			Path:       "/v1/models/uploads",
			Body:       `{"file_name":"adapter.safetensors"}`,
			Headers:    map[string]string{"Content-Type": "application/json"},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(modelSessionResp.StatusCode).To(Equal(http.StatusCreated))

		Expect(client.requests).To(HaveLen(4))
		Expect(client.requests[0].url).To(Equal("http://data-registry.service/v1/data/registry"))
		Expect(client.requests[1].url).To(Equal("http://ingestion.service/v1/data/store/2ef65f05-dc98-4be8-b952-ff73c84e10f1"))
		Expect(client.requests[1].body).To(Equal("file-bytes"))
		Expect(client.requests[2].url).To(Equal("http://ingestion.service/v1/data/uploads/2ef65f05-dc98-4be8-b952-ff73c84e10f1"))
		Expect(client.requests[2].body).To(Equal(`{"file_name":"dataset.csv"}`))
		Expect(client.requests[3].url).To(Equal("http://ingestion.service/v1/models/uploads"))
		Expect(client.requests[3].body).To(Equal(`{"file_name":"adapter.safetensors"}`))
	})

	It("forwards authenticated user and session context to profile logout", func() {
		handler := testRouter(client)

		resp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod: http.MethodPost,
			Path:       "/private/v1/profiles/logout",
			RequestContext: events.APIGatewayProxyRequestContext{
				Authorizer: map[string]any{
					"userId": "user-123",
					"sid":    "session-456",
				},
			},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
		Expect(client.requests).To(HaveLen(1))
		Expect(client.requests[0].headers.Get(userHeader)).To(Equal("user-123"))
		Expect(client.requests[0].headers.Get(sessionHeader)).To(Equal("session-456"))
	})

	It("does not forward session context outside the logout route", func() {
		handler := testRouter(client)

		resp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod: http.MethodGet,
			Path:       "/private/v1/profiles",
			RequestContext: events.APIGatewayProxyRequestContext{
				Authorizer: map[string]any{
					"userId": "user-123",
					"sid":    "session-456",
				},
			},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
		Expect(client.requests).To(HaveLen(1))
		Expect(client.requests[0].headers.Get(userHeader)).To(Equal("user-123"))
		Expect(client.requests[0].headers.Values(sessionHeader)).To(BeEmpty())
	})

	DescribeTable("rejects spoofed gateway identity headers",
		func(headers map[string]string) {
			handler := testRouter(client)

			resp, err := handler(ctx, events.APIGatewayProxyRequest{
				HTTPMethod: http.MethodGet,
				Path:       "/private/v1/profiles",
				Headers:    headers,
			})

			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusBadRequest))
			Expect(resp.Body).To(Equal("Invalid request. Please check your headers"))
			Expect(client.requests).To(BeEmpty())
		},
		Entry("user id header", map[string]string{"X-User-ID": "spoofed-user"}),
		Entry("session id header", map[string]string{"x-session-id": "spoofed-session"}),
	)

	It("returns CORS headers for preflight requests", func() {
		handler := testRouter(client)

		resp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod: http.MethodOptions,
			Path:       "/v1/data/registry",
			Headers:    map[string]string{"Origin": "https://app.example"},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusNoContent))
		Expect(resp.Headers).To(HaveKeyWithValue("Access-Control-Allow-Origin", corsAllowOrigin))
		Expect(resp.Headers).To(HaveKeyWithValue("Access-Control-Allow-Methods", corsAllowMethods))
		Expect(resp.Headers).To(HaveKeyWithValue("Access-Control-Allow-Headers", corsAllowHeaders))
		Expect(client.requests).To(BeEmpty())
	})

	It("returns the downstream error body when routing fails after retries", func() {
		client.errors = []error{errors.New("dial failed")}
		handler := testRouter(client)

		resp, err := handler(ctx, events.APIGatewayProxyRequest{
			HTTPMethod: http.MethodGet,
			Path:       "/v1/data/registry",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusBadGateway))
		Expect(resp.Body).To(Equal("no response. Bighill gateway routing error"))
		Expect(client.requests).To(HaveLen(1))
	})
})
