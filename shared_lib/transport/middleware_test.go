package transport_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"shared_lib/transport"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type headerError struct {
	retryAfter time.Duration
}

func (e *headerError) Error() string {
	return "header-error"
}

func (e *headerError) HTTPHeaders() map[string]string {
	return map[string]string{"Retry-After": "2"}
}

var _ = Describe("HTTP middleware", func() {
	var (
		req            *http.Request
		err            error
		recorder       *httptest.ResponseRecorder
		nextBody       []byte
		nextErrorMsg   string
		nextStatusCode int
	)
	BeforeEach(func() {
		req, err = http.NewRequest("GET", "/test", nil)
		Expect(err).ToNot(HaveOccurred())

		recorder = httptest.NewRecorder()
	})

	Context("Postive tests", func() {
		When("the next handler returns a successful response", func() {
			It("should write the body and set the correct status code", func() {
				nextBody = []byte("test-body")
				nextStatusCode = http.StatusOK
				mockHandler := func(ctx context.Context, r *http.Request) (int, []byte, error) {
					return nextStatusCode, nextBody, nil
				}

				handler := transport.Middleware(tracer, "test-span-name", mockHandler)
				handler.ServeHTTP(recorder, req)

				Expect(recorder.Body).ToNot(BeNil())

				Expect(recorder.Body.String()).To(Equal(string(nextBody)))
				Expect(recorder.Code).To(Equal(nextStatusCode))
				Expect(recorder.Header().Get("Content-Type")).To(Equal("application/json"))
			})
		})
	})

	Context("Negative tests", func() {
		When("the handler returns an error", func() {
			It("should write the error and an error status code", func() {
				nextErrorMsg = "test-error"
				nextStatusCode = http.StatusInternalServerError
				mockHandler := func(ctx context.Context, r *http.Request) (int, []byte, error) {
					return nextStatusCode, nextBody, errors.New(nextErrorMsg)
				}

				handler := transport.Middleware(tracer, "test-span-name", mockHandler)
				handler.ServeHTTP(recorder, req)

				Expect(recorder.Code).To(Equal(nextStatusCode))
				errJson := transport.ErrorMessage{}
				err := json.Unmarshal(recorder.Body.Bytes(), &errJson)
				Expect(err).ToNot(HaveOccurred())
				Expect(errJson.Message).To(ContainSubstring(nextErrorMsg))
			})

			It("should write custom headers from the error", func() {
				mockHandler := func(ctx context.Context, r *http.Request) (int, []byte, error) {
					return http.StatusServiceUnavailable, nil, &headerError{retryAfter: 1500 * time.Millisecond}
				}

				handler := transport.Middleware(tracer, "test-span-name", mockHandler)
				handler.ServeHTTP(recorder, req)

				Expect(recorder.Code).To(Equal(http.StatusServiceUnavailable))
				Expect(recorder.Header().Get("Retry-After")).To(Equal("2"))
			})
		})
	})
})
