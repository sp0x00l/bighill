package modelserving

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"inference_service/pkg/domain"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestModelServing(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Inference model serving unit test suite")
}

var _ = Describe("HTTPLoadTrigger", func() {
	It("posts a load request for the model", func() {
		modelID := uuid.New()
		var method string
		var path string
		client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			method = r.Method
			path = r.URL.Path
			return response(http.StatusAccepted), nil
		})}
		trigger, err := NewHTTPLoadTrigger(LoadTriggerConfig{
			Endpoint:   "http://model-serving",
			HTTPClient: client,
		})
		Expect(err).NotTo(HaveOccurred())

		err = trigger.TriggerModelLoad(context.Background(), uuid.New(), modelID)

		Expect(err).NotTo(HaveOccurred())
		Expect(method).To(Equal(http.MethodPost))
		Expect(path).To(Equal("/v1/private/served-models/" + modelID.String() + "/load"))
	})

	It("rejects missing endpoint configuration", func() {
		_, err := NewHTTPLoadTrigger(LoadTriggerConfig{RequestTimeout: time.Second})

		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
	})

	It("rejects missing request timeout when constructing the default client", func() {
		_, err := NewHTTPLoadTrigger(LoadTriggerConfig{Endpoint: "http://model-serving"})

		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
	})

	It("returns an error when the load endpoint rejects the request", func() {
		client := &http.Client{Transport: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
			return response(http.StatusBadGateway), nil
		})}
		trigger, err := NewHTTPLoadTrigger(LoadTriggerConfig{
			Endpoint:   "http://model-serving",
			HTTPClient: client,
		})
		Expect(err).NotTo(HaveOccurred())

		err = trigger.TriggerModelLoad(context.Background(), uuid.New(), uuid.New())

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("status 502"))
	})
})

func response(status int) *http.Response {
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(bytes.NewReader(nil)),
		Header:     http.Header{},
	}
}
