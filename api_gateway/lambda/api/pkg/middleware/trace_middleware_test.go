package middleware

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestMiddleware(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "API Gateway middleware unit test suite")
}

var _ = Describe("TraceMiddleware", func() {
	BeforeEach(func() {
		Tracer = noop.NewTracerProvider().Tracer("test")
	})

	It("calls the wrapped handler and returns its response", func() {
		called := false
		handler := TraceMiddleware(func(context.Context, events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
			called = true
			return events.APIGatewayProxyResponse{StatusCode: 202}, nil
		})

		res, err := handler(context.Background(), events.APIGatewayProxyRequest{HTTPMethod: "GET", Path: "/health"})

		Expect(err).NotTo(HaveOccurred())
		Expect(called).To(BeTrue())
		Expect(res.StatusCode).To(Equal(202))
	})

	It("propagates wrapped handler errors", func() {
		expectedErr := errors.New("downstream failed")
		handler := TraceMiddleware(func(context.Context, events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
			return events.APIGatewayProxyResponse{StatusCode: 503}, expectedErr
		})

		res, err := handler(context.Background(), events.APIGatewayProxyRequest{HTTPMethod: "POST", Path: "/v1"})

		Expect(err).To(MatchError(expectedErr))
		Expect(res.StatusCode).To(Equal(503))
	})
})
