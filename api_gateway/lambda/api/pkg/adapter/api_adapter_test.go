package adapter

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestAdapter(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "API Gateway adapter unit test suite")
}

var _ = Describe("HandlerFunc and Middleware", func() {
	It("allows middleware to wrap handler execution", func() {
		called := false
		handler := HandlerFunc(func(context.Context, events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
			called = true
			return events.APIGatewayProxyResponse{StatusCode: 204}, nil
		})
		wrapper := Middleware(func(next HandlerFunc) HandlerFunc {
			return func(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
				res, err := next(ctx, req)
				res.Headers = map[string]string{"X-Test": "wrapped"}
				return res, err
			}
		})

		res, err := wrapper(handler)(context.Background(), events.APIGatewayProxyRequest{})

		Expect(err).NotTo(HaveOccurred())
		Expect(called).To(BeTrue())
		Expect(res.StatusCode).To(Equal(204))
		Expect(res.Headers).To(HaveKeyWithValue("X-Test", "wrapped"))
	})

	It("propagates handler errors", func() {
		expectedErr := errors.New("handler failed")
		handler := HandlerFunc(func(context.Context, events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
			return events.APIGatewayProxyResponse{StatusCode: 500}, expectedErr
		})

		res, err := handler(context.Background(), events.APIGatewayProxyRequest{})

		Expect(err).To(MatchError(expectedErr))
		Expect(res.StatusCode).To(Equal(500))
	})
})
