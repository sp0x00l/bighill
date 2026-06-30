package middleware

import (
	"context"
	"fmt"

	"api/pkg/adapter"

	"github.com/aws/aws-lambda-go/events"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/trace"
)

var Tracer trace.Tracer

// Middleware to start and end spans for each request
func TraceMiddleware(next adapter.HandlerFunc) adapter.HandlerFunc {
	return func(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
		log.Trace("API Gateway TraceMiddleware")

		ctx, span := Tracer.Start(ctx, fmt.Sprintf("API Gateway Request: %s %s", req.HTTPMethod, req.Path))
		defer span.End()

		handler, err := next(ctx, req)
		if err != nil {
			log.WithContext(ctx).WithError(err).Error("trace middleware error, failed to call next handler")
			return handler, err
		}
		// additional span attributes from response go here

		return handler, nil
	}
}
