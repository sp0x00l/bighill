package adapter

import (
	"context"

	"github.com/aws/aws-lambda-go/events"
)

// Middleware function type takes a HandlerFunc function and returns a new HandlerFunc function
// so that they can be chained together to form a middleware chain of responsibility pattern.

type HandlerFunc func(context.Context, events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error)
type Middleware func(HandlerFunc) HandlerFunc
