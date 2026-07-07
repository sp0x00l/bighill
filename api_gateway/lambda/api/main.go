package main

import (
	"api/pkg/adapter"
	"api/pkg/middleware"
	"api/pkg/router"
	"net/http"
	"time"

	"context"
	"encoding/json"
	env "lib/shared_lib/env"
	"lib/shared_lib/logs"
	"lib/shared_lib/observability"

	log "github.com/sirupsen/logrus"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-lambda-go/otellambda"
	"go.opentelemetry.io/otel"
)

// Version set at compile time
var Version string

func init() {
	logs.Init()

	ctx := context.Background()
	traceName := "api-gateway"
	// gracefull shutdown is not possible in an AWS lambda
	_ = observability.Init(ctx, traceName, Version)
	middleware.Tracer = otel.Tracer(traceName)
}

func middlewareChain(h adapter.HandlerFunc, middleware ...adapter.Middleware) adapter.HandlerFunc {
	log.Trace("middleware chain")
	if len(middleware) > 0 {
		for _, m := range middleware {
			h = m(h)
		}
	}
	return h
}

// implements lambda.Handler interface.
type APIHandler struct {
	handler adapter.HandlerFunc
}

func NewAPIHandler() APIHandler {
	httpClientTimeoutSeconds := env.WithDefaultInt("API_GATEWAY_SERVICE_HTTP_CLIENT_TIMEOUT_SECONDS", "10")
	httpClientTimeout := time.Duration(httpClientTimeoutSeconds) * time.Second

	client := &http.Client{Timeout: httpClientTimeout}
	cfg := router.Config{
		DataRegistryServiceRoute: serviceBaseRoute(
			env.WithDefaultString("DATA_REGISTRY_SERVICE_HTTP_HOST", "127.0.0.1"),
			env.WithDefaultString("DATA_REGISTRY_SERVICE_HTTP_PORT", "8081"),
		),
		IngestionServiceRoute: serviceBaseRoute(
			env.WithDefaultString("INGESTION_SERVICE_HTTP_HOST", "127.0.0.1"),
			env.WithDefaultString("INGESTION_SERVICE_HTTP_PORT", "8086"),
		),
		ModelRegistryServiceRoute: serviceBaseRoute(
			env.WithDefaultString("MODEL_REGISTRY_SERVICE_HTTP_HOST", "127.0.0.1"),
			env.WithDefaultString("MODEL_REGISTRY_SERVICE_HTTP_PORT", "8084"),
		),
		ProfileServiceRoute: serviceBaseRoute(
			env.WithDefaultString("PROFILE_SERVICE_HTTP_HOST", "127.0.0.1"),
			env.WithDefaultString("PROFILE_SERVICE_HTTP_PORT", "8082"),
		),
		TrainingServiceRoute: serviceBaseRoute(
			env.WithDefaultString("TRAINING_SERVICE_HTTP_HOST", "127.0.0.1"),
			env.WithDefaultString("TRAINING_SERVICE_HTTP_PORT", "8085"),
		),
		InferenceServiceRoute: serviceBaseRoute(
			env.WithDefaultString("INFERENCE_SERVICE_HTTP_HOST", "127.0.0.1"),
			env.WithDefaultString("INFERENCE_SERVICE_HTTP_PORT", "8087"),
		),
	}

	return APIHandler{
		handler: middlewareChain(router.NewRouter(client, http.NewRequest, cfg), middleware.TraceMiddleware),
	}
}

func serviceBaseRoute(host, port string) string {
	return "http://" + host + ":" + port
}

func (h APIHandler) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	log.Trace("APIHandler Invoke")

	var req events.APIGatewayProxyRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		log.WithContext(ctx).WithError(err).Warn("failed to unmarshal APIGateway request")
		errorResponse := events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       "Unable to parse request. It may be malformed or too large.",
		}
		responseBytes, _ := json.Marshal(errorResponse)
		return responseBytes, nil
	}

	resp, err := h.handler(ctx, req)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("APIGateway middleware handler failed")
		return []byte{}, err
	}

	return json.Marshal(resp)
}

func main() {
	log.Trace("API Gateway main")

	handler := NewAPIHandler()
	wrappedHandler := otellambda.WrapHandler(
		handler,
		otellambda.WithTracerProvider(otel.GetTracerProvider()),
	)
	lambda.Start(wrappedHandler)
}
