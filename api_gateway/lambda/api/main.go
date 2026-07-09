package main

import (
	"api/pkg/adapter"
	"api/pkg/middleware"
	"api/pkg/router"
	"net/http"
	"os"
	"strings"
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
	cfg := routerConfigFromEnv()

	return APIHandler{
		handler: middlewareChain(router.NewRouter(client, http.NewRequest, cfg), middleware.TraceMiddleware),
	}
}

func routerConfigFromEnv() router.Config {
	return router.Config{
		DataRegistryServiceRoute: serviceBaseRoute(
			serviceHost("DATA_REGISTRY_SERVICE_HTTP_HOST", "127.0.0.1"),
			servicePort("DATA_REGISTRY_SERVICE_HTTP_PORT", "8081"),
		),
		IngestionServiceRoute: serviceBaseRoute(
			serviceHost("INGESTION_SERVICE_HTTP_HOST", "127.0.0.1"),
			servicePort("INGESTION_SERVICE_HTTP_PORT", "8086"),
		),
		ModelRegistryServiceRoute: serviceBaseRoute(
			serviceHost("MODEL_REGISTRY_SERVICE_HTTP_HOST", "127.0.0.1"),
			servicePort("MODEL_REGISTRY_SERVICE_HTTP_PORT", "8084"),
		),
		ProfileServiceRoute: serviceBaseRoute(
			serviceHost("PROFILE_SERVICE_HTTP_HOST", "127.0.0.1"),
			servicePort("PROFILE_SERVICE_HTTP_PORT", "8082"),
		),
		TrainingServiceRoute: serviceBaseRoute(
			serviceHost("TRAINING_SERVICE_HTTP_HOST", "127.0.0.1"),
			servicePort("TRAINING_SERVICE_HTTP_PORT", "8085"),
		),
		InferenceServiceRoute: serviceBaseRoute(
			serviceHost("INFERENCE_SERVICE_HTTP_HOST", "127.0.0.1"),
			servicePort("INFERENCE_SERVICE_HTTP_PORT", "8087"),
		),
	}
}

func serviceBaseRoute(host, port string) string {
	return "http://" + host + ":" + port
}

func serviceHost(key, localDefault string) string {
	value := serviceEnv(key, localDefault)
	if !usesLocalServiceDefaults() && isLocalhost(value) {
		log.Fatalf("%s=%q is only valid for local-dev or cicd", key, value)
	}
	return value
}

func servicePort(key, localDefault string) string {
	return serviceEnv(key, localDefault)
}

func serviceEnv(key, localDefault string) string {
	if usesLocalServiceDefaults() {
		return env.WithDefaultString(key, localDefault)
	}
	value := strings.TrimSpace(env.MustString(key))
	if value == "" {
		log.Fatalf("environment variable %s must not be empty", key)
	}
	return value
}

func usesLocalServiceDefaults() bool {
	switch normalizedEnvironment() {
	case "", "local-dev", "cicd":
		return true
	case "staging", "prod":
		return false
	default:
		log.Fatalf("ENVIRONMENT must be one of local-dev, cicd, staging, prod")
		return false
	}
}

func normalizedEnvironment() string {
	return strings.ToLower(strings.TrimSpace(os.Getenv("ENVIRONMENT")))
}

func isLocalhost(host string) bool {
	switch strings.ToLower(strings.TrimSpace(host)) {
	case "127.0.0.1", "localhost", "::1", "0.0.0.0":
		return true
	default:
		return false
	}
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
