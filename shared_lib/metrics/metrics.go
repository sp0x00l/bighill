package metrics

import (
	"context"
	"net/url"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.25.0"
)

func Init(ctx context.Context, serviceName, serviceVersion string) func() {
	log.Trace("Initializing metrics")

	otlpEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")

	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String(serviceName),
		semconv.ServiceVersionKey.String(serviceVersion),
		semconv.TelemetrySDKLanguageGo,
	)

	if otlpEndpoint == "" {
		provider := sdkmetric.NewMeterProvider(sdkmetric.WithResource(res))
		otel.SetMeterProvider(provider)
		return func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := provider.Shutdown(shutdownCtx); err != nil {
				log.WithError(err).Warn("failed to shutdown metrics provider")
			}
		}
	}

	log.Infof("Configuring OTLP metrics exporter with endpoint: %s", otlpEndpoint)

	// Parse URL to extract host:port for WithEndpoint
	opts := []otlpmetrichttp.Option{otlpmetrichttp.WithInsecure()}
	if parsed, err := url.Parse(otlpEndpoint); err == nil && parsed.Host != "" {
		opts = append([]otlpmetrichttp.Option{otlpmetrichttp.WithEndpoint(parsed.Host)}, opts...)
		if parsed.Path != "" && parsed.Path != "/" {
			opts = append(opts, otlpmetrichttp.WithURLPath(parsed.Path))
		}
	} else {
		// Fallback: treat as host:port directly
		opts = append([]otlpmetrichttp.Option{otlpmetrichttp.WithEndpoint(strings.TrimPrefix(strings.TrimPrefix(otlpEndpoint, "http://"), "https://"))}, opts...)
	}

	exporter, err := otlpmetrichttp.New(ctx, opts...)
	if err != nil {
		log.Fatalf("failed to initialize otlp metrics: %v", err)
	}

	reader := sdkmetric.NewPeriodicReader(exporter)
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(res),
		sdkmetric.WithView(
			sdkmetric.NewView(
				sdkmetric.Instrument{Name: "infra_request_duration_seconds"},
				sdkmetric.Stream{
					Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
						Boundaries: []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 1, 2, 5, 10},
					},
				},
			),
		),
	)
	otel.SetMeterProvider(provider)

	return func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := provider.Shutdown(shutdownCtx); err != nil {
			log.WithError(err).Warn("failed to shutdown metrics provider")
		}
	}
}
