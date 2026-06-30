package trace

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"time"

	sharedlogs "lib/shared_lib/logs"

	log "github.com/sirupsen/logrus"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"

	semconv "go.opentelemetry.io/otel/semconv/v1.25.0"
)

type CustomWriter struct {
	Buffer bytes.Buffer
}

func (w *CustomWriter) Write(p []byte) (n int, err error) {
	return w.Buffer.Write(p)
}

type InitOption func(*initOptions)

type initOptions struct {
	stdoutWriter io.Writer
}

func WithStdoutWriter(writer io.Writer) InitOption {
	return func(opts *initOptions) {
		opts.stdoutWriter = writer
	}
}

func Init(ctx context.Context, serviceName, serviceVersion string, opts ...InitOption) func() {
	log.Trace("Initializing distributed trace")
	sharedlogs.InstallTraceFieldHook()
	sharedlogs.InstallOpenTelemetryHook()
	initCfg := initOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&initCfg)
		}
	}

	var err error
	var exporter trace.SpanExporter

	otlpEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")

	if otlpEndpoint == "" {
		if initCfg.stdoutWriter != nil {
			exporter, err = stdouttrace.New(
				stdouttrace.WithWriter(initCfg.stdoutWriter),
				stdouttrace.WithPrettyPrint(),
			)
		} else if strings.EqualFold(os.Getenv("OTEL_TRACE_STDOUT"), "true") {
			exporter, err = stdouttrace.New(stdouttrace.WithPrettyPrint())
		} else {
			otel.SetTracerProvider(oteltrace.NewNoopTracerProvider())
			otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
				propagation.TraceContext{},
				propagation.Baggage{},
			))
			return func() {}
		}
		if err != nil {
			log.Fatalf("failed to initialize stdout trace: %v", err)
		}
	} else {
		log.Infof("Configuring OTLP exporter with endpoint: %s", otlpEndpoint)
		// use otlp exporter - endpoint should be host:port or full URL
		// otlptracehttp handles both formats
		exporter, err = otlptrace.New(
			ctx,
			otlptracehttp.NewClient(
				otlptracehttp.WithEndpointURL(otlpEndpoint),
				otlptracehttp.WithInsecure(),
			),
		)
		if err != nil {
			log.Fatalf("failed to initialize otlp trace: %v", err)
		}
	}

	tp := trace.NewTracerProvider(
		trace.WithSampler(trace.AlwaysSample()),
		trace.WithBatcher(exporter),
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceName),
			semconv.ServiceVersionKey.String(serviceVersion),
			semconv.TelemetrySDKLanguageGo,
		)),
	)
	otel.SetTracerProvider(tp)

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tp.Shutdown(shutdownCtx); err != nil {
			log.WithError(err).Error("trace provider shutdown failed")
		}
	}
}
