package app

import (
	"context"

	usecasetrace "lib/shared_lib/usecasetrace"

	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
)

const inferenceAppTracerName = "inference_service/app"

func startInferenceSpan(ctx context.Context, spanName string, attrs ...attribute.KeyValue) (context.Context, oteltrace.Span) {
	return usecasetrace.StartSpan(ctx, inferenceAppTracerName, spanName, attrs...)
}

func endInferenceSpanOnReturn(ctx context.Context, span oteltrace.Span, err *error) {
	usecasetrace.EndSpanOnReturn(ctx, span, err)
}
