package usecasetrace

import (
	"context"

	sharedtrace "lib/shared_lib/trace"

	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type ExpectedErrorClassifier = sharedtrace.ExpectedErrorClassifier

func StartSpan(ctx context.Context, tracerName, spanName string, attrs ...attribute.KeyValue) (context.Context, oteltrace.Span) {
	return StartSpanWithExpectedErrors(ctx, tracerName, spanName, nil, attrs...)
}

func StartSpanWithExpectedErrors(ctx context.Context, tracerName, spanName string, classifier ExpectedErrorClassifier, attrs ...attribute.KeyValue) (context.Context, oteltrace.Span) {
	ctx, span := sharedtrace.StartSpan(ctx, tracerName, spanName, attrs...)
	if classifier != nil {
		ctx = sharedtrace.ContextWithExpectedErrorClassifier(ctx, classifier)
	}
	return ctx, span
}

func EndSpanOnReturn(ctx context.Context, span oteltrace.Span, err *error) {
	sharedtrace.EndSpanOnReturnFromContext(ctx, span, err)
}
