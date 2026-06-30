package trace

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type ExpectedErrorClassifier func(error) bool

type expectedErrorClassifierKey struct{}

func StartSpan(ctx context.Context, tracerName, spanName string, attrs ...attribute.KeyValue) (context.Context, oteltrace.Span) {
	if len(attrs) == 0 {
		return otel.Tracer(tracerName).Start(ctx, spanName)
	}
	return otel.Tracer(tracerName).Start(ctx, spanName, oteltrace.WithAttributes(attrs...))
}

func ContextWithExpectedErrorClassifier(ctx context.Context, classifier ExpectedErrorClassifier) context.Context {
	if ctx == nil || classifier == nil {
		return ctx
	}
	if existing, ok := ctx.Value(expectedErrorClassifierKey{}).(ExpectedErrorClassifier); ok && existing != nil {
		return context.WithValue(ctx, expectedErrorClassifierKey{}, ExpectedErrorClassifier(func(err error) bool {
			return existing(err) || classifier(err)
		}))
	}
	return context.WithValue(ctx, expectedErrorClassifierKey{}, classifier)
}

func IsExpectedSpanError(ctx context.Context, err error) bool {
	if ctx == nil || err == nil {
		return false
	}
	classifier, ok := ctx.Value(expectedErrorClassifierKey{}).(ExpectedErrorClassifier)
	return ok && classifier != nil && classifier(err)
}

func EndSpan(span oteltrace.Span, err error) {
	RecordSpanError(span, err)
	span.End()
}

func EndSpanFromContext(ctx context.Context, span oteltrace.Span, err error) {
	RecordSpanErrorFromContext(ctx, span, err)
	span.End()
}

func EndSpanOnReturn(span oteltrace.Span, err *error) {
	if err == nil {
		EndSpan(span, nil)
		return
	}
	EndSpan(span, *err)
}

func EndSpanOnReturnFromContext(ctx context.Context, span oteltrace.Span, err *error) {
	if err == nil {
		EndSpanFromContext(ctx, span, nil)
		return
	}
	EndSpanFromContext(ctx, span, *err)
}

func RecordSpanError(span oteltrace.Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(otelcodes.Error, err.Error())
}

func RecordSpanErrorFromContext(ctx context.Context, span oteltrace.Span, err error) {
	if IsExpectedSpanError(ctx, err) {
		return
	}
	RecordSpanError(span, err)
}
