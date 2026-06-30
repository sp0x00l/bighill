package app

import (
	"context"
	"errors"

	"feature_materializer_service/pkg/domain"
	sharedtrace "lib/shared_lib/trace"

	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
)

func startFeatureMaterializerSpan(ctx context.Context, tracerName, spanName string, attrs ...attribute.KeyValue) (context.Context, oteltrace.Span) {
	ctx, span := sharedtrace.StartSpan(ctx, tracerName, spanName, attrs...)
	return sharedtrace.ContextWithExpectedErrorClassifier(ctx, IsExpectedTraceError), span
}

func endFeatureMaterializerSpanOnReturn(ctx context.Context, span oteltrace.Span, err *error) {
	sharedtrace.EndSpanOnReturnFromContext(ctx, span, err)
}

func IsExpectedTraceError(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := domain.IsRawSnapshotAlreadyMaterialized(err); ok {
		return true
	}
	if _, ok := domain.IsFeatureSnapshotAlreadyBuilt(err); ok {
		return true
	}
	if _, ok := domain.IsEmbeddingsAlreadyMaterialized(err); ok {
		return true
	}
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, domain.ErrRawSnapshotNotFound) ||
		errors.Is(err, domain.ErrFeatureSnapshotNotFound) ||
		errors.Is(err, domain.ErrEmbeddingSnapshotNotFound)
}
