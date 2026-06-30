package database

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.25.0"
	"go.opentelemetry.io/otel/trace"
)

type pgxSpanKey struct{}

type pgxOtelTracer struct {
	dbName string
	tracer trace.Tracer
}

func newPgxOtelTracer(dbName string) *pgxOtelTracer {
	return &pgxOtelTracer{
		dbName: dbName,
		tracer: otel.Tracer("shared_lib/db/pgx"),
	}
}

func (t *pgxOtelTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	operation := sqlOperation(data.SQL)
	attrs := []attribute.KeyValue{
		semconv.DBSystemPostgreSQL,
		semconv.DBName(t.dbName),
		semconv.DBStatement(data.SQL),
	}
	if operation != "" {
		attrs = append(attrs, attribute.String("db.operation", operation))
	}
	ctx, span := t.tracer.Start(ctx, "postgres."+strings.ToLower(defaultString(operation, "query")),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrs...),
	)
	return context.WithValue(ctx, pgxSpanKey{}, span)
}

func (t *pgxOtelTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	endPgxSpan(ctx, data.Err)
}

func (t *pgxOtelTracer) TraceBatchStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceBatchStartData) context.Context {
	ctx, span := t.tracer.Start(ctx, "postgres.batch",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.DBSystemPostgreSQL,
			semconv.DBName(t.dbName),
		),
	)
	return context.WithValue(ctx, pgxSpanKey{}, span)
}

func (t *pgxOtelTracer) TraceBatchQuery(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchQueryData) {
	attrs := []attribute.KeyValue{semconv.DBStatement(data.SQL)}
	if operation := sqlOperation(data.SQL); operation != "" {
		attrs = append(attrs, attribute.String("db.operation", operation))
	}
	span := trace.SpanFromContext(ctx)
	span.AddEvent("postgres.batch.query", trace.WithAttributes(attrs...))
	if data.Err != nil {
		span.RecordError(data.Err)
		span.SetStatus(otelcodes.Error, data.Err.Error())
	}
}

func (t *pgxOtelTracer) TraceBatchEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchEndData) {
	endPgxSpan(ctx, data.Err)
}

func (t *pgxOtelTracer) TraceConnectStart(ctx context.Context, data pgx.TraceConnectStartData) context.Context {
	attrs := []attribute.KeyValue{semconv.DBSystemPostgreSQL}
	if data.ConnConfig != nil {
		if data.ConnConfig.Database != "" {
			attrs = append(attrs, semconv.DBName(data.ConnConfig.Database))
		}
		if data.ConnConfig.Host != "" {
			attrs = append(attrs, semconv.ServerAddress(data.ConnConfig.Host))
		}
		if data.ConnConfig.Port != 0 {
			attrs = append(attrs, semconv.NetworkPeerPort(int(data.ConnConfig.Port)))
		}
	}
	ctx, span := t.tracer.Start(ctx, "postgres.connect",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrs...),
	)
	return context.WithValue(ctx, pgxSpanKey{}, span)
}

func (t *pgxOtelTracer) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {
	endPgxSpan(ctx, data.Err)
}

func endPgxSpan(ctx context.Context, err error) {
	span, ok := ctx.Value(pgxSpanKey{}).(trace.Span)
	if !ok {
		return
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
	}
	span.End()
}

func sqlOperation(sql string) string {
	trimmed := strings.TrimSpace(sql)
	if trimmed == "" {
		return ""
	}
	fields := strings.Fields(trimmed)
	if len(fields) == 0 {
		return ""
	}
	return strings.ToUpper(fields[0])
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
