package ctxutil

import (
	"context"
	"errors"

	"github.com/google/uuid"
)

type tenantIDKey struct{}
type systemContextKey struct{}

func WithTenantID(ctx context.Context, tenantID uuid.UUID) context.Context {
	if tenantID == uuid.Nil {
		return ctx
	}
	return context.WithValue(ctx, tenantIDKey{}, tenantID)
}

func TenantID(ctx context.Context) (uuid.UUID, bool) {
	tenantID, ok := ctx.Value(tenantIDKey{}).(uuid.UUID)
	if !ok || tenantID == uuid.Nil {
		return uuid.Nil, false
	}
	return tenantID, true
}

func WithSystemContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, systemContextKey{}, true)
}

func IsSystemContext(ctx context.Context) bool {
	value, ok := ctx.Value(systemContextKey{}).(bool)
	return ok && value
}

func IsCanceled(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
