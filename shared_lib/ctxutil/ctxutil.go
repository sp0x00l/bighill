package ctxutil

import (
	"context"
	"errors"

	"github.com/google/uuid"
)

type tenantIDKey struct{}
type orgIDKey struct{}
type systemContextKey struct{}
type transactionContextKey struct{}

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

func WithOrgID(ctx context.Context, orgID uuid.UUID) context.Context {
	if orgID == uuid.Nil {
		return ctx
	}
	return context.WithValue(ctx, orgIDKey{}, orgID)
}

func WithActorOrg(ctx context.Context, userID uuid.UUID, orgID uuid.UUID) context.Context {
	ctx = WithTenantID(ctx, userID)
	ctx = WithOrgID(ctx, orgID)
	return ctx
}

func OrgID(ctx context.Context) (uuid.UUID, bool) {
	orgID, ok := ctx.Value(orgIDKey{}).(uuid.UUID)
	if !ok || orgID == uuid.Nil {
		return uuid.Nil, false
	}
	return orgID, true
}

func WithSystemContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, systemContextKey{}, true)
}

func IsSystemContext(ctx context.Context) bool {
	value, ok := ctx.Value(systemContextKey{}).(bool)
	return ok && value
}

func WithTransactionContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, transactionContextKey{}, true)
}

func IsTransactionContext(ctx context.Context) bool {
	value, ok := ctx.Value(transactionContextKey{}).(bool)
	return ok && value
}

func IsCanceled(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
