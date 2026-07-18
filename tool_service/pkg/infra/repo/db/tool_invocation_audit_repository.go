package db

import (
	"context"
	"fmt"

	"tool_service/pkg/domain/model"

	"lib/shared_lib/ctxutil"
	coreDB "lib/shared_lib/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
)

type InvocationAuditRepository struct {
	coreDB.Database
}

func NewInvocationAuditRepository(db *coreDB.Database) *InvocationAuditRepository {
	log.Trace("NewInvocationAuditRepository")

	return &InvocationAuditRepository{Database: *db}
}

func (r *InvocationAuditRepository) RecordInvocation(ctx context.Context, audit model.ToolInvocationAudit) error {
	log.Trace("InvocationAuditRepository RecordInvocation")

	query := `INSERT INTO ` + r.Name + `.tool_invocation_audit (
		invocation_id, org_id, user_id, tool_name, tool_impl_version,
		executor_kind, status, error_code, error_type, latency_ms,
		egress_host, trace_id, args_hash, args_preview
	) VALUES (
		@invocation_id, @org_id, @user_id, @tool_name, @tool_impl_version,
		@executor_kind::tool_executor_kind_enum, @status::tool_invocation_audit_status_enum,
		@error_code, @error_type::tool_error_type_enum, @latency_ms,
		@egress_host, @trace_id, @args_hash, @args_preview
	)
	ON CONFLICT (invocation_id) DO NOTHING`
	if _, err := r.Pool.Exec(ctxutil.WithOrgID(ctx, audit.OrgID), query, invocationAuditArgs(audit)); err != nil {
		r.LogPoolStatsOnError(ctx, "record tool invocation audit failed", err)
		return fmt.Errorf("record tool invocation audit: %w", err)
	}
	return nil
}

func invocationAuditArgs(audit model.ToolInvocationAudit) pgx.NamedArgs {
	log.Trace("invocationAuditArgs")

	return pgx.NamedArgs{
		"invocation_id":     requiredUUID(audit.InvocationID),
		"org_id":            requiredUUID(audit.OrgID),
		"user_id":           requiredUUID(audit.UserID),
		"tool_name":         audit.ToolName,
		"tool_impl_version": audit.ImplementationVersion,
		"executor_kind":     audit.ExecutorKind.String(),
		"status":            audit.Status.String(),
		"error_code":        audit.ErrorCode,
		"error_type":        nullableToolErrorType(audit.ErrorType),
		"latency_ms":        audit.LatencyMs,
		"egress_host":       audit.EgressHost,
		"trace_id":          audit.TraceID,
		"args_hash":         audit.ArgsHash,
		"args_preview":      audit.ArgsPreview,
	}
}

func requiredUUID(value uuid.UUID) pgtype.UUID {
	log.Trace("requiredUUID")

	return pgtype.UUID{Bytes: value, Valid: value != uuid.Nil}
}

func nullableToolErrorType(value model.ToolErrorType) pgtype.Text {
	log.Trace("nullableToolErrorType")

	if value == model.ToolErrorTypeUnknown {
		return pgtype.Text{}
	}
	return pgtype.Text{String: value.String(), Valid: true}
}
