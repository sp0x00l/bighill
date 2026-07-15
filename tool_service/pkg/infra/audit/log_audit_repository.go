package audit

import (
	"context"

	"tool_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
)

type LogInvocationAuditRepository struct{}

func NewLogInvocationAuditRepository() *LogInvocationAuditRepository {
	log.Trace("NewLogInvocationAuditRepository")

	return &LogInvocationAuditRepository{}
}

func (r *LogInvocationAuditRepository) RecordInvocation(ctx context.Context, audit model.ToolInvocationAudit) error {
	log.Trace("LogInvocationAuditRepository RecordInvocation")

	log.WithContext(ctx).WithFields(log.Fields{
		"invocation_id":          audit.InvocationID.String(),
		"org_id":                 audit.OrgID.String(),
		"user_id":                audit.UserID.String(),
		"tool_name":              audit.ToolName,
		"implementation_version": audit.ImplementationVersion,
		"status":                 audit.Status.String(),
		"error_code":             audit.ErrorCode,
		"error_type":             audit.ErrorType.String(),
		"latency_ms":             audit.LatencyMs,
	}).Info("tool invocation boundary audit")
	return nil
}
