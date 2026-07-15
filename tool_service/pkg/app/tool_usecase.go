package app

import (
	"context"
	"errors"
	"fmt"

	"tool_service/pkg/domain"
	"tool_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
)

type toolUsecase struct {
	registry  ToolRegistry
	executors map[model.ToolExecutorKind]ToolExecutor
	auditor   InvocationAuditRepository
}

type ToolUsecaseOption func(*toolUsecase)

func WithInvocationAuditRepository(auditor InvocationAuditRepository) ToolUsecaseOption {
	log.Trace("WithInvocationAuditRepository")

	return func(u *toolUsecase) {
		u.auditor = auditor
	}
}

func NewToolUsecase(registry ToolRegistry, executors map[model.ToolExecutorKind]ToolExecutor, opts ...ToolUsecaseOption) ToolUsecase {
	log.Trace("NewToolUsecase")

	if registry == nil {
		log.Fatal("tool registry is required")
	}
	if len(executors) == 0 {
		log.Fatal("tool executors are required")
	}
	usecase := &toolUsecase{
		registry:  registry,
		executors: executors,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(usecase)
		}
	}
	return usecase
}

func (u *toolUsecase) ListAvailableTools(ctx context.Context, command model.ListAvailableToolsCommand) ([]*model.ToolDefinition, error) {
	log.Trace("toolUsecase ListAvailableTools")

	return u.registry.ListAvailableTools(ctx, command.OrgID, command.UserID)
}

func (u *toolUsecase) Invoke(ctx context.Context, command model.InvokeToolCommand) (*model.ToolInvocationResult, error) {
	log.Trace("toolUsecase Invoke")

	tool, err := u.registry.ResolveTool(ctx, command.OrgID, command.UserID, command.ToolName)
	if err != nil {
		u.recordInvocationAudit(ctx, command, nil, nil, err)
		return nil, err
	}
	executor, ok := u.executors[tool.ExecutorKind]
	if !ok {
		err := domain.ErrToolPolicy.Extend(fmt.Sprintf("executor %s is not configured", tool.ExecutorKind.String()))
		u.recordInvocationAudit(ctx, command, tool, nil, err)
		return nil, err
	}
	result, err := executor.Execute(ctx, tool, command)
	u.recordInvocationAudit(ctx, command, tool, result, err)
	return result, err
}

func (u *toolUsecase) recordInvocationAudit(ctx context.Context, command model.InvokeToolCommand, tool *model.ToolDefinition, result *model.ToolInvocationResult, err error) {
	log.Trace("toolUsecase recordInvocationAudit")

	if u.auditor == nil {
		return
	}
	audit := model.ToolInvocationAudit{
		InvocationID: command.InvocationID,
		OrgID:        command.OrgID,
		UserID:       command.UserID,
		ToolName:     command.ToolName,
		Status:       model.ToolInvocationAuditStatusCompleted,
	}
	if tool != nil {
		audit.ImplementationVersion = tool.ImplementationVersion
	}
	if result != nil {
		audit.ErrorCode = result.ErrorCode
		audit.ErrorType = result.ErrorType
		audit.LatencyMs = result.LatencyMs
		if result.ImplementationVersion != "" {
			audit.ImplementationVersion = result.ImplementationVersion
		}
		if result.IsError {
			audit.Status = model.ToolInvocationAuditStatusFailed
		}
	}
	if err != nil {
		audit.Status = auditStatusForError(err)
		audit.ErrorType = auditErrorTypeForError(err)
		var serviceErr *domain.ServiceError
		if errors.As(err, &serviceErr) {
			audit.ErrorCode = serviceErr.Code
		}
	}
	if audit.Status == model.ToolInvocationAuditStatusCompleted && audit.ErrorType != model.ToolErrorTypeUnknown {
		audit.Status = model.ToolInvocationAuditStatusFailed
	}
	if recordErr := u.auditor.RecordInvocation(ctx, audit); recordErr != nil {
		log.WithContext(ctx).WithError(recordErr).Warn("tool invocation audit failed")
	}
}

func auditStatusForError(err error) model.ToolInvocationAuditStatus {
	log.Trace("auditStatusForError")

	if errors.Is(err, domain.ErrToolDenied) || errors.Is(err, domain.ErrToolPolicy) || errors.Is(err, domain.ErrToolNotFound) {
		return model.ToolInvocationAuditStatusDenied
	}
	return model.ToolInvocationAuditStatusFailed
}

func auditErrorTypeForError(err error) model.ToolErrorType {
	log.Trace("auditErrorTypeForError")

	if errors.Is(err, domain.ErrToolDenied) || errors.Is(err, domain.ErrToolPolicy) || errors.Is(err, domain.ErrToolNotFound) {
		return model.ToolErrorTypePolicyDenied
	}
	return model.ToolErrorTypePermanent
}
