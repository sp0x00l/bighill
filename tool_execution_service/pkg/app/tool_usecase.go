package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"tool_execution_service/pkg/domain"
	"tool_execution_service/pkg/domain/model"

	"lib/shared_lib/userevents"

	log "github.com/sirupsen/logrus"
)

const (
	argsPreviewMaxKeys = 20
)

type toolUsecase struct {
	registry       ToolRegistry
	executors      map[model.ToolExecutorKind]ToolExecutor
	policyResolver BoundaryPolicyResolver
	auditor        InvocationAuditRepository
}

type ToolUsecaseOption func(*toolUsecase)

func WithBoundaryPolicyResolver(resolver BoundaryPolicyResolver) ToolUsecaseOption {
	log.Trace("WithBoundaryPolicyResolver")

	return func(u *toolUsecase) {
		u.policyResolver = resolver
	}
}

func WithInvocationAuditRepository(auditor InvocationAuditRepository) ToolUsecaseOption {
	log.Trace("WithInvocationAuditRepository")

	return func(u *toolUsecase) {
		u.auditor = auditor
	}
}

func NewToolUsecase(registry ToolRegistry, executors map[model.ToolExecutorKind]ToolExecutor, opts ...ToolUsecaseOption) ToolUsecase {
	log.Trace("NewToolUsecase")

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
	policy, err := u.policyResolver.ResolvePolicy(tool)
	if err != nil {
		u.recordInvocationAudit(ctx, command, tool, nil, err)
		return nil, err
	}
	result, err := executor.Execute(ctx, tool, command, policy)
	u.recordInvocationAudit(ctx, command, tool, result, err)
	return result, err
}

func (u *toolUsecase) recordInvocationAudit(ctx context.Context, command model.InvokeToolCommand, tool *model.ToolDefinition, result *model.ToolInvocationResult, err error) {
	log.Trace("toolUsecase recordInvocationAudit")

	audit := model.ToolInvocationAudit{
		InvocationID: command.InvocationID,
		OrgID:        command.OrgID,
		UserID:       command.UserID,
		ToolName:     command.ToolName,
		Status:       model.ToolInvocationAuditStatusCompleted,
		TraceID:      strings.TrimSpace(command.TraceID),
		ArgsHash:     argsHash(command.ArgumentsJSON),
		ArgsPreview:  argsPreview(command.ArgumentsJSON),
	}
	if tool != nil {
		audit.ImplementationVersion = tool.ImplementationVersion
		audit.ExecutorKind = tool.ExecutorKind
	}
	if result != nil {
		audit.ErrorCode = result.ErrorCode
		audit.ErrorType = result.ErrorType
		audit.LatencyMs = result.LatencyMs
		if strings.TrimSpace(result.EgressHost) != "" {
			audit.EgressHost = strings.TrimSpace(result.EgressHost)
		}
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

func argsHash(argsJSON []byte) string {
	log.Trace("argsHash")

	return "sha256:" + userevents.SHA256String(string(argsJSON))
}

func argsPreview(argsJSON []byte) string {
	log.Trace("argsPreview")

	var value map[string]any
	if err := json.Unmarshal(argsJSON, &value); err != nil {
		return "invalid-json"
	}
	keys := make([]string, 0, len(value))
	for key := range value {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) > argsPreviewMaxKeys {
		keys = keys[:argsPreviewMaxKeys]
	}
	return "keys:" + strings.Join(keys, ",")
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
