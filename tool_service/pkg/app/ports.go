package app

import (
	"context"

	"tool_service/pkg/domain/model"

	"github.com/google/uuid"
)

type ToolRegistry interface {
	ListAvailableTools(ctx context.Context, orgID uuid.UUID, userID uuid.UUID) ([]*model.ToolDefinition, error)
	ResolveTool(ctx context.Context, orgID uuid.UUID, userID uuid.UUID, toolName string) (*model.ToolDefinition, error)
}

type ToolExecutor interface {
	Execute(ctx context.Context, tool *model.ToolDefinition, command model.InvokeToolCommand, policy model.PolicySet) (*model.ToolInvocationResult, error)
}

type BoundaryPolicyResolver interface {
	ResolvePolicy(tool *model.ToolDefinition) (model.PolicySet, error)
}

type InvocationAuditRepository interface {
	RecordInvocation(ctx context.Context, audit model.ToolInvocationAudit) error
}

type ToolUsecase interface {
	ListAvailableTools(ctx context.Context, command model.ListAvailableToolsCommand) ([]*model.ToolDefinition, error)
	Invoke(ctx context.Context, command model.InvokeToolCommand) (*model.ToolInvocationResult, error)
}
