package app

import (
	"context"

	"tool_execution_service/pkg/domain/model"

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

type ToolCatalogProjectionRepository interface {
	ApplyCapabilityProjection(ctx context.Context, projection model.ToolCapabilityProjection) error
	ApplyGrantProjection(ctx context.Context, projection model.ToolGrantProjection) error
	ApplyCredentialBindingProjection(ctx context.Context, projection model.ToolCredentialBindingProjection) error
}

type ToolCatalogProjectionUsecase interface {
	ApplyCapabilityProjection(ctx context.Context, projection model.ToolCapabilityProjection) error
	ApplyGrantProjection(ctx context.Context, projection model.ToolGrantProjection) error
	ApplyCredentialBindingProjection(ctx context.Context, projection model.ToolCredentialBindingProjection) error
}

type ToolUsecase interface {
	ListAvailableTools(ctx context.Context, command model.ListAvailableToolsCommand) ([]*model.ToolDefinition, error)
	Invoke(ctx context.Context, command model.InvokeToolCommand) (*model.ToolInvocationResult, error)
}
