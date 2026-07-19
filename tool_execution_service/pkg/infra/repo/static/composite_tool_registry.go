package static

import (
	"context"
	"errors"

	"tool_execution_service/pkg/app"
	"tool_execution_service/pkg/domain"
	"tool_execution_service/pkg/domain/model"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type CompositeToolRegistry struct {
	registries []app.ToolRegistry
}

func NewCompositeToolRegistry(registries ...app.ToolRegistry) (*CompositeToolRegistry, error) {
	log.Trace("NewCompositeToolRegistry")

	filtered := make([]app.ToolRegistry, 0, len(registries))
	for _, registry := range registries {
		if registry != nil {
			filtered = append(filtered, registry)
		}
	}
	return &CompositeToolRegistry{registries: filtered}, nil
}

func (r *CompositeToolRegistry) ListAvailableTools(ctx context.Context, orgID uuid.UUID, userID uuid.UUID) ([]*model.ToolDefinition, error) {
	log.Trace("CompositeToolRegistry ListAvailableTools")

	seen := map[string]bool{}
	result := []*model.ToolDefinition{}
	for _, registry := range r.registries {
		tools, err := registry.ListAvailableTools(ctx, orgID, userID)
		if err != nil {
			return nil, err
		}
		for _, tool := range tools {
			key := toolKey(tool.Name)
			if seen[key] {
				return nil, domain.ErrToolPolicy.Extend("duplicate projected tool name " + tool.Name)
			}
			seen[key] = true
			result = append(result, tool)
		}
	}
	return result, nil
}

func (r *CompositeToolRegistry) ResolveTool(ctx context.Context, orgID uuid.UUID, userID uuid.UUID, toolName string) (*model.ToolDefinition, error) {
	log.Trace("CompositeToolRegistry ResolveTool")

	var denied error
	for _, registry := range r.registries {
		tool, err := registry.ResolveTool(ctx, orgID, userID, toolName)
		if err == nil {
			return tool, nil
		}
		if errors.Is(err, domain.ErrToolDenied) || errors.Is(err, domain.ErrToolPolicy) {
			denied = err
		}
	}
	if denied != nil {
		return nil, denied
	}
	return nil, domain.ErrToolNotFound.Extend(toolName)
}
