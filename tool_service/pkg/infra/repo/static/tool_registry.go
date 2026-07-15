package static

import (
	"context"
	"strings"

	"tool_service/pkg/domain"
	"tool_service/pkg/domain/model"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type ToolRegistry struct {
	tools map[string]*model.ToolDefinition
}

func NewToolRegistry(tools []*model.ToolDefinition) *ToolRegistry {
	log.Trace("NewToolRegistry")

	indexed := make(map[string]*model.ToolDefinition, len(tools))
	for _, tool := range tools {
		if tool == nil {
			continue
		}
		indexed[toolKey(tool.Name)] = tool
	}
	return &ToolRegistry{tools: indexed}
}

func (r *ToolRegistry) ListAvailableTools(ctx context.Context, orgID uuid.UUID, userID uuid.UUID) ([]*model.ToolDefinition, error) {
	log.Trace("ToolRegistry ListAvailableTools")

	_ = ctx
	tools := make([]*model.ToolDefinition, 0, len(r.tools))
	for _, tool := range r.tools {
		if tool.Enabled && toolAllowedForActor(tool, orgID, userID) {
			tools = append(tools, tool)
		}
	}
	return tools, nil
}

func (r *ToolRegistry) ResolveTool(ctx context.Context, orgID uuid.UUID, userID uuid.UUID, toolName string) (*model.ToolDefinition, error) {
	log.Trace("ToolRegistry ResolveTool")

	_ = ctx
	tool, ok := r.tools[toolKey(toolName)]
	if !ok || !tool.Enabled {
		return nil, domain.ErrToolNotFound.Extend(toolName)
	}
	if !toolAllowedForActor(tool, orgID, userID) {
		return nil, domain.ErrToolDenied.Extend("tool is not allowlisted for tenant")
	}
	return tool, nil
}

func toolAllowedForActor(tool *model.ToolDefinition, orgID uuid.UUID, userID uuid.UUID) bool {
	log.Trace("toolAllowedForActor")

	if tool == nil || orgID == uuid.Nil || userID == uuid.Nil {
		return false
	}
	for _, allowedOrgID := range tool.AllowedOrgIDs {
		if allowedOrgID == orgID {
			return true
		}
	}
	return false
}

func toolKey(value string) string {
	log.Trace("toolKey")

	return strings.ToLower(strings.TrimSpace(value))
}
