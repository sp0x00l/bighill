package grpc

import (
	"encoding/json"
	"strings"

	"tool_service/pkg/domain"
	"tool_service/pkg/domain/model"

	toolspb "lib/data_contracts_lib/tools"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type listAvailableToolsRequestDTO struct {
	OrgID  string `validate:"required,uuid"`
	UserID string `validate:"required,uuid"`
}

type invokeToolRequestDTO struct {
	InvocationID  string `validate:"required,uuid"`
	ToolName      string `validate:"required"`
	ArgumentsJSON []byte `validate:"required"`
	OrgID         string `validate:"required,uuid"`
	UserID        string `validate:"required,uuid"`
	TraceID       string
}

type ToolDTOAdapter struct {
	validator *validator.Validate
}

func NewToolDTOAdapter(v *validator.Validate) *ToolDTOAdapter {
	log.Trace("NewToolDTOAdapter")

	if v == nil {
		log.Fatal("tool dto validator is required")
	}
	return &ToolDTOAdapter{validator: v}
}

func (a *ToolDTOAdapter) FromListAvailableToolsRequest(req *toolspb.ListAvailableToolsRequest) (model.ListAvailableToolsCommand, error) {
	log.Trace("ToolDTOAdapter FromListAvailableToolsRequest")

	if req == nil {
		return model.ListAvailableToolsCommand{}, domain.ErrValidationFailed.Extend("list tools request is required")
	}
	dto := listAvailableToolsRequestDTO{
		OrgID:  strings.TrimSpace(req.GetOrgId()),
		UserID: strings.TrimSpace(req.GetUserId()),
	}
	if err := a.validator.Struct(dto); err != nil {
		return model.ListAvailableToolsCommand{}, domain.ErrValidationFailed.Extend(err.Error())
	}
	orgID, userID, err := parseActor(dto.OrgID, dto.UserID)
	if err != nil {
		return model.ListAvailableToolsCommand{}, err
	}
	return model.ListAvailableToolsCommand{
		OrgID:  orgID,
		UserID: userID,
	}, nil
}

func (a *ToolDTOAdapter) FromInvokeToolRequest(req *toolspb.InvokeToolRequest) (model.InvokeToolCommand, error) {
	log.Trace("ToolDTOAdapter FromInvokeToolRequest")

	if req == nil {
		return model.InvokeToolCommand{}, domain.ErrValidationFailed.Extend("invoke tool request is required")
	}
	dto := invokeToolRequestDTO{
		InvocationID:  strings.TrimSpace(req.GetInvocationId()),
		ToolName:      strings.TrimSpace(req.GetToolName()),
		ArgumentsJSON: req.GetArgumentsJson(),
		OrgID:         strings.TrimSpace(req.GetOrgId()),
		UserID:        strings.TrimSpace(req.GetUserId()),
		TraceID:       strings.TrimSpace(req.GetTraceId()),
	}
	if err := a.validator.Struct(dto); err != nil {
		return model.InvokeToolCommand{}, domain.ErrValidationFailed.Extend(err.Error())
	}
	var js any
	if err := json.Unmarshal(dto.ArgumentsJSON, &js); err != nil {
		return model.InvokeToolCommand{}, domain.ErrValidationFailed.Extend("arguments_json must contain valid JSON")
	}
	command := model.InvokeToolCommand{
		ToolName:      dto.ToolName,
		ArgumentsJSON: dto.ArgumentsJSON,
		TraceID:       dto.TraceID,
	}
	var err error
	command.InvocationID, err = parseRequiredUUID("invocation_id", dto.InvocationID)
	if err != nil {
		return model.InvokeToolCommand{}, err
	}
	command.OrgID, command.UserID, err = parseActor(dto.OrgID, dto.UserID)
	if err != nil {
		return model.InvokeToolCommand{}, err
	}
	return command, nil
}

func (a *ToolDTOAdapter) ToListAvailableToolsResponse(tools []*model.ToolDefinition) *toolspb.ListAvailableToolsResponse {
	log.Trace("ToolDTOAdapter ToListAvailableToolsResponse")

	response := &toolspb.ListAvailableToolsResponse{Tools: []*toolspb.ToolDefinition{}}
	for _, tool := range tools {
		if tool == nil {
			continue
		}
		response.Tools = append(response.Tools, &toolspb.ToolDefinition{
			Name:                  tool.Name,
			Description:           tool.Description,
			ParametersJson:        tool.ParametersJSON,
			ImplementationVersion: tool.ImplementationVersion,
		})
	}
	return response
}

func parseActor(orgIDRaw string, userIDRaw string) (uuid.UUID, uuid.UUID, error) {
	log.Trace("parseActor")

	orgID, err := parseRequiredUUID("org_id", orgIDRaw)
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	userID, err := parseRequiredUUID("user_id", userIDRaw)
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	return orgID, userID, nil
}

func parseRequiredUUID(name string, raw string) (uuid.UUID, error) {
	log.Trace("parseRequiredUUID")

	value, err := uuid.Parse(raw)
	if err != nil || value == uuid.Nil {
		return uuid.Nil, domain.ErrValidationFailed.Extend(name + " is invalid")
	}
	return value, nil
}

func (a *ToolDTOAdapter) ToInvokeToolResponse(result *model.ToolInvocationResult) *toolspb.InvokeToolResponse {
	log.Trace("ToolDTOAdapter ToInvokeToolResponse")

	if result == nil {
		return &toolspb.InvokeToolResponse{}
	}
	return &toolspb.InvokeToolResponse{
		ResultJson:            result.ResultJSON,
		IsError:               result.IsError,
		ErrorCode:             result.ErrorCode,
		ErrorMessage:          result.ErrorMessage,
		ImplementationVersion: result.ImplementationVersion,
		LatencyMs:             result.LatencyMs,
		ErrorType:             result.ErrorType.String(),
	}
}
