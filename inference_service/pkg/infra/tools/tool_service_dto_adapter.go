package tools

import (
	"encoding/json"
	"fmt"
	"strings"

	"inference_service/pkg/app"
	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"

	toolspb "lib/data_contracts_lib/tools"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type toolServiceDefinitionDTO struct {
	Name                  string `validate:"required"`
	Description           string
	ParametersJSON        []byte `validate:"required"`
	ImplementationVersion string `validate:"required"`
}

type toolServiceDTOAdapter struct {
	validator *validator.Validate
}

func newToolServiceDTOAdapter(v *validator.Validate) *toolServiceDTOAdapter {
	log.Trace("newToolServiceDTOAdapter")

	if v == nil {
		log.Fatal("tool service dto validator is required")
	}
	return &toolServiceDTOAdapter{validator: v}
}

func (a *toolServiceDTOAdapter) ToListAvailableToolsRequest(resolution app.ToolResolutionContext) (*toolspb.ListAvailableToolsRequest, error) {
	log.Trace("toolServiceDTOAdapter ToListAvailableToolsRequest")

	if resolution.OrgID == uuid.Nil || resolution.UserID == uuid.Nil {
		return nil, domain.ErrValidationFailed.Extend("tool resolution context requires org_id and user_id")
	}
	return &toolspb.ListAvailableToolsRequest{
		OrgId:  resolution.OrgID.String(),
		UserId: resolution.UserID.String(),
	}, nil
}

func (a *toolServiceDTOAdapter) FromListAvailableToolsResponse(resp *toolspb.ListAvailableToolsResponse, bindings []model.ToolBinding) ([]model.ToolSpec, error) {
	log.Trace("toolServiceDTOAdapter FromListAvailableToolsResponse")

	if resp == nil {
		return nil, domain.ErrValidationFailed.Extend("tool service list response is required")
	}
	available := map[string]model.ToolSpec{}
	for _, tool := range resp.GetTools() {
		if tool == nil {
			continue
		}
		dto := toolServiceDefinitionDTO{
			Name:                  strings.TrimSpace(tool.GetName()),
			Description:           strings.TrimSpace(tool.GetDescription()),
			ParametersJSON:        tool.GetParametersJson(),
			ImplementationVersion: strings.TrimSpace(tool.GetImplementationVersion()),
		}
		if err := a.validator.Struct(dto); err != nil {
			return nil, domain.ErrValidationFailed.Extend(fmt.Sprintf("tool service definition is invalid: %v", err))
		}
		if !json.Valid(dto.ParametersJSON) {
			return nil, domain.ErrValidationFailed.Extend("tool service parameters_json must contain valid JSON")
		}
		available[toolNameKey(dto.Name)] = model.ToolSpec{
			Name:                  dto.Name,
			Description:           dto.Description,
			Parameters:            json.RawMessage(dto.ParametersJSON),
			ImplementationVersion: dto.ImplementationVersion,
			Locality:              "remote",
		}
	}
	specs := make([]model.ToolSpec, 0, len(bindings))
	for _, binding := range bindings {
		name := strings.TrimSpace(binding.Name)
		spec, ok := available[toolNameKey(name)]
		if !ok {
			return nil, domain.ErrValidationFailed.Extend("agent spec references unavailable tool " + name)
		}
		specs = append(specs, spec)
	}
	return specs, nil
}

func (a *toolServiceDTOAdapter) ToInvokeToolRequest(invocation app.ToolInvocationContext, call model.ToolCall, invocationID uuid.UUID) (*toolspb.InvokeToolRequest, error) {
	log.Trace("toolServiceDTOAdapter ToInvokeToolRequest")

	if invocation.OrgID == uuid.Nil || invocation.UserID == uuid.Nil || invocation.RunID == uuid.Nil {
		return nil, domain.ErrValidationFailed.Extend("tool invocation context requires org_id, user_id, and run_id")
	}
	if strings.TrimSpace(call.Name) == "" {
		return nil, domain.ErrValidationFailed.Extend("tool call name is required")
	}
	if !json.Valid(call.Arguments) {
		return nil, domain.ErrValidationFailed.Extend("tool call arguments must contain valid JSON")
	}
	return &toolspb.InvokeToolRequest{
		ToolName:      strings.TrimSpace(call.Name),
		ArgumentsJson: call.Arguments,
		OrgId:         invocation.OrgID.String(),
		UserId:        invocation.UserID.String(),
		TraceId:       invocation.RunID.String(),
		InvocationId:  invocationID.String(),
	}, nil
}

func (a *toolServiceDTOAdapter) FromInvokeToolResponse(resp *toolspb.InvokeToolResponse, call model.ToolCall) (model.ToolResult, error) {
	log.Trace("toolServiceDTOAdapter FromInvokeToolResponse")

	if resp == nil {
		return model.ToolResult{}, domain.ErrValidationFailed.Extend("tool service invoke response is required")
	}
	resultJSON := resp.GetResultJson()
	if len(resultJSON) == 0 {
		resultJSON = []byte(`{}`)
	}
	if !json.Valid(resultJSON) {
		return model.ToolResult{}, domain.ErrValidationFailed.Extend("tool service result_json must contain valid JSON")
	}
	errorType := model.ToolErrorTypeUnknown
	if resp.GetIsError() {
		errorType = toolServiceErrorType(resp.GetErrorType(), resp.GetErrorCode())
	}
	return model.ToolResult{
		CallID:          call.ID,
		Name:            strings.TrimSpace(call.Name),
		Content:         string(resultJSON),
		IsError:         resp.GetIsError(),
		ErrorType:       errorType,
		ToolImplVersion: strings.TrimSpace(resp.GetImplementationVersion()),
	}, nil
}

func toolServiceErrorType(errorType string, errorCode string) model.ToolErrorType {
	log.Trace("toolServiceErrorType")

	if parsed, err := model.ToToolErrorType(errorType); err == nil {
		return parsed
	}
	switch strings.TrimSpace(errorCode) {
	case "tool_policy_violation", "tool_denied", "validation_failed", "tool_not_found":
		return model.ToolErrorTypePolicyDenied
	case "tool_execution_failed":
		return model.ToolErrorTypeTransient
	case "http_tool_request_failed":
		return model.ToolErrorTypeTransient
	default:
		return model.ToolErrorTypeUnknown
	}
}

func toolNameKey(value string) string {
	log.Trace("toolNameKey")

	return strings.ToLower(strings.TrimSpace(value))
}
