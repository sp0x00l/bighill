package adapter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"

	contractschemas "lib/data_contracts_lib/schemas"
	serializers "lib/shared_lib/serializer"

	"github.com/go-playground/validator/v10"
	"github.com/santhosh-tekuri/jsonschema/v6"
	log "github.com/sirupsen/logrus"
	"go.yaml.in/yaml/v3"
)

const (
	agentSpecSchemaResource = "https://bighill.ai/schemas/agent_spec.schema.json"
)

type AgentSpecDTOAdapter interface {
	FromDTO(ctx context.Context, body []byte) (model.AgentSpecPublication, error)
	ToDTO(ctx context.Context, spec *model.AgentSpec) ([]byte, error)
}

type agentSpecDTOAdapter struct {
	validator           *validator.Validate
	encoder             *serializers.Encoder
	agentMaxStepsCap    int
	agentTokenBudgetCap int
}

type AgentSpecDTOAdapterOption func(*agentSpecDTOAdapter)

type AgentSpecDTO struct {
	AgentSpecID      string `json:"agent_spec_id"`
	AgentLineage     string `json:"agent_lineage"`
	SchemaVersion    string `json:"schema_version"`
	ContentHash      string `json:"content_hash"`
	ValidationReport string `json:"validation_report"`
	ModelID          string `json:"model_id"`
	EffectiveBaseID  string `json:"effective_base_id,omitempty"`
	RuntimeMode      string `json:"runtime_mode"`
	Status           string `json:"status"`
}

type agentSpecDocumentDTO struct {
	SchemaVersion   string                `json:"schema_version" yaml:"schema_version" validate:"required"`
	AgentLineage    string                `json:"agent_lineage" yaml:"agent_lineage" validate:"required"`
	SystemPrompt    string                `json:"system_prompt,omitempty" yaml:"system_prompt"`
	RuntimeMode     string                `json:"runtime_mode" yaml:"runtime_mode" validate:"required,oneof=interactive durable"`
	ModelBinding    agentModelBindingDTO  `json:"model_binding" yaml:"model_binding" validate:"required"`
	Tools           []agentToolBindingDTO `json:"tools" yaml:"tools" validate:"omitempty,dive"`
	RetrievalConfig map[string]any        `json:"retrieval_config,omitempty" yaml:"retrieval_config"`
	Budgets         agentBudgetsDTO       `json:"budgets" yaml:"budgets" validate:"required"`
	StopConditions  map[string]any        `json:"stop_conditions,omitempty" yaml:"stop_conditions"`
	Guardrails      map[string]any        `json:"guardrails,omitempty" yaml:"guardrails"`
}

type agentBudgetsDTO struct {
	MaxSteps int `json:"max_steps" yaml:"max_steps" validate:"required,min=1"`
	Token    int `json:"token" yaml:"token" validate:"required,min=1"`
}

type agentModelBindingDTO struct {
	ModelID         string `json:"model_id" yaml:"model_id" validate:"required,uuid,ne=00000000-0000-0000-0000-000000000000"`
	EffectiveBaseID string `json:"effective_base_id" yaml:"effective_base_id" validate:"required,uuid,ne=00000000-0000-0000-0000-000000000000"`
}

type agentToolBindingDTO struct {
	Name       string         `json:"name" yaml:"name" validate:"required"`
	Required   bool           `json:"required" yaml:"required"`
	ToolChoice string         `json:"tool_choice,omitempty" yaml:"tool_choice"`
	Config     map[string]any `json:"config,omitempty" yaml:"config"`
}

func WithAgentSpecBudgetCaps(maxSteps int, tokenBudget int) AgentSpecDTOAdapterOption {
	log.Trace("WithAgentSpecBudgetCaps")

	return func(a *agentSpecDTOAdapter) {
		a.agentMaxStepsCap = maxSteps
		a.agentTokenBudgetCap = tokenBudget
	}
}

func NewAgentSpecDTOAdapter(encoder *serializers.Encoder, opts ...AgentSpecDTOAdapterOption) *agentSpecDTOAdapter {
	log.Trace("NewAgentSpecDTOAdapter")

	adapter := &agentSpecDTOAdapter{
		validator: validator.New(),
		encoder:   encoder,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(adapter)
		}
	}
	return adapter
}

func (a *agentSpecDTOAdapter) FromDTO(ctx context.Context, body []byte) (model.AgentSpecPublication, error) {
	log.Trace("AgentSpecDTOAdapter FromDTO")

	sourceCanonical, report, err := validateAgentSpecSchema(body)
	if err != nil {
		return model.AgentSpecPublication{}, domain.ErrValidationFailed.Extend(err.Error())
	}
	var dto agentSpecDocumentDTO
	if err := json.Unmarshal(sourceCanonical, &dto); err != nil {
		return model.AgentSpecPublication{}, domain.ErrValidationFailed.Extend(err.Error())
	}
	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("AgentSpecDocumentDTO validation failed")
		return model.AgentSpecPublication{}, domain.ErrValidationFailed.Extend(err.Error())
	}
	canonical, err := canonicalAgentSpecJSON(dto)
	if err != nil {
		return model.AgentSpecPublication{}, domain.ErrValidationFailed.Extend(err.Error())
	}
	if err := validateAgentSpecJSON(canonical); err != nil {
		return model.AgentSpecPublication{}, domain.ErrValidationFailed.Extend(err.Error())
	}
	report = report + ";canonical_status=validated"
	return a.fromDTO(dto, string(body), canonical, report)
}

func (a *agentSpecDTOAdapter) ToDTO(ctx context.Context, spec *model.AgentSpec) ([]byte, error) {
	log.Trace("AgentSpecDTOAdapter ToDTO")

	dtos := []AgentSpecDTO{}
	if spec != nil {
		dtos = append(dtos, AgentSpecDTO{
			AgentSpecID:      spec.AgentSpecID.String(),
			AgentLineage:     spec.AgentLineage,
			SchemaVersion:    spec.SchemaVersion,
			ContentHash:      spec.ContentHash,
			ValidationReport: spec.ValidationReport,
			ModelID:          spec.ModelID.String(),
			EffectiveBaseID:  optionalUUIDString(spec.EffectiveBaseID),
			RuntimeMode:      spec.RuntimeMode.String(),
			Status:           spec.Status.String(),
		})
	}
	encoded, err := a.encoder.EncodeDataToString(dtos)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("AgentSpecDTO encoding failed")
		return nil, err
	}
	return []byte(encoded), nil
}

func (a *agentSpecDTOAdapter) fromDTO(dto agentSpecDocumentDTO, sourceYAML string, canonical []byte, validationReport string) (model.AgentSpecPublication, error) {
	log.Trace("AgentSpecDTOAdapter fromDTO")

	modelID, err := parseRequiredUUID(dto.ModelBinding.ModelID, "agent spec model_binding.model_id is invalid")
	if err != nil {
		return model.AgentSpecPublication{}, err
	}
	effectiveBaseID, err := parseRequiredUUID(dto.ModelBinding.EffectiveBaseID, "agent spec model_binding.effective_base_id is invalid")
	if err != nil {
		return model.AgentSpecPublication{}, err
	}
	toolBindings, err := toolBindingsFromDTO(dto.Tools)
	if err != nil {
		return model.AgentSpecPublication{}, err
	}
	retrievalConfig, err := marshalMap(dto.RetrievalConfig)
	if err != nil {
		return model.AgentSpecPublication{}, err
	}
	stopConditions, err := marshalMap(dto.StopConditions)
	if err != nil {
		return model.AgentSpecPublication{}, err
	}
	guardrails, err := marshalMap(dto.Guardrails)
	if err != nil {
		return model.AgentSpecPublication{}, err
	}
	if err := a.validateBudgetCaps(dto.Budgets, toolBindings); err != nil {
		return model.AgentSpecPublication{}, err
	}
	runtimeMode, err := agentRuntimeModeFromDTO(dto.RuntimeMode)
	if err != nil {
		return model.AgentSpecPublication{}, err
	}
	validationReport = validationReport + ";policy_status=validated"
	spec := &model.AgentSpec{
		AgentLineage:     strings.TrimSpace(dto.AgentLineage),
		SystemPrompt:     strings.TrimSpace(dto.SystemPrompt),
		SourceYAML:       sourceYAML,
		CanonicalJSON:    canonical,
		SchemaVersion:    strings.TrimSpace(dto.SchemaVersion),
		ContentHash:      model.CanonicalAgentSpecHash(canonical),
		ValidationReport: validationReport,
		ModelID:          modelID,
		EffectiveBaseID:  effectiveBaseID,
		ToolBindings:     toolBindings,
		RetrievalConfig:  retrievalConfig,
		Budgets: model.AgentBudgets{
			MaxSteps: dto.Budgets.MaxSteps,
			Token:    dto.Budgets.Token,
		},
		StopConditions: stopConditions,
		Guardrails:     guardrails,
		RuntimeMode:    runtimeMode,
		Status:         model.AgentSpecStatusValidated,
	}
	return model.AgentSpecPublication{Spec: spec}, nil
}

func (a *agentSpecDTOAdapter) validateBudgetCaps(dto agentBudgetsDTO, toolBindings []model.ToolBinding) error {
	log.Trace("AgentSpecDTOAdapter validateBudgetCaps")

	if len(toolBindings) > 0 && dto.MaxSteps < 2 {
		return domain.ErrValidationFailed.Extend("agent spec max_steps must be at least 2 when tools are configured")
	}
	if a.agentMaxStepsCap > 0 && dto.MaxSteps > a.agentMaxStepsCap {
		return domain.ErrValidationFailed.Extend("agent spec max_steps exceeds deployment cap")
	}
	if a.agentTokenBudgetCap > 0 && dto.Token > a.agentTokenBudgetCap {
		return domain.ErrValidationFailed.Extend("agent spec token budget exceeds deployment cap")
	}
	return nil
}

func validateAgentSpecSchema(sourceYAML []byte) ([]byte, string, error) {
	log.Trace("validateAgentSpecSchema")

	var document any
	if err := yaml.Unmarshal(sourceYAML, &document); err != nil {
		return nil, "", err
	}
	normalized := normalizeYAMLValue(document)
	canonical, err := canonicalJSON(normalized)
	if err != nil {
		return nil, "", err
	}
	var instance any
	if err := json.Unmarshal(canonical, &instance); err != nil {
		return nil, "", err
	}
	if err := validateAgentSpecInstance(instance); err != nil {
		return nil, "", err
	}
	return canonical, "schema=agent_spec.schema.json;schema_status=validated", nil
}

func validateAgentSpecJSON(document []byte) error {
	log.Trace("validateAgentSpecJSON")

	var instance any
	if err := json.Unmarshal(document, &instance); err != nil {
		return err
	}
	return validateAgentSpecInstance(instance)
}

func validateAgentSpecInstance(instance any) error {
	log.Trace("validateAgentSpecInstance")

	schema, err := compileAgentSpecSchema()
	if err != nil {
		return err
	}
	return schema.Validate(instance)
}

func canonicalAgentSpecJSON(dto agentSpecDocumentDTO) ([]byte, error) {
	log.Trace("canonicalAgentSpecJSON")

	if dto.Tools == nil {
		dto.Tools = []agentToolBindingDTO{}
	}
	return canonicalJSON(dto)
}

func compileAgentSpecSchema() (*jsonschema.Schema, error) {
	log.Trace("compileAgentSpecSchema")

	var schemaDocument any
	if err := json.Unmarshal(contractschemas.AgentSpecSchema(), &schemaDocument); err != nil {
		return nil, err
	}
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource(agentSpecSchemaResource, schemaDocument); err != nil {
		return nil, err
	}
	return compiler.Compile(agentSpecSchemaResource)
}

func canonicalJSON(value any) ([]byte, error) {
	log.Trace("canonicalJSON")

	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return bytes.TrimSpace(buffer.Bytes()), nil
}

func normalizeYAMLValue(value any) any {
	log.Trace("normalizeYAMLValue")

	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = normalizeYAMLValue(item)
		}
		return out
	case map[any]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[fmt.Sprint(key)] = normalizeYAMLValue(item)
		}
		return out
	case []any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, normalizeYAMLValue(item))
		}
		return out
	default:
		return typed
	}
}

func agentRuntimeModeFromDTO(value string) (model.AgentRuntimeMode, error) {
	log.Trace("agentRuntimeModeFromDTO")

	runtimeMode, err := model.ToAgentRuntimeMode(value)
	if err != nil {
		return model.AgentRuntimeModeUnknown, domain.ErrValidationFailed.Extend(err.Error())
	}
	return runtimeMode, nil
}

func toolBindingsFromDTO(dtos []agentToolBindingDTO) ([]model.ToolBinding, error) {
	log.Trace("toolBindingsFromDTO")

	out := make([]model.ToolBinding, 0, len(dtos))
	for _, dto := range dtos {
		name := strings.TrimSpace(dto.Name)
		config, err := marshalMap(dto.Config)
		if err != nil {
			return nil, err
		}
		out = append(out, model.ToolBinding{
			Name:       name,
			Required:   dto.Required,
			ToolChoice: strings.TrimSpace(dto.ToolChoice),
			Config:     config,
		})
	}
	return out, nil
}
