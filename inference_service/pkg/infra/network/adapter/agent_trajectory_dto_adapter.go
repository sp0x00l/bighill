package adapter

import (
	"context"
	"encoding/json"
	"time"

	"inference_service/pkg/domain/model"
	serializers "lib/shared_lib/serializer"

	log "github.com/sirupsen/logrus"
)

type AgentTrajectoryDTOAdapter interface {
	ToDTO(ctx context.Context, trajectory *model.AgentTrajectory) ([]byte, error)
}

type agentTrajectoryDTOAdapter struct {
	encoder *serializers.Encoder
}

type AgentTrajectoryDTO struct {
	Run             AgentRunDTO              `json:"run"`
	Steps           []AgentStepDTO           `json:"steps"`
	ToolInvocations []AgentToolInvocationDTO `json:"tool_invocations"`
}

type AgentRunDTO struct {
	RunID                   string          `json:"run_id"`
	UserID                  string          `json:"user_id"`
	OrgID                   string          `json:"org_id"`
	EndpointID              string          `json:"endpoint_id,omitempty"`
	AgentSpecHash           string          `json:"agent_spec_hash"`
	EffectiveBaseID         string          `json:"effective_base_id,omitempty"`
	ModelVersion            int             `json:"model_version"`
	ToolsetHash             string          `json:"toolset_hash"`
	RubricVersion           string          `json:"rubric_version"`
	TrajectorySchemaVersion string          `json:"trajectory_schema_version"`
	SystemTemplateVersion   string          `json:"system_template_version"`
	DecodingParams          json.RawMessage `json:"decoding_params"`
	Status                  string          `json:"status"`
	StopReason              string          `json:"stop_reason,omitempty"`
	StartedAt               string          `json:"started_at"`
	FinishedAt              string          `json:"finished_at,omitempty"`
	TotalTokens             int             `json:"total_tokens"`
	TrainingEligibility     string          `json:"training_eligibility"`
}

type AgentStepDTO struct {
	StepID               string          `json:"step_id"`
	RunID                string          `json:"run_id"`
	StepIndex            int             `json:"step_index"`
	PresentedToolSchemas json.RawMessage `json:"presented_tool_schemas"`
	GenerationResult     json.RawMessage `json:"generation_result"`
	FinishReason         string          `json:"finish_reason"`
	PromptTokens         int             `json:"prompt_tokens"`
	CompletionTokens     int             `json:"completion_tokens"`
	CreatedAt            string          `json:"created_at"`
}

type AgentToolInvocationDTO struct {
	InvocationID    string          `json:"invocation_id"`
	StepID          string          `json:"step_id"`
	RunID           string          `json:"run_id"`
	ToolName        string          `json:"tool_name"`
	ToolImplVersion string          `json:"tool_impl_version"`
	Arguments       json.RawMessage `json:"arguments"`
	Result          json.RawMessage `json:"result"`
	ErrorType       string          `json:"error_type,omitempty"`
	LatencyMs       int64           `json:"latency_ms"`
	CreatedAt       string          `json:"created_at"`
}

func NewAgentTrajectoryDTOAdapter(encoder *serializers.Encoder) *agentTrajectoryDTOAdapter {
	log.Trace("NewAgentTrajectoryDTOAdapter")

	return &agentTrajectoryDTOAdapter{encoder: encoder}
}

func (a *agentTrajectoryDTOAdapter) ToDTO(ctx context.Context, trajectory *model.AgentTrajectory) ([]byte, error) {
	log.Trace("AgentTrajectoryDTOAdapter ToDTO")

	dto := AgentTrajectoryDTO{
		Steps:           []AgentStepDTO{},
		ToolInvocations: []AgentToolInvocationDTO{},
	}
	if trajectory != nil && trajectory.Run != nil {
		dto.Run = agentRunDTO(trajectory.Run)
		for _, step := range trajectory.Steps {
			if step == nil {
				continue
			}
			dto.Steps = append(dto.Steps, agentStepDTO(step))
		}
		for _, invocation := range trajectory.ToolInvocations {
			if invocation == nil {
				continue
			}
			dto.ToolInvocations = append(dto.ToolInvocations, agentToolInvocationDTO(invocation))
		}
	}
	encoded, err := a.encoder.EncodeDataToString(dto)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("AgentTrajectoryDTO encoding failed")
		return nil, err
	}
	return []byte(encoded), nil
}

func agentRunDTO(run *model.AgentRun) AgentRunDTO {
	log.Trace("agentRunDTO")

	return AgentRunDTO{
		RunID:                   run.RunID.String(),
		UserID:                  run.UserID.String(),
		OrgID:                   run.OrgID.String(),
		EndpointID:              optionalUUIDString(run.EndpointID),
		AgentSpecHash:           run.AgentSpecHash,
		EffectiveBaseID:         optionalUUIDString(run.EffectiveBaseID),
		ModelVersion:            run.ModelVersion,
		ToolsetHash:             run.ToolsetHash,
		RubricVersion:           run.RubricVersion,
		TrajectorySchemaVersion: run.TrajectorySchemaVersion,
		SystemTemplateVersion:   run.SystemTemplateVersion,
		DecodingParams:          jsonRawOrEmptyObject(run.DecodingParams),
		Status:                  run.Status.String(),
		StopReason:              optionalStopReason(run.StopReason),
		StartedAt:               timeString(run.StartedAt),
		FinishedAt:              timeString(run.FinishedAt),
		TotalTokens:             run.TotalTokens,
		TrainingEligibility:     run.TrainingEligibility.String(),
	}
}

func agentStepDTO(step *model.AgentStep) AgentStepDTO {
	log.Trace("agentStepDTO")

	return AgentStepDTO{
		StepID:               step.StepID.String(),
		RunID:                step.RunID.String(),
		StepIndex:            step.StepIndex,
		PresentedToolSchemas: jsonRawOrEmptyArray(step.PresentedToolSchemas),
		GenerationResult:     jsonRawOrEmptyObject(step.GenerationResult),
		FinishReason:         string(step.FinishReason),
		PromptTokens:         step.PromptTokens,
		CompletionTokens:     step.CompletionTokens,
		CreatedAt:            timeString(step.CreatedAt),
	}
}

func agentToolInvocationDTO(invocation *model.AgentToolInvocation) AgentToolInvocationDTO {
	log.Trace("agentToolInvocationDTO")

	return AgentToolInvocationDTO{
		InvocationID:    invocation.InvocationID.String(),
		StepID:          invocation.StepID.String(),
		RunID:           invocation.RunID.String(),
		ToolName:        invocation.ToolName,
		ToolImplVersion: invocation.ToolImplVersion,
		Arguments:       jsonRawOrEmptyObject(invocation.Arguments),
		Result:          jsonRawOrEmptyObject(invocation.Result),
		ErrorType:       optionalToolErrorType(invocation.ErrorType),
		LatencyMs:       invocation.LatencyMs,
		CreatedAt:       timeString(invocation.CreatedAt),
	}
}

func optionalStopReason(value model.AgentStopReason) string {
	log.Trace("optionalStopReason")

	if value == model.AgentStopReasonUnknown {
		return ""
	}
	return value.String()
}

func optionalToolErrorType(value model.ToolErrorType) string {
	log.Trace("optionalToolErrorType")

	if value == model.ToolErrorTypeUnknown {
		return ""
	}
	return value.String()
}

func timeString(value time.Time) string {
	log.Trace("timeString")

	if value.IsZero() || value.Equal(time.Unix(0, 0).UTC()) {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func jsonRawOrEmptyObject(value json.RawMessage) json.RawMessage {
	log.Trace("jsonRawOrEmptyObject")

	if len(value) == 0 {
		return json.RawMessage(`{}`)
	}
	return value
}

func jsonRawOrEmptyArray(value json.RawMessage) json.RawMessage {
	log.Trace("jsonRawOrEmptyArray")

	if len(value) == 0 {
		return json.RawMessage(`[]`)
	}
	return value
}
