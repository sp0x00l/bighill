package client

import (
	"encoding/json"
	"strings"

	"agent_registry_service/pkg/domain"
	"agent_registry_service/pkg/domain/model"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type inferenceVerifierDTOAdapter struct{}

type agentSpecDTO struct {
	AgentLineage string `json:"agent_lineage"`
	ContentHash  string `json:"content_hash"`
	ModelID      string `json:"model_id"`
}

type endpointDTO struct {
	EndpointID string `json:"endpoint_id"`
}

type generateResponseDTO struct {
	AgentRunID string `json:"agent_run_id"`
	Status     string `json:"status"`
}

type agentTaskRunRequestDTO struct {
	QueryText      string `json:"query_text"`
	ServingModelID string `json:"serving_model_id,omitempty"`
}

type agentTrajectoryDTO struct {
	Run             agentRunDTO              `json:"run"`
	Steps           []agentStepDTO           `json:"steps"`
	ToolInvocations []agentToolInvocationDTO `json:"tool_invocations"`
}

type agentRunDTO struct {
	RunID            string `json:"run_id"`
	UserID           string `json:"user_id"`
	OrgID            string `json:"org_id"`
	EndpointID       string `json:"endpoint_id"`
	AgentSpecHash    string `json:"agent_spec_hash"`
	ToolsetHash      string `json:"toolset_hash"`
	EffectiveBaseID  string `json:"effective_base_id"`
	DataSnapshotHash string `json:"data_snapshot_hash"`
	Status           string `json:"status"`
	StopReason       string `json:"stop_reason"`
}

type agentToolInvocationDTO struct {
	ToolName  string          `json:"tool_name"`
	ErrorType string          `json:"error_type"`
	Result    json.RawMessage `json:"result"`
}

type agentStepDTO struct {
	StepIndex        int             `json:"step_index"`
	GenerationResult json.RawMessage `json:"generation_result"`
}

type agentGenerationResultDTO struct {
	Content string `json:"content"`
}

type agentToolResultDTO struct {
	Contexts []agentRetrievedContextDTO `json:"contexts"`
}

type agentRetrievedContextDTO struct {
	SourceText string `json:"source_text"`
}

func newInferenceVerifierDTOAdapter() inferenceVerifierDTOAdapter {
	log.Trace("newInferenceVerifierDTOAdapter")

	return inferenceVerifierDTOAdapter{}
}

func (a inferenceVerifierDTOAdapter) FromAgentSpecDTO(orgID uuid.UUID, dto agentSpecDTO) (*model.AgentSpecRef, error) {
	log.Trace("inferenceVerifierDTOAdapter FromAgentSpecDTO")

	modelID, err := uuid.Parse(dto.ModelID)
	if err != nil {
		return nil, domain.ErrAgentSpecUnavailable.Extend("inference agent spec model_id is invalid")
	}
	return &model.AgentSpecRef{
		OrgID:         orgID,
		AgentSpecHash: dto.ContentHash,
		AgentLineage:  dto.AgentLineage,
		ModelID:       modelID,
	}, nil
}

func (a inferenceVerifierDTOAdapter) FromEndpointDTO(orgID uuid.UUID, dto endpointDTO) (*model.EndpointRef, error) {
	log.Trace("inferenceVerifierDTOAdapter FromEndpointDTO")

	endpointID, err := uuid.Parse(dto.EndpointID)
	if err != nil {
		return nil, domain.ErrEndpointUnavailable.Extend("inference endpoint_id is invalid")
	}
	return &model.EndpointRef{OrgID: orgID, EndpointID: endpointID}, nil
}

func (a inferenceVerifierDTOAdapter) ToAgentTaskRunDTO(command model.AgentTaskRunCommand) agentTaskRunRequestDTO {
	log.Trace("inferenceVerifierDTOAdapter ToAgentTaskRunDTO")

	dto := agentTaskRunRequestDTO{QueryText: command.QueryText}
	if command.ServingModelID != uuid.Nil {
		dto.ServingModelID = command.ServingModelID.String()
	}
	return dto
}

func (a inferenceVerifierDTOAdapter) FromGenerateResponseDTO(dto generateResponseDTO) (uuid.UUID, error) {
	log.Trace("inferenceVerifierDTOAdapter FromGenerateResponseDTO")

	runID, err := uuid.Parse(dto.AgentRunID)
	if err != nil || runID == uuid.Nil {
		return uuid.Nil, domain.ErrAgentEvalFailed.Extend("inference returned invalid eval run id")
	}
	return runID, nil
}

func (a inferenceVerifierDTOAdapter) FromAgentTaskRunResultDTO(dto agentTrajectoryDTO) (model.AgentTaskRunResult, error) {
	log.Trace("inferenceVerifierDTOAdapter FromAgentTaskRunResultDTO")

	runID, err := parseAgentRunIDFromDTO(dto.Run)
	if err != nil {
		return model.AgentTaskRunResult{}, err
	}
	answer, err := a.finalAnswerFromDTO(dto.Steps)
	if err != nil {
		return model.AgentTaskRunResult{}, err
	}
	groundedContextTexts, err := a.groundedContextTextsFromDTO(dto.ToolInvocations)
	if err != nil {
		return model.AgentTaskRunResult{}, err
	}
	invocations := make([]model.AgentTaskToolInvocation, 0, len(dto.ToolInvocations))
	for _, invocation := range dto.ToolInvocations {
		invocations = append(invocations, model.AgentTaskToolInvocation{
			ToolName:  invocation.ToolName,
			ErrorType: invocation.ErrorType,
		})
	}
	return model.AgentTaskRunResult{
		RunID:                runID,
		Status:               dto.Run.Status,
		StopReason:           dto.Run.StopReason,
		Answer:               answer,
		GroundedContextCount: len(groundedContextTexts),
		GroundedContextTexts: groundedContextTexts,
		ToolInvocations:      invocations,
	}, nil
}

func (a inferenceVerifierDTOAdapter) FromAgentTrajectoryDTO(dto agentTrajectoryDTO) (*model.AgentTrajectoryRef, error) {
	log.Trace("inferenceVerifierDTOAdapter FromAgentTrajectoryDTO")

	runID, err := parseAgentRunIDFromDTO(dto.Run)
	if err != nil {
		return nil, err
	}
	orgID, err := uuid.Parse(dto.Run.OrgID)
	if err != nil {
		return nil, domain.ErrAgentEvalFailed.Extend("agent trajectory org_id is invalid")
	}
	userID, err := uuid.Parse(dto.Run.UserID)
	if err != nil {
		return nil, domain.ErrAgentEvalFailed.Extend("agent trajectory user_id is invalid")
	}
	endpointID := uuid.Nil
	if strings.TrimSpace(dto.Run.EndpointID) != "" {
		parsed, err := uuid.Parse(dto.Run.EndpointID)
		if err != nil {
			return nil, domain.ErrAgentEvalFailed.Extend("agent trajectory endpoint_id is invalid")
		}
		endpointID = parsed
	}
	return &model.AgentTrajectoryRef{
		RunID:            runID,
		OrgID:            orgID,
		UserID:           userID,
		EndpointID:       endpointID,
		AgentSpecHash:    dto.Run.AgentSpecHash,
		ToolsetHash:      dto.Run.ToolsetHash,
		EffectiveBaseID:  dto.Run.EffectiveBaseID,
		DataSnapshotHash: dto.Run.DataSnapshotHash,
		Status:           dto.Run.Status,
		StopReason:       dto.Run.StopReason,
	}, nil
}

func (a inferenceVerifierDTOAdapter) finalAnswerFromDTO(steps []agentStepDTO) (string, error) {
	log.Trace("inferenceVerifierDTOAdapter finalAnswerFromDTO")

	if len(steps) == 0 {
		return "", nil
	}
	latestIndex := -1
	answer := ""
	for _, step := range steps {
		if len(step.GenerationResult) == 0 {
			continue
		}
		var generation agentGenerationResultDTO
		if err := json.Unmarshal(step.GenerationResult, &generation); err != nil {
			return "", domain.ErrAgentEvalFailed.Extend("agent generation result is invalid")
		}
		if step.StepIndex >= latestIndex {
			latestIndex = step.StepIndex
			answer = strings.TrimSpace(generation.Content)
		}
	}
	return answer, nil
}

func (a inferenceVerifierDTOAdapter) groundedContextTextsFromDTO(invocations []agentToolInvocationDTO) ([]string, error) {
	log.Trace("inferenceVerifierDTOAdapter groundedContextTextsFromDTO")

	contexts := []string{}
	for _, invocation := range invocations {
		if len(invocation.Result) == 0 {
			continue
		}
		var result agentToolResultDTO
		if err := json.Unmarshal(invocation.Result, &result); err != nil {
			return nil, domain.ErrAgentEvalFailed.Extend("agent tool result is invalid")
		}
		for _, context := range result.Contexts {
			sourceText := strings.TrimSpace(context.SourceText)
			if sourceText != "" {
				contexts = append(contexts, sourceText)
			}
		}
	}
	return contexts, nil
}

func parseAgentRunIDFromDTO(dto agentRunDTO) (uuid.UUID, error) {
	log.Trace("parseAgentRunIDFromDTO")

	runID, err := uuid.Parse(dto.RunID)
	if err != nil {
		return uuid.Nil, domain.ErrAgentEvalFailed.Extend("agent trajectory run_id is invalid")
	}
	return runID, nil
}
