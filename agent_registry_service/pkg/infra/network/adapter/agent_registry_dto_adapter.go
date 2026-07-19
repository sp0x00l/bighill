package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"agent_registry_service/pkg/domain"
	"agent_registry_service/pkg/domain/model"
	serializers "lib/shared_lib/serializer"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type AgentRegistryDTOAdapter interface {
	FromRegisterSpecVersionDTO(ctx context.Context, body []byte) (model.RegisterAgentSpecVersionCommand, error)
	FromRegisterEndpointBindingDTO(ctx context.Context, body []byte) (model.RegisterEndpointBindingCommand, error)
	FromPromoteSpecChampionDTO(ctx context.Context, body []byte) (model.PromoteSpecChampionCommand, error)
	FromImportGoldenTasksDTO(ctx context.Context, body []byte) (model.ImportGoldenTasksCommand, error)
	FromListGoldenTasksDTO(ctx context.Context, values map[string]string) (model.ListGoldenTasksCommand, error)
	FromLabelAgentRunDTO(ctx context.Context, body []byte) (model.LabelAgentRunCommand, error)
	FromListAgentRunLabelsDTO(ctx context.Context, values map[string]string) (model.ListAgentRunLabelsCommand, error)
	FromBuildTrajectoryDatasetDTO(ctx context.Context, body []byte) (model.BuildTrajectoryDatasetCommand, error)
	FromDispatchAgentAdapterTrainingDTO(ctx context.Context, body []byte) (model.DispatchAgentAdapterTrainingCommand, error)
	FromEvaluateAdapterCandidateDTO(ctx context.Context, body []byte) (model.EvaluateAdapterCandidateCommand, error)
	FromEvaluateSpecChampionDTO(ctx context.Context, body []byte) (model.EvaluateSpecChampionCommand, error)
	FromPromoteAgentAdapterDTO(ctx context.Context, body []byte) (model.PromoteAgentAdapterCommand, error)
	ToSpecVersionDTO(ctx context.Context, version *model.AgentSpecVersion) ([]byte, error)
	ToEndpointBindingDTO(ctx context.Context, binding *model.AgentEndpointBinding) ([]byte, error)
	ToChampionStateDTO(ctx context.Context, state *model.AgentChampionState) ([]byte, error)
	ToGoldenTaskDTOs(ctx context.Context, tasks []*model.GoldenTask) ([]byte, error)
	ToAgentRunLabelDTOs(ctx context.Context, labels []*model.AgentRunLabel) ([]byte, error)
	ToTrajectoryDatasetDTO(ctx context.Context, dataset *model.AgentTrajectoryDataset) ([]byte, error)
	ToAgentAdapterDTO(ctx context.Context, adapter *model.AgentAdapter) ([]byte, error)
	ToAgentEvalReportDTO(ctx context.Context, report *model.AgentEvalReport) ([]byte, error)
}

type agentRegistryDTOAdapter struct {
	validator *validator.Validate
	encoder   *serializers.Encoder
}

type RegisterAgentSpecVersionDTO struct {
	AgentLineage  string `json:"agent_lineage" validate:"required"`
	AgentSpecHash string `json:"agent_spec_hash" validate:"required"`
}

type RegisterEndpointBindingDTO struct {
	AgentLineage string `json:"agent_lineage" validate:"required"`
	EndpointID   string `json:"endpoint_id" validate:"required,uuid,ne=00000000-0000-0000-0000-000000000000"`
}

type PromoteSpecChampionDTO struct {
	AgentLineage  string `json:"agent_lineage" validate:"required"`
	AgentSpecHash string `json:"agent_spec_hash" validate:"required"`
	DecisionID    string `json:"decision_id" validate:"omitempty,uuid,ne=00000000-0000-0000-0000-000000000000"`
	DecidedAt     string `json:"decided_at" validate:"omitempty"`
}

type GoldenTaskInputDTO struct {
	GroupKey               string `json:"group_key,omitempty"`
	Prompt                 string `json:"prompt" validate:"required"`
	ExpectedToolPlanHash   string `json:"expected_tool_plan_hash,omitempty"`
	ExpectedAnswer         string `json:"expected_answer" validate:"required"`
	ExpectedAnswerRubricID string `json:"expected_answer_rubric_id" validate:"required"`
	LabelsHash             string `json:"labels_hash,omitempty"`
}

type ImportGoldenTasksDTO struct {
	AgentLineage string               `json:"agent_lineage" validate:"required"`
	Split        string               `json:"split" validate:"required"`
	SplitVersion int                  `json:"split_version" validate:"required,min=1"`
	Tasks        []GoldenTaskInputDTO `json:"tasks" validate:"required,min=1,dive"`
}

type ListGoldenTasksDTO struct {
	AgentLineage string `json:"agent_lineage" validate:"required"`
	Split        string `json:"split" validate:"required"`
	SplitVersion int    `json:"split_version" validate:"required,min=1"`
}

type EvaluateSpecChampionDTO struct {
	AgentLineage        string   `json:"agent_lineage" validate:"required"`
	AgentSpecHash       string   `json:"agent_spec_hash" validate:"required"`
	EndpointID          string   `json:"endpoint_id" validate:"required,uuid,ne=00000000-0000-0000-0000-000000000000"`
	SplitVersion        int      `json:"split_version" validate:"required,min=1"`
	MinTaskSuccessRate  *float64 `json:"min_task_success_rate" validate:"omitempty,min=0,max=1"`
	MinToolSuccessRate  *float64 `json:"min_tool_success_rate" validate:"omitempty,min=0,max=1"`
	MinGroundednessRate *float64 `json:"min_groundedness_rate" validate:"omitempty,min=0,max=1"`
}

type LabelAgentRunDTO struct {
	AgentLineage       string  `json:"agent_lineage" validate:"required"`
	RunID              string  `json:"run_id" validate:"required,uuid,ne=00000000-0000-0000-0000-000000000000"`
	Prompt             string  `json:"prompt" validate:"required"`
	Evaluator          string  `json:"evaluator" validate:"required"`
	TaskSuccess        bool    `json:"task_success"`
	ToolSelectionScore float64 `json:"tool_selection_score" validate:"min=0,max=1"`
	Groundedness       float64 `json:"groundedness" validate:"min=0,max=1"`
	PolicyViolations   int     `json:"policy_violations" validate:"min=0"`
	Confidence         float64 `json:"confidence" validate:"min=0,max=1"`
	LabelSource        string  `json:"label_source" validate:"required"`
	RubricVersion      string  `json:"rubric_version" validate:"required"`
}

type ListAgentRunLabelsDTO struct {
	AgentLineage string `json:"agent_lineage" validate:"required"`
}

type BuildTrajectoryDatasetDTO struct {
	AgentLineage       string `json:"agent_lineage" validate:"required"`
	GoldenSplitVersion int    `json:"golden_split_version" validate:"required,min=1"`
}

type DispatchAgentAdapterTrainingDTO struct {
	AgentLineage    string `json:"agent_lineage" validate:"required"`
	DatasetID       string `json:"dataset_id" validate:"required,uuid,ne=00000000-0000-0000-0000-000000000000"`
	TrainingProfile string `json:"training_profile,omitempty"`
}

type EvaluateAdapterCandidateDTO struct {
	AgentLineage        string   `json:"agent_lineage" validate:"required"`
	AdapterID           string   `json:"adapter_id" validate:"required,uuid,ne=00000000-0000-0000-0000-000000000000"`
	EndpointID          string   `json:"endpoint_id" validate:"required,uuid,ne=00000000-0000-0000-0000-000000000000"`
	SplitVersion        int      `json:"split_version" validate:"required,min=1"`
	MinTaskSuccessRate  *float64 `json:"min_task_success_rate" validate:"omitempty,min=0,max=1"`
	MinToolSuccessRate  *float64 `json:"min_tool_success_rate" validate:"omitempty,min=0,max=1"`
	MinGroundednessRate *float64 `json:"min_groundedness_rate" validate:"omitempty,min=0,max=1"`
}

type PromoteAgentAdapterDTO struct {
	AgentLineage string   `json:"agent_lineage" validate:"required"`
	AdapterID    string   `json:"adapter_id" validate:"required,uuid,ne=00000000-0000-0000-0000-000000000000"`
	ReportID     string   `json:"report_id" validate:"required,uuid,ne=00000000-0000-0000-0000-000000000000"`
	MinDelta     *float64 `json:"min_delta" validate:"omitempty,min=0,max=1"`
}

type AgentSpecVersionDTO struct {
	AgentLineage       string `json:"agent_lineage"`
	AgentSpecHash      string `json:"agent_spec_hash"`
	ModelID            string `json:"model_id"`
	RegisteredByUserID string `json:"registered_by_user_id"`
	RegisteredAt       string `json:"registered_at"`
}

type GoldenTaskDTO struct {
	TaskID                   string `json:"task_id"`
	AgentLineage             string `json:"agent_lineage"`
	Split                    string `json:"split"`
	SplitVersion             int    `json:"split_version"`
	GroupKey                 string `json:"group_key,omitempty"`
	Prompt                   string `json:"prompt"`
	NormalizedPromptHash     string `json:"normalized_prompt_hash"`
	ContentFingerprint       string `json:"content_fingerprint"`
	NearDuplicateFingerprint string `json:"near_duplicate_fingerprint"`
	ExpectedToolPlanHash     string `json:"expected_tool_plan_hash,omitempty"`
	ExpectedAnswer           string `json:"expected_answer"`
	ExpectedAnswerRubricID   string `json:"expected_answer_rubric_id"`
	LabelsHash               string `json:"labels_hash,omitempty"`
	CreatedByUserID          string `json:"created_by_user_id"`
	CreatedAt                string `json:"created_at"`
}

type AgentEvalReportDTO struct {
	ReportID           string                   `json:"report_id"`
	AgentLineage       string                   `json:"agent_lineage"`
	AgentSpecHash      string                   `json:"agent_spec_hash"`
	AdapterID          string                   `json:"adapter_id,omitempty"`
	EndpointID         string                   `json:"endpoint_id"`
	Split              string                   `json:"split"`
	SplitVersion       int                      `json:"split_version"`
	RubricVersion      string                   `json:"rubric_version"`
	TaskCount          int                      `json:"task_count"`
	TaskSuccessRate    float64                  `json:"task_success_rate"`
	ToolSuccessRate    float64                  `json:"tool_success_rate"`
	GroundednessRate   float64                  `json:"groundedness_rate"`
	Passed             bool                     `json:"passed"`
	GateReason         string                   `json:"gate_reason"`
	PromotedDecisionID string                   `json:"promoted_decision_id,omitempty"`
	EvaluatedBy        string                   `json:"evaluated_by"`
	EvaluatedAt        string                   `json:"evaluated_at"`
	TaskResults        []AgentEvalTaskResultDTO `json:"task_results"`
}

type AgentRunLabelDTO struct {
	LabelID                  string  `json:"label_id"`
	RunID                    string  `json:"run_id"`
	AgentLineage             string  `json:"agent_lineage"`
	AgentSpecHash            string  `json:"agent_spec_hash"`
	ToolsetHash              string  `json:"toolset_hash"`
	EffectiveBaseID          string  `json:"effective_base_id"`
	DataSnapshotHash         string  `json:"data_snapshot_hash"`
	ContentFingerprint       string  `json:"content_fingerprint"`
	NearDuplicateFingerprint string  `json:"near_duplicate_fingerprint"`
	Evaluator                string  `json:"evaluator"`
	TaskSuccess              bool    `json:"task_success"`
	ToolSelectionScore       float64 `json:"tool_selection_score"`
	Groundedness             float64 `json:"groundedness"`
	PolicyViolations         int     `json:"policy_violations"`
	Confidence               float64 `json:"confidence"`
	LabelSource              string  `json:"label_source"`
	RubricVersion            string  `json:"rubric_version"`
	CreatedByUserID          string  `json:"created_by_user_id"`
	CreatedAt                string  `json:"created_at"`
}

type TrajectoryDatasetDTO struct {
	DatasetID          string          `json:"dataset_id"`
	AgentLineage       string          `json:"agent_lineage"`
	GoldenSplitVersion int             `json:"golden_split_version"`
	ContentHash        string          `json:"content_hash"`
	DatasetURI         string          `json:"dataset_uri"`
	Format             string          `json:"format"`
	LabelCount         int             `json:"label_count"`
	Manifest           json.RawMessage `json:"manifest"`
	EffectiveBaseID    string          `json:"effective_base_id"`
	AgentSpecHash      string          `json:"agent_spec_hash"`
	ToolsetHash        string          `json:"toolset_hash"`
	DataSnapshotHash   string          `json:"data_snapshot_hash"`
	CreatedByUserID    string          `json:"created_by_user_id"`
	CreatedAt          string          `json:"created_at"`
}

type AgentAdapterDTO struct {
	AdapterID                        string `json:"adapter_id"`
	AgentLineage                     string `json:"agent_lineage"`
	DatasetID                        string `json:"dataset_id"`
	TrainingRunID                    string `json:"training_run_id,omitempty"`
	ServingModelID                   string `json:"serving_model_id,omitempty"`
	AdapterURI                       string `json:"adapter_uri"`
	AdapterChecksum                  string `json:"adapter_checksum"`
	TrainingProvider                 string `json:"training_provider"`
	TrainedAgainstEffectiveBaseID    string `json:"trained_against_effective_base_id"`
	TrainedAgainstAgentSpecHash      string `json:"trained_against_agent_spec_hash"`
	TrainedAgainstToolsetHash        string `json:"trained_against_toolset_hash"`
	TrainedAgainstDataSnapshotHash   string `json:"trained_against_data_snapshot_hash"`
	TrainedAgainstRubricVersion      string `json:"trained_against_rubric_version"`
	TrainedAgainstGoldenSplitVersion int    `json:"trained_against_golden_split_version"`
	Status                           string `json:"status"`
	PromotionPassed                  bool   `json:"promotion_passed"`
	CreatedByUserID                  string `json:"created_by_user_id"`
	CreatedAt                        string `json:"created_at"`
	UpdatedAt                        string `json:"updated_at"`
}

type AgentEvalTaskResultDTO struct {
	TaskID        string `json:"task_id"`
	RunID         string `json:"run_id,omitempty"`
	Status        string `json:"status"`
	StopReason    string `json:"stop_reason"`
	TaskSuccess   bool   `json:"task_success"`
	ToolSuccess   bool   `json:"tool_success"`
	Groundedness  bool   `json:"groundedness"`
	FailureReason string `json:"failure_reason,omitempty"`
}

type AgentEndpointBindingDTO struct {
	AgentLineage    string `json:"agent_lineage"`
	EndpointID      string `json:"endpoint_id"`
	CreatedByUserID string `json:"created_by_user_id"`
	CreatedAt       string `json:"created_at"`
}

type AgentChampionStateDTO struct {
	AgentLineage          string `json:"agent_lineage"`
	ChampionAgentSpecHash string `json:"champion_agent_spec_hash"`
	ChampionAdapterID     string `json:"champion_adapter_id,omitempty"`
	ServingModelID        string `json:"serving_model_id,omitempty"`
	PreviousAgentSpecHash string `json:"previous_agent_spec_hash,omitempty"`
	DecisionID            string `json:"decision_id"`
	DecidedBy             string `json:"decided_by"`
	DecidedAt             string `json:"decided_at"`
}

func NewAgentRegistryDTOAdapter(encoder *serializers.Encoder) *agentRegistryDTOAdapter {
	log.Trace("NewAgentRegistryDTOAdapter")

	if encoder == nil {
		encoder = serializers.NewJSONSerializer()
	}
	return &agentRegistryDTOAdapter{
		validator: validator.New(),
		encoder:   encoder,
	}
}

func (a *agentRegistryDTOAdapter) FromRegisterSpecVersionDTO(ctx context.Context, body []byte) (model.RegisterAgentSpecVersionCommand, error) {
	log.Trace("AgentRegistryDTOAdapter FromRegisterSpecVersionDTO")

	var dto RegisterAgentSpecVersionDTO
	if err := a.encoder.Deserialize(body, &dto); err != nil {
		return model.RegisterAgentSpecVersionCommand{}, domain.ErrAgentRegistryValidation.Extend(err.Error())
	}
	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("RegisterAgentSpecVersionDTO validation failed")
		return model.RegisterAgentSpecVersionCommand{}, domain.ErrAgentRegistryValidation.Extend(err.Error())
	}
	return model.RegisterAgentSpecVersionCommand{
		AgentLineage:  dto.AgentLineage,
		AgentSpecHash: dto.AgentSpecHash,
	}, nil
}

func (a *agentRegistryDTOAdapter) FromRegisterEndpointBindingDTO(ctx context.Context, body []byte) (model.RegisterEndpointBindingCommand, error) {
	log.Trace("AgentRegistryDTOAdapter FromRegisterEndpointBindingDTO")

	var dto RegisterEndpointBindingDTO
	if err := a.encoder.Deserialize(body, &dto); err != nil {
		return model.RegisterEndpointBindingCommand{}, domain.ErrAgentRegistryValidation.Extend(err.Error())
	}
	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("RegisterEndpointBindingDTO validation failed")
		return model.RegisterEndpointBindingCommand{}, domain.ErrAgentRegistryValidation.Extend(err.Error())
	}
	endpointID, err := uuid.Parse(dto.EndpointID)
	if err != nil {
		return model.RegisterEndpointBindingCommand{}, domain.ErrAgentRegistryValidation.Extend("endpoint_id is invalid")
	}
	return model.RegisterEndpointBindingCommand{
		AgentLineage: dto.AgentLineage,
		EndpointID:   endpointID,
	}, nil
}

func (a *agentRegistryDTOAdapter) FromPromoteSpecChampionDTO(ctx context.Context, body []byte) (model.PromoteSpecChampionCommand, error) {
	log.Trace("AgentRegistryDTOAdapter FromPromoteSpecChampionDTO")

	var dto PromoteSpecChampionDTO
	if err := a.encoder.Deserialize(body, &dto); err != nil {
		return model.PromoteSpecChampionCommand{}, domain.ErrAgentRegistryValidation.Extend(err.Error())
	}
	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("PromoteSpecChampionDTO validation failed")
		return model.PromoteSpecChampionCommand{}, domain.ErrAgentRegistryValidation.Extend(err.Error())
	}
	decisionID := uuid.Nil
	if dto.DecisionID != "" {
		parsed, err := uuid.Parse(dto.DecisionID)
		if err != nil {
			return model.PromoteSpecChampionCommand{}, domain.ErrAgentRegistryValidation.Extend("decision_id is invalid")
		}
		decisionID = parsed
	}
	decidedAt := time.Time{}
	if dto.DecidedAt != "" {
		parsed, err := time.Parse(time.RFC3339Nano, dto.DecidedAt)
		if err != nil {
			return model.PromoteSpecChampionCommand{}, domain.ErrAgentRegistryValidation.Extend("decided_at is invalid")
		}
		decidedAt = parsed
	}
	return model.PromoteSpecChampionCommand{
		AgentLineage:  dto.AgentLineage,
		AgentSpecHash: dto.AgentSpecHash,
		DecisionID:    decisionID,
		DecidedAt:     decidedAt,
	}, nil
}

func (a *agentRegistryDTOAdapter) FromImportGoldenTasksDTO(ctx context.Context, body []byte) (model.ImportGoldenTasksCommand, error) {
	log.Trace("AgentRegistryDTOAdapter FromImportGoldenTasksDTO")

	var dto ImportGoldenTasksDTO
	if err := a.encoder.Deserialize(body, &dto); err != nil {
		return model.ImportGoldenTasksCommand{}, domain.ErrAgentRegistryValidation.Extend(err.Error())
	}
	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("ImportGoldenTasksDTO validation failed")
		return model.ImportGoldenTasksCommand{}, domain.ErrAgentRegistryValidation.Extend(err.Error())
	}
	split, err := model.ToGoldenTaskSplit(dto.Split)
	if err != nil {
		return model.ImportGoldenTasksCommand{}, domain.ErrAgentRegistryValidation.Extend("split is invalid")
	}
	tasks := make([]model.GoldenTaskInput, 0, len(dto.Tasks))
	for _, task := range dto.Tasks {
		tasks = append(tasks, model.GoldenTaskInput{
			GroupKey:               task.GroupKey,
			Prompt:                 task.Prompt,
			ExpectedToolPlanHash:   task.ExpectedToolPlanHash,
			ExpectedAnswer:         task.ExpectedAnswer,
			ExpectedAnswerRubricID: task.ExpectedAnswerRubricID,
			LabelsHash:             task.LabelsHash,
		})
	}
	return model.ImportGoldenTasksCommand{
		AgentLineage: dto.AgentLineage,
		Split:        split,
		SplitVersion: dto.SplitVersion,
		Tasks:        tasks,
	}, nil
}

func (a *agentRegistryDTOAdapter) FromListGoldenTasksDTO(ctx context.Context, values map[string]string) (model.ListGoldenTasksCommand, error) {
	log.Trace("AgentRegistryDTOAdapter FromListGoldenTasksDTO")

	var dto ListGoldenTasksDTO
	if raw, ok := values["agent_lineage"]; ok {
		dto.AgentLineage = raw
	}
	if raw, ok := values["split"]; ok {
		dto.Split = raw
	}
	if raw, ok := values["split_version"]; ok {
		var parsed int
		if _, err := fmt.Sscanf(raw, "%d", &parsed); err != nil {
			return model.ListGoldenTasksCommand{}, domain.ErrAgentRegistryValidation.Extend("split_version is invalid")
		}
		dto.SplitVersion = parsed
	}
	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("ListGoldenTasksDTO validation failed")
		return model.ListGoldenTasksCommand{}, domain.ErrAgentRegistryValidation.Extend(err.Error())
	}
	split, err := model.ToGoldenTaskSplit(dto.Split)
	if err != nil {
		return model.ListGoldenTasksCommand{}, domain.ErrAgentRegistryValidation.Extend("split is invalid")
	}
	return model.ListGoldenTasksCommand{
		AgentLineage: dto.AgentLineage,
		Split:        split,
		SplitVersion: dto.SplitVersion,
	}, nil
}

func (a *agentRegistryDTOAdapter) FromLabelAgentRunDTO(ctx context.Context, body []byte) (model.LabelAgentRunCommand, error) {
	log.Trace("AgentRegistryDTOAdapter FromLabelAgentRunDTO")

	var dto LabelAgentRunDTO
	if err := a.encoder.Deserialize(body, &dto); err != nil {
		return model.LabelAgentRunCommand{}, domain.ErrAgentRegistryValidation.Extend(err.Error())
	}
	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("LabelAgentRunDTO validation failed")
		return model.LabelAgentRunCommand{}, domain.ErrAgentRegistryValidation.Extend(err.Error())
	}
	runID, err := uuid.Parse(dto.RunID)
	if err != nil {
		return model.LabelAgentRunCommand{}, domain.ErrAgentRegistryValidation.Extend("run_id is invalid")
	}
	return model.LabelAgentRunCommand{
		AgentLineage:       dto.AgentLineage,
		RunID:              runID,
		Prompt:             dto.Prompt,
		Evaluator:          dto.Evaluator,
		TaskSuccess:        dto.TaskSuccess,
		ToolSelectionScore: dto.ToolSelectionScore,
		Groundedness:       dto.Groundedness,
		PolicyViolations:   dto.PolicyViolations,
		Confidence:         dto.Confidence,
		LabelSource:        dto.LabelSource,
		RubricVersion:      dto.RubricVersion,
	}, nil
}

func (a *agentRegistryDTOAdapter) FromListAgentRunLabelsDTO(ctx context.Context, values map[string]string) (model.ListAgentRunLabelsCommand, error) {
	log.Trace("AgentRegistryDTOAdapter FromListAgentRunLabelsDTO")

	var dto ListAgentRunLabelsDTO
	if raw, ok := values["agent_lineage"]; ok {
		dto.AgentLineage = raw
	}
	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("ListAgentRunLabelsDTO validation failed")
		return model.ListAgentRunLabelsCommand{}, domain.ErrAgentRegistryValidation.Extend(err.Error())
	}
	return model.ListAgentRunLabelsCommand{
		AgentLineage: dto.AgentLineage,
	}, nil
}

func (a *agentRegistryDTOAdapter) FromBuildTrajectoryDatasetDTO(ctx context.Context, body []byte) (model.BuildTrajectoryDatasetCommand, error) {
	log.Trace("AgentRegistryDTOAdapter FromBuildTrajectoryDatasetDTO")

	var dto BuildTrajectoryDatasetDTO
	if err := a.encoder.Deserialize(body, &dto); err != nil {
		return model.BuildTrajectoryDatasetCommand{}, domain.ErrAgentRegistryValidation.Extend(err.Error())
	}
	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("BuildTrajectoryDatasetDTO validation failed")
		return model.BuildTrajectoryDatasetCommand{}, domain.ErrAgentRegistryValidation.Extend(err.Error())
	}
	return model.BuildTrajectoryDatasetCommand{
		AgentLineage:       dto.AgentLineage,
		GoldenSplitVersion: dto.GoldenSplitVersion,
	}, nil
}

func (a *agentRegistryDTOAdapter) FromDispatchAgentAdapterTrainingDTO(ctx context.Context, body []byte) (model.DispatchAgentAdapterTrainingCommand, error) {
	log.Trace("AgentRegistryDTOAdapter FromDispatchAgentAdapterTrainingDTO")

	var dto DispatchAgentAdapterTrainingDTO
	if err := a.encoder.Deserialize(body, &dto); err != nil {
		return model.DispatchAgentAdapterTrainingCommand{}, domain.ErrAgentRegistryValidation.Extend(err.Error())
	}
	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("DispatchAgentAdapterTrainingDTO validation failed")
		return model.DispatchAgentAdapterTrainingCommand{}, domain.ErrAgentRegistryValidation.Extend(err.Error())
	}
	datasetID, err := uuid.Parse(dto.DatasetID)
	if err != nil {
		return model.DispatchAgentAdapterTrainingCommand{}, domain.ErrAgentRegistryValidation.Extend("dataset_id is invalid")
	}
	return model.DispatchAgentAdapterTrainingCommand{
		AgentLineage:    dto.AgentLineage,
		DatasetID:       datasetID,
		TrainingProfile: dto.TrainingProfile,
	}, nil
}

func (a *agentRegistryDTOAdapter) FromEvaluateAdapterCandidateDTO(ctx context.Context, body []byte) (model.EvaluateAdapterCandidateCommand, error) {
	log.Trace("AgentRegistryDTOAdapter FromEvaluateAdapterCandidateDTO")

	var dto EvaluateAdapterCandidateDTO
	if err := a.encoder.Deserialize(body, &dto); err != nil {
		return model.EvaluateAdapterCandidateCommand{}, domain.ErrAgentRegistryValidation.Extend(err.Error())
	}
	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("EvaluateAdapterCandidateDTO validation failed")
		return model.EvaluateAdapterCandidateCommand{}, domain.ErrAgentRegistryValidation.Extend(err.Error())
	}
	adapterID, err := uuid.Parse(dto.AdapterID)
	if err != nil {
		return model.EvaluateAdapterCandidateCommand{}, domain.ErrAgentRegistryValidation.Extend("adapter_id is invalid")
	}
	endpointID, err := uuid.Parse(dto.EndpointID)
	if err != nil {
		return model.EvaluateAdapterCandidateCommand{}, domain.ErrAgentRegistryValidation.Extend("endpoint_id is invalid")
	}
	command := model.EvaluateAdapterCandidateCommand{
		AgentLineage: dto.AgentLineage,
		AdapterID:    adapterID,
		EndpointID:   endpointID,
		SplitVersion: dto.SplitVersion,
	}
	if dto.MinTaskSuccessRate != nil {
		command.MinTaskSuccessRate = *dto.MinTaskSuccessRate
	}
	if dto.MinToolSuccessRate != nil {
		command.MinToolSuccessRate = *dto.MinToolSuccessRate
	}
	if dto.MinGroundednessRate != nil {
		command.MinGroundednessRate = *dto.MinGroundednessRate
	}
	return command, nil
}

func (a *agentRegistryDTOAdapter) FromEvaluateSpecChampionDTO(ctx context.Context, body []byte) (model.EvaluateSpecChampionCommand, error) {
	log.Trace("AgentRegistryDTOAdapter FromEvaluateSpecChampionDTO")

	var dto EvaluateSpecChampionDTO
	if err := a.encoder.Deserialize(body, &dto); err != nil {
		return model.EvaluateSpecChampionCommand{}, domain.ErrAgentRegistryValidation.Extend(err.Error())
	}
	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("EvaluateSpecChampionDTO validation failed")
		return model.EvaluateSpecChampionCommand{}, domain.ErrAgentRegistryValidation.Extend(err.Error())
	}
	endpointID, err := uuid.Parse(dto.EndpointID)
	if err != nil {
		return model.EvaluateSpecChampionCommand{}, domain.ErrAgentRegistryValidation.Extend("endpoint_id is invalid")
	}
	command := model.EvaluateSpecChampionCommand{
		AgentLineage:  dto.AgentLineage,
		AgentSpecHash: dto.AgentSpecHash,
		EndpointID:    endpointID,
		SplitVersion:  dto.SplitVersion,
	}
	if dto.MinTaskSuccessRate != nil {
		command.MinTaskSuccessRate = *dto.MinTaskSuccessRate
	}
	if dto.MinToolSuccessRate != nil {
		command.MinToolSuccessRate = *dto.MinToolSuccessRate
	}
	if dto.MinGroundednessRate != nil {
		command.MinGroundednessRate = *dto.MinGroundednessRate
	}
	return command, nil
}

func (a *agentRegistryDTOAdapter) FromPromoteAgentAdapterDTO(ctx context.Context, body []byte) (model.PromoteAgentAdapterCommand, error) {
	log.Trace("AgentRegistryDTOAdapter FromPromoteAgentAdapterDTO")

	var dto PromoteAgentAdapterDTO
	if err := a.encoder.Deserialize(body, &dto); err != nil {
		return model.PromoteAgentAdapterCommand{}, domain.ErrAgentRegistryValidation.Extend(err.Error())
	}
	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("PromoteAgentAdapterDTO validation failed")
		return model.PromoteAgentAdapterCommand{}, domain.ErrAgentRegistryValidation.Extend(err.Error())
	}
	adapterID, err := uuid.Parse(dto.AdapterID)
	if err != nil {
		return model.PromoteAgentAdapterCommand{}, domain.ErrAgentRegistryValidation.Extend("adapter_id is invalid")
	}
	reportID, err := uuid.Parse(dto.ReportID)
	if err != nil {
		return model.PromoteAgentAdapterCommand{}, domain.ErrAgentRegistryValidation.Extend("report_id is invalid")
	}
	command := model.PromoteAgentAdapterCommand{
		AgentLineage: dto.AgentLineage,
		AdapterID:    adapterID,
		ReportID:     reportID,
	}
	if dto.MinDelta != nil {
		command.MinDelta = *dto.MinDelta
	}
	return command, nil
}

func (a *agentRegistryDTOAdapter) ToSpecVersionDTO(ctx context.Context, version *model.AgentSpecVersion) ([]byte, error) {
	log.Trace("AgentRegistryDTOAdapter ToSpecVersionDTO")

	dtos := []AgentSpecVersionDTO{}
	if version != nil {
		dtos = append(dtos, AgentSpecVersionDTO{
			AgentLineage:       version.AgentLineage,
			AgentSpecHash:      version.AgentSpecHash,
			ModelID:            version.ModelID.String(),
			RegisteredByUserID: version.RegisteredByUserID.String(),
			RegisteredAt:       version.RegisteredAt.UTC().Format(time.RFC3339Nano),
		})
	}
	out, err := a.encoder.Serialize(dtos)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("AgentSpecVersionDTO encoding failed")
		return nil, err
	}
	return out, nil
}

func (a *agentRegistryDTOAdapter) ToGoldenTaskDTOs(ctx context.Context, tasks []*model.GoldenTask) ([]byte, error) {
	log.Trace("AgentRegistryDTOAdapter ToGoldenTaskDTOs")

	dtos := make([]GoldenTaskDTO, 0, len(tasks))
	for _, task := range tasks {
		if task == nil {
			continue
		}
		dtos = append(dtos, GoldenTaskDTO{
			TaskID:                   task.TaskID.String(),
			AgentLineage:             task.AgentLineage,
			Split:                    task.Split.String(),
			SplitVersion:             task.SplitVersion,
			GroupKey:                 task.GroupKey,
			Prompt:                   task.Prompt,
			NormalizedPromptHash:     task.NormalizedPromptHash,
			ContentFingerprint:       task.ContentFingerprint,
			NearDuplicateFingerprint: task.NearDuplicateFingerprint,
			ExpectedToolPlanHash:     task.ExpectedToolPlanHash,
			ExpectedAnswer:           task.ExpectedAnswer,
			ExpectedAnswerRubricID:   task.ExpectedAnswerRubricID,
			LabelsHash:               task.LabelsHash,
			CreatedByUserID:          task.CreatedByUserID.String(),
			CreatedAt:                task.CreatedAt.UTC().Format(time.RFC3339Nano),
		})
	}
	out, err := a.encoder.Serialize(dtos)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("GoldenTaskDTO encoding failed")
		return nil, err
	}
	return out, nil
}

func (a *agentRegistryDTOAdapter) ToAgentRunLabelDTOs(ctx context.Context, labels []*model.AgentRunLabel) ([]byte, error) {
	log.Trace("AgentRegistryDTOAdapter ToAgentRunLabelDTOs")

	dtos := make([]AgentRunLabelDTO, 0, len(labels))
	for _, label := range labels {
		if label == nil {
			continue
		}
		dtos = append(dtos, AgentRunLabelDTO{
			LabelID:                  label.LabelID.String(),
			RunID:                    label.RunID.String(),
			AgentLineage:             label.AgentLineage,
			AgentSpecHash:            label.AgentSpecHash,
			ToolsetHash:              label.ToolsetHash,
			EffectiveBaseID:          label.EffectiveBaseID,
			DataSnapshotHash:         label.DataSnapshotHash,
			ContentFingerprint:       label.ContentFingerprint,
			NearDuplicateFingerprint: label.NearDuplicateFingerprint,
			Evaluator:                label.Evaluator,
			TaskSuccess:              label.TaskSuccess,
			ToolSelectionScore:       label.ToolSelectionScore,
			Groundedness:             label.Groundedness,
			PolicyViolations:         label.PolicyViolations,
			Confidence:               label.Confidence,
			LabelSource:              label.LabelSource,
			RubricVersion:            label.RubricVersion,
			CreatedByUserID:          label.CreatedByUserID.String(),
			CreatedAt:                label.CreatedAt.UTC().Format(time.RFC3339Nano),
		})
	}
	out, err := a.encoder.Serialize(dtos)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("AgentRunLabelDTO encoding failed")
		return nil, err
	}
	return out, nil
}

func (a *agentRegistryDTOAdapter) ToTrajectoryDatasetDTO(ctx context.Context, dataset *model.AgentTrajectoryDataset) ([]byte, error) {
	log.Trace("AgentRegistryDTOAdapter ToTrajectoryDatasetDTO")

	dtos := []TrajectoryDatasetDTO{}
	if dataset != nil {
		dtos = append(dtos, TrajectoryDatasetDTO{
			DatasetID:          dataset.DatasetID.String(),
			AgentLineage:       dataset.AgentLineage,
			GoldenSplitVersion: dataset.GoldenSplitVersion,
			ContentHash:        dataset.ContentHash,
			DatasetURI:         dataset.DatasetURI,
			Format:             dataset.Format,
			LabelCount:         dataset.LabelCount,
			Manifest:           dataset.Manifest,
			EffectiveBaseID:    dataset.EffectiveBaseID,
			AgentSpecHash:      dataset.AgentSpecHash,
			ToolsetHash:        dataset.ToolsetHash,
			DataSnapshotHash:   dataset.DataSnapshotHash,
			CreatedByUserID:    dataset.CreatedByUserID.String(),
			CreatedAt:          dataset.CreatedAt.UTC().Format(time.RFC3339Nano),
		})
	}
	out, err := a.encoder.Serialize(dtos)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("TrajectoryDatasetDTO encoding failed")
		return nil, err
	}
	return out, nil
}

func (a *agentRegistryDTOAdapter) ToAgentAdapterDTO(ctx context.Context, adapter *model.AgentAdapter) ([]byte, error) {
	log.Trace("AgentRegistryDTOAdapter ToAgentAdapterDTO")

	dtos := []AgentAdapterDTO{}
	if adapter != nil {
		dtos = append(dtos, agentAdapterDTO(adapter))
	}
	out, err := a.encoder.Serialize(dtos)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("AgentAdapterDTO encoding failed")
		return nil, err
	}
	return out, nil
}

func agentAdapterDTO(adapter *model.AgentAdapter) AgentAdapterDTO {
	trainingRunID := ""
	if adapter.TrainingRunID != uuid.Nil {
		trainingRunID = adapter.TrainingRunID.String()
	}
	servingModelID := ""
	if adapter.ServingModelID != uuid.Nil {
		servingModelID = adapter.ServingModelID.String()
	}
	return AgentAdapterDTO{
		AdapterID:                        adapter.AdapterID.String(),
		AgentLineage:                     adapter.AgentLineage,
		DatasetID:                        adapter.DatasetID.String(),
		TrainingRunID:                    trainingRunID,
		ServingModelID:                   servingModelID,
		AdapterURI:                       adapter.AdapterURI,
		AdapterChecksum:                  adapter.AdapterChecksum,
		TrainingProvider:                 adapter.TrainingProvider,
		TrainedAgainstEffectiveBaseID:    adapter.TrainedAgainstEffectiveBaseID,
		TrainedAgainstAgentSpecHash:      adapter.TrainedAgainstAgentSpecHash,
		TrainedAgainstToolsetHash:        adapter.TrainedAgainstToolsetHash,
		TrainedAgainstDataSnapshotHash:   adapter.TrainedAgainstDataSnapshotHash,
		TrainedAgainstRubricVersion:      adapter.TrainedAgainstRubricVersion,
		TrainedAgainstGoldenSplitVersion: adapter.TrainedAgainstGoldenSplitVersion,
		Status:                           adapter.Status.String(),
		PromotionPassed:                  adapter.PromotionPassed,
		CreatedByUserID:                  adapter.CreatedByUserID.String(),
		CreatedAt:                        adapter.CreatedAt.UTC().Format(time.RFC3339Nano),
		UpdatedAt:                        adapter.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}
}

func (a *agentRegistryDTOAdapter) ToAgentEvalReportDTO(ctx context.Context, report *model.AgentEvalReport) ([]byte, error) {
	log.Trace("AgentRegistryDTOAdapter ToAgentEvalReportDTO")

	dtos := []AgentEvalReportDTO{}
	if report != nil {
		dtos = append(dtos, agentEvalReportDTO(report))
	}
	out, err := a.encoder.Serialize(dtos)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("AgentEvalReportDTO encoding failed")
		return nil, err
	}
	return out, nil
}

func agentEvalReportDTO(report *model.AgentEvalReport) AgentEvalReportDTO {
	results := make([]AgentEvalTaskResultDTO, 0, len(report.TaskResults))
	for _, result := range report.TaskResults {
		if result == nil {
			continue
		}
		runID := ""
		if result.RunID != uuid.Nil {
			runID = result.RunID.String()
		}
		results = append(results, AgentEvalTaskResultDTO{
			TaskID:        result.TaskID.String(),
			RunID:         runID,
			Status:        result.Status,
			StopReason:    result.StopReason,
			TaskSuccess:   result.TaskSuccess,
			ToolSuccess:   result.ToolSuccess,
			Groundedness:  result.Groundedness,
			FailureReason: result.FailureReason,
		})
	}
	promotedDecisionID := ""
	if report.PromotedDecisionID != uuid.Nil {
		promotedDecisionID = report.PromotedDecisionID.String()
	}
	adapterID := ""
	if report.AdapterID != uuid.Nil {
		adapterID = report.AdapterID.String()
	}
	return AgentEvalReportDTO{
		ReportID:           report.ReportID.String(),
		AgentLineage:       report.AgentLineage,
		AgentSpecHash:      report.AgentSpecHash,
		AdapterID:          adapterID,
		EndpointID:         report.EndpointID.String(),
		Split:              report.Split.String(),
		SplitVersion:       report.SplitVersion,
		RubricVersion:      report.RubricVersion,
		TaskCount:          report.TaskCount,
		TaskSuccessRate:    report.TaskSuccessRate,
		ToolSuccessRate:    report.ToolSuccessRate,
		GroundednessRate:   report.GroundednessRate,
		Passed:             report.Passed,
		GateReason:         report.GateReason,
		PromotedDecisionID: promotedDecisionID,
		EvaluatedBy:        report.EvaluatedBy.String(),
		EvaluatedAt:        report.EvaluatedAt.UTC().Format(time.RFC3339Nano),
		TaskResults:        results,
	}
}

func (a *agentRegistryDTOAdapter) ToEndpointBindingDTO(ctx context.Context, binding *model.AgentEndpointBinding) ([]byte, error) {
	log.Trace("AgentRegistryDTOAdapter ToEndpointBindingDTO")

	dtos := []AgentEndpointBindingDTO{}
	if binding != nil {
		dtos = append(dtos, AgentEndpointBindingDTO{
			AgentLineage:    binding.AgentLineage,
			EndpointID:      binding.EndpointID.String(),
			CreatedByUserID: binding.CreatedByUserID.String(),
			CreatedAt:       binding.CreatedAt.UTC().Format(time.RFC3339Nano),
		})
	}
	out, err := a.encoder.Serialize(dtos)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("AgentEndpointBindingDTO encoding failed")
		return nil, err
	}
	return out, nil
}

func (a *agentRegistryDTOAdapter) ToChampionStateDTO(ctx context.Context, state *model.AgentChampionState) ([]byte, error) {
	log.Trace("AgentRegistryDTOAdapter ToChampionStateDTO")

	dtos := []AgentChampionStateDTO{}
	if state != nil {
		championAdapterID := ""
		if state.ChampionAdapterID != uuid.Nil {
			championAdapterID = state.ChampionAdapterID.String()
		}
		servingModelID := ""
		if state.ServingModelID != uuid.Nil {
			servingModelID = state.ServingModelID.String()
		}
		dtos = append(dtos, AgentChampionStateDTO{
			AgentLineage:          state.AgentLineage,
			ChampionAgentSpecHash: state.ChampionAgentSpecHash,
			ChampionAdapterID:     championAdapterID,
			ServingModelID:        servingModelID,
			PreviousAgentSpecHash: state.PreviousAgentSpecHash,
			DecisionID:            state.DecisionID.String(),
			DecidedBy:             state.DecidedBy.String(),
			DecidedAt:             state.DecidedAt.UTC().Format(time.RFC3339Nano),
		})
	}
	out, err := a.encoder.Serialize(dtos)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("AgentChampionStateDTO encoding failed")
		return nil, err
	}
	return out, nil
}
