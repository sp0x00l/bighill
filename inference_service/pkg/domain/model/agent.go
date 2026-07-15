package model

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"lib/shared_lib/userevents"

	"github.com/google/uuid"
)

type AgentMaturity int

const (
	AgentMaturityUnknown AgentMaturity = iota
	AgentMaturityConfigOnly
	AgentMaturityBootstrapped
	AgentMaturityLearning
	AgentMaturitySpecialized
)

func (m AgentMaturity) String() string {
	switch m {
	case AgentMaturityConfigOnly:
		return "CONFIG_ONLY"
	case AgentMaturityBootstrapped:
		return "BOOTSTRAPPED"
	case AgentMaturityLearning:
		return "LEARNING"
	case AgentMaturitySpecialized:
		return "SPECIALIZED"
	default:
		return "UNKNOWN"
	}
}

func (m AgentMaturity) MarshalText() ([]byte, error) {
	return []byte(m.String()), nil
}

func ToAgentMaturity(value string) (AgentMaturity, error) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "CONFIG_ONLY":
		return AgentMaturityConfigOnly, nil
	case "BOOTSTRAPPED":
		return AgentMaturityBootstrapped, nil
	case "LEARNING":
		return AgentMaturityLearning, nil
	case "SPECIALIZED":
		return AgentMaturitySpecialized, nil
	default:
		return AgentMaturityUnknown, fmt.Errorf("invalid agent maturity %q", value)
	}
}

type AgentCandidatePipelineState int

const (
	AgentCandidatePipelineUnknown AgentCandidatePipelineState = iota
	AgentCandidatePipelineNone
	AgentCandidatePipelineTraining
	AgentCandidatePipelineEvaluating
	AgentCandidatePipelineGated
	AgentCandidatePipelineRolledBack
)

func (s AgentCandidatePipelineState) String() string {
	switch s {
	case AgentCandidatePipelineNone:
		return "NONE"
	case AgentCandidatePipelineTraining:
		return "TRAINING"
	case AgentCandidatePipelineEvaluating:
		return "EVALUATING"
	case AgentCandidatePipelineGated:
		return "GATED"
	case AgentCandidatePipelineRolledBack:
		return "ROLLED_BACK"
	default:
		return "UNKNOWN"
	}
}

func (s AgentCandidatePipelineState) MarshalText() ([]byte, error) {
	return []byte(s.String()), nil
}

func ToAgentCandidatePipelineState(value string) (AgentCandidatePipelineState, error) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "NONE":
		return AgentCandidatePipelineNone, nil
	case "TRAINING":
		return AgentCandidatePipelineTraining, nil
	case "EVALUATING":
		return AgentCandidatePipelineEvaluating, nil
	case "GATED":
		return AgentCandidatePipelineGated, nil
	case "ROLLED_BACK":
		return AgentCandidatePipelineRolledBack, nil
	default:
		return AgentCandidatePipelineUnknown, fmt.Errorf("invalid agent candidate pipeline state %q", value)
	}
}

type AgentChampionState struct {
	ChampionAdapterID      uuid.UUID
	CandidatePipelineState AgentCandidatePipelineState
}

type EffectiveBaseStatus int

const (
	EffectiveBaseStatusUnknown EffectiveBaseStatus = iota
	EffectiveBaseStatusDraft
	EffectiveBaseStatusPromoted
	EffectiveBaseStatusRetired
)

func (s EffectiveBaseStatus) String() string {
	switch s {
	case EffectiveBaseStatusDraft:
		return "DRAFT"
	case EffectiveBaseStatusPromoted:
		return "PROMOTED"
	case EffectiveBaseStatusRetired:
		return "RETIRED"
	default:
		return "UNKNOWN"
	}
}

func (s EffectiveBaseStatus) MarshalText() ([]byte, error) {
	return []byte(s.String()), nil
}

func ToEffectiveBaseStatus(value string) (EffectiveBaseStatus, error) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "DRAFT":
		return EffectiveBaseStatusDraft, nil
	case "PROMOTED":
		return EffectiveBaseStatusPromoted, nil
	case "RETIRED":
		return EffectiveBaseStatusRetired, nil
	default:
		return EffectiveBaseStatusUnknown, fmt.Errorf("invalid effective base status %q", value)
	}
}

type EffectiveBaseVersion struct {
	EffectiveBaseID        uuid.UUID
	FoundationModelID      uuid.UUID
	FoundationChecksum     string
	SharedAdapterID        uuid.UUID
	MergeRecipeID          string
	MergeToolVersion       string
	TokenizerHash          string
	ChatTemplateHash       string
	QuantizationFormat     string
	QuantizationParamsHash string
	ServedArtifactChecksum string
	CapabilityReportID     uuid.UUID
	Status                 EffectiveBaseStatus
	CreatedAt              time.Time
}

type CapabilityReport struct {
	CapabilityReportID       uuid.UUID
	OrgID                    uuid.UUID
	EffectiveBaseID          uuid.UUID
	ModelID                  uuid.UUID
	SupportsChat             bool
	SupportsToolCalls        bool
	SupportsJSONSchemaOutput bool
	SupportsSystemPrompt     bool
	ContextWindowTokens      int
	MaxOutputTokens          int
	CreatedAt                time.Time
}

type AgentAdapterStatus int

const (
	AgentAdapterStatusUnknown AgentAdapterStatus = iota
	AgentAdapterStatusCandidate
	AgentAdapterStatusEvaluated
	AgentAdapterStatusPromoted
	AgentAdapterStatusStale
	AgentAdapterStatusQuarantined
)

func (s AgentAdapterStatus) String() string {
	switch s {
	case AgentAdapterStatusCandidate:
		return "CANDIDATE"
	case AgentAdapterStatusEvaluated:
		return "EVALUATED"
	case AgentAdapterStatusPromoted:
		return "PROMOTED"
	case AgentAdapterStatusStale:
		return "STALE"
	case AgentAdapterStatusQuarantined:
		return "QUARANTINED"
	default:
		return "UNKNOWN"
	}
}

func (s AgentAdapterStatus) MarshalText() ([]byte, error) {
	return []byte(s.String()), nil
}

func ToAgentAdapterStatus(value string) (AgentAdapterStatus, error) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "CANDIDATE":
		return AgentAdapterStatusCandidate, nil
	case "EVALUATED":
		return AgentAdapterStatusEvaluated, nil
	case "PROMOTED":
		return AgentAdapterStatusPromoted, nil
	case "STALE":
		return AgentAdapterStatusStale, nil
	case "QUARANTINED":
		return AgentAdapterStatusQuarantined, nil
	default:
		return AgentAdapterStatusUnknown, fmt.Errorf("invalid agent adapter status %q", value)
	}
}

type AgentAdapter struct {
	AdapterID                        uuid.UUID
	LineageName                      string
	TrainedAgainstEffectiveBaseID    uuid.UUID
	TrainedAgainstAgentSpecHash      string
	TrainedAgainstToolsetHash        string
	TrainedAgainstRubricVersion      string
	TrainedAgainstGoldenSplitVersion int
	TrajectorySchemaVersion          string
	Status                           AgentAdapterStatus
	PromotionPassed                  bool
	CreatedAt                        time.Time
}

type AgentSpecStatus int

const (
	AgentSpecStatusUnknown AgentSpecStatus = iota
	AgentSpecStatusDraft
	AgentSpecStatusValidated
	AgentSpecStatusPromoted
	AgentSpecStatusFailed
)

func (s AgentSpecStatus) String() string {
	switch s {
	case AgentSpecStatusDraft:
		return "DRAFT"
	case AgentSpecStatusValidated:
		return "VALIDATED"
	case AgentSpecStatusPromoted:
		return "PROMOTED"
	case AgentSpecStatusFailed:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

func (s AgentSpecStatus) MarshalText() ([]byte, error) {
	return []byte(s.String()), nil
}

func ToAgentSpecStatus(value string) (AgentSpecStatus, error) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "DRAFT":
		return AgentSpecStatusDraft, nil
	case "VALIDATED":
		return AgentSpecStatusValidated, nil
	case "PROMOTED":
		return AgentSpecStatusPromoted, nil
	case "FAILED":
		return AgentSpecStatusFailed, nil
	default:
		return AgentSpecStatusUnknown, fmt.Errorf("invalid agent spec status %q", value)
	}
}

type AgentRuntimeMode int

const (
	AgentRuntimeModeUnknown AgentRuntimeMode = iota
	AgentRuntimeModeInteractive
	AgentRuntimeModeDurable
)

func (m AgentRuntimeMode) String() string {
	switch m {
	case AgentRuntimeModeInteractive:
		return "interactive"
	case AgentRuntimeModeDurable:
		return "durable"
	default:
		return "unknown"
	}
}

func (m AgentRuntimeMode) MarshalText() ([]byte, error) {
	return []byte(m.String()), nil
}

func ToAgentRuntimeMode(value string) (AgentRuntimeMode, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "interactive":
		return AgentRuntimeModeInteractive, nil
	case "durable":
		return AgentRuntimeModeDurable, nil
	default:
		return AgentRuntimeModeUnknown, fmt.Errorf("invalid agent runtime mode %q", value)
	}
}

type AgentEndpointMode int

const (
	AgentEndpointModeUnknown AgentEndpointMode = iota
	AgentEndpointModeRAG
	AgentEndpointModeAgent
)

func (m AgentEndpointMode) String() string {
	switch m {
	case AgentEndpointModeRAG:
		return "rag"
	case AgentEndpointModeAgent:
		return "agent"
	default:
		return "unknown"
	}
}

func (m AgentEndpointMode) MarshalText() ([]byte, error) {
	return []byte(m.String()), nil
}

func ToAgentEndpointMode(value string) (AgentEndpointMode, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "rag":
		return AgentEndpointModeRAG, nil
	case "agent":
		return AgentEndpointModeAgent, nil
	default:
		return AgentEndpointModeUnknown, fmt.Errorf("invalid endpoint mode %q", value)
	}
}

type AgentBudgets struct {
	MaxSteps int `json:"max_steps"`
	Token    int `json:"token"`
}

type ToolBinding struct {
	Name       string          `json:"name"`
	Required   bool            `json:"required"`
	ToolChoice string          `json:"tool_choice,omitempty"`
	Config     json.RawMessage `json:"config,omitempty"`
}

type AgentSpec struct {
	AgentSpecID      uuid.UUID
	OrgID            uuid.UUID
	AgentLineage     string
	SystemPrompt     string
	SourceYAML       string
	CanonicalJSON    []byte
	SchemaVersion    string
	ContentHash      string
	ValidationReport string
	ModelID          uuid.UUID
	EffectiveBaseID  uuid.UUID
	ToolBindings     []ToolBinding
	RetrievalConfig  json.RawMessage
	Budgets          AgentBudgets
	StopConditions   json.RawMessage
	Guardrails       json.RawMessage
	RuntimeMode      AgentRuntimeMode
	Status           AgentSpecStatus
	CreatedAt        time.Time
}

type AgentSpecPublication struct {
	UserID uuid.UUID
	OrgID  uuid.UUID
	Spec   *AgentSpec
}

func CanonicalAgentSpecHash(canonicalJSON []byte) string {
	return userevents.SHA256String(string(canonicalJSON))
}

type GoldenTaskSplit int

const (
	GoldenTaskSplitUnknown GoldenTaskSplit = iota
	GoldenTaskSplitSeedTrain
	GoldenTaskSplitDevEval
	GoldenTaskSplitPromotionHoldout
)

func (s GoldenTaskSplit) String() string {
	switch s {
	case GoldenTaskSplitSeedTrain:
		return "seed_train"
	case GoldenTaskSplitDevEval:
		return "dev_eval"
	case GoldenTaskSplitPromotionHoldout:
		return "promotion_holdout"
	default:
		return "unknown"
	}
}

func (s GoldenTaskSplit) MarshalText() ([]byte, error) {
	return []byte(s.String()), nil
}

func ToGoldenTaskSplit(value string) (GoldenTaskSplit, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "seed_train":
		return GoldenTaskSplitSeedTrain, nil
	case "dev_eval":
		return GoldenTaskSplitDevEval, nil
	case "promotion_holdout":
		return GoldenTaskSplitPromotionHoldout, nil
	default:
		return GoldenTaskSplitUnknown, fmt.Errorf("invalid golden task split %q", value)
	}
}

type GoldenTask struct {
	TaskID                 uuid.UUID
	OrgID                  uuid.UUID
	AgentLineage           string
	Split                  GoldenTaskSplit
	SplitVersion           int
	GroupKey               string
	InputHash              string
	NormalizedPromptHash   string
	ContentFingerprint     string
	ExpectedToolPlanHash   string
	ExpectedAnswerRubricID string
	LabelsHash             string
	SourceTaskLineage      string
	CreatedAt              time.Time
}

type AgentRunStatus int

const (
	AgentRunStatusUnknown AgentRunStatus = iota
	AgentRunStatusRunning
	AgentRunStatusCompleted
	AgentRunStatusFailed
)

func (s AgentRunStatus) String() string {
	switch s {
	case AgentRunStatusRunning:
		return "RUNNING"
	case AgentRunStatusCompleted:
		return "COMPLETED"
	case AgentRunStatusFailed:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

func (s AgentRunStatus) MarshalText() ([]byte, error) {
	return []byte(s.String()), nil
}

func ToAgentRunStatus(value string) (AgentRunStatus, error) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "RUNNING":
		return AgentRunStatusRunning, nil
	case "COMPLETED":
		return AgentRunStatusCompleted, nil
	case "FAILED":
		return AgentRunStatusFailed, nil
	default:
		return AgentRunStatusUnknown, fmt.Errorf("invalid agent run status %q", value)
	}
}

type AgentStopReason int

const (
	AgentStopReasonUnknown AgentStopReason = iota
	AgentStopReasonFinalAnswer
	AgentStopReasonMaxSteps
	AgentStopReasonBudget
	AgentStopReasonCancelled
	AgentStopReasonToolError
	AgentStopReasonRuntimeError
	AgentStopReasonLoopDetected
)

func (r AgentStopReason) String() string {
	switch r {
	case AgentStopReasonFinalAnswer:
		return "FINAL_ANSWER"
	case AgentStopReasonMaxSteps:
		return "MAX_STEPS"
	case AgentStopReasonBudget:
		return "BUDGET_EXCEEDED"
	case AgentStopReasonCancelled:
		return "CANCELLED"
	case AgentStopReasonToolError:
		return "TOOL_ERROR"
	case AgentStopReasonRuntimeError:
		return "RUNTIME_ERROR"
	case AgentStopReasonLoopDetected:
		return "LOOP_DETECTED"
	default:
		return "UNKNOWN"
	}
}

func (r AgentStopReason) MarshalText() ([]byte, error) {
	return []byte(r.String()), nil
}

func ToAgentStopReason(value string) (AgentStopReason, error) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "FINAL_ANSWER":
		return AgentStopReasonFinalAnswer, nil
	case "MAX_STEPS":
		return AgentStopReasonMaxSteps, nil
	case "BUDGET_EXCEEDED":
		return AgentStopReasonBudget, nil
	case "CANCELLED":
		return AgentStopReasonCancelled, nil
	case "TOOL_ERROR":
		return AgentStopReasonToolError, nil
	case "RUNTIME_ERROR":
		return AgentStopReasonRuntimeError, nil
	case "LOOP_DETECTED":
		return AgentStopReasonLoopDetected, nil
	default:
		return AgentStopReasonUnknown, fmt.Errorf("invalid agent stop reason %q", value)
	}
}

type AgentTrainingEligibility int

const (
	AgentTrainingEligibilityUnknown AgentTrainingEligibility = iota
	AgentTrainingEligibilityTenantOnly
	AgentTrainingEligibilityPoolable
)

func (e AgentTrainingEligibility) String() string {
	switch e {
	case AgentTrainingEligibilityTenantOnly:
		return "TENANT_ONLY"
	case AgentTrainingEligibilityPoolable:
		return "POOLABLE"
	default:
		return "UNKNOWN"
	}
}

func (e AgentTrainingEligibility) MarshalText() ([]byte, error) {
	return []byte(e.String()), nil
}

func ToAgentTrainingEligibility(value string) (AgentTrainingEligibility, error) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "TENANT_ONLY":
		return AgentTrainingEligibilityTenantOnly, nil
	case "POOLABLE":
		return AgentTrainingEligibilityPoolable, nil
	default:
		return AgentTrainingEligibilityUnknown, fmt.Errorf("invalid agent training eligibility %q", value)
	}
}

type AgentRun struct {
	RunID                   uuid.UUID
	OrgID                   uuid.UUID
	UserID                  uuid.UUID
	EndpointID              uuid.UUID
	AgentSpecHash           string
	EffectiveBaseID         uuid.UUID
	ModelVersion            int
	ToolsetHash             string
	RubricVersion           string
	TrajectorySchemaVersion string
	SystemTemplateVersion   string
	DecodingParams          json.RawMessage
	Status                  AgentRunStatus
	StopReason              AgentStopReason
	StartedAt               time.Time
	FinishedAt              time.Time
	TotalTokens             int
	TrainingEligibility     AgentTrainingEligibility
}

type AgentStep struct {
	StepID               uuid.UUID
	RunID                uuid.UUID
	OrgID                uuid.UUID
	StepIndex            int
	PresentedToolSchemas json.RawMessage
	GenerationResult     json.RawMessage
	FinishReason         GenerationFinishReason
	PromptTokens         int
	CompletionTokens     int
	CreatedAt            time.Time
}

type ToolErrorType int

const (
	ToolErrorTypeUnknown ToolErrorType = iota
	ToolErrorTypeTransient
	ToolErrorTypePermanent
	ToolErrorTypePolicyDenied
)

func (t ToolErrorType) String() string {
	switch t {
	case ToolErrorTypeTransient:
		return "TRANSIENT"
	case ToolErrorTypePermanent:
		return "PERMANENT"
	case ToolErrorTypePolicyDenied:
		return "POLICY_DENIED"
	default:
		return "UNKNOWN"
	}
}

func (t ToolErrorType) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func ToToolErrorType(value string) (ToolErrorType, error) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "TRANSIENT":
		return ToolErrorTypeTransient, nil
	case "PERMANENT":
		return ToolErrorTypePermanent, nil
	case "POLICY_DENIED":
		return ToolErrorTypePolicyDenied, nil
	default:
		return ToolErrorTypeUnknown, fmt.Errorf("invalid tool error type %q", value)
	}
}

type AgentToolInvocation struct {
	InvocationID    uuid.UUID
	StepID          uuid.UUID
	RunID           uuid.UUID
	OrgID           uuid.UUID
	ToolName        string
	ToolImplVersion string
	Arguments       json.RawMessage
	Result          json.RawMessage
	ErrorType       ToolErrorType
	LatencyMs       int64
	CreatedAt       time.Time
}

type AgentTrajectory struct {
	Run             *AgentRun
	Steps           []*AgentStep
	ToolInvocations []*AgentToolInvocation
}

type ToolResult struct {
	InvocationID    uuid.UUID
	CallID          string
	Name            string
	Content         string
	IsError         bool
	ErrorType       ToolErrorType
	ToolImplVersion string
	TokenEstimate   int
}

type AgentSession struct {
	RunID           uuid.UUID
	OrgID           uuid.UUID
	UserID          uuid.UUID
	Endpoint        *PublishedEndpoint
	Spec            *AgentSpec
	Model           *InferenceModel
	Datasets        []*InferenceDataset
	Messages        []ChatMessage
	DecodingOptions GenerationOptions
	TotalTokens     int
}

type AgentResult struct {
	RequestID  uuid.UUID
	RunID      uuid.UUID
	Answer     string
	Contexts   []RetrievedContext
	StopReason AgentStopReason
	Steps      int
}

type AgentRunLabel struct {
	RunID              uuid.UUID
	Evaluator          string
	TaskSuccess        bool
	ToolSelectionScore float64
	Groundedness       float64
	PolicyViolations   int
	Confidence         float64
	LabelSource        string
	RubricVersion      string
}
