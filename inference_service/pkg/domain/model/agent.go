package model

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"lib/shared_lib/userevents"

	"github.com/google/uuid"
)

type CapabilityReport struct {
	CapabilityReportID   uuid.UUID
	EffectiveBaseID      string
	SupportsChat         bool
	SupportsToolCalls    bool
	SupportsSystemPrompt bool
	CreatedAt            time.Time
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
	WallMs   int `json:"wall_ms"`
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
	ToolBindings     []ToolBinding
	RetrievalConfig  json.RawMessage
	Budgets          AgentBudgets
	StopConditions   json.RawMessage
	Guardrails       json.RawMessage
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
	AgentStopReasonToolError
	AgentStopReasonRuntimeError
	AgentStopReasonLoopDetected
	AgentStopReasonDeadline
)

func (r AgentStopReason) String() string {
	switch r {
	case AgentStopReasonFinalAnswer:
		return "FINAL_ANSWER"
	case AgentStopReasonMaxSteps:
		return "MAX_STEPS"
	case AgentStopReasonBudget:
		return "BUDGET_EXCEEDED"
	case AgentStopReasonToolError:
		return "TOOL_ERROR"
	case AgentStopReasonRuntimeError:
		return "RUNTIME_ERROR"
	case AgentStopReasonLoopDetected:
		return "LOOP_DETECTED"
	case AgentStopReasonDeadline:
		return "DEADLINE_EXCEEDED"
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
	case "TOOL_ERROR":
		return AgentStopReasonToolError, nil
	case "RUNTIME_ERROR":
		return AgentStopReasonRuntimeError, nil
	case "LOOP_DETECTED":
		return AgentStopReasonLoopDetected, nil
	case "DEADLINE_EXCEEDED":
		return AgentStopReasonDeadline, nil
	default:
		return AgentStopReasonUnknown, fmt.Errorf("invalid agent stop reason %q", value)
	}
}

type AgentRun struct {
	RunID                   uuid.UUID
	OrgID                   uuid.UUID
	UserID                  uuid.UUID
	EndpointID              uuid.UUID
	AgentSpecHash           string
	ToolsetHash             string
	TrajectorySchemaVersion string
	DecodingParams          json.RawMessage
	Status                  AgentRunStatus
	StopReason              AgentStopReason
	StartedAt               time.Time
	DeadlineAt              time.Time
	FinishedAt              time.Time
	TotalTokens             int
	WallMs                  int
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
	Contexts        []RetrievedContext
	IsError         bool
	ErrorType       ToolErrorType
	ToolImplVersion string
	TokenEstimate   int
}

type AgentSession struct {
	RunID             uuid.UUID
	OrgID             uuid.UUID
	UserID            uuid.UUID
	Endpoint          *PublishedEndpoint
	Spec              *AgentSpec
	Model             *InferenceModel
	Datasets          []*InferenceDataset
	Messages          []ChatMessage
	ResolvedToolSpecs []ToolSpec
	DecodingOptions   GenerationOptions
	TotalTokens       int
}

type AgentResult struct {
	RequestID  uuid.UUID
	RunID      uuid.UUID
	Answer     string
	Contexts   []RetrievedContext
	StopReason AgentStopReason
	Steps      int
}
