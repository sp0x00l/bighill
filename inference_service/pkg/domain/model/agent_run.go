package model

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

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
	AgentStopReasonAbandoned
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
	case AgentStopReasonAbandoned:
		return "ABANDONED"
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
	case "ABANDONED":
		return AgentStopReasonAbandoned, nil
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

type AgentTrajectory struct {
	Run             *AgentRun
	Steps           []*AgentStep
	ToolInvocations []*AgentToolInvocation
}
