package model

import (
	"time"

	"github.com/google/uuid"
)

type EvaluateSpecChampionCommand struct {
	OrgID               uuid.UUID
	UserID              uuid.UUID
	AgentLineage        string
	AgentSpecHash       string
	AdapterID           uuid.UUID
	EndpointID          uuid.UUID
	SplitVersion        int
	MinTaskSuccessRate  float64
	MinToolSuccessRate  float64
	MinGroundednessRate float64
}

type AgentEvalReport struct {
	ReportID           uuid.UUID
	OrgID              uuid.UUID
	AgentLineage       string
	AgentSpecHash      string
	AdapterID          uuid.UUID
	EndpointID         uuid.UUID
	Split              GoldenTaskSplit
	SplitVersion       int
	RubricVersion      string
	TaskCount          int
	TaskSuccessRate    float64
	ToolSuccessRate    float64
	GroundednessRate   float64
	Passed             bool
	GateReason         string
	PromotedDecisionID uuid.UUID
	EvaluatedBy        uuid.UUID
	EvaluatedAt        time.Time
	TaskResults        []*AgentEvalTaskResult
}

type AgentEvalTaskResult struct {
	OrgID         uuid.UUID
	ReportID      uuid.UUID
	TaskID        uuid.UUID
	RunID         uuid.UUID
	Status        string
	StopReason    string
	TaskSuccess   bool
	ToolSuccess   bool
	Groundedness  bool
	FailureReason string
}

type AgentTaskRunCommand struct {
	OrgID          uuid.UUID
	UserID         uuid.UUID
	EndpointID     uuid.UUID
	AgentSpecHash  string
	ServingModelID uuid.UUID
	TaskID         uuid.UUID
	QueryText      string
}

type AgentTaskRunResult struct {
	RunID                uuid.UUID
	Status               string
	StopReason           string
	Answer               string
	GroundedContextCount int
	GroundedContextTexts []string
	ToolInvocations      []AgentTaskToolInvocation
}

type AgentTaskToolInvocation struct {
	ToolName  string
	ErrorType string
}
