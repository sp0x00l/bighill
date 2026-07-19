package model

import (
	"time"

	"github.com/google/uuid"
)

type LabelAgentRunCommand struct {
	OrgID              uuid.UUID
	UserID             uuid.UUID
	RunID              uuid.UUID
	AgentLineage       string
	Prompt             string
	Evaluator          string
	TaskSuccess        bool
	ToolSelectionScore float64
	Groundedness       float64
	PolicyViolations   int
	Confidence         float64
	LabelSource        string
	RubricVersion      string
}

type AgentRunLabel struct {
	LabelID                  uuid.UUID
	OrgID                    uuid.UUID
	RunID                    uuid.UUID
	AgentLineage             string
	AgentSpecHash            string
	ToolsetHash              string
	EffectiveBaseID          string
	DataSnapshotHash         string
	ContentFingerprint       string
	NearDuplicateFingerprint string
	Evaluator                string
	TaskSuccess              bool
	ToolSelectionScore       float64
	Groundedness             float64
	PolicyViolations         int
	Confidence               float64
	LabelSource              string
	RubricVersion            string
	CreatedByUserID          uuid.UUID
	CreatedAt                time.Time
}

type ListAgentRunLabelsCommand struct {
	OrgID        uuid.UUID
	AgentLineage string
}

type AgentTrajectoryRef struct {
	RunID            uuid.UUID
	OrgID            uuid.UUID
	UserID           uuid.UUID
	EndpointID       uuid.UUID
	AgentSpecHash    string
	ToolsetHash      string
	EffectiveBaseID  string
	DataSnapshotHash string
	Status           string
	StopReason       string
}
