package model

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type AgentAdapterStatus int

const (
	AgentAdapterStatusUnknown AgentAdapterStatus = iota
	AgentAdapterStatusTraining
	AgentAdapterStatusCandidate
	AgentAdapterStatusEvaluated
	AgentAdapterStatusPromoted
	AgentAdapterStatusRejected
	AgentAdapterStatusFailed
)

func (s AgentAdapterStatus) String() string {
	switch s {
	case AgentAdapterStatusTraining:
		return "TRAINING"
	case AgentAdapterStatusCandidate:
		return "CANDIDATE"
	case AgentAdapterStatusEvaluated:
		return "EVALUATED"
	case AgentAdapterStatusPromoted:
		return "PROMOTED"
	case AgentAdapterStatusRejected:
		return "REJECTED"
	case AgentAdapterStatusFailed:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

func ToAgentAdapterStatus(value string) (AgentAdapterStatus, error) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "TRAINING":
		return AgentAdapterStatusTraining, nil
	case "CANDIDATE":
		return AgentAdapterStatusCandidate, nil
	case "EVALUATED":
		return AgentAdapterStatusEvaluated, nil
	case "PROMOTED":
		return AgentAdapterStatusPromoted, nil
	case "REJECTED":
		return AgentAdapterStatusRejected, nil
	case "FAILED":
		return AgentAdapterStatusFailed, nil
	default:
		return AgentAdapterStatusUnknown, fmt.Errorf("invalid agent adapter status %q", value)
	}
}

type DispatchAgentAdapterTrainingCommand struct {
	OrgID           uuid.UUID
	UserID          uuid.UUID
	AgentLineage    string
	DatasetID       uuid.UUID
	TrainingProfile string
}

type AgentAdapterTrainingRequest struct {
	OrgID            uuid.UUID
	UserID           uuid.UUID
	AgentLineage     string
	DatasetID        uuid.UUID
	DatasetURI       string
	ContentHash      string
	SourceModelID    uuid.UUID
	TrainingProfile  string
	EffectiveBaseID  string
	AgentSpecHash    string
	ToolsetHash      string
	DataSnapshotHash string
}

type AgentAdapterTrainingResult struct {
	TrainingRunID    uuid.UUID
	ServingModelID   uuid.UUID
	AdapterURI       string
	AdapterChecksum  string
	TrainingProvider string
}

type AgentAdapterTrainingCompletion struct {
	TrainingRunID    uuid.UUID
	OrgID            uuid.UUID
	ServingModelID   uuid.UUID
	AdapterURI       string
	AdapterChecksum  string
	TrainingProvider string
}

type AgentAdapterTrainingFailure struct {
	TrainingRunID uuid.UUID
	OrgID         uuid.UUID
	FailureReason string
}

type AgentAdapter struct {
	AdapterID                        uuid.UUID
	OrgID                            uuid.UUID
	AgentLineage                     string
	DatasetID                        uuid.UUID
	TrainingRunID                    uuid.UUID
	ServingModelID                   uuid.UUID
	AdapterURI                       string
	AdapterChecksum                  string
	TrainingProvider                 string
	TrainedAgainstEffectiveBaseID    string
	TrainedAgainstAgentSpecHash      string
	TrainedAgainstToolsetHash        string
	TrainedAgainstDataSnapshotHash   string
	TrainedAgainstRubricVersion      string
	TrainedAgainstGoldenSplitVersion int
	Status                           AgentAdapterStatus
	PromotionPassed                  bool
	CreatedByUserID                  uuid.UUID
	CreatedAt                        time.Time
	UpdatedAt                        time.Time
}

type EvaluateAdapterCandidateCommand struct {
	OrgID               uuid.UUID
	UserID              uuid.UUID
	AgentLineage        string
	AdapterID           uuid.UUID
	EndpointID          uuid.UUID
	SplitVersion        int
	MinTaskSuccessRate  float64
	MinToolSuccessRate  float64
	MinGroundednessRate float64
}

type PromoteAgentAdapterCommand struct {
	OrgID        uuid.UUID
	UserID       uuid.UUID
	AgentLineage string
	AdapterID    uuid.UUID
	ReportID     uuid.UUID
	MinDelta     float64
}
