package model

import (
	"time"

	"github.com/google/uuid"
)

type AgentChampionUpdate struct {
	OrgID                 uuid.UUID
	EndpointID            uuid.UUID
	AgentLineage          string
	AgentSpecHash         string
	ServingModelID        uuid.UUID
	PreviousAgentSpecHash string
	DecisionID            uuid.UUID
	DecidedAt             time.Time
}
