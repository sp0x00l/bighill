package model

import (
	"time"

	"github.com/google/uuid"
)

type AgentSpecVersion struct {
	OrgID              uuid.UUID
	AgentLineage       string
	AgentSpecHash      string
	ModelID            uuid.UUID
	RegisteredByUserID uuid.UUID
	RegisteredAt       time.Time
}

type AgentEndpointBinding struct {
	OrgID           uuid.UUID
	AgentLineage    string
	EndpointID      uuid.UUID
	CreatedByUserID uuid.UUID
	CreatedAt       time.Time
}

type AgentChampionState struct {
	OrgID                 uuid.UUID
	AgentLineage          string
	ChampionAgentSpecHash string
	ChampionAdapterID     uuid.UUID
	ServingModelID        uuid.UUID
	PreviousAgentSpecHash string
	DecisionID            uuid.UUID
	DecidedBy             uuid.UUID
	DecidedAt             time.Time
}

type RegisterAgentSpecVersionCommand struct {
	OrgID         uuid.UUID
	UserID        uuid.UUID
	AgentLineage  string
	AgentSpecHash string
}

type RegisterEndpointBindingCommand struct {
	OrgID        uuid.UUID
	UserID       uuid.UUID
	AgentLineage string
	EndpointID   uuid.UUID
}

type PromoteSpecChampionCommand struct {
	OrgID         uuid.UUID
	UserID        uuid.UUID
	AgentLineage  string
	AgentSpecHash string
	DecisionID    uuid.UUID
	DecidedAt     time.Time
}

type AgentSpecRef struct {
	OrgID         uuid.UUID
	AgentSpecHash string
	AgentLineage  string
	ModelID       uuid.UUID
}

type EndpointRef struct {
	OrgID      uuid.UUID
	EndpointID uuid.UUID
}
