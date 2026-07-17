package model

import "github.com/google/uuid"

type AgentResult struct {
	RequestID  uuid.UUID
	RunID      uuid.UUID
	Answer     string
	Contexts   []RetrievedContext
	StopReason AgentStopReason
	Steps      int
}
