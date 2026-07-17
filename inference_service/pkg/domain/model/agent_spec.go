package model

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"lib/shared_lib/userevents"

	"github.com/google/uuid"
)

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

type AgentBudgets struct {
	MaxSteps int
	Token    int
	WallMs   int
}

type ToolBinding struct {
	Name       string
	Required   bool
	ToolChoice string
	Config     json.RawMessage
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
