package model

import (
	"fmt"
	"strings"
)

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
