package model

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

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
