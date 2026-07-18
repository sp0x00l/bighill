package model

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type ToolExecutorKind int

const (
	ToolExecutorKindUnknown ToolExecutorKind = iota
	ToolExecutorKindHTTPGet
	ToolExecutorKindCalculator
)

func (k ToolExecutorKind) String() string {
	switch k {
	case ToolExecutorKindHTTPGet:
		return "HTTP_GET"
	case ToolExecutorKindCalculator:
		return "CALCULATOR"
	default:
		return "UNKNOWN"
	}
}

func (k ToolExecutorKind) MarshalText() ([]byte, error) {
	return []byte(k.String()), nil
}

func ToToolExecutorKind(value string) (ToolExecutorKind, error) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "HTTP_GET":
		return ToolExecutorKindHTTPGet, nil
	case "CALCULATOR":
		return ToolExecutorKindCalculator, nil
	default:
		return ToolExecutorKindUnknown, fmt.Errorf("invalid tool executor kind %q", value)
	}
}

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

type ToolInvocationAuditStatus int

const (
	ToolInvocationAuditStatusUnknown ToolInvocationAuditStatus = iota
	ToolInvocationAuditStatusCompleted
	ToolInvocationAuditStatusDenied
	ToolInvocationAuditStatusFailed
)

func (s ToolInvocationAuditStatus) String() string {
	switch s {
	case ToolInvocationAuditStatusCompleted:
		return "COMPLETED"
	case ToolInvocationAuditStatusDenied:
		return "DENIED"
	case ToolInvocationAuditStatusFailed:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

type ToolDefinition struct {
	Name                  string
	Description           string
	ParametersJSON        []byte
	ImplementationVersion string
	ExecutorKind          ToolExecutorKind
	EgressHosts           []string
	AllowedOrgIDs         []uuid.UUID
	Enabled               bool
}

type ListAvailableToolsCommand struct {
	OrgID  uuid.UUID
	UserID uuid.UUID
}

type InvokeToolCommand struct {
	InvocationID  uuid.UUID
	ToolName      string
	ArgumentsJSON []byte
	OrgID         uuid.UUID
	UserID        uuid.UUID
	TraceID       string
}

type ToolInvocationResult struct {
	ResultJSON            []byte
	IsError               bool
	ErrorCode             string
	ErrorMessage          string
	ErrorType             ToolErrorType
	ImplementationVersion string
	LatencyMs             int64
	EgressHost            string
}

type ToolInvocationAudit struct {
	InvocationID          uuid.UUID
	OrgID                 uuid.UUID
	UserID                uuid.UUID
	ToolName              string
	ImplementationVersion string
	ExecutorKind          ToolExecutorKind
	Status                ToolInvocationAuditStatus
	ErrorCode             string
	ErrorType             ToolErrorType
	LatencyMs             int64
	EgressHost            string
	TraceID               string
	ArgsHash              string
	ArgsPreview           string
}

type PolicySet struct {
	Egress      EgressPolicy
	Timeout     TimeoutPolicy
	ResponseCap ResponseCapPolicy
	Credential  CredentialPolicy
	Schema      SchemaPolicy
}

type EgressPolicy struct {
	AllowedSchemes []string
	AllowedHosts   []string
}

type TimeoutPolicy struct {
	CallTimeout time.Duration
}

type ResponseCapPolicy struct {
	MaxBytes int64
}

type CredentialPolicy struct {
	Mode string
}

type SchemaPolicy struct {
	InputSchemaJSON []byte
}
