package model

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type CapabilityKind int

const (
	CapabilityKindUnknown CapabilityKind = iota
	CapabilityKindHTTPGet
	CapabilityKindMCP
)

func (k CapabilityKind) String() string {
	switch k {
	case CapabilityKindHTTPGet:
		return "HTTP_GET"
	case CapabilityKindMCP:
		return "MCP"
	default:
		return "UNKNOWN"
	}
}

func ToCapabilityKind(value string) (CapabilityKind, error) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "HTTP_GET":
		return CapabilityKindHTTPGet, nil
	case "MCP":
		return CapabilityKindMCP, nil
	default:
		return CapabilityKindUnknown, fmt.Errorf("invalid capability kind %q", value)
	}
}

type CapabilityLifecycleStatus int

const (
	CapabilityLifecycleStatusUnknown CapabilityLifecycleStatus = iota
	CapabilityLifecycleStatusActive
)

func (s CapabilityLifecycleStatus) String() string {
	switch s {
	case CapabilityLifecycleStatusActive:
		return "ACTIVE"
	default:
		return "UNKNOWN"
	}
}

func ToCapabilityLifecycleStatus(value string) (CapabilityLifecycleStatus, error) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "ACTIVE":
		return CapabilityLifecycleStatusActive, nil
	default:
		return CapabilityLifecycleStatusUnknown, fmt.Errorf("invalid capability lifecycle status %q", value)
	}
}

type TenantGrantStatus int

const (
	TenantGrantStatusUnknown TenantGrantStatus = iota
	TenantGrantStatusActive
	TenantGrantStatusRevoked
)

func (s TenantGrantStatus) String() string {
	switch s {
	case TenantGrantStatusActive:
		return "ACTIVE"
	case TenantGrantStatusRevoked:
		return "REVOKED"
	default:
		return "UNKNOWN"
	}
}

func ToTenantGrantStatus(value string) (TenantGrantStatus, error) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "ACTIVE":
		return TenantGrantStatusActive, nil
	case "REVOKED":
		return TenantGrantStatusRevoked, nil
	default:
		return TenantGrantStatusUnknown, fmt.Errorf("invalid tenant grant status %q", value)
	}
}

type ToolCapabilityVersion struct {
	CapabilityVersionID   uuid.UUID
	CapabilityID          string
	Version               string
	ToolName              string
	Kind                  CapabilityKind
	MCPServerEndpoint     string
	Description           string
	ParametersJSON        []byte
	ImplementationVersion string
	EgressHosts           []string
	TimeoutMs             int64
	MaxResponseBytes      int64
	CredentialName        string
	CredentialRequired    bool
	LifecycleStatus       CapabilityLifecycleStatus
	ContentHash           string
	PublishedByUserID     uuid.UUID
	PublishedAt           time.Time
}

type TenantCapabilityGrant struct {
	GrantID             uuid.UUID
	OrgID               uuid.UUID
	CapabilityVersionID uuid.UUID
	Status              TenantGrantStatus
	GrantedByUserID     uuid.UUID
	GrantedAt           time.Time
}

type ToolCredentialBinding struct {
	BindingID     uuid.UUID
	OrgID         uuid.UUID
	CapabilityID  string
	CredentialRef string
	BoundByUserID uuid.UUID
	BoundAt       time.Time
}

type PublishCapabilityCommand struct {
	UserID             uuid.UUID
	CapabilityID       string
	Version            string
	ToolName           string
	Kind               CapabilityKind
	MCPServerEndpoint  string
	Description        string
	ParametersJSON     []byte
	EgressHosts        []string
	TimeoutMs          int64
	MaxResponseBytes   int64
	CredentialName     string
	CredentialRequired bool
}

type GrantCapabilityCommand struct {
	UserID              uuid.UUID
	OrgID               uuid.UUID
	CapabilityVersionID uuid.UUID
}

type BindCredentialCommand struct {
	UserID        uuid.UUID
	OrgID         uuid.UUID
	CapabilityID  string
	CredentialRef string
}
