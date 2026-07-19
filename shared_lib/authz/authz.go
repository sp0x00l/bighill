package authz

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

const (
	HeaderUserID      = "X-User-ID"
	HeaderSessionID   = "X-Session-ID"
	HeaderOrgID       = "X-Org-ID"
	HeaderRoles       = "X-Roles"
	HeaderPermissions = "X-Permissions"
)

const (
	RoleConsumer      = "consumer"
	RoleMLResearcher  = "ml_researcher"
	RoleOrgAdmin      = "org_admin"
	RolePlatformAdmin = "platform_admin"
)

const (
	PermissionInferenceEndpointsRead = "inference:endpoints:read"
	PermissionInferenceAgentRunsRead = "inference:agent-runs:read"
	PermissionInferenceInvoke        = "inference:invoke"
	PermissionInferenceFeedback      = "inference:feedback"
	PermissionDataRead               = "data:read"
	PermissionDataWrite              = "data:write"
	PermissionModelRead              = "model:read"
	PermissionModelWrite             = "model:write"
	PermissionTrainingRead           = "training:read"
	PermissionTrainingStart          = "training:start"
	PermissionPublishEndpoints       = "inference:endpoints:publish"
	PermissionOrgMembersRead         = "org:members:read"
	PermissionOrgMembersWrite        = "org:members:write"
	PermissionToolCatalogPublish     = "tool-catalog:capabilities:publish"
)

var rolePermissions = map[string][]string{
	RoleConsumer: {
		PermissionInferenceEndpointsRead,
		PermissionInferenceAgentRunsRead,
		PermissionInferenceInvoke,
		PermissionInferenceFeedback,
	},
	RoleMLResearcher: {
		PermissionInferenceEndpointsRead,
		PermissionInferenceAgentRunsRead,
		PermissionInferenceInvoke,
		PermissionInferenceFeedback,
		PermissionDataRead,
		PermissionDataWrite,
		PermissionModelRead,
		PermissionModelWrite,
		PermissionTrainingRead,
		PermissionTrainingStart,
		PermissionPublishEndpoints,
	},
	RoleOrgAdmin: {
		PermissionInferenceEndpointsRead,
		PermissionInferenceAgentRunsRead,
		PermissionInferenceInvoke,
		PermissionInferenceFeedback,
		PermissionDataRead,
		PermissionDataWrite,
		PermissionModelRead,
		PermissionModelWrite,
		PermissionTrainingRead,
		PermissionTrainingStart,
		PermissionPublishEndpoints,
		PermissionOrgMembersRead,
		PermissionOrgMembersWrite,
		PermissionToolCatalogPublish,
	},
	RolePlatformAdmin: {
		PermissionInferenceEndpointsRead,
		PermissionInferenceAgentRunsRead,
		PermissionInferenceInvoke,
		PermissionInferenceFeedback,
		PermissionDataRead,
		PermissionDataWrite,
		PermissionModelRead,
		PermissionModelWrite,
		PermissionTrainingRead,
		PermissionTrainingStart,
		PermissionPublishEndpoints,
		PermissionOrgMembersRead,
		PermissionOrgMembersWrite,
		PermissionToolCatalogPublish,
	},
}

type TokenClaims struct {
	UserID      string   `json:"userId"`
	SessionID   string   `json:"sid"`
	OrgID       string   `json:"orgId"`
	Roles       []string `json:"roles"`
	Permissions []string `json:"permissions"`
	TokenType   string   `json:"tokenType"`
}

type RequestContext struct {
	UserID      uuid.UUID
	SessionID   string
	OrgID       uuid.UUID
	Roles       []string
	Permissions []string
}

type requestContextKey struct{}

func ValidRole(role string) bool {
	_, ok := rolePermissions[strings.TrimSpace(role)]
	return ok
}

func PermissionsForRole(role string) []string {
	permissions := append([]string(nil), rolePermissions[strings.TrimSpace(role)]...)
	sort.Strings(permissions)
	return permissions
}

func HasPermission(permissions []string, required string) bool {
	required = strings.TrimSpace(required)
	for _, permission := range permissions {
		if strings.TrimSpace(permission) == required {
			return true
		}
	}
	return false
}

func WithRequestContext(ctx context.Context, auth RequestContext) context.Context {
	return context.WithValue(ctx, requestContextKey{}, auth)
}

func FromContext(ctx context.Context) (RequestContext, bool) {
	auth, ok := ctx.Value(requestContextKey{}).(RequestContext)
	if !ok || auth.UserID == uuid.Nil || auth.OrgID == uuid.Nil {
		return RequestContext{}, false
	}
	return auth, true
}

func ReadUserIDHeader(ctx context.Context, r *http.Request) (uuid.UUID, error) {
	return readUUIDHeader(ctx, r, HeaderUserID, "user ID")
}

func ReadOrgIDHeader(ctx context.Context, r *http.Request) (uuid.UUID, error) {
	return readUUIDHeader(ctx, r, HeaderOrgID, "org ID")
}

func ReadSessionIDHeader(ctx context.Context, r *http.Request) (string, error) {
	sessionID := strings.TrimSpace(r.Header.Get(HeaderSessionID))
	if sessionID == "" {
		err := fmt.Errorf("%s header missing", HeaderSessionID)
		log.WithContext(ctx).Error(err.Error())
		return "", err
	}
	return sessionID, nil
}

func ReadPermissionsHeader(ctx context.Context, r *http.Request) ([]string, error) {
	value := strings.TrimSpace(r.Header.Get(HeaderPermissions))
	if value == "" {
		err := fmt.Errorf("%s header missing", HeaderPermissions)
		log.WithContext(ctx).Error(err.Error())
		return nil, err
	}
	return DecodeStringSlice(value)
}

func EncodeStringSlice(values []string) string {
	normalized := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			normalized = append(normalized, value)
		}
	}
	payload, err := json.Marshal(normalized)
	if err != nil {
		return "[]"
	}
	return string(payload)
}

func DecodeStringSlice(value string) ([]string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	var values []string
	if strings.HasPrefix(value, "[") {
		if err := json.Unmarshal([]byte(value), &values); err != nil {
			return nil, fmt.Errorf("decode string array: %w", err)
		}
		return normalizeStrings(values), nil
	}
	return normalizeStrings(strings.Split(value, ",")), nil
}

func normalizeStrings(values []string) []string {
	normalized := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			normalized = append(normalized, value)
		}
	}
	return normalized
}

func readUUIDHeader(ctx context.Context, r *http.Request, header string, label string) (uuid.UUID, error) {
	value := strings.TrimSpace(r.Header.Get(header))
	if value == "" {
		err := fmt.Errorf("%s header missing", header)
		log.WithContext(ctx).Error(err.Error())
		return uuid.Nil, err
	}
	parsed, err := uuid.Parse(value)
	if err != nil || parsed == uuid.Nil {
		err := fmt.Errorf("invalid %s", label)
		log.WithContext(ctx).WithError(err).Error(err.Error())
		return uuid.Nil, err
	}
	return parsed, nil
}
