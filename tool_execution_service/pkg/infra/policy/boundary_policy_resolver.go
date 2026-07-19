package policy

import (
	"strings"
	"time"

	"tool_execution_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
)

const credentialModeNone = "NONE"
const credentialModeBearer = "BEARER"
const credentialHeaderAuthorization = "Authorization"
const credentialPrefixBearer = "Bearer "

type BoundaryPolicyConfig struct {
	HTTPTimeout            time.Duration
	HTTPMaxResponseBytes   int64
	PinnedMCPCredentialRef string
}

type BoundaryPolicyResolver struct {
	config BoundaryPolicyConfig
}

func NewBoundaryPolicyResolver(config BoundaryPolicyConfig) *BoundaryPolicyResolver {
	log.Trace("NewBoundaryPolicyResolver")

	return &BoundaryPolicyResolver{config: config}
}

func (r *BoundaryPolicyResolver) ResolvePolicy(tool *model.ToolDefinition) (model.PolicySet, error) {
	log.Trace("BoundaryPolicyResolver ResolvePolicy")

	timeout := r.config.HTTPTimeout
	if tool.TimeoutMs > 0 {
		timeout = time.Duration(tool.TimeoutMs) * time.Millisecond
	}
	maxResponseBytes := r.config.HTTPMaxResponseBytes
	if tool.MaxResponseBytes > 0 {
		maxResponseBytes = tool.MaxResponseBytes
	}
	credential := model.CredentialPolicy{
		Mode: credentialModeNone,
	}
	if tool.ExecutorKind == model.ToolExecutorKindMCP {
		secretRef := strings.TrimSpace(tool.CredentialRef)
		if secretRef == "" {
			secretRef = strings.TrimSpace(r.config.PinnedMCPCredentialRef)
		}
		credential = model.CredentialPolicy{
			Mode:       credentialModeBearer,
			SecretRef:  secretRef,
			HeaderName: credentialHeaderAuthorization,
			Prefix:     credentialPrefixBearer,
		}
	}
	return model.PolicySet{
		Egress: model.EgressPolicy{
			AllowedSchemes: []string{"http", "https"},
			AllowedHosts:   append([]string(nil), tool.EgressHosts...),
		},
		Timeout: model.TimeoutPolicy{
			CallTimeout: timeout,
		},
		ResponseCap: model.ResponseCapPolicy{
			MaxBytes: maxResponseBytes,
		},
		Credential: credential,
		Schema: model.SchemaPolicy{
			InputSchemaJSON: append([]byte(nil), tool.ParametersJSON...),
		},
	}, nil
}
