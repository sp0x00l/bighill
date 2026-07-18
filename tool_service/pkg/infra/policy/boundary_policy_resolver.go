package policy

import (
	"strings"
	"time"

	"tool_service/pkg/domain/model"

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

	credential := model.CredentialPolicy{
		Mode: credentialModeNone,
	}
	if tool.ExecutorKind == model.ToolExecutorKindMCP {
		credential = model.CredentialPolicy{
			Mode:       credentialModeBearer,
			SecretRef:  strings.TrimSpace(r.config.PinnedMCPCredentialRef),
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
			CallTimeout: r.config.HTTPTimeout,
		},
		ResponseCap: model.ResponseCapPolicy{
			MaxBytes: r.config.HTTPMaxResponseBytes,
		},
		Credential: credential,
		Schema: model.SchemaPolicy{
			InputSchemaJSON: append([]byte(nil), tool.ParametersJSON...),
		},
	}, nil
}
