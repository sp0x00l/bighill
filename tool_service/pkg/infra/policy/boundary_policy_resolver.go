package policy

import (
	"time"

	"tool_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
)

const credentialModeNone = "NONE"

type BoundaryPolicyConfig struct {
	HTTPTimeout          time.Duration
	HTTPMaxResponseBytes int64
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
		Credential: model.CredentialPolicy{
			Mode: credentialModeNone,
		},
		Schema: model.SchemaPolicy{
			InputSchemaJSON: append([]byte(nil), tool.ParametersJSON...),
		},
	}, nil
}
