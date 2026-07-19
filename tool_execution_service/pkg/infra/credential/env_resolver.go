package credential

import (
	"context"
	"os"
	"strings"

	"tool_execution_service/pkg/domain"

	log "github.com/sirupsen/logrus"
)

type EnvResolver struct {
	values map[string]string
}

func NewEnvResolver(values map[string]string) *EnvResolver {
	log.Trace("NewEnvResolver")

	return &EnvResolver{values: values}
}

func (r *EnvResolver) ResolveCredential(_ context.Context, ref string) (string, error) {
	log.Trace("EnvResolver ResolveCredential")

	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", domain.ErrToolPolicy.Extend("tool credential ref is not configured")
	}
	if r.values != nil {
		if value := strings.TrimSpace(r.values[ref]); value != "" {
			return value, nil
		}
	}
	if value := strings.TrimSpace(os.Getenv(ref)); value != "" {
		return value, nil
	}
	return "", domain.ErrToolDenied.Extend("tool credential is unavailable")
}
