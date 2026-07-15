package static

import (
	"context"
	"testing"

	"tool_service/pkg/domain"
	"tool_service/pkg/domain/model"

	"github.com/google/uuid"
	. "github.com/onsi/gomega"
)

func TestToolRegistryTenantAllowlist(t *testing.T) {
	RegisterTestingT(t)

	orgID := uuid.New()
	userID := uuid.New()
	deniedOrgID := uuid.New()
	registry := NewToolRegistry([]*model.ToolDefinition{
		{Name: "http_get", Enabled: true, AllowedOrgIDs: []uuid.UUID{orgID}},
		{Name: "calculator", Enabled: true, AllowedOrgIDs: []uuid.UUID{deniedOrgID}},
	})

	tool, err := registry.ResolveTool(context.Background(), orgID, userID, "http_get")
	Expect(err).NotTo(HaveOccurred())
	Expect(tool.Name).To(Equal("http_get"))

	_, err = registry.ResolveTool(context.Background(), deniedOrgID, userID, "http_get")
	Expect(err).To(MatchError(MatchRegexp(domain.ErrToolDenied.Error() + ".*")))

	tools, err := registry.ListAvailableTools(context.Background(), orgID, userID)
	Expect(err).NotTo(HaveOccurred())
	Expect(tools).To(HaveLen(1))
	Expect(tools[0].Name).To(Equal("http_get"))
}

func TestToolRegistryDeniesEmptyAllowlist(t *testing.T) {
	RegisterTestingT(t)

	registry := NewToolRegistry([]*model.ToolDefinition{{Name: "http_get", Enabled: true}})

	_, err := registry.ResolveTool(context.Background(), uuid.New(), uuid.New(), "http_get")
	Expect(err).To(MatchError(MatchRegexp(domain.ErrToolDenied.Error() + ".*")))
}
