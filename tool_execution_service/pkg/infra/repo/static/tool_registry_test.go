package static

import (
	"context"
	"testing"

	"tool_execution_service/pkg/domain"
	"tool_execution_service/pkg/domain/model"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestToolRegistry(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Tool execution static registry suite")
}

var _ = Describe("ToolRegistry", func() {
	It("lists and resolves only tenant-allowlisted tools", func() {
		orgID := uuid.New()
		userID := uuid.New()
		deniedOrgID := uuid.New()
		registry, err := NewToolRegistry([]*model.ToolDefinition{
			{Name: "http_get", Enabled: true, AllowedOrgIDs: []uuid.UUID{orgID}},
			{Name: "calculator", Enabled: true, AllowedOrgIDs: []uuid.UUID{deniedOrgID}},
		})
		Expect(err).NotTo(HaveOccurred())

		tool, err := registry.ResolveTool(context.Background(), orgID, userID, "http_get")
		Expect(err).NotTo(HaveOccurred())
		Expect(tool.Name).To(Equal("http_get"))

		_, err = registry.ResolveTool(context.Background(), deniedOrgID, userID, "http_get")
		Expect(err).To(MatchError(MatchRegexp(domain.ErrToolDenied.Error() + ".*")))

		tools, err := registry.ListAvailableTools(context.Background(), orgID, userID)
		Expect(err).NotTo(HaveOccurred())
		Expect(tools).To(HaveLen(1))
		Expect(tools[0].Name).To(Equal("http_get"))
	})

	It("denies tools with an empty tenant allowlist", func() {
		registry, err := NewToolRegistry([]*model.ToolDefinition{{Name: "http_get", Enabled: true}})
		Expect(err).NotTo(HaveOccurred())

		_, err = registry.ResolveTool(context.Background(), uuid.New(), uuid.New(), "http_get")
		Expect(err).To(MatchError(MatchRegexp(domain.ErrToolDenied.Error() + ".*")))
	})

	It("rejects duplicate tool names at registry construction", func() {
		_, err := NewToolRegistry([]*model.ToolDefinition{
			{Name: "http_get", Enabled: true},
			{Name: "HTTP_GET", Enabled: true},
		})

		Expect(err).To(MatchError(ContainSubstring(`duplicate tool name "HTTP_GET"`)))
	})
})
