package static

import (
	"context"
	"errors"

	"tool_execution_service/pkg/domain"
	"tool_execution_service/pkg/domain/model"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("CompositeToolRegistry", func() {
	It("merges static and projected registries", func() {
		local := &compositeRegistryStub{tools: []*model.ToolDefinition{{Name: "search_knowledge", Enabled: true}}}
		projected := &compositeRegistryStub{tools: []*model.ToolDefinition{{Name: "crm_lookup", Enabled: true}}}
		registry, err := NewCompositeToolRegistry(local, projected)
		Expect(err).NotTo(HaveOccurred())

		tools, err := registry.ListAvailableTools(context.Background(), uuid.New(), uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Expect(tools).To(HaveLen(2))
		Expect(tools[0].Name).To(Equal("search_knowledge"))
		Expect(tools[1].Name).To(Equal("crm_lookup"))
	})

	It("fails closed on duplicate projected tool names", func() {
		local := &compositeRegistryStub{tools: []*model.ToolDefinition{{Name: "crm_lookup", Enabled: true}}}
		projected := &compositeRegistryStub{tools: []*model.ToolDefinition{{Name: "CRM_LOOKUP", Enabled: true}}}
		registry, err := NewCompositeToolRegistry(local, projected)
		Expect(err).NotTo(HaveOccurred())

		tools, err := registry.ListAvailableTools(context.Background(), uuid.New(), uuid.New())

		Expect(tools).To(BeNil())
		Expect(errors.Is(err, domain.ErrToolPolicy)).To(BeTrue())
		Expect(err).To(MatchError(ContainSubstring("duplicate projected tool name CRM_LOOKUP")))
	})

	It("preserves tenant-denied errors when resolving across registries", func() {
		registry, err := NewCompositeToolRegistry(
			&compositeRegistryStub{resolveErr: domain.ErrToolNotFound.Extend("crm_lookup")},
			&compositeRegistryStub{resolveErr: domain.ErrToolDenied.Extend("tool is not granted")},
		)
		Expect(err).NotTo(HaveOccurred())

		tool, err := registry.ResolveTool(context.Background(), uuid.New(), uuid.New(), "crm_lookup")

		Expect(tool).To(BeNil())
		Expect(errors.Is(err, domain.ErrToolDenied)).To(BeTrue())
	})

	It("returns not-found only when no registry resolves or denies the tool", func() {
		registry, err := NewCompositeToolRegistry(
			&compositeRegistryStub{resolveErr: domain.ErrToolNotFound.Extend("missing")},
			&compositeRegistryStub{resolveErr: domain.ErrToolNotFound.Extend("missing")},
		)
		Expect(err).NotTo(HaveOccurred())

		tool, err := registry.ResolveTool(context.Background(), uuid.New(), uuid.New(), "missing")

		Expect(tool).To(BeNil())
		Expect(errors.Is(err, domain.ErrToolNotFound)).To(BeTrue())
	})
})

type compositeRegistryStub struct {
	tools      []*model.ToolDefinition
	tool       *model.ToolDefinition
	listErr    error
	resolveErr error
}

func (s *compositeRegistryStub) ListAvailableTools(context.Context, uuid.UUID, uuid.UUID) ([]*model.ToolDefinition, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	return s.tools, nil
}

func (s *compositeRegistryStub) ResolveTool(context.Context, uuid.UUID, uuid.UUID, string) (*model.ToolDefinition, error) {
	if s.resolveErr != nil {
		return nil, s.resolveErr
	}
	if s.tool != nil {
		return s.tool, nil
	}
	return nil, domain.ErrToolNotFound
}
