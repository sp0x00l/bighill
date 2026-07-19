package credential

import (
	"context"
	"testing"

	"tool_execution_service/pkg/domain"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestCredentialResolver(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Tool execution credential resolver suite")
}

var _ = Describe("EnvResolver", func() {
	It("resolves a credential by opaque environment reference", func() {
		t := GinkgoT()
		t.Setenv("PARTNER_MCP_TOKEN", "secret-token")
		resolver := NewEnvResolver(nil)

		value, err := resolver.ResolveCredential(context.Background(), "PARTNER_MCP_TOKEN")

		Expect(err).NotTo(HaveOccurred())
		Expect(value).To(Equal("secret-token"))
	})

	It("fails closed when the referenced credential is missing", func() {
		resolver := NewEnvResolver(nil)

		_, err := resolver.ResolveCredential(context.Background(), "MISSING_MCP_TOKEN")

		Expect(err).To(MatchError(ContainSubstring("tool credential is unavailable")))
		Expect(err).To(MatchError(MatchRegexp(domain.ErrToolDenied.Error() + ".*")))
	})
})
