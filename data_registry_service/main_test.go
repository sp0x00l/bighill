package main

import (
	"os"
	"strings"
	"testing"

	env "lib/shared_lib/env"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDataRegistryMain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Data registry main unit test suite")
}

var _ = Describe("staging Helm values", func() {
	It("uses Polaris and does not point the DLQ at LocalStack", func() {
		values := readTextFile("helm/staging-values.yaml")

		Expect(values).To(ContainSubstring(`catalogProvider: "polaris"`))
		Expect(values).NotTo(ContainSubstring(`catalogProvider: "local"`))
		Expect(values).NotTo(ContainSubstring("localhost:4566"))
	})
})

func readTextFile(path string) string {
	content, err := os.ReadFile(path)
	Expect(err).NotTo(HaveOccurred())
	return strings.TrimSpace(string(content))
}

var _ = Describe("readRegistryConfig", func() {
	BeforeEach(func() {
		env.ResetEnvironmentCache()
		Expect(os.Unsetenv("DATA_REGISTRY_SERVICE_PROFILE_SUBSCRIBER_TOPIC")).To(Succeed())
	})

	It("uses the profile service topic for tenant projections by default", func() {
		cfg := readRegistryConfig()

		Expect(cfg.ProfileTopic).To(Equal("profile"))
	})
})
