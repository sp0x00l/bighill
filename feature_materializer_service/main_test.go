package main

import (
	"os"
	"strings"
	"testing"

	env "lib/shared_lib/env"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestFeatureMaterializerMain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Feature materializer main unit test suite")
}

var _ = Describe("staging Helm values", func() {
	It("does not inherit local bucket or deterministic embeddings", func() {
		values := readTextFile("helm/staging-values.yaml")

		Expect(values).To(ContainSubstring(`artifactBucketName: "bighill-mlops-lakehouse"`))
		Expect(values).To(ContainSubstring(`embeddingProvider: "tei"`))
		Expect(values).NotTo(ContainSubstring(`artifactBucketName: "local-dev-bucket"`))
		Expect(values).NotTo(ContainSubstring(`embeddingProvider: "deterministic"`))
		Expect(values).NotTo(ContainSubstring("localhost:4566"))
	})
})

func readTextFile(path string) string {
	content, err := os.ReadFile(path)
	Expect(err).NotTo(HaveOccurred())
	return strings.TrimSpace(string(content))
}

var _ = Describe("readMaterializerConfig", func() {
	BeforeEach(func() {
		env.ResetEnvironmentCache()
		Expect(os.Unsetenv("FEATURE_MATERIALIZER_SERVICE_PROFILE_SUBSCRIBER_TOPIC")).To(Succeed())
	})

	It("uses the profile service topic for tenant projections by default", func() {
		cfg := readMaterializerConfig()

		Expect(cfg.ProfileTopic).To(Equal("profile"))
	})
})
