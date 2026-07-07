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

var _ = Describe("Helm values", func() {
	It("configures local embedding adapters", func() {
		values := readTextFile("helm/values.yaml")

		Expect(values).To(ContainSubstring(`embeddingProvider: "tei"`))
		Expect(values).To(ContainSubstring(`embeddingUrl: "http://text-embeddings-inference:80"`))
	})

	It("does not inherit local bucket settings", func() {
		values := readTextFile("helm/staging-values.yaml")

		Expect(values).To(ContainSubstring(`artifactBucketName: "bighill-mlops-lakehouse"`))
		Expect(values).To(ContainSubstring(`embeddingProvider: "tei"`))
		Expect(values).NotTo(ContainSubstring(`artifactBucketName: "local-dev-bucket"`))
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
		Expect(os.Setenv("ENVIRONMENT", "LOCAL-DEV")).To(Succeed())
		env.ResetEnvironmentCache()
		Expect(os.Setenv("FEATURE_MATERIALIZER_SERVICE_ARTIFACT_BUCKET_NAME", "local-dev-bucket")).To(Succeed())
		Expect(os.Setenv("FEATURE_MATERIALIZER_SERVICE_EMBEDDING_PROVIDER", "tei")).To(Succeed())
		Expect(os.Setenv("FEATURE_MATERIALIZER_SERVICE_EMBEDDING_URL", "http://tei.local")).To(Succeed())
		Expect(os.Unsetenv("FEATURE_MATERIALIZER_SERVICE_PROFILE_SUBSCRIBER_TOPIC")).To(Succeed())
	})

	It("uses the profile service topic for tenant projections by default", func() {
		cfg := readMaterializerConfig()

		Expect(cfg.ProfileTopic).To(Equal("profile"))
	})
})

var _ = Describe("runtime embedding validation", func() {
	BeforeEach(func() {
		Expect(os.Setenv("ENVIRONMENT", "LOCAL-DEV")).To(Succeed())
		env.ResetEnvironmentCache()
	})

	It("rejects unknown embedding providers", func() {
		err := validateEmbeddingConfig(embeddingConfig{Provider: "unknown", Dimensions: 384})

		Expect(err).To(MatchError(ContainSubstring("unsupported embedding provider")))
	})

	It("rejects local-dev artifact buckets outside dev environments", func() {
		Expect(os.Setenv("ENVIRONMENT", "STAGING")).To(Succeed())
		env.ResetEnvironmentCache()

		err := validateMaterializerConfig(materializerConfig{
			ArtifactBucket: artifactBucketConfig{Name: "local-dev-bucket"},
			Embedding: embeddingConfig{
				Provider:   "tei",
				URL:        "http://tei.local",
				Model:      "bge-small-en-v1.5",
				Dimensions: 384,
			},
		})

		Expect(err).To(MatchError(ContainSubstring("must not be local-dev-bucket outside dev environments")))
	})
})
