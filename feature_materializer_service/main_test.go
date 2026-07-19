package main

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"feature_materializer_service/pkg/domain/model"
	env "lib/shared_lib/env"

	temporalclient "go.temporal.io/sdk/client"

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
		Expect(os.Unsetenv("FEATURE_MATERIALIZER_SERVICE_TENANT_SUBSCRIBER_TOPIC")).To(Succeed())
		Expect(os.Unsetenv("FEATURE_MATERIALIZER_SERVICE_TEMPORAL_CONNECT_TIMEOUT_SECONDS")).To(Succeed())
		Expect(os.Unsetenv("FEATURE_MATERIALIZER_SERVICE_TEMPORAL_CONNECT_RETRY_INTERVAL_SECONDS")).To(Succeed())
		Expect(os.Unsetenv("FEATURE_MATERIALIZER_SERVICE_GRAPH_ENABLED")).To(Succeed())
		Expect(os.Unsetenv("FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTOR")).To(Succeed())
		Expect(os.Unsetenv("FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_ENDPOINT")).To(Succeed())
		Expect(os.Unsetenv("FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_AUTH_TOKEN")).To(Succeed())
		Expect(os.Unsetenv("FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_MODEL")).To(Succeed())
		Expect(os.Unsetenv("FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_PROMPT_VERSION")).To(Succeed())
		Expect(os.Unsetenv("FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_SCHEMA_VERSION")).To(Succeed())
		Expect(os.Unsetenv("FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_REQUEST_TIMEOUT_SECONDS")).To(Succeed())
		Expect(os.Unsetenv("FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_MAX_RESPONSE_BYTES")).To(Succeed())
		Expect(os.Unsetenv("FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_MAX_RETRIES")).To(Succeed())
	})

	It("uses the tenant service topic for tenant projections by default", func() {
		cfg := readMaterializerConfig()

		Expect(cfg.TenantTopic).To(Equal("tenant"))
		Expect(cfg.Temporal.ConnectTimeout).To(Equal(60 * time.Second))
		Expect(cfg.Temporal.ConnectRetryInterval).To(Equal(time.Second))
		Expect(cfg.Embedding.VectorStore).To(Equal("pgvector"))
		Expect(cfg.Graph.Enabled).To(BeFalse())
		Expect(cfg.Graph.Extractor).To(Equal(graphExtractorDisabled))
	})

	It("allows service-specific Temporal connection retry overrides", func() {
		Expect(os.Setenv("FEATURE_MATERIALIZER_SERVICE_TEMPORAL_CONNECT_TIMEOUT_SECONDS", "90")).To(Succeed())
		Expect(os.Setenv("FEATURE_MATERIALIZER_SERVICE_TEMPORAL_CONNECT_RETRY_INTERVAL_SECONDS", "3")).To(Succeed())

		cfg := readMaterializerConfig()

		Expect(cfg.Temporal.ConnectTimeout).To(Equal(90 * time.Second))
		Expect(cfg.Temporal.ConnectRetryInterval).To(Equal(3 * time.Second))
	})

	It("configures model-serving graph extraction from explicit endpoint settings", func() {
		Expect(os.Setenv("FEATURE_MATERIALIZER_SERVICE_GRAPH_ENABLED", "true")).To(Succeed())
		Expect(os.Setenv("FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTOR", graphExtractorModel)).To(Succeed())
		Expect(os.Setenv("FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_ENDPOINT", "http://graph-model/v1/chat/completions")).To(Succeed())
		Expect(os.Setenv("FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_AUTH_TOKEN", "token-1")).To(Succeed())
		Expect(os.Setenv("FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_MODEL", "graph-model")).To(Succeed())
		Expect(os.Setenv("FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_PROMPT_VERSION", model.DefaultGraphExtractionPromptVersion)).To(Succeed())
		Expect(os.Setenv("FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_SCHEMA_VERSION", model.DefaultGraphExtractionSchemaVersion)).To(Succeed())
		Expect(os.Setenv("FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_REQUEST_TIMEOUT_SECONDS", "12")).To(Succeed())
		Expect(os.Setenv("FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_MAX_RESPONSE_BYTES", "4096")).To(Succeed())
		Expect(os.Setenv("FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_MAX_OUTPUT_TOKENS", "256")).To(Succeed())
		Expect(os.Setenv("FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_MAX_RETRIES", "1")).To(Succeed())

		cfg := readMaterializerConfig()

		Expect(cfg.Graph.Enabled).To(BeTrue())
		Expect(cfg.Graph.Extractor).To(Equal(graphExtractorModel))
		Expect(cfg.Graph.ExtractionEndpoint).To(Equal("http://graph-model/v1/chat/completions"))
		Expect(cfg.Graph.ExtractionAuthToken).To(Equal("token-1"))
		Expect(cfg.Graph.ExtractionModel).To(Equal("graph-model"))
		Expect(cfg.Graph.ExtractionRequestTimeout).To(Equal(12 * time.Second))
		Expect(cfg.Graph.ExtractionMaxResponseBytes).To(Equal(int64(4096)))
		Expect(cfg.Graph.ExtractionMaxOutputTokens).To(Equal(256))
		Expect(cfg.Graph.ExtractionMaxRetries).To(Equal(1))
	})
})

var _ = Describe("dialTemporalClientWith", func() {
	It("retries Temporal dial failures until the server is reachable", func() {
		attempts := 0

		temporalClient, err := dialTemporalClientWith(context.Background(), temporalConfig{
			Address:              "localhost:7233",
			Namespace:            "default",
			ConnectTimeout:       time.Second,
			ConnectRetryInterval: time.Millisecond,
		}, func(temporalclient.Options) (temporalclient.Client, error) {
			attempts++
			if attempts < 3 {
				return nil, errors.New("not ready")
			}
			return nil, nil
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(temporalClient).To(BeNil())
		Expect(attempts).To(Equal(3))
	})

	It("returns a bounded error when Temporal never becomes reachable", func() {
		attempts := 0

		temporalClient, err := dialTemporalClientWith(context.Background(), temporalConfig{
			Address:              "localhost:7233",
			Namespace:            "default",
			ConnectTimeout:       5 * time.Millisecond,
			ConnectRetryInterval: time.Millisecond,
		}, func(temporalclient.Options) (temporalclient.Client, error) {
			attempts++
			return nil, errors.New("not ready")
		})

		Expect(temporalClient).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring("connect to Temporal at localhost:7233 namespace default")))
		Expect(attempts).To(BeNumerically(">=", 1))
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

	It("rejects heuristic graph extraction as a service runtime mode", func() {
		err := validateGraphConfig(graphConfig{
			Enabled:   true,
			Extractor: "heuristic",
		})

		Expect(err).To(MatchError(ContainSubstring("unsupported FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTOR")))
	})
})
