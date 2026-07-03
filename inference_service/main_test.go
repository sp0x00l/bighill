package main

import (
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestInferenceMain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Inference service main unit test suite")
}

var _ = Describe("readInferenceConfig", func() {
	BeforeEach(func() {
		Expect(os.Unsetenv("INFERENCE_SERVICE_DB_NAME")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_DB_USER")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_DB_PASSWORD")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_MODEL_REGISTRY_SUBSCRIBER_TOPIC")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_DATA_REGISTRY_SUBSCRIBER_TOPIC")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_OUTBOX")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_OUTBOX_RELAY_POLL_MS")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_OUTBOX_RELAY_FAILURE_BACKOFF_MS")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_OUTBOX_RELAY_BATCH_SIZE")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_API_GRPC_PORT")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_FEATURE_MATERIALIZER_GRPC_ADDRESS")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_KAFKA_BASE_GROUP_ID")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_GENERATION_PROVIDER")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_GENERATION_ENDPOINT")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_GENERATION_MODEL")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_GENERATION_REQUEST_TIMEOUT_SECONDS")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_RERANKER_PROVIDER")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_RERANKER_URL")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_RERANKER_MODEL")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_RERANKER_REQUEST_TIMEOUT_SECONDS")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_RERANKER_CANDIDATE_MULTIPLIER")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_PREFERENCE_DATASET_EXPORT_ENABLED")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_PREFERENCE_DATASET_URI_TEMPLATE")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_PREFERENCE_DATASET_MIN_EXAMPLES")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_PREFERENCE_DATASET_LIMIT")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_PREFERENCE_DATASET_BUCKET_REGION")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_PREFERENCE_DATASET_UPLOAD_PART_SIZE_MB")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_PROMPT_STRATEGY_VERSION")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_PROMPT_MAX_CONTEXT_TOKENS")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_PROMPT_MAX_CONTEXT_CHUNKS")).To(Succeed())
	})

	It("uses local defaults", func() {
		cfg := readInferenceConfig()

		Expect(cfg.ServiceName).To(Equal("inference-service"))
		Expect(cfg.DBName).To(Equal("bighill_inference_db"))
		Expect(cfg.Messaging.GroupID).To(Equal("inference"))
		Expect(cfg.OutboxBackend).To(Equal("postgres"))
		Expect(cfg.OutboxRelay.PollInterval).To(Equal(250 * time.Millisecond))
		Expect(cfg.OutboxRelay.FailureBackoff).To(Equal(2 * time.Second))
		Expect(cfg.OutboxRelay.BatchSize).To(Equal(int32(100)))
		Expect(cfg.Topics.ModelRegistry).To(Equal("model_registry"))
		Expect(cfg.Topics.DataRegistry).To(Equal("data_registry"))
		Expect(cfg.FeatureMaterializer.Address).To(Equal("localhost:7072"))
		Expect(cfg.Generation.Provider).To(Equal("deterministic"))
		Expect(cfg.Generation.Endpoint).To(Equal("http://localhost:11434"))
		Expect(cfg.Generation.Model).To(Equal("llama3.1:8b"))
		Expect(cfg.Reranker.Provider).To(Equal("disabled"))
		Expect(cfg.Reranker.URL).To(BeEmpty())
		Expect(cfg.Reranker.Model).To(BeEmpty())
		Expect(cfg.Reranker.RequestTimeout).To(Equal(30 * time.Second))
		Expect(cfg.Reranker.CandidateMultiplier).To(Equal(5))
		Expect(cfg.PreferenceDataset.ExportEnabled).To(BeFalse())
		Expect(cfg.PreferenceDataset.URITemplate).To(Equal("s3://local-dev-bucket/preferences/{dataset_id}/{preference_dataset_id}.jsonl"))
		Expect(cfg.PreferenceDataset.MinExamples).To(Equal(1))
		Expect(cfg.PreferenceDataset.Limit).To(Equal(1000))
		Expect(cfg.PreferenceDataset.BucketRegion).To(Equal("local-dev"))
		Expect(cfg.PreferenceDataset.UploadPartSizeMB).To(Equal(int64(10)))
		Expect(cfg.Generation.PromptStrategy).To(Equal("rag-prompt-v1"))
		Expect(cfg.Generation.MaxContextTokens).To(Equal(3000))
		Expect(cfg.Generation.MaxContextChunks).To(Equal(8))
		Expect(cfg.GRPCPort).To(Equal(7073))
		Expect(cfg.Health.HealthCheckPort).To(Equal(5059))
	})

	It("builds a Postgres connection string", func() {
		connection := postgresConnectionString("inference user", "pa:ss/word", "localhost", "5432", "bighill_inference_db", "disable", 7)

		Expect(connection).To(ContainSubstring("postgres://inference%20user:pa%3Ass%2Fword@localhost:5432/bighill_inference_db?"))
		Expect(connection).To(ContainSubstring("pool_max_conns=7"))
		Expect(connection).To(ContainSubstring("sslmode=disable"))
	})
})

var _ = Describe("newGenerationAdapter", func() {
	It("creates the deterministic local generator", func() {
		generator, err := newGenerationAdapter(generationConfig{Provider: "deterministic"})

		Expect(err).NotTo(HaveOccurred())
		Expect(generator.Provider()).To(Equal("deterministic"))
	})

	It("rejects unsupported providers", func() {
		_, err := newGenerationAdapter(generationConfig{Provider: "unknown"})

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("unsupported generation provider"))
	})

	It("creates a vLLM HTTP generator", func() {
		generator, err := newGenerationAdapter(generationConfig{
			Provider:       "vllm",
			Endpoint:       "http://vllm.local",
			Model:          "base-model",
			RequestTimeout: time.Second,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(generator.Provider()).To(Equal("vllm"))
		Expect(generator.Model()).To(Equal("base-model"))
	})
})

var _ = Describe("newRerankerAdapter", func() {
	It("disables reranking when configured as disabled", func() {
		reranker, err := newRerankerAdapter(rerankerConfig{Provider: "disabled"})

		Expect(err).NotTo(HaveOccurred())
		Expect(reranker).To(BeNil())
	})

	It("creates a TEI reranker", func() {
		reranker, err := newRerankerAdapter(rerankerConfig{
			Provider:            "tei",
			URL:                 "http://tei.local",
			Model:               "bge-reranker",
			RequestTimeout:      time.Second,
			CandidateMultiplier: 5,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(reranker).NotTo(BeNil())
	})

	It("rejects a TEI reranker without over-fetch", func() {
		_, err := newRerankerAdapter(rerankerConfig{
			Provider:            "tei",
			URL:                 "http://tei.local",
			Model:               "bge-reranker",
			RequestTimeout:      time.Second,
			CandidateMultiplier: 1,
		})

		Expect(err).To(MatchError(ContainSubstring("reranker candidate multiplier must be at least 2")))
	})

	It("rejects unsupported providers", func() {
		_, err := newRerankerAdapter(rerankerConfig{Provider: "unknown"})

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("unsupported reranker provider"))
	})
})

var _ = Describe("promptStrategyFromConfig", func() {
	It("builds an explicit prompt strategy from config", func() {
		strategy, err := promptStrategyFromConfig(generationConfig{
			PromptStrategy:   "rag-v1",
			MaxContextTokens: 1200,
			MaxContextChunks: 6,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(strategy.Version).To(Equal("rag-v1"))
		Expect(strategy.SystemPrompt).NotTo(BeEmpty())
		Expect(strategy.MaxContextTokens).To(Equal(1200))
		Expect(strategy.MaxContextChunks).To(Equal(6))
	})

	It("rejects missing prompt config", func() {
		_, err := promptStrategyFromConfig(generationConfig{})

		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("newHealthCheckConfig", func() {
	It("maps health settings", func() {
		cfg := newHealthCheckConfig(healthConfig{
			CpuThresholdPercentage:                    70,
			MemFreeThresholdPercent:                   30,
			HealthCheckPort:                           5059,
			DBConnectionString:                        "postgres://localhost/db",
			DbLatencyThreshold:                        4 * time.Second,
			ServiceLatencyThreshold:                   3 * time.Second,
			MessageBrokerSubscriberMaxPollSilence:     30 * time.Second,
			MessageBrokerSubscriberMaxProgressSilence: 90 * time.Second,
			MessageBrokerSubscriberMaxLag:             100,
		})

		Expect(cfg.CpuThresholdPercentage).To(Equal(70))
		Expect(cfg.MemFreeThresholdPercentage).To(Equal(30))
		Expect(cfg.HealthCheckPort).To(Equal(5059))
		Expect(cfg.DBConnectionString).To(Equal("postgres://localhost/db"))
		Expect(cfg.DbLatencyThresholdSec).To(Equal(4 * time.Second))
		Expect(cfg.ServiceLatencyThresholdSec).To(Equal(3 * time.Second))
		Expect(cfg.MessageBrokerSubscriberMaxPollSilenceSec).To(Equal(30 * time.Second))
		Expect(cfg.MessageBrokerSubscriberMaxProgressSilenceSec).To(Equal(90 * time.Second))
		Expect(cfg.MessageBrokerSubscriberMaxLag).To(Equal(int64(100)))
	})
})
