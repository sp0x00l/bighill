package main

import (
	"os"
	"strings"
	"testing"
	"time"

	"inference_service/pkg/app"
	"inference_service/pkg/domain/model"
	inferencetools "inference_service/pkg/infra/tools"
	env "lib/shared_lib/env"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestInferenceMain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Inference service main unit test suite")
}

var _ = Describe("staging Helm values", func() {
	It("does not configure a global generation provider", func() {
		values := readTextFile("helm/values.yaml")

		Expect(values).To(ContainSubstring(`rerankerProvider: "tei"`))
		Expect(values).To(ContainSubstring(`queryTransformerProvider: ""`))
		Expect(values).NotTo(ContainSubstring("generationProvider"))
		Expect(values).NotTo(ContainSubstring("generationEndpoint"))
	})

	It("does not inherit local generation and retrieval adapters", func() {
		values := readTextFile("helm/staging-values.yaml")

		Expect(values).To(ContainSubstring(`rerankerProvider: "tei"`))
		Expect(values).To(ContainSubstring(`queryTransformerProvider: ""`))
		Expect(values).To(ContainSubstring(`preferenceDatasetUriTemplate: "s3://bighill-mlops-lakehouse/preferences/{dataset_id}/{preference_dataset_id}.jsonl"`))
		Expect(values).NotTo(ContainSubstring("generationProvider"))
		Expect(values).NotTo(ContainSubstring("generationEndpoint"))
		Expect(values).NotTo(ContainSubstring("local-dev-bucket"))
		Expect(values).NotTo(ContainSubstring("localhost:4566"))
	})
})

func readTextFile(path string) string {
	content, err := os.ReadFile(path)
	Expect(err).NotTo(HaveOccurred())
	return strings.TrimSpace(string(content))
}

var _ = Describe("readInferenceConfig", func() {
	BeforeEach(func() {
		configureLocalEnv()
		Expect(os.Unsetenv("INFERENCE_SERVICE_DB_NAME")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_DB_USER")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_DB_PASSWORD")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_MODEL_REGISTRY_SUBSCRIBER_TOPIC")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_AGENT_REGISTRY_SUBSCRIBER_TOPIC")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_DATA_REGISTRY_SUBSCRIBER_TOPIC")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_TENANT_SUBSCRIBER_TOPIC")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_OUTBOX")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_OUTBOX_RELAY_POLL_MS")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_OUTBOX_RELAY_FAILURE_BACKOFF_MS")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_OUTBOX_RELAY_BATCH_SIZE")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_API_GRPC_PORT")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_API_HTTP_PORT")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_HTTP_READ_TIMEOUT_SECONDS")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_HTTP_WRITE_TIMEOUT_SECONDS")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_HTTP_IDLE_TIMEOUT_SECONDS")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_FEATURE_MATERIALIZER_GRPC_ADDRESS")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_TOOL_EXECUTION_GRPC_ADDRESS")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_TOOL_EXECUTION_GRPC_DIAL_TIMEOUT_MS")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_TOOL_EXECUTION_GRPC_CALL_TIMEOUT_MS")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_TOOL_EXECUTION_GRPC_RETRY_COUNT")).To(Succeed())
		Expect(os.Setenv("INFERENCE_SERVICE_TEMPORAL_ADDRESS", "localhost:7233")).To(Succeed())
		Expect(os.Setenv("INFERENCE_SERVICE_TEMPORAL_NAMESPACE", "default")).To(Succeed())
		Expect(os.Setenv("INFERENCE_SERVICE_TEMPORAL_TASK_QUEUE", "inference-service")).To(Succeed())
		Expect(os.Setenv("INFERENCE_SERVICE_TEMPORAL_CONNECT_TIMEOUT_SECONDS", "60")).To(Succeed())
		Expect(os.Setenv("INFERENCE_SERVICE_TEMPORAL_CONNECT_RETRY_INTERVAL_SECONDS", "1")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_KAFKA_BASE_GROUP_ID")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_GENERATION_REQUEST_TIMEOUT_SECONDS")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_GENERATION_MAX_OUTPUT_TOKENS")).To(Succeed())
		Expect(os.Setenv("INFERENCE_SERVICE_RERANKER_PROVIDER", "tei")).To(Succeed())
		Expect(os.Setenv("INFERENCE_SERVICE_RERANKER_URL", "http://tei.local")).To(Succeed())
		Expect(os.Setenv("INFERENCE_SERVICE_RERANKER_MODEL", "bge-reranker")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_RERANKER_REQUEST_TIMEOUT_SECONDS")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_RERANKER_CANDIDATE_MULTIPLIER")).To(Succeed())
		Expect(os.Setenv("INFERENCE_SERVICE_RAG_MERGE_STRATEGY", "reranker")).To(Succeed())
		Expect(os.Setenv("INFERENCE_SERVICE_QUERY_TRANSFORMER_PROVIDER", "self_query")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_QUERY_TRANSFORMER_REQUEST_TIMEOUT_SECONDS")).To(Succeed())
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

	It("uses explicit local ML providers", func() {
		cfg := readInferenceConfig()

		Expect(cfg.ServiceName).To(Equal("inference-service"))
		Expect(cfg.DBName).To(Equal("bighill_inference_db"))
		Expect(cfg.Messaging.GroupID).To(Equal("inference"))
		Expect(cfg.OutboxBackend).To(Equal("postgres"))
		Expect(cfg.OutboxRelay.PollInterval).To(Equal(250 * time.Millisecond))
		Expect(cfg.OutboxRelay.FailureBackoff).To(Equal(2 * time.Second))
		Expect(cfg.OutboxRelay.BatchSize).To(Equal(int32(100)))
		Expect(cfg.Topics.ModelRegistry).To(Equal("model_registry"))
		Expect(cfg.Topics.AgentRegistry).To(Equal("agent_registry"))
		Expect(cfg.Topics.DataRegistry).To(Equal("data_registry"))
		Expect(cfg.TenantTopic).To(Equal("tenant"))
		Expect(cfg.FeatureMaterializer.Address).To(Equal("localhost:7072"))
		Expect(cfg.ToolExecutionService.Address).To(BeEmpty())
		Expect(cfg.Temporal.Address).To(Equal("localhost:7233"))
		Expect(cfg.Temporal.Namespace).To(Equal("default"))
		Expect(cfg.Temporal.TaskQueue).To(Equal("inference-service"))
		Expect(cfg.Temporal.ConnectTimeout).To(Equal(60 * time.Second))
		Expect(cfg.Temporal.ConnectRetryInterval).To(Equal(time.Second))
		Expect(cfg.Reranker.Provider).To(Equal("tei"))
		Expect(cfg.Reranker.URL).To(Equal("http://tei.local"))
		Expect(cfg.Reranker.Model).To(Equal("bge-reranker"))
		Expect(cfg.Reranker.RequestTimeout).To(Equal(30 * time.Second))
		Expect(cfg.Reranker.CandidateMultiplier).To(Equal(5))
		Expect(cfg.Generation.MaxOutputTokens).To(Equal(256))
		Expect(cfg.Generation.RAGMergeStrategy).To(Equal("reranker"))
		Expect(cfg.PreferenceDataset.ExportEnabled).To(BeFalse())
		Expect(cfg.PreferenceDataset.URITemplate).To(BeEmpty())
		Expect(cfg.PreferenceDataset.MinExamples).To(Equal(1))
		Expect(cfg.PreferenceDataset.Limit).To(Equal(1000))
		Expect(cfg.PreferenceDataset.BucketRegion).To(Equal("local-dev"))
		Expect(cfg.PreferenceDataset.UploadPartSizeMB).To(Equal(int64(10)))
		Expect(cfg.Generation.PromptStrategy).To(Equal("rag-prompt-v1"))
		Expect(cfg.Generation.MaxContextTokens).To(Equal(3000))
		Expect(cfg.Generation.MaxContextChunks).To(Equal(8))
		Expect(cfg.QueryTransformer.Provider).To(Equal("self_query"))
		Expect(cfg.QueryTransformer.RequestTimeout).To(Equal(30 * time.Second))
		Expect(cfg.GRPCPort).To(Equal(7073))
		Expect(cfg.HTTPPort).To(Equal(8087))
		Expect(cfg.HTTPServer.ReadTimeout).To(Equal(30 * time.Second))
		Expect(cfg.HTTPServer.WriteTimeout).To(Equal(120 * time.Second))
		Expect(cfg.HTTPServer.IdleTimeout).To(Equal(120 * time.Second))
		Expect(cfg.Health.HealthCheckPort).To(Equal(5059))
	})

	It("builds a Postgres connection string", func() {
		connection := postgresConnectionString("inference user", "pa:ss/word", "localhost", "5432", "bighill_inference_db", "disable", 7)

		Expect(connection).To(ContainSubstring("postgres://inference%20user:pa%3Ass%2Fword@localhost:5432/bighill_inference_db?"))
		Expect(connection).To(ContainSubstring("pool_max_conns=7"))
		Expect(connection).To(ContainSubstring("sslmode=disable"))
	})
})

var _ = Describe("newGenerationAdapters", func() {
	BeforeEach(func() {
		configureLocalEnv()
	})

	It("registers supported serving protocols", func() {
		adapters := newGenerationAdapters(generationConfig{RequestTimeout: time.Second, MaxOutputTokens: 128})

		Expect(adapters).To(HaveKey("OPENAI_CHAT_COMPLETIONS"))
		Expect(adapters).To(HaveKey("OLLAMA_GENERATE"))
	})
})

var _ = Describe("newRerankerAdapter", func() {
	BeforeEach(func() {
		configureLocalEnv()
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

	It("allows reranking to be disabled", func() {
		reranker, err := newRerankerAdapter(rerankerConfig{})

		Expect(err).NotTo(HaveOccurred())
		Expect(reranker).To(BeNil())
	})
})

var _ = Describe("runtime ML provider validation", func() {
	BeforeEach(func() {
		configureLocalEnv()
	})

	It("rejects non-positive generation timeouts", func() {
		err := validateGenerationConfig(generationConfig{})

		Expect(err).To(MatchError(ContainSubstring("INFERENCE_SERVICE_GENERATION_REQUEST_TIMEOUT_SECONDS must be greater than zero")))
	})

	It("rejects non-positive generation max output tokens", func() {
		err := validateGenerationConfig(generationConfig{RequestTimeout: time.Second})

		Expect(err).To(MatchError(ContainSubstring("INFERENCE_SERVICE_GENERATION_MAX_OUTPUT_TOKENS must be greater than zero")))
	})

	It("rejects unknown reranking providers", func() {
		err := validateRerankerConfig(rerankerConfig{Provider: "unknown"})

		Expect(err).To(MatchError(ContainSubstring("unsupported reranker provider")))
	})

	It("allows empty reranker config when reranking is not selected", func() {
		err := validateInferenceConfig(inferenceConfig{
			Generation: generationConfig{
				RequestTimeout:   time.Second,
				MaxOutputTokens:  128,
				RAGMergeStrategy: model.RAGMergeStrategyScoreNormalized.String(),
			},
			HTTPServer: httpServerConfig{
				ReadTimeout:  time.Second,
				WriteTimeout: 2 * time.Second,
				IdleTimeout:  time.Second,
			},
			Agent: agentConfig{
				MaxStepsCap:       3,
				TokenBudgetCap:    512,
				WallMsCap:         60000,
				RunReaperInterval: time.Second,
				RunReaperGrace:    time.Second,
			},
			Temporal: validTemporalConfig(),
		})

		Expect(err).NotTo(HaveOccurred())
	})

	It("rejects reranker merge strategy without a reranker provider at startup", func() {
		err := validateInferenceConfig(inferenceConfig{
			Generation: generationConfig{
				RequestTimeout:   time.Second,
				MaxOutputTokens:  128,
				RAGMergeStrategy: model.RAGMergeStrategyReranker.String(),
			},
			HTTPServer: httpServerConfig{
				ReadTimeout:  time.Second,
				WriteTimeout: 2 * time.Second,
				IdleTimeout:  time.Second,
			},
			Agent: agentConfig{
				MaxStepsCap:       3,
				TokenBudgetCap:    512,
				WallMsCap:         60000,
				RunReaperInterval: time.Second,
				RunReaperGrace:    time.Second,
			},
			Temporal: validTemporalConfig(),
		})

		Expect(err).To(MatchError(ContainSubstring("INFERENCE_SERVICE_RAG_MERGE_STRATEGY=reranker requires INFERENCE_SERVICE_RERANKER_PROVIDER")))
	})

	It("rejects non-positive agent wall clock caps", func() {
		err := validateAgentConfig(agentConfig{
			MaxStepsCap:       3,
			TokenBudgetCap:    512,
			RunReaperInterval: time.Second,
			RunReaperGrace:    time.Second,
		})

		Expect(err).To(MatchError(ContainSubstring("INFERENCE_SERVICE_AGENT_WALL_MS_CAP must be greater than zero")))
	})

	It("rejects missing Temporal task queue", func() {
		cfg := validTemporalConfig()
		cfg.TaskQueue = ""

		err := validateTemporalConfig(cfg)

		Expect(err).To(MatchError(ContainSubstring("INFERENCE_SERVICE_TEMPORAL_TASK_QUEUE is required")))
	})

	It("rejects unknown query transformation providers", func() {
		err := validateQueryTransformerConfig(queryTransformerConfig{Provider: "unknown"})

		Expect(err).To(MatchError(ContainSubstring("unsupported query transformer provider")))
	})

	It("rejects non-positive HTTP server timeouts", func() {
		err := validateHTTPServerConfig(httpServerConfig{}, generationConfig{RequestTimeout: time.Second})

		Expect(err).To(MatchError(ContainSubstring("INFERENCE_SERVICE_HTTP_READ_TIMEOUT_SECONDS must be greater than zero")))
	})

	It("rejects HTTP write timeouts that cannot carry generation responses", func() {
		err := validateHTTPServerConfig(httpServerConfig{
			ReadTimeout:  time.Second,
			WriteTimeout: 60 * time.Second,
			IdleTimeout:  time.Second,
		}, generationConfig{RequestTimeout: 60 * time.Second})

		Expect(err).To(MatchError(ContainSubstring("INFERENCE_SERVICE_HTTP_WRITE_TIMEOUT_SECONDS must be greater than INFERENCE_SERVICE_GENERATION_REQUEST_TIMEOUT_SECONDS")))
	})

	It("rejects incomplete tool execution service grpc configuration when enabled", func() {
		err := validateToolExecutionServiceConfig(inferencetools.ToolExecutionServiceClientConfig{Address: "tool-execution-service:7084"})

		Expect(err).To(MatchError(ContainSubstring("tool execution service grpc dial timeout must be greater than zero")))
	})
})

func configureLocalEnv() {
	Expect(os.Setenv("ENVIRONMENT", "LOCAL-DEV")).To(Succeed())
	env.ResetEnvironmentCache()
}

func validTemporalConfig() temporalConfig {
	return temporalConfig{
		Address:              "localhost:7233",
		Namespace:            "default",
		TaskQueue:            app.DefaultAgentRunWorkflowTaskQueue,
		ConnectTimeout:       time.Second,
		ConnectRetryInterval: time.Millisecond,
	}
}

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
