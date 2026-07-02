package integration_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"inference_service/pkg/app"
	"inference_service/pkg/domain/model"
	"inference_service/pkg/infra/generation"
	inferencegrpc "inference_service/pkg/infra/network/grpc"
	inferencemessaging "inference_service/pkg/infra/network/messaging"
	repo "inference_service/pkg/infra/repo/db"

	datasetpb "lib/data_contracts_lib/dataset"
	inferencepb "lib/data_contracts_lib/inference"
	modelregistrypb "lib/data_contracts_lib/model_registry"
	dbconn "lib/shared_lib/db"
	env "lib/shared_lib/env"
	sharedmessaging "lib/shared_lib/messaging"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
	stdgrpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestInferenceIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Inference service integration test suite")
}

var _ = Describe("Inference service integration", Ordered, func() {
	var (
		ctx       context.Context
		cancel    context.CancelFunc
		database  *dbconn.Database
		models    *repo.InferenceModelRepository
		datasets  *repo.InferenceDatasetRepository
		modelsUse app.InferenceUsecase
	)

	BeforeAll(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 90*time.Second)

		dbName := env.WithDefaultString("INFERENCE_DB_NAME", "bighill_inference_db")
		connectionString := testPostgresConnectionString(dbName)

		var err error
		database, err = dbconn.InitDatabase(ctx, dbName, connectionString, log.StandardLogger())
		Expect(err).NotTo(HaveOccurred())

		models = repo.NewInferenceModelRepository(database)
		datasets = repo.NewInferenceDatasetRepository(database)
		requests := repo.NewInferenceRequestRepository(database)
		feedbacks := repo.NewInferenceFeedbackRepository(database)
		promptStrategy := model.PromptStrategy{
			Version:          "rag-prompt-v1",
			SystemPrompt:     "Use context only.",
			MaxContextTokens: 3000,
			MaxContextChunks: 8,
		}
		modelsUse = app.NewInferenceUsecase(
			models,
			app.WithInferenceDatasetRepository(datasets),
			app.WithInferenceRequestRepository(requests),
			app.WithInferenceFeedbackRepository(feedbacks),
			app.WithRetrievalClient(&integrationRetrievalClient{}),
			app.WithGenerationAdapter(generation.NewDeterministicGenerator()),
			app.WithPromptStrategy(promptStrategy),
			app.WithContextPacker(app.NewContextWindowPacker(promptStrategy)),
			app.WithPromptBuilder(app.NewDefaultPromptBuilder(promptStrategy)),
		)
	})

	BeforeEach(func() {
		Expect(cleanInferenceTables(ctx, database)).To(Succeed())
	})

	AfterAll(func() {
		if database != nil {
			database.Close()
		}
		if cancel != nil {
			cancel()
		}
	})

	It("persists inference model updates from Kafka model registry facts", func() {
		brokers := env.WithDefaultString("KAFKA_BROKER", "localhost:9092")
		suffix := fmt.Sprintf("%d", rand.Int64())
		topics := inferencemessaging.InferenceTopics{
			ModelRegistry: "model_registry",
			DataRegistry:  "data_registry",
		}
		runCtx, runCancel := context.WithCancel(ctx)
		defer runCancel()

		serviceMessenger := sharedmessaging.NewMessenger(sharedmessaging.MessengerConfig{
			Brokers:         brokers,
			GroupID:         "inference-integration-service-" + suffix,
			DlqURL:          "http://localhost:4566/inference-dev-env-queue/",
			AutoOffsetReset: "earliest",
		}, runCancel)
		defer func() {
			_ = serviceMessenger.Close(runCtx)
		}()
		serviceSubscriber, err := serviceMessenger.Subscriber(runCtx)
		Expect(err).NotTo(HaveOccurred())
		publisher, err := serviceMessenger.Publisher(runCtx)
		Expect(err).NotTo(HaveOccurred())

		modelUpdatedSubscriber := inferencemessaging.NewModelUpdatedSubscriber(serviceSubscriber, modelsUse, topics)
		go func() {
			_ = modelUpdatedSubscriber.Start(runCtx)
		}()
		time.Sleep(750 * time.Millisecond)

		modelID := uuid.New()
		trainingRunID := uuid.New()
		datasetID := uuid.New()
		Expect(publisher.Publish(runCtx, topics.ModelRegistry, sharedmessaging.Message{
			ResourceKey: modelID,
			MsgType:     sharedmessaging.MsgTypeModelUpdated,
		}, &modelregistrypb.ModelUpdatedEvent{
			ModelId:           modelID.String(),
			TrainingRunId:     trainingRunID.String(),
			DatasetId:         datasetID.String(),
			Name:              "movie-ranker",
			ModelVersion:      3,
			BaseModel:         "mistral-7b",
			ArtifactLocation:  "s3://local-dev-bucket/models/" + modelID.String(),
			ArtifactFormat:    "HF_PEFT_ADAPTER",
			ArtifactChecksum:  "sha256:abc",
			ArtifactSizeBytes: 128,
			AdapterUri:        "s3://local-dev-bucket/models/" + modelID.String(),
			ServingTarget:     "vllm-local",
			ServingModel:      "movie-ranker-v1",
			ServingLoadStatus: model.ModelLoadStatusLoaded.String(),
			MetricsMetadata:   `{"eval_loss":0.12}`,
			Status:            "READY",
		})).To(Succeed())

		Eventually(func(g Gomega) {
			record, err := models.ReadByID(ctx, modelID)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(record.TrainingRunID).To(Equal(trainingRunID))
			g.Expect(record.DatasetID).To(Equal(datasetID))
			g.Expect(record.Status).To(Equal(model.ModelStatusReady))
			g.Expect(record.ArtifactLocation).To(Equal("s3://local-dev-bucket/models/" + modelID.String()))
			g.Expect(record.ArtifactFormat).To(Equal("HF_PEFT_ADAPTER"))
		}, 30*time.Second, 500*time.Millisecond).Should(Succeed())
	})

	It("persists registry dataset updates and generates with materialized contexts", func() {
		brokers := env.WithDefaultString("KAFKA_BROKER", "localhost:9092")
		suffix := fmt.Sprintf("%d", rand.Int64())
		topics := inferencemessaging.InferenceTopics{
			ModelRegistry: "model_registry",
			DataRegistry:  "data_registry",
		}
		runCtx, runCancel := context.WithCancel(ctx)
		defer runCancel()

		serviceMessenger := sharedmessaging.NewMessenger(sharedmessaging.MessengerConfig{
			Brokers:         brokers,
			GroupID:         "inference-integration-dataset-service-" + suffix,
			DlqURL:          "http://localhost:4566/inference-dev-env-queue/",
			AutoOffsetReset: "earliest",
		}, runCancel)
		defer func() {
			_ = serviceMessenger.Close(runCtx)
		}()
		serviceSubscriber, err := serviceMessenger.Subscriber(runCtx)
		Expect(err).NotTo(HaveOccurred())
		publisher, err := serviceMessenger.Publisher(runCtx)
		Expect(err).NotTo(HaveOccurred())

		inferenceSubscriber := inferencemessaging.NewModelUpdatedSubscriber(serviceSubscriber, modelsUse, topics)
		go func() {
			_ = inferenceSubscriber.Start(runCtx)
		}()
		time.Sleep(750 * time.Millisecond)

		datasetID := uuid.New()
		userID := uuid.New()
		modelID := uuid.New()
		embeddingSnapshotID := uuid.New()
		_, err = models.UpsertModel(ctx, &model.InferenceModel{
			ModelID:           modelID,
			TrainingRunID:     uuid.New(),
			DatasetID:         datasetID,
			Name:              "movie-ranker",
			ModelVersion:      1,
			BaseModel:         "mistral-7b",
			ArtifactLocation:  "s3://local-dev-bucket/models/" + modelID.String(),
			ArtifactFormat:    "HF_PEFT_ADAPTER",
			ArtifactChecksum:  "sha256:abc",
			ArtifactSizeBytes: 128,
			AdapterURI:        "s3://local-dev-bucket/models/" + modelID.String(),
			ServingTarget:     "vllm-local",
			ServingModel:      "movie-ranker-v1",
			ServingLoadStatus: model.ModelLoadStatusLoaded,
			MetricsMetadata:   `{"eval_loss":0.12}`,
			Status:            model.ModelStatusReady,
		}, uuid.New())
		Expect(err).NotTo(HaveOccurred())
		Expect(publisher.Publish(runCtx, topics.DataRegistry, sharedmessaging.Message{
			ResourceKey: datasetID,
			MsgType:     sharedmessaging.MsgTypeDatasetUpdated,
		}, &datasetpb.DatasetUpdatedEvent{
			DatasetId:                datasetID.String(),
			UserId:                   userID.String(),
			DatasetVersion:           6,
			ProcessingState:          "EMBEDDINGS_MATERIALIZED",
			StorageLocation:          "s3://local-dev-bucket/features/movies.parquet",
			TableNamespace:           "features",
			TableName:                "movies",
			TableFormat:              "PARQUET",
			CatalogProvider:          "LOCAL",
			SchemaVersion:            2,
			SchemaMetadata:           `{"columns":[{"name":"text"}]}`,
			RawSnapshotId:            uuid.NewString(),
			FeatureSnapshotId:        uuid.NewString(),
			EmbeddingSnapshotId:      embeddingSnapshotID.String(),
			VectorStore:              "pgvector",
			CollectionName:           "movies",
			EmbeddingDimensions:      384,
			EmbeddingCount:           12,
			ProcessingProfile:        "RAG",
			EmbeddingStrategyVersion: "rag-v1",
			EmbeddingChunkerName:     "go-token-window",
			EmbeddingChunkerVersion:  "v1",
			EmbeddingChunkSize:       384,
			EmbeddingChunkOverlap:    64,
			EmbeddingProvider:        "ollama",
			EmbeddingModel:           "bge-small-en-v1.5",
		})).To(Succeed())

		Eventually(func(g Gomega) {
			record, err := datasets.ReadDataset(ctx, datasetID)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(record.UserID).To(Equal(userID))
			g.Expect(record.ProcessingState).To(Equal(model.DatasetProcessingEmbeddingsMaterialized))
			g.Expect(record.EmbeddingSnapshotID).To(Equal(embeddingSnapshotID))
			g.Expect(record.EmbeddingDimensions).To(Equal(384))
		}, 30*time.Second, 500*time.Millisecond).Should(Succeed())

		lis, err := net.Listen("tcp", "127.0.0.1:0")
		Expect(err).NotTo(HaveOccurred())
		inferenceServer := inferencegrpc.NewInferenceGrpcServer(modelsUse)
		defer inferenceServer.Close()
		go func() {
			_ = inferenceServer.Serve(lis)
		}()
		conn, err := stdgrpc.NewClient(lis.Addr().String(), stdgrpc.WithTransportCredentials(insecure.NewCredentials()))
		Expect(err).NotTo(HaveOccurred())
		defer func() {
			_ = conn.Close()
		}()
		client := inferencepb.NewInferenceServiceClient(conn)

		response, err := client.Generate(ctx, &inferencepb.GenerateRequest{
			RequestId: uuid.NewString(),
			DatasetId: datasetID.String(),
			ModelId:   modelID.String(),
			QueryText: "what movie context is available?",
			TopK:      3,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(response.GetAnswer()).To(ContainSubstring("integration retrieved context"))
		Expect(response.GetRequestId()).NotTo(BeEmpty())
		Expect(response.GetGenerationProvider()).To(Equal("deterministic"))
		Expect(response.GetContexts()).To(HaveLen(1))

		feedbackID := uuid.New()
		feedback, err := client.RecordFeedback(ctx, &inferencepb.RecordFeedbackRequest{
			FeedbackId: feedbackID.String(),
			RequestId:  response.GetRequestId(),
			UserId:     userID.String(),
			Accepted:   false,
			Rating:     -1,
			Comment:    "not enough detail",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(feedback.GetFeedbackId()).To(Equal(feedbackID.String()))
		var label string
		var rejectedAnswer string
		Expect(database.Pool.QueryRow(ctx, `
			SELECT feedback_label, rejected_answer
			FROM `+database.Name+`.preference_examples
			WHERE feedback_id = $1
		`, feedbackID).Scan(&label, &rejectedAnswer)).To(Succeed())
		Expect(label).To(Equal("REJECTED"))
		Expect(rejectedAnswer).To(ContainSubstring("integration retrieved context"))
	})

	It("over-fetches, reranks, packs, generates, and audits the reranked context", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		modelID := uuid.New()
		embeddingSnapshotID := uuid.New()
		requestID := uuid.New()
		_, err := models.UpsertModel(ctx, &model.InferenceModel{
			ModelID:           modelID,
			TrainingRunID:     uuid.New(),
			DatasetID:         datasetID,
			Name:              "movie-ranker",
			ModelVersion:      1,
			BaseModel:         "mistral-7b",
			ArtifactLocation:  "s3://local-dev-bucket/models/" + modelID.String(),
			ArtifactFormat:    "HF_PEFT_ADAPTER",
			ArtifactChecksum:  "sha256:abc",
			ArtifactSizeBytes: 128,
			AdapterURI:        "s3://local-dev-bucket/models/" + modelID.String(),
			ServingTarget:     "vllm-local",
			ServingModel:      "movie-ranker-v1",
			ServingLoadStatus: model.ModelLoadStatusLoaded,
			MetricsMetadata:   `{"eval_loss":0.12}`,
			Status:            model.ModelStatusReady,
		}, uuid.New())
		Expect(err).NotTo(HaveOccurred())
		_, err = datasets.UpsertDataset(ctx, &model.InferenceDataset{
			DatasetID:                datasetID,
			UserID:                   userID,
			DatasetVersion:           1,
			ProcessingState:          model.DatasetProcessingEmbeddingsMaterialized,
			EmbeddingSnapshotID:      embeddingSnapshotID,
			EmbeddingDimensions:      384,
			EmbeddingCount:           10,
			EmbeddingStrategyVersion: "rag-v1",
		}, uuid.New())
		Expect(err).NotTo(HaveOccurred())

		retrieval := &rerankIntegrationRetrievalClient{embeddingSnapshotID: embeddingSnapshotID}
		reranker := &rerankIntegrationReranker{}
		promptStrategy := model.PromptStrategy{
			Version:          "rag-prompt-v1",
			SystemPrompt:     "Use context only.",
			MaxContextTokens: 3000,
			MaxContextChunks: 2,
		}
		usecase := app.NewInferenceUsecase(
			models,
			app.WithInferenceDatasetRepository(datasets),
			app.WithInferenceRequestRepository(repo.NewInferenceRequestRepository(database)),
			app.WithRetrievalClient(retrieval),
			app.WithReranker(reranker),
			app.WithRerankCandidateMultiplier(5),
			app.WithGenerationAdapter(generation.NewDeterministicGenerator()),
			app.WithPromptStrategy(promptStrategy),
			app.WithContextPacker(app.NewContextWindowPacker(promptStrategy)),
			app.WithPromptBuilder(app.NewDefaultPromptBuilder(promptStrategy)),
		)

		response, err := usecase.Generate(ctx, model.GenerateRequest{
			RequestID: requestID,
			DatasetID: datasetID,
			ModelID:   modelID,
			QueryText: "which context wins?",
			TopK:      2,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(retrieval.topK).To(Equal(10))
		Expect(reranker.topK).To(Equal(2))
		Expect(response.Contexts).To(HaveLen(2))
		Expect(response.Contexts[0].SourceText).To(Equal("highest relevance context"))
		Expect(response.Answer).To(ContainSubstring("highest relevance context"))
		var auditedContexts string
		Expect(database.Pool.QueryRow(ctx, `
			SELECT retrieved_contexts::text
			FROM `+database.Name+`.inference_requests
			WHERE request_id = $1
		`, requestID).Scan(&auditedContexts)).To(Succeed())
		Expect(auditedContexts).To(ContainSubstring("highest relevance context"))
		Expect(auditedContexts).To(ContainSubstring("rerank_score"))
	})
})

type integrationRetrievalClient struct{}

func (c *integrationRetrievalClient) SearchEmbeddings(context.Context, uuid.UUID, string, int, map[string]string) ([]model.RetrievedContext, error) {
	return []model.RetrievedContext{{
		EmbeddingRecordID:   uuid.New(),
		EmbeddingSnapshotID: uuid.New(),
		ChunkIndex:          1,
		SourceText:          "integration retrieved context",
		Similarity:          0.91,
	}}, nil
}

func (c *integrationRetrievalClient) Close() error {
	return nil
}

type rerankIntegrationRetrievalClient struct {
	embeddingSnapshotID uuid.UUID
	topK                int
}

func (c *rerankIntegrationRetrievalClient) SearchEmbeddings(_ context.Context, _ uuid.UUID, _ string, topK int, _ map[string]string) ([]model.RetrievedContext, error) {
	c.topK = topK
	contexts := make([]model.RetrievedContext, 0, topK)
	for i := 0; i < topK; i++ {
		contexts = append(contexts, model.RetrievedContext{
			EmbeddingRecordID:   uuid.New(),
			EmbeddingSnapshotID: c.embeddingSnapshotID,
			ChunkIndex:          i,
			SourceText:          fmt.Sprintf("candidate context %d", i),
			Similarity:          0.5,
		})
	}
	contexts[3].SourceText = "highest relevance context"
	return contexts, nil
}

func (c *rerankIntegrationRetrievalClient) Close() error {
	return nil
}

type rerankIntegrationReranker struct {
	topK int
}

func (r *rerankIntegrationReranker) Rerank(_ context.Context, _ string, candidates []model.RetrievedContext, topK int) ([]model.RetrievedContext, error) {
	r.topK = topK
	reranked := []model.RetrievedContext{candidates[3], candidates[1]}
	reranked[0].RerankScore = 0.99
	reranked[1].RerankScore = 0.72
	return reranked[:topK], nil
}

func cleanInferenceTables(ctx context.Context, database *dbconn.Database) error {
	for _, table := range []string{"preference_examples", "inference_feedback", "inference_requests", "inference_models", "inference_datasets"} {
		if _, err := database.Pool.Exec(ctx, "DELETE FROM "+database.Name+"."+table); err != nil {
			return err
		}
	}
	return nil
}

func testPostgresConnectionString(dbName string) string {
	user := env.WithDefaultString("INFERENCE_DB_USER", "bighill_inference_db_user")
	password := env.WithDefaultString("INFERENCE_DB_PASSWORD", env.WithDefaultString("BIGHILL_DB_PASSWORD", "LrDwb53E7DmFc2j4qw77n4pUUfKtULDVh4vrHjWw"))
	host := env.WithDefaultString("INFERENCE_DB_HOST", env.WithDefaultString("PGHOST", "127.0.0.1"))
	if host == "" || host == "/private/tmp" {
		host = "127.0.0.1"
	}
	port := env.WithDefaultString("INFERENCE_DB_PORT", env.WithDefaultString("PGPORT", "5432"))
	sslMode := env.WithDefaultString("INFERENCE_DB_SSLMODE", env.WithDefaultString("PGSSLMODE", "disable"))
	maxConnections := env.WithDefaultInt("INFERENCE_DB_MAX_CONNECTIONS", "20")
	if value := os.Getenv("INFERENCE_DB_NAME"); value != "" {
		dbName = value
	}

	q := url.Values{}
	q.Set("sslmode", sslMode)
	q.Set("pool_max_conns", strconv.Itoa(maxConnections))
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?%s", url.QueryEscape(user), url.QueryEscape(password), host, port, dbName, q.Encode())
}
