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
		promptStrategy := model.PromptStrategy{
			Version:          "rag-prompt-v1",
			SystemPrompt:     "Use context only.",
			MaxContextChars:  12000,
			MaxContextChunks: 8,
		}
		modelsUse = app.NewInferenceUsecase(
			models,
			app.WithInferenceDatasetRepository(datasets),
			app.WithInferenceRequestRepository(requests),
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

func cleanInferenceTables(ctx context.Context, database *dbconn.Database) error {
	for _, table := range []string{"inference_requests", "inference_models", "inference_datasets"} {
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
