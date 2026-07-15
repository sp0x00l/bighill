package integration_test

import (
	"context"
	"errors"
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
	inferencegrpc "inference_service/pkg/infra/network/grpc"
	inferencemessaging "inference_service/pkg/infra/network/messaging"
	repo "inference_service/pkg/infra/repo/db"

	datasetpb "lib/data_contracts_lib/data_registry"
	inferencepb "lib/data_contracts_lib/inference"
	modelregistrypb "lib/data_contracts_lib/model_registry"
	"lib/shared_lib/ctxutil"
	dbconn "lib/shared_lib/db"
	env "lib/shared_lib/env"
	sharedmessaging "lib/shared_lib/messaging"
	shareduow "lib/shared_lib/uow"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
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

		dbName := env.WithDefaultString("INFERENCE_SERVICE_DB_NAME", "bighill_inference_db")
		connectionString := testPostgresConnectionString(dbName)

		var err error
		database, err = dbconn.InitDatabase(ctx, dbName, connectionString, log.StandardLogger())
		Expect(err).NotTo(HaveOccurred())

		models = repo.NewInferenceModelRepository(database)
		datasets = repo.NewInferenceDatasetRepository(database)
		endpoints := repo.NewPublishedEndpointRepository(database)
		capabilities := repo.NewCapabilityReportRepository(database)
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
			app.WithPublishedEndpointRepository(endpoints),
			app.WithCapabilityReportRepository(capabilities),
			app.WithInferenceRequestRepository(requests),
			app.WithInferenceFeedbackRepository(feedbacks),
			app.WithInferenceUnitOfWork(shareduow.New(database.Pool)),
			app.WithRetrievalClient(&integrationRetrievalClient{}),
			app.WithGenerationAdapters(map[string]app.GenerationAdapter{
				model.ServingProtocolOpenAIChatCompletions.String(): integrationGenerationAdapter{},
			}),
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
		Expect(purgeTopic(ctx, brokers, topics.ModelRegistry)).To(Succeed())
		Expect(purgeTopic(ctx, brokers, topics.DataRegistry)).To(Succeed())
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
		publisher, err := sharedmessaging.NewPublisher(brokers)
		Expect(err).NotTo(HaveOccurred())

		modelUpdatedSubscriber := inferencemessaging.NewModelUpdatedSubscriber(serviceSubscriber, modelsUse, topics)
		go func() {
			_ = modelUpdatedSubscriber.Start(runCtx)
		}()
		Eventually(func(g Gomega) {
			g.Expect(sharedmessaging.CheckSubscriberHealth(runCtx, serviceSubscriber, sharedmessaging.SubscriberHealthCheckConfig{
				RequireAssignment: true,
				MaxPollSilence:    10 * time.Second,
			})).To(Succeed())
		}, 10*time.Second, 250*time.Millisecond).Should(Succeed())

		modelID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
		trainingRunID := uuid.New()
		datasetID := uuid.New()
		Expect(upsertInferenceTenant(ctx, database, userID)).To(Succeed())
		Expect(publisher.Publish(runCtx, topics.ModelRegistry, sharedmessaging.Message{
			ResourceKey: modelID,
			MsgType:     sharedmessaging.MsgTypeModelUpdated,
		}, &modelregistrypb.ModelUpdatedEvent{
			ModelId:           modelID.String(),
			UserId:            userID.String(),
			OrgId:             orgID.String(),
			TrainingRunId:     trainingRunID.String(),
			DatasetId:         datasetID.String(),
			ModelKind:         model.ModelKindFineTuned.String(),
			Source:            model.ModelSourceTraining.String(),
			SourceMetadata:    "{}",
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
			ServingProtocol:   model.ServingProtocolOpenAIChatCompletions.String(),
			ServingLoadStatus: model.ModelLoadStatusLoaded.String(),
			MetricsMetadata:   `{"eval_loss":0.12}`,
			Status:            "READY",
		})).To(Succeed())
		publisher.Close()

		Eventually(func(g Gomega) {
			record, err := models.ReadByID(ctxutil.WithActorOrg(ctx, userID, orgID), orgID, modelID)
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
		Expect(purgeTopic(ctx, brokers, topics.ModelRegistry)).To(Succeed())
		Expect(purgeTopic(ctx, brokers, topics.DataRegistry)).To(Succeed())
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
		publisher, err := sharedmessaging.NewPublisher(brokers)
		Expect(err).NotTo(HaveOccurred())

		inferenceSubscriber := inferencemessaging.NewModelUpdatedSubscriber(serviceSubscriber, modelsUse, topics)
		go func() {
			_ = inferenceSubscriber.Start(runCtx)
		}()
		Eventually(func(g Gomega) {
			g.Expect(sharedmessaging.CheckSubscriberHealth(runCtx, serviceSubscriber, sharedmessaging.SubscriberHealthCheckConfig{
				RequireAssignment: true,
				MaxPollSilence:    10 * time.Second,
			})).To(Succeed())
		}, 10*time.Second, 250*time.Millisecond).Should(Succeed())

		datasetID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
		modelID := uuid.New()
		embeddingSnapshotID := uuid.New()
		Expect(upsertInferenceTenant(ctx, database, userID)).To(Succeed())
		_, err = models.UpsertModel(ctxutil.WithActorOrg(ctx, userID, orgID), &model.InferenceModel{
			ModelID:           modelID,
			UserID:            userID,
			OrgID:             orgID,
			TrainingRunID:     uuid.New(),
			DatasetID:         datasetID,
			ModelKind:         model.ModelKindFineTuned,
			Source:            model.ModelSourceTraining,
			SourceMetadata:    "{}",
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
			ServingProtocol:   model.ServingProtocolOpenAIChatCompletions,
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
			OrgId:                    orgID.String(),
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
			ProcessingProfile:        model.ProcessingProfileTextRAG.String(),
			EmbeddingStrategyVersion: "rag-v1",
			EmbeddingChunkerName:     "go-token-window",
			EmbeddingChunkerVersion:  "v1",
			EmbeddingChunkSize:       384,
			EmbeddingChunkOverlap:    64,
			EmbeddingProvider:        "ollama",
			EmbeddingModel:           "bge-small-en-v1.5",
		})).To(Succeed())
		publisher.Close()

		Eventually(func(g Gomega) {
			record, err := datasets.ReadDataset(ctxutil.WithActorOrg(ctx, userID, orgID), orgID, datasetID)
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
			UserId:    userID.String(),
			OrgId:     orgID.String(),
			DatasetId: datasetID.String(),
			ModelId:   modelID.String(),
			QueryText: "what movie context is available?",
			TopK:      3,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(response.GetAnswer()).To(ContainSubstring("integration retrieved context"))
		Expect(response.GetRequestId()).NotTo(BeEmpty())
		Expect(response.GetGenerationProtocol()).To(Equal(model.ServingProtocolOpenAIChatCompletions.String()))
		Expect(response.GetContexts()).To(HaveLen(1))

		feedbackID := uuid.New()
		feedback, err := client.RecordFeedback(ctx, &inferencepb.RecordFeedbackRequest{
			FeedbackId: feedbackID.String(),
			RequestId:  response.GetRequestId(),
			UserId:     userID.String(),
			OrgId:      orgID.String(),
			Accepted:   false,
			Rating:     -1,
			Comment:    "not enough detail",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(feedback.GetFeedbackId()).To(Equal(feedbackID.String()))
		var label string
		var rejectedAnswer string
		Expect(database.Pool.QueryRow(ctxutil.WithActorOrg(ctx, userID, orgID), `
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
		orgID := uuid.New()
		modelID := uuid.New()
		embeddingSnapshotID := uuid.New()
		requestID := uuid.New()
		Expect(upsertInferenceTenant(ctx, database, userID)).To(Succeed())
		_, err := models.UpsertModel(ctxutil.WithActorOrg(ctx, userID, orgID), &model.InferenceModel{
			ModelID:           modelID,
			UserID:            userID,
			OrgID:             orgID,
			TrainingRunID:     uuid.New(),
			DatasetID:         datasetID,
			ModelKind:         model.ModelKindFineTuned,
			Source:            model.ModelSourceTraining,
			SourceMetadata:    "{}",
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
			ServingProtocol:   model.ServingProtocolOpenAIChatCompletions,
			ServingLoadStatus: model.ModelLoadStatusLoaded,
			MetricsMetadata:   `{"eval_loss":0.12}`,
			Status:            model.ModelStatusReady,
		}, uuid.New())
		Expect(err).NotTo(HaveOccurred())
		_, err = datasets.UpsertDataset(ctxutil.WithActorOrg(ctx, userID, orgID), &model.InferenceDataset{
			DatasetID:                datasetID,
			UserID:                   userID,
			OrgID:                    orgID,
			DatasetVersion:           1,
			ProcessingState:          model.DatasetProcessingEmbeddingsMaterialized,
			StorageLocation:          "s3://local-dev-bucket/features/rerank.parquet",
			TableNamespace:           "features",
			TableName:                "rerank_contexts",
			TableFormat:              "PARQUET",
			CatalogProvider:          "LOCAL",
			ProcessingProfile:        model.ProcessingProfileTextRAG.String(),
			SchemaVersion:            1,
			SchemaMetadata:           `{"columns":[{"name":"text"}]}`,
			EmbeddingSnapshotID:      embeddingSnapshotID,
			VectorStore:              "pgvector",
			CollectionName:           "rerank_contexts",
			EmbeddingDimensions:      384,
			EmbeddingCount:           10,
			EmbeddingStrategyVersion: "rag-v1",
			EmbeddingChunkerName:     "go-token-window",
			EmbeddingChunkerVersion:  "v1",
			EmbeddingChunkSize:       384,
			EmbeddingChunkOverlap:    64,
			EmbeddingProvider:        "ollama",
			EmbeddingModel:           "bge-small-en-v1.5",
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
			app.WithGenerationAdapters(map[string]app.GenerationAdapter{
				model.ServingProtocolOpenAIChatCompletions.String(): integrationGenerationAdapter{},
			}),
			app.WithPromptStrategy(promptStrategy),
			app.WithContextPacker(app.NewContextWindowPacker(promptStrategy)),
			app.WithPromptBuilder(app.NewDefaultPromptBuilder(promptStrategy)),
		)

		response, err := usecase.Generate(ctx, model.GenerateRequest{
			RequestID: requestID,
			UserID:    userID,
			OrgID:     orgID,
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
		Expect(database.Pool.QueryRow(ctxutil.WithActorOrg(ctx, userID, orgID), `
			SELECT retrieved_contexts::text
			FROM `+database.Name+`.inference_requests
			WHERE request_id = $1
		`, requestID).Scan(&auditedContexts)).To(Succeed())
		Expect(auditedContexts).To(ContainSubstring("highest relevance context"))
		Expect(auditedContexts).To(ContainSubstring("rerank_score"))
	})
})

type integrationRetrievalClient struct{}

func (c *integrationRetrievalClient) SearchEmbeddings(context.Context, uuid.UUID, uuid.UUID, string, int, map[string]string) ([]model.RetrievedContext, error) {
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

type integrationGenerationAdapter struct{}

func (a integrationGenerationAdapter) Generate(_ context.Context, request model.GenerationRequest) (model.GenerationResult, error) {
	if len(request.Contexts) == 0 {
		return model.GenerationResult{}, fmt.Errorf("retrieved context is required")
	}
	return model.GenerationResult{
		Content:      "Based on the retrieved context: " + request.Contexts[0].SourceText,
		FinishReason: model.GenerationFinishReasonStop,
	}, nil
}

type rerankIntegrationRetrievalClient struct {
	embeddingSnapshotID uuid.UUID
	topK                int
}

func (c *rerankIntegrationRetrievalClient) SearchEmbeddings(_ context.Context, _ uuid.UUID, _ uuid.UUID, _ string, topK int, _ map[string]string) ([]model.RetrievedContext, error) {
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
	for _, table := range []string{
		"agent_run_labels",
		"agent_tool_invocations",
		"agent_steps",
		"agent_runs",
		"golden_tasks",
		"agent_adapters",
		"agent_champion_states",
		"agent_specs",
		"capability_reports",
		"effective_base_versions",
		"lineage_eval_examples",
		"lineage_eval_sets",
		"preference_dataset_snapshots",
		"preference_examples",
		"inference_feedback",
		"inference_requests",
		"published_endpoint_datasets",
		"published_inference_endpoints",
		"inference_models",
		"inference_datasets",
		"tenants",
	} {
		if _, err := database.Pool.Exec(ctx, "DELETE FROM "+database.Name+"."+table); err != nil {
			return err
		}
	}
	return nil
}

func upsertInferenceTenant(ctx context.Context, database *dbconn.Database, userID uuid.UUID) error {
	ctx = ctxutil.WithSystemContext(ctx)
	_, err := database.Pool.Exec(ctx, `
		INSERT INTO `+database.Name+`.tenants (id, email, deleted)
		VALUES ($1, $2, false)
		ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email, deleted = false
	`, userID, userID.String()+"@example.test")
	return err
}

func purgeTopic(ctx context.Context, brokers string, topic string) error {
	log.Trace("purgeTopic")

	Expect(sharedmessaging.CreateTopic(ctx, brokers, topic)).To(Succeed())

	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
	if err != nil {
		return err
	}
	defer admin.Close()

	md, err := admin.GetMetadata(&topic, false, 10000)
	if err != nil {
		return err
	}
	tmd, ok := md.Topics[topic]
	if !ok || tmd.Error.Code() != kafka.ErrNoError {
		return nil
	}

	partitions := make([]kafka.TopicPartition, 0, len(tmd.Partitions))
	for _, partition := range tmd.Partitions {
		partitions = append(partitions, kafka.TopicPartition{
			Topic:     &topic,
			Partition: partition.ID,
			Offset:    kafka.OffsetEnd,
		})
	}
	if len(partitions) == 0 {
		return nil
	}

	for attempt := 0; attempt < 5; attempt++ {
		if err := deleteTopicRecords(ctx, admin, partitions); err != nil {
			if isRetriableTopicPurgeError(err) && attempt < 4 {
				time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
				continue
			}
			return err
		}
		return nil
	}
	return nil
}

func deleteTopicRecords(ctx context.Context, admin *kafka.AdminClient, partitions []kafka.TopicPartition) error {
	log.Trace("deleteTopicRecords")

	res, err := admin.DeleteRecords(
		ctx,
		partitions,
		kafka.SetAdminOperationTimeout(30*time.Second),
	)
	if err != nil {
		if !isKafkaErrorCode(err, -186) {
			return err
		}
		return nil
	}

	for _, result := range res.DeleteRecordsResults {
		if result.TopicPartition.Error != nil {
			if !isKafkaErrorCode(result.TopicPartition.Error, -186) {
				return result.TopicPartition.Error
			}
		}
	}
	return nil
}

func isRetriableTopicPurgeError(err error) bool {
	log.Trace("isRetriableTopicPurgeError")

	return isKafkaErrorCode(err, kafka.ErrNotLeaderForPartition) ||
		isKafkaErrorCode(err, kafka.ErrLeaderNotAvailable)
}

func isKafkaErrorCode(err error, code kafka.ErrorCode) bool {
	log.Trace("isKafkaErrorCode")

	var kafkaErr kafka.Error
	return errors.As(err, &kafkaErr) && kafkaErr.Code() == code
}

func testPostgresConnectionString(dbName string) string {
	user := env.WithDefaultString("INFERENCE_SERVICE_DB_USER", "bighill_inference_db_user")
	password := env.WithDefaultString("INFERENCE_SERVICE_DB_PASSWORD", env.WithDefaultString("BIGHILL_DB_PASSWORD", "LrDwb53E7DmFc2j4qw77n4pUUfKtULDVh4vrHjWw"))
	host := env.WithDefaultString("INFERENCE_SERVICE_DB_HOST", env.WithDefaultString("PGHOST", "127.0.0.1"))
	if host == "" || host == "/private/tmp" {
		host = "127.0.0.1"
	}
	port := env.WithDefaultString("INFERENCE_SERVICE_DB_PORT", env.WithDefaultString("PGPORT", "5432"))
	sslMode := env.WithDefaultString("INFERENCE_SERVICE_DB_SSLMODE", env.WithDefaultString("PGSSLMODE", "disable"))
	maxConnections := env.WithDefaultInt("INFERENCE_SERVICE_DB_MAX_CONNECTIONS", "20")
	if value := os.Getenv("INFERENCE_SERVICE_DB_NAME"); value != "" {
		dbName = value
	}

	q := url.Values{}
	q.Set("sslmode", sslMode)
	q.Set("pool_max_conns", strconv.Itoa(maxConnections))
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?%s", url.QueryEscape(user), url.QueryEscape(password), host, port, dbName, q.Encode())
}
