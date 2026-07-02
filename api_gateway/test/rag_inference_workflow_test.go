package test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	inferencepb "lib/data_contracts_lib/inference"
	modelregistrypb "lib/data_contracts_lib/model_registry"
	sharedmessaging "lib/shared_lib/messaging"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const defaultInferenceGRPCAddress = "localhost:7073"

var _ = Describe("RAG inference workflow", Ordered, func() {
	var user profileTestUser

	BeforeAll(func() {
		user = createVerifiedProfileAndLogin()
	})

	It("generates from materialized embedding context", func() {
		datasetID := createRAGInferenceDataset(user)
		uploadRAGInferenceDocument(user, datasetID)
		waitForRAGDatasetMaterialized(user, datasetID)

		modelID := uuid.New()
		publishReadyModelForInference(modelID, uuid.MustParse(datasetID))

		client, closeClient := newInferenceClient()
		defer closeClient()

		var response *inferencepb.GenerateResponse
		Eventually(func(g Gomega) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			var err error
			response, err = client.Generate(ctx, &inferencepb.GenerateRequest{
				RequestId: uuid.NewString(),
				DatasetId: datasetID,
				ModelId:   modelID.String(),
				QueryText: "What phrase identifies the embedded knowledge base?",
				TopK:      3,
			})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(response.GetContexts()).NotTo(BeEmpty())
			g.Expect(response.GetAnswer()).To(ContainSubstring("RAG e2e verification phrase"))
		}, 45*time.Second, 1*time.Second).Should(Succeed())

		Expect(response.GetDatasetId()).To(Equal(datasetID))
		Expect(response.GetModelId()).To(Equal(modelID.String()))
		Expect(response.GetGenerationProvider()).To(Equal("deterministic"))
		Expect(response.GetGenerationModel()).To(Equal("deterministic"))
		Expect(response.GetPromptStrategyVersion()).To(Equal("rag-prompt-v1"))
		Expect(response.GetContexts()[0].GetEmbeddingRecordId()).NotTo(BeEmpty())
		Expect(response.GetContexts()[0].GetEmbeddingSnapshotId()).NotTo(BeEmpty())
		Expect(response.GetContexts()[0].GetSourceText()).To(ContainSubstring("RAG e2e verification phrase"))
	})
})

func createRAGInferenceDataset(user profileTestUser) string {
	createPayload := map[string]any{
		"title":             "RAG Inference Knowledge Upload",
		"description":       "HTML document used by the end-to-end RAG inference workflow",
		"category":          "documents",
		"tableNamespace":    "features",
		"tableName":         "rag_inference_knowledge_upload",
		"tableFormat":       "PARQUET",
		"catalogProvider":   "LOCAL",
		"processingProfile": "TEXT_RAG",
	}

	status, body := doJSON(http.MethodPost, "/v1/data/registry", createPayload, user.Token, uuid.New())
	Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
	created := decodeObject(body)
	return stringField(created, "id")
}

func uploadRAGInferenceDocument(user profileTestUser, datasetID string) {
	html := []byte("<!doctype html><html><body><main><h1>RAG verification</h1><p>RAG e2e verification phrase: the citadel index stores normalized feature context.</p></main></body></html>")
	Eventually(func(g Gomega) {
		status, body := doMultipartFile(http.MethodPost, "/v1/data/store/"+datasetID, "file", "rag-inference.html", html, user.Token, uuid.New())
		g.Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
	}, 30*time.Second, 1*time.Second).Should(Succeed())
}

func waitForRAGDatasetMaterialized(user profileTestUser, datasetID string) {
	Eventually(func(g Gomega) {
		status, body := doJSON(http.MethodGet, "/v1/data/registry/"+datasetID, nil, user.Token, uuid.Nil)
		g.Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))

		read := decodeObject(body)
		g.Expect(read["processingState"]).To(Equal("EMBEDDINGS_MATERIALIZED"))
		metadata := schemaMetadataObject(g, read)
		g.Expect(metadata["source_format"]).To(Equal("html"))
		g.Expect(metadata["rows"]).To(BeNumerically(">=", 1))
		expectSchemaField(g, metadata, "source_text")
	}, 45*time.Second, 1*time.Second).Should(Succeed())
}

func publishReadyModelForInference(modelID uuid.UUID, datasetID uuid.UUID) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	publisher, err := sharedmessaging.NewPublisher(kafkaBroker())
	Expect(err).NotTo(HaveOccurred())
	defer publisher.Close()

	err = publisher.Publish(ctx, modelRegistryTopic(), sharedmessaging.Message{
		ResourceKey: modelID,
		MsgType:     sharedmessaging.MsgTypeModelUpdated,
	}, &modelregistrypb.ModelUpdatedEvent{
		ModelId:           modelID.String(),
		TrainingRunId:     uuid.NewString(),
		DatasetId:         datasetID.String(),
		Name:              "rag-e2e-generator",
		ModelVersion:      1,
		BaseModel:         "deterministic",
		ArtifactLocation:  "s3://local-dev-bucket/models/" + modelID.String(),
		ArtifactFormat:    "DETERMINISTIC",
		ArtifactChecksum:  "sha256:" + strings.ReplaceAll(modelID.String(), "-", ""),
		ArtifactSizeBytes: 1,
		MetricsMetadata:   `{"passed":true}`,
		Status:            "READY",
		ServingLoadStatus: "LOADED",
	})
	Expect(err).NotTo(HaveOccurred())
}

func newInferenceClient() (inferencepb.InferenceServiceClient, func()) {
	conn, err := grpc.NewClient(inferenceGRPCAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	Expect(err).NotTo(HaveOccurred())
	return inferencepb.NewInferenceServiceClient(conn), func() {
		Expect(conn.Close()).To(Succeed())
	}
}

func inferenceGRPCAddress() string {
	host := strings.TrimSpace(os.Getenv("INFERENCE_API_GRPC_HOST"))
	if host == "" {
		host = "localhost"
	}
	port := strings.TrimSpace(os.Getenv("INFERENCE_API_GRPC_PORT"))
	if port == "" {
		return defaultInferenceGRPCAddress
	}
	if _, err := strconv.Atoi(port); err != nil {
		return defaultInferenceGRPCAddress
	}
	return fmt.Sprintf("%s:%s", host, port)
}

func kafkaBroker() string {
	broker := strings.TrimSpace(os.Getenv("KAFKA_BROKER"))
	if broker == "" {
		return "localhost:9092"
	}
	return broker
}

func modelRegistryTopic() string {
	topic := strings.TrimSpace(os.Getenv("INFERENCE_SERVICE_MODEL_REGISTRY_SUBSCRIBER_TOPIC"))
	if topic == "" {
		return "model_registry"
	}
	return topic
}
