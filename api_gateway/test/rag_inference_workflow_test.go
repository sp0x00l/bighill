package test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	dataregistrypb "lib/data_contracts_lib/data_registry"
	featurepb "lib/data_contracts_lib/feature_materializer"
	"net/http"
	"sort"
	"strings"
	"time"

	ingestionpb "lib/data_contracts_lib/ingestion"
	env "lib/shared_lib/env"
	msgConn "lib/shared_lib/messaging"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	ragE2EGenerateCallTimeout = 90 * time.Second
	ragE2EGenerateWaitTimeout = 3 * time.Minute
	ragE2EModelPollTimeout    = 5 * time.Second
	ragE2EModelSelectTimeout  = 3 * time.Minute
	ragE2EGraphWaitTimeout    = 3 * time.Minute
	ragE2EOllamaPollTimeout   = 90 * time.Second
	ragE2EOllamaCallTimeout   = 20 * time.Second
)

var ragE2EBaseModelTag string

var _ = Describe("RAG inference workflow", Ordered, func() {
	var user profileTestUser

	BeforeAll(func() {
		user = createVerifiedProfileAndLogin()
	})

	It("generates from materialized embedding context, records feedback, and exports preferences", func() {
		datasetID := createRAGInferenceDataset(user)
		materializeRAGInferenceDataset(user, datasetID)

		modelID := uploadBaseModelThroughIngestion(user, datasetID)
		selectedModel := assertModelSelectable(user, modelID, "UPLOAD", "rag-e2e-uploaded-base")
		endpointID := publishRAGEndpoint(user, modelID, datasetID, "rag-e2e-uploaded-base")

		requestID := uuid.New()
		generation := waitForEndpointRAGGeneration(user.Token, endpointID, requestID, "What phrase identifies the embedded knowledge base?")

		Expect(generation["dataset_id"]).To(Equal(datasetID))
		Expect(generation["generation_protocol"]).To(Equal(stringField(selectedModel, "serving_protocol")))
		Expect(generation["generation_model"]).To(Equal(stringField(selectedModel, "serving_model")))
		Expect(ragResponseContextTextObject(generation)).To(ContainSubstring("RAG e2e verification phrase"))

		recordRejectedFeedback := func(comment string, preferredAnswer string) {
			feedbackID := uuid.New()
			status, body := doJSON(http.MethodPost, "/v1/private/inference/feedback", map[string]any{
				"request_id":       stringField(generation, "request_id"),
				"accepted":         false,
				"rating":           -1,
				"comment":          comment,
				"preferred_answer": preferredAnswer,
			}, user.Token, feedbackID)
			Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
			feedback := decodeObject(body)
			Expect(feedback["feedback_id"]).To(Equal(feedbackID.String()))
			Expect(feedback["request_id"]).To(Equal(stringField(generation, "request_id")))
		}
		recordRejectedFeedback(
			"Prefer the explicit verification phrase.",
			"RAG e2e verification phrase: the citadel index stores normalized feature context.",
		)
		recordRejectedFeedback(
			"Prefer the correction with source phrase.",
			"RAG e2e verification phrase: the citadel index stores normalized feature context, with explicit source grounding.",
		)

		status, body := doJSON(http.MethodPost, "/v1/private/inference/endpoints/"+endpointID.String()+"/preference-datasets", map[string]any{
			"output_uri":   "s3://local-dev-bucket/preference-datasets/rag-e2e/{preference_dataset_id}.jsonl",
			"min_examples": 1,
			"limit":        10,
		}, user.Token, uuid.New())
		Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
		export := decodeSingleObject(body)
		Expect(export["dataset_id"]).To(Equal(datasetID))
		Expect(export["model_id"]).To(Equal(modelID.String()))
		Expect(export["example_count"]).To(BeNumerically(">=", 1))
		Expect(stringField(export, "integrity_key")).To(MatchRegexp(`^sha256:[0-9a-f]{64}$`))
		Expect(stringField(export, "integrity_key")).NotTo(Equal(stringField(export, "preference_dataset_id")))
		Expect(stringField(export, "output_uri")).To(MatchRegexp(`^s3://local-dev-bucket/preference-datasets/rag-e2e/.+\.jsonl$`))
		Expect(stringField(export, "evaluation_output_uri")).To(MatchRegexp(`^s3://local-dev-bucket/preference-datasets/rag-e2e/.+-eval\.jsonl$`))
		Expect(preferenceExampleIDsFromJSONL(readLocalS3ObjectURI(stringField(export, "evaluation_output_uri")))).NotTo(BeEmpty())

		recordRejectedFeedback(
			"Prefer the later correction with source phrase.",
			"RAG e2e verification phrase: the citadel index stores normalized feature context, with explicit source grounding and audit-ready citations.",
		)

		status, body = doJSON(http.MethodPost, "/v1/private/inference/endpoints/"+endpointID.String()+"/preference-datasets", map[string]any{
			"output_uri":   "s3://local-dev-bucket/preference-datasets/rag-e2e/{preference_dataset_id}.jsonl",
			"min_examples": 1,
			"limit":        10,
			"max_per_user": 1,
		}, user.Token, uuid.New())
		Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
		secondExport := decodeSingleObject(body)
		Expect(secondExport["evaluation_output_uri"]).To(Equal(export["evaluation_output_uri"]))
		Expect(secondExport["output_uri"]).NotTo(Equal(export["output_uri"]))
		Expect(secondExport["example_count"]).To(BeNumerically("<=", 1))

		status, body = doJSON(http.MethodGet, "/v1/private/inference/preference-datasets/"+stringField(secondExport, "preference_dataset_id"), nil, user.Token, uuid.Nil)
		Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))
		readExport := decodeSingleObject(body)
		Expect(readExport["preference_dataset_id"]).To(Equal(secondExport["preference_dataset_id"]))

		status, body = doJSON(http.MethodPost, "/v1/private/training-runs/dpo", map[string]any{
			"preference_dataset_id": stringField(secondExport, "preference_dataset_id"),
			"training_profile":      "dpo-default@v1",
			"evaluation_profile":    "dpo-default@v1",
		}, user.Token, uuid.New())
		Expect(status).To(Equal(http.StatusAccepted), "body: %s", string(body))
		dpoRun := decodeObject(body)
		Expect(strings.TrimSpace(stringField(dpoRun, "training_run_id"))).NotTo(BeEmpty())

		trainIDs := preferenceExampleIDsFromJSONL(readLocalS3ObjectURI(stringField(secondExport, "output_uri")))
		evalIDs := preferenceExampleIDsFromJSONL(readLocalS3ObjectURIIfExists(stringField(secondExport, "evaluation_output_uri")))
		for id := range trainIDs {
			Expect(evalIDs).NotTo(HaveKey(id))
		}
	})

	It("uploads a base model artifact and uses the served model for RAG", func() {
		datasetID := createRAGInferenceDataset(user)
		materializeRAGInferenceDataset(user, datasetID)

		modelID := uploadBaseModelThroughIngestion(user, datasetID)
		selectedModel := assertModelSelectable(user, modelID, "UPLOAD", "rag-e2e-uploaded-base")
		endpointID := publishRAGEndpoint(user, modelID, datasetID, "rag-e2e-uploaded-base")

		response := waitForEndpointRAGGeneration(user.Token, endpointID, uuid.New(), "Which phrase proves the uploaded base model can serve RAG?")

		Expect(response["dataset_id"]).To(Equal(datasetID))
		Expect(response["generation_protocol"]).To(Equal(stringField(selectedModel, "serving_protocol")))
		Expect(response["generation_model"]).To(Equal(stringField(selectedModel, "serving_model")))
	})

	It("selects a base model and starts an idempotent training run for a materialized dataset", func() {
		datasetID := createRAGInferenceDataset(user)
		materializeRAGInferenceDataset(user, datasetID)

		modelID := uploadBaseModelThroughIngestion(user, datasetID)
		assertModelSelectable(user, modelID, "UPLOAD", "rag-e2e-uploaded-base")

		requestID := uuid.New()
		trainingRunID, statusURL := startTrainingRun(user, datasetID, modelID, requestID)
		duplicateRunID, duplicateStatusURL := startTrainingRun(user, datasetID, modelID, requestID)
		Expect(duplicateRunID).To(Equal(trainingRunID))
		Expect(duplicateStatusURL).To(Equal(statusURL))

		status, body := doJSON(http.MethodGet, statusURL, nil, user.Token, uuid.Nil)
		Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))
		read := decodeObject(body)
		Expect(read["training_run_id"]).To(Equal(trainingRunID))
		Expect(read["status"]).To(BeElementOf("RUNNING", "COMPLETED", "FAILED"))

		status, body = doJSON(http.MethodPost, "/v1/private/training-runs", map[string]any{
			"dataset_id":      datasetID,
			"source_model_id": modelID.String(),
		}, user.Token, uuid.Nil)
		Expect(status).To(Equal(http.StatusBadRequest), "body: %s", string(body))
	})

	It("rejects invalid model archive uploads before they become selectable", func() {
		datasetID := createRAGInferenceDataset(user)
		archive := invalidHFModelArchive()
		initiatePayload := map[string]any{
			"file_name":           "invalid-model.zip",
			"dataset_id":          datasetID,
			"artifact_type":       "BASE_MODEL",
			"artifact_format":     "HF_MODEL",
			"content_type":        "application/zip",
			"declared_size_bytes": len(archive),
			"client_nonce":        "invalid-model-" + uuid.NewString(),
			"model_name":          "invalid-model",
			"model_version":       "1",
			"base_model":          "bighill/invalid-model",
		}

		status, body := doJSON(http.MethodPost, "/v1/private/models/uploads", initiatePayload, user.Token, uuid.New())
		Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
		initiated := decodeObject(body)
		uploadID := stringField(initiated, "upload_id")
		fields, ok := initiated["fields"].(map[string]any)
		Expect(ok).To(BeTrue(), "fields: %#v", initiated["fields"])
		writeLocalS3Object("local-dev-bucket", fields["key"].(string), "application/zip", archive)

		status, body = doJSON(http.MethodPost, "/v1/private/models/uploads/"+uploadID+"/complete", nil, user.Token, uuid.New())
		Expect(status).To(Equal(http.StatusBadRequest), "body: %s", string(body))
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
		"processingProfile": "TEXT_RAG_PROCESSING_PROFILE",
	}

	created := createDataRegistryDataset(user, createPayload)
	return stringField(created, "id")
}

func uploadRAGInferenceDocument(user profileTestUser, datasetID string) {
	html := []byte("<!doctype html><html><body><main><h1>RAG verification</h1><p>RAG e2e verification phrase: the citadel index stores normalized feature context.</p></main></body></html>")
	var lastErr error
	Eventually(func() bool {
		status, body := doMultipartFile(http.MethodPost, "/v1/private/data/store/"+datasetID, "file", "rag-inference.html", html, user.Token, uuid.New())
		if status != http.StatusCreated {
			lastErr = fmt.Errorf("status %d body %s", status, string(body))
			return false
		}
		lastErr = nil
		return true
	}, 30*time.Second, 1*time.Second).Should(BeTrue(), "RAG document upload failed: %v", lastErr)
}

func materializeRAGInferenceDataset(user profileTestUser, datasetID string) {
	datasetUpdatedEvents, embeddingSnapshotReadyEvents, stop := newRAGMaterializationEventCollectors()
	defer stop()

	uploadRAGInferenceDocument(user, datasetID)
	waitForRAGDatasetMaterialized(user, datasetID, datasetUpdatedEvents, embeddingSnapshotReadyEvents)
}

func createGraphRAGInferenceDataset(user profileTestUser) string {
	createPayload := map[string]any{
		"title":             "Graph RAG Knowledge Upload",
		"description":       "HTML document used by the end-to-end graph RAG workflow",
		"category":          "documents",
		"tableNamespace":    "features",
		"tableName":         "graph_rag_knowledge_upload_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:8],
		"tableFormat":       "PARQUET",
		"catalogProvider":   "LOCAL",
		"processingProfile": "TEXT_RAG_PROCESSING_PROFILE",
	}

	created := createDataRegistryDataset(user, createPayload)
	return stringField(created, "id")
}

func uploadGraphRAGInferenceDocument(user profileTestUser, datasetID string) {
	html := []byte("<!doctype html><html><body><main><h1>Aurora Relay</h1><p>Aurora Relay connects Beacon Hub. Beacon Hub connects Citadel Index. Graph e2e verification phrase: the Aurora Relay graph path returns connected context.</p></main></body></html>")
	var lastErr error
	Eventually(func() bool {
		status, body := doMultipartFile(http.MethodPost, "/v1/private/data/store/"+datasetID, "file", "graph-rag-inference.html", html, user.Token, uuid.New())
		if status != http.StatusCreated {
			lastErr = fmt.Errorf("status %d body %s", status, string(body))
			return false
		}
		lastErr = nil
		return true
	}, 30*time.Second, 1*time.Second).Should(BeTrue(), "Graph RAG document upload failed: %v", lastErr)
}

func materializeGraphRAGInferenceDataset(user profileTestUser, datasetID string) string {
	datasetUpdatedEvents, embeddingSnapshotReadyEvents, graphSnapshotReadyEvents, stop := newGraphRAGMaterializationEventCollectors()
	defer stop()

	uploadGraphRAGInferenceDocument(user, datasetID)
	return waitForGraphRAGDatasetMaterialized(user, datasetID, datasetUpdatedEvents, embeddingSnapshotReadyEvents, graphSnapshotReadyEvents)
}

func newRAGMaterializationEventCollectors() (*kafkaEventCollector[*dataregistrypb.DatasetUpdatedEvent], *kafkaEventCollector[*featurepb.EmbeddingSnapshotReadyEvent], context.CancelFunc) {
	dataTopic := env.WithDefaultString("DATA_REGISTRY_SERVICE_TOPIC", "data_registry")
	featureTopic := env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_TOPIC", "feature_materializer")
	subscriber, start, stop := newKafkaAssertsSubscriber(context.Background(), topicList(dataTopic+","+featureTopic))
	datasetUpdatedEvents := newKafkaEventCollector(msgConn.MsgTypeDatasetUpdated, func() *dataregistrypb.DatasetUpdatedEvent {
		return &dataregistrypb.DatasetUpdatedEvent{}
	})
	embeddingSnapshotReadyEvents := newKafkaEventCollector(msgConn.MsgTypeEmbeddingSnapshotReady, func() *featurepb.EmbeddingSnapshotReadyEvent {
		return &featurepb.EmbeddingSnapshotReadyEvent{}
	})
	msgConn.AddListener(subscriber, datasetUpdatedEvents)
	msgConn.AddListener(subscriber, embeddingSnapshotReadyEvents)
	start()
	return datasetUpdatedEvents, embeddingSnapshotReadyEvents, stop
}

func newGraphRAGMaterializationEventCollectors() (*kafkaEventCollector[*dataregistrypb.DatasetUpdatedEvent], *kafkaEventCollector[*featurepb.EmbeddingSnapshotReadyEvent], *kafkaEventCollector[*featurepb.GraphSnapshotReadyEvent], context.CancelFunc) {
	dataTopic := env.WithDefaultString("DATA_REGISTRY_SERVICE_TOPIC", "data_registry")
	featureTopic := env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_TOPIC", "feature_materializer")
	subscriber, start, stop := newKafkaAssertsSubscriber(context.Background(), topicList(dataTopic+","+featureTopic))
	datasetUpdatedEvents := newKafkaEventCollector(msgConn.MsgTypeDatasetUpdated, func() *dataregistrypb.DatasetUpdatedEvent {
		return &dataregistrypb.DatasetUpdatedEvent{}
	})
	embeddingSnapshotReadyEvents := newKafkaEventCollector(msgConn.MsgTypeEmbeddingSnapshotReady, func() *featurepb.EmbeddingSnapshotReadyEvent {
		return &featurepb.EmbeddingSnapshotReadyEvent{}
	})
	graphSnapshotReadyEvents := newKafkaEventCollector(msgConn.MsgTypeGraphSnapshotReady, func() *featurepb.GraphSnapshotReadyEvent {
		return &featurepb.GraphSnapshotReadyEvent{}
	})
	msgConn.AddListener(subscriber, datasetUpdatedEvents)
	msgConn.AddListener(subscriber, embeddingSnapshotReadyEvents)
	msgConn.AddListener(subscriber, graphSnapshotReadyEvents)
	start()
	return datasetUpdatedEvents, embeddingSnapshotReadyEvents, graphSnapshotReadyEvents, stop
}

func waitForRAGDatasetMaterialized(
	user profileTestUser,
	datasetID string,
	datasetUpdatedEvents *kafkaEventCollector[*dataregistrypb.DatasetUpdatedEvent],
	embeddingSnapshotReadyEvents *kafkaEventCollector[*featurepb.EmbeddingSnapshotReadyEvent],
) {
	datasetUUID, err := uuid.Parse(datasetID)
	Expect(err).NotTo(HaveOccurred())

	embeddingEvent := embeddingSnapshotReadyEvents.waitFor(datasetUUID, 2*time.Minute, func(event *featurepb.EmbeddingSnapshotReadyEvent) bool {
		return event.GetDatasetId() == datasetID &&
			strings.TrimSpace(event.GetEmbeddingSnapshotId()) != "" &&
			event.GetEmbeddingCount() > 0
	})
	datasetUpdatedEvents.waitFor(datasetUUID, 2*time.Minute, func(event *dataregistrypb.DatasetUpdatedEvent) bool {
		return event.GetDatasetId() == datasetID &&
			event.GetProcessingState() == "EMBEDDINGS_MATERIALIZED" &&
			strings.TrimSpace(event.GetEmbeddingSnapshotId()) == embeddingEvent.GetEmbeddingSnapshotId()
	})

	var lastErr error
	Eventually(func() bool {
		status, body := doJSON(http.MethodGet, "/v1/private/data/registry/"+datasetID, nil, user.Token, uuid.Nil)
		if status != http.StatusOK {
			lastErr = fmt.Errorf("status %d body %s", status, string(body))
			return false
		}

		read := decodeObject(body)
		if !isRAGDatasetProcessingStateReady(read["processingState"]) {
			lastErr = fmt.Errorf("dataset not materialized: %#v", read)
			return false
		}
		metadata, ok := read["schemaMetadata"].(map[string]any)
		if !ok {
			lastErr = fmt.Errorf("schemaMetadata: %#v", read["schemaMetadata"])
			return false
		}
		if metadata["source_format"] != "html" {
			lastErr = fmt.Errorf("unexpected source format: %#v", metadata)
			return false
		}
		rows, ok := metadata["rows"].(float64)
		if !ok || rows < 1 {
			lastErr = fmt.Errorf("unexpected row count: %#v", metadata)
			return false
		}
		if !materializationSchemaMetadataHasField(metadata, "source_text") {
			lastErr = fmt.Errorf("source_text field missing: %#v", metadata)
			return false
		}
		lastErr = nil
		return true
	}, 30*time.Second, 1*time.Second).Should(BeTrue(), "RAG dataset was not materialized: %v", lastErr)
}

func waitForGraphRAGDatasetMaterialized(
	user profileTestUser,
	datasetID string,
	datasetUpdatedEvents *kafkaEventCollector[*dataregistrypb.DatasetUpdatedEvent],
	embeddingSnapshotReadyEvents *kafkaEventCollector[*featurepb.EmbeddingSnapshotReadyEvent],
	graphSnapshotReadyEvents *kafkaEventCollector[*featurepb.GraphSnapshotReadyEvent],
) string {
	datasetUUID, err := uuid.Parse(datasetID)
	Expect(err).NotTo(HaveOccurred())

	embeddingEvent := embeddingSnapshotReadyEvents.waitFor(datasetUUID, 2*time.Minute, func(event *featurepb.EmbeddingSnapshotReadyEvent) bool {
		return event.GetDatasetId() == datasetID &&
			strings.TrimSpace(event.GetEmbeddingSnapshotId()) != "" &&
			event.GetEmbeddingCount() > 0
	})
	graphEvent := graphSnapshotReadyEvents.waitFor(datasetUUID, ragE2EGraphWaitTimeout, func(event *featurepb.GraphSnapshotReadyEvent) bool {
		return event.GetDatasetId() == datasetID &&
			strings.TrimSpace(event.GetGraphSnapshotId()) != "" &&
			strings.TrimSpace(event.GetEmbeddingSnapshotId()) == embeddingEvent.GetEmbeddingSnapshotId() &&
			strings.TrimSpace(event.GetProvenanceHash()) != "" &&
			event.GetChunkCount() > 0 &&
			event.GetChunksProcessed() == event.GetChunkCount() &&
			event.GetEntityCount() > 0
	})
	datasetUpdatedEvents.waitFor(datasetUUID, 2*time.Minute, func(event *dataregistrypb.DatasetUpdatedEvent) bool {
		return event.GetDatasetId() == datasetID &&
			event.GetProcessingState() == "GRAPH_MATERIALIZED" &&
			strings.TrimSpace(event.GetGraphSnapshotId()) == graphEvent.GetGraphSnapshotId()
	})

	var lastErr error
	Eventually(func() bool {
		status, body := doJSON(http.MethodGet, "/v1/private/data/registry/"+datasetID, nil, user.Token, uuid.Nil)
		if status != http.StatusOK {
			lastErr = fmt.Errorf("status %d body %s", status, string(body))
			return false
		}
		read := decodeObject(body)
		metadata, ok := read["schemaMetadata"].(map[string]any)
		if !ok {
			lastErr = fmt.Errorf("schemaMetadata: %#v", read["schemaMetadata"])
			return false
		}
		if read["processingState"] != "GRAPH_MATERIALIZED" {
			lastErr = fmt.Errorf("dataset not graph materialized: %#v", read)
			return false
		}
		if metadata["source_format"] != "html" || !materializationSchemaMetadataHasField(metadata, "source_text") {
			lastErr = fmt.Errorf("unexpected metadata: %#v", metadata)
			return false
		}
		lastErr = nil
		return true
	}, 30*time.Second, 1*time.Second).Should(BeTrue(), "Graph RAG dataset was not materialized: %v", lastErr)
	return graphEvent.GetGraphSnapshotId()
}

func materializationSchemaMetadataHasField(metadata map[string]any, fieldName string) bool {
	fields, ok := metadata["fields"].([]any)
	if !ok {
		return false
	}
	for _, field := range fields {
		fieldMap, ok := field.(map[string]any)
		if ok && fieldMap["name"] == fieldName {
			return true
		}
	}
	return false
}

func waitForEndpointRAGGeneration(token string, endpointID uuid.UUID, requestID uuid.UUID, query string) map[string]any {
	var response map[string]any
	var lastErr error
	Eventually(func() bool {
		status, body, err := requestWithTimeout(http.MethodPost, "/v1/private/inference/endpoints/"+endpointID.String()+"/generations", map[string]any{
			"query_text": query,
			"top_k":      3,
		}, token, requestID, ragE2EGenerateCallTimeout)
		if err != nil {
			lastErr = err
			return false
		}
		if status != http.StatusOK {
			lastErr = fmt.Errorf("status %d body %s", status, string(body))
			return false
		}
		response = decodeObject(body)
		if strings.TrimSpace(stringField(response, "answer")) == "" {
			lastErr = fmt.Errorf("empty answer in %#v", response)
			return false
		}
		if !hasRAGVerificationContextObject(response) {
			lastErr = fmt.Errorf("missing RAG verification context in %#v", response)
			return false
		}
		lastErr = nil
		return true
	}, ragE2EGenerateWaitTimeout, 1*time.Second).Should(BeTrue(), "endpoint generation failed: %v", lastErr)
	return response
}

func publishRAGEndpoint(user profileTestUser, modelID uuid.UUID, datasetID string, displayName string) uuid.UUID {
	status, body := doJSON(http.MethodPost, "/v1/private/inference/endpoints", map[string]any{
		"model_id":       modelID.String(),
		"dataset_ids":    []string{datasetID},
		"display_name":   displayName,
		"mode":           "rag",
		"merge_strategy": "score_normalized",
	}, user.Token, uuid.New())
	Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
	endpoint := decodeSingleObject(body)
	Expect(endpoint["display_name"]).To(Equal(displayName))
	Expect(endpoint["status"]).To(Equal("ready"))
	Expect(endpoint["mode"]).To(Equal("rag"))
	endpointID, err := uuid.Parse(stringField(endpoint, "endpoint_id"))
	Expect(err).NotTo(HaveOccurred())
	return endpointID
}

func expectRAGVerificationContextObject(response map[string]any) {
	Expect(response["contexts"]).NotTo(BeEmpty())
	Expect(ragResponseContextTextObject(response)).To(ContainSubstring("RAG e2e verification phrase"))
}

func hasRAGVerificationContextObject(response map[string]any) bool {
	rawContexts, ok := response["contexts"].([]any)
	if !ok || len(rawContexts) == 0 {
		return false
	}
	for _, raw := range rawContexts {
		context, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if strings.Contains(strings.TrimSpace(fmt.Sprint(context["source_text"])), "RAG e2e verification phrase") {
			return true
		}
	}
	return false
}

func ragResponseContextTextObject(response map[string]any) string {
	rawContexts, ok := response["contexts"].([]any)
	Expect(ok).To(BeTrue(), "expected contexts array in %#v", response)
	contexts := make([]string, 0, len(rawContexts))
	for _, raw := range rawContexts {
		context, ok := raw.(map[string]any)
		Expect(ok).To(BeTrue(), "expected context object in %#v", raw)
		contexts = append(contexts, strings.TrimSpace(fmt.Sprint(context["source_text"])))
	}
	return strings.Join(contexts, "\n")
}

func decodeSingleObject(body []byte) map[string]any {
	var decoded []map[string]any
	err := json.Unmarshal(body, &decoded)
	Expect(err).NotTo(HaveOccurred(), "body: %s", string(body))
	Expect(decoded).To(HaveLen(1), "body: %s", string(body))
	return decoded[0]
}

func expectLocalOllamaModelAvailable(modelName string) {
	var tags []string
	var lastErr error
	Eventually(func() bool {
		tags, lastErr = localOllamaGenerationTags()
		if lastErr != nil {
			return false
		}
		for _, candidate := range tags {
			if ollamaTagMatches(candidate, modelName) {
				return true
			}
		}
		return false
	}, ragE2EOllamaPollTimeout, 1*time.Second).Should(BeTrue(), "local Ollama model %q is not available; run `ollama pull %s`; last error: %v; available tags: %v", modelName, modelName, lastErr, tags)
}

func ollamaTagMatches(candidate string, expected string) bool {
	return normalizeOllamaTag(candidate) == normalizeOllamaTag(expected)
}

func normalizeOllamaTag(tag string) string {
	trimmed := strings.TrimSpace(tag)
	if trimmed == "" || strings.Contains(trimmed, ":") {
		return trimmed
	}
	return trimmed + ":latest"
}

func uploadBaseModelThroughIngestion(user profileTestUser, datasetID string) uuid.UUID {
	archive := minimalHFModelArchive()
	baseModel := ragE2EBaseModel()
	modelEvents, stopModelEvents := newModelArtifactIngestedEventCollector()
	defer stopModelEvents()

	initiatePayload := map[string]any{
		"file_name":           "rag-base-model.zip",
		"dataset_id":          datasetID,
		"artifact_type":       "BASE_MODEL",
		"artifact_format":     "HF_MODEL",
		"content_type":        "application/zip",
		"declared_size_bytes": len(archive),
		"client_nonce":        "rag-base-model-" + uuid.NewString(),
		"model_name":          "rag-e2e-uploaded-base",
		"model_version":       "1",
		"base_model":          baseModel,
	}

	var uploadID string
	var resourceID uuid.UUID
	var fields map[string]any
	var lastErr error
	Eventually(func() bool {
		status, body := doJSON(http.MethodPost, "/v1/private/models/uploads", initiatePayload, user.Token, uuid.New())
		if status != http.StatusCreated {
			lastErr = fmt.Errorf("status %d body %s", status, string(body))
			return false
		}
		initiated := decodeObject(body)
		uploadID = stringField(initiated, "upload_id")
		parsedResourceID, err := uuid.Parse(stringField(initiated, "resource_id"))
		if err != nil {
			lastErr = err
			return false
		}
		resourceID = parsedResourceID
		if stringField(initiated, "url") != "local-s3://local-dev-bucket" {
			lastErr = fmt.Errorf("unexpected upload URL in %#v", initiated)
			return false
		}
		var ok bool
		fields, ok = initiated["fields"].(map[string]any)
		if !ok {
			lastErr = fmt.Errorf("fields: %#v", initiated["fields"])
			return false
		}
		key, ok := fields["key"].(string)
		if !ok || !strings.HasPrefix(key, "staging/model_artifact/") {
			lastErr = fmt.Errorf("unexpected upload key in %#v", fields)
			return false
		}
		if fields["Content-Type"] != "application/zip" {
			lastErr = fmt.Errorf("unexpected content type in %#v", fields)
			return false
		}
		lastErr = nil
		return true
	}, 30*time.Second, 1*time.Second).Should(BeTrue(), "model upload initiation failed: %v", lastErr)

	writeLocalS3Object("local-dev-bucket", fields["key"].(string), "application/zip", archive)

	status, body := doJSON(http.MethodPost, "/v1/private/models/uploads/"+uploadID+"/complete", nil, user.Token, uuid.New())
	Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
	completed := decodeObject(body)
	Expect(completed["status"]).To(Equal("PROMOTED"))
	Expect(completed["resource_id"]).To(Equal(resourceID.String()))
	Expect(completed["dataset_id"]).To(Equal(datasetID))
	Expect(completed["artifact_type"]).To(Equal("BASE_MODEL"))
	Expect(completed["artifact_format"]).To(Equal("hf_model"))
	Expect(completed["model_name"]).To(Equal("rag-e2e-uploaded-base"))
	Expect(completed["model_version"]).To(Equal("1"))
	Expect(completed["base_model"]).To(Equal(baseModel))
	Expect(completed["storage_location"]).To(MatchRegexp(`^s3://local-dev-bucket/models/artifacts/`))
	Expect(completed["checksum"]).To(MatchRegexp(`^sha256:[0-9a-f]{64}$`))
	Expect(completed["actual_size_bytes"]).To(BeNumerically("==", len(archive)))
	modelEvents.waitFor(resourceID, 30*time.Second, func(event *ingestionpb.ModelArtifactIngestedEvent) bool {
		return event.GetArtifactId() == resourceID.String() &&
			event.GetDatasetId() == datasetID &&
			event.GetOrgId() == user.OrgID.String() &&
			event.GetUserId() == user.ID.String()
	})
	return resourceID
}

func startTrainingRun(user profileTestUser, datasetID string, modelID uuid.UUID, requestID uuid.UUID) (string, string) {
	status, body := doJSON(http.MethodPost, "/v1/private/training-runs", map[string]any{
		"dataset_id":         datasetID,
		"source_model_id":    modelID.String(),
		"training_profile":   "sft-default@v1",
		"evaluation_profile": "ragas-default@v1",
	}, user.Token, requestID)
	Expect(status).To(Equal(http.StatusAccepted), "body: %s", string(body))
	started := decodeObject(body)
	trainingRunID := stringField(started, "training_run_id")
	statusURL := stringField(started, "status_url")
	Expect(statusURL).To(Equal("/v1/private/training-runs/" + trainingRunID))
	return trainingRunID, statusURL
}

func replaceHuggingFaceToken(user profileTestUser, token string) {
	status, body := doJSON(http.MethodPut, "/v1/private/profiles/huggingface-token", map[string]any{"token": token}, user.Token, uuid.Nil)
	Expect(status).To(Equal(http.StatusNoContent), "body: %s", string(body))
}

func assertModelSelectable(user profileTestUser, modelID uuid.UUID, source string, name string) map[string]any {
	var selected map[string]any
	lastErr := fmt.Errorf("model %s was not observed yet", modelID)
	Eventually(func() bool {
		status, body, err := requestWithTimeout(http.MethodGet, "/v1/private/models/"+modelID.String(), nil, user.Token, uuid.Nil, ragE2EModelPollTimeout)
		if err != nil {
			lastErr = err
			return false
		}
		if status != http.StatusOK {
			lastErr = fmt.Errorf("model read status %d body %s", status, string(body))
			return false
		}
		selected = decodeObject(body)
		if err := baseModelSelectableError(selected, modelID, source, name); err != nil {
			lastErr = err
			return false
		}

		status, body, err = requestWithTimeout(http.MethodGet, "/v1/private/models?source="+source+"&kind=BASE&status=READY&limit=25&page=1", nil, user.Token, uuid.Nil, ragE2EModelPollTimeout)
		if err != nil {
			lastErr = err
			return false
		}
		if status != http.StatusOK {
			lastErr = fmt.Errorf("model list status %d body %s", status, string(body))
			return false
		}
		list := decodeObject(body)
		resources, ok := list["resources"].([]any)
		if !ok {
			lastErr = fmt.Errorf("model list response: %#v", list)
			return false
		}
		for _, resource := range resources {
			object, ok := resource.(map[string]any)
			if !ok {
				continue
			}
			if baseModelSelectableError(object, modelID, source, name) == nil {
				lastErr = nil
				return true
			}
		}
		lastErr = fmt.Errorf("model list did not contain selectable model %s: %#v", modelID, resources)
		return false
	}, ragE2EModelSelectTimeout, 1*time.Second).Should(BeTrue(), "model was not selectable: %v", lastErr)
	return selected
}

func baseModelSelectableError(resource map[string]any, modelID uuid.UUID, source string, name string) error {
	if resource["id"] != modelID.String() {
		return fmt.Errorf("model id mismatch in %#v", resource)
	}
	if resource["source"] != source {
		return fmt.Errorf("model source mismatch in %#v", resource)
	}
	if resource["model_kind"] != "BASE" {
		return fmt.Errorf("model kind mismatch in %#v", resource)
	}
	if resource["status"] != "READY" {
		return fmt.Errorf("model status mismatch in %#v", resource)
	}
	if resource["serving_load_status"] != "LOADED" {
		return fmt.Errorf("model serving status mismatch in %#v", resource)
	}
	if resource["serving_model"] != ragE2EBaseModel() {
		return fmt.Errorf("model serving tag mismatch in %#v", resource)
	}
	if _, ok := resource["serving_protocol"]; !ok {
		return fmt.Errorf("model serving protocol missing in %#v", resource)
	}
	if resource["name"] != name {
		return fmt.Errorf("model name mismatch in %#v", resource)
	}
	return nil
}

func ragE2EBaseModel() string {
	if ragE2EBaseModelTag != "" {
		return ragE2EBaseModelTag
	}
	ragE2EBaseModelTag = discoverLocalOllamaGenerationModel()
	return ragE2EBaseModelTag
}

func discoverLocalOllamaGenerationModel() string {
	var candidates []string
	var lastErr error
	Eventually(func() bool {
		candidates, lastErr = localOllamaGenerationTags()
		return lastErr == nil && len(candidates) > 0
	}, ragE2EOllamaPollTimeout, 1*time.Second).Should(BeTrue(), "local Ollama has no generation model tags; pull or provision a chat/generation model before running the RAG e2e; last error: %v", lastErr)
	if len(candidates) == 0 {
		Fail("local Ollama has no generation model tags; pull or provision a chat/generation model before running the RAG e2e")
	}
	return candidates[0]
}

func localOllamaGenerationTags() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ragE2EOllamaCallTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost:11434/api/tags", nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("local Ollama must be running for full-stack RAG e2e: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ollama tags status %d", resp.StatusCode)
	}

	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	models, ok := payload["models"].([]any)
	if !ok {
		return nil, fmt.Errorf("ollama tags payload missing models: %#v", payload)
	}

	candidates := make([]string, 0, len(models))
	for _, candidate := range models {
		object, ok := candidate.(map[string]any)
		if !ok {
			continue
		}
		name := strings.TrimSpace(fmt.Sprint(object["name"]))
		if name == "" || looksLikeEmbeddingModel(name) {
			continue
		}
		candidates = append(candidates, name)
	}
	sort.Strings(candidates)
	return candidates, nil
}

func looksLikeEmbeddingModel(name string) bool {
	lower := strings.ToLower(name)
	return strings.Contains(lower, "embed") ||
		strings.Contains(lower, "embedding") ||
		strings.Contains(lower, "bge") ||
		strings.Contains(lower, "nomic")
}

func minimalHFModelArchive() []byte {
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	writeZipFile(writer, "config.json", []byte(`{"architectures":["BighillE2EModel"],"model_type":"bighill_e2e"}`))
	writeZipFile(writer, "model.safetensors", minimalSafetensorsObject())
	Expect(writer.Close()).To(Succeed())
	return buffer.Bytes()
}

func minimalSafetensorsObject() []byte {
	header := []byte(`{"weight":{"dtype":"F32","shape":[1],"data_offsets":[0,4]}}`)
	payload := make([]byte, 8+len(header)+4)
	binary.LittleEndian.PutUint64(payload[:8], uint64(len(header)))
	copy(payload[8:], header)
	copy(payload[8+len(header):], []byte{0, 0, 0, 0})
	return payload
}

func invalidHFModelArchive() []byte {
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	writeZipFile(writer, "README.md", []byte("not a model"))
	Expect(writer.Close()).To(Succeed())
	return buffer.Bytes()
}

func writeZipFile(writer *zip.Writer, name string, content []byte) {
	file, err := writer.Create(name)
	Expect(err).NotTo(HaveOccurred())
	_, err = file.Write(content)
	Expect(err).NotTo(HaveOccurred())
}

func preferenceExampleIDsFromJSONL(content []byte) map[string]struct{} {
	ids := map[string]struct{}{}
	for _, line := range bytes.Split(content, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		var record map[string]any
		Expect(json.Unmarshal(line, &record)).To(Succeed())
		id := strings.TrimSpace(fmt.Sprint(record["preference_example_id"]))
		Expect(id).NotTo(BeEmpty())
		ids[id] = struct{}{}
	}
	return ids
}
