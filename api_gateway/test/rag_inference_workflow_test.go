package test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	inferencepb "lib/data_contracts_lib/inference"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultInferenceGRPCAddress  = "localhost:7073"
	defaultRAGE2EGenerationModel = "llama3.1:8b"
	e2eGenerationModelEnv        = "E2E_GENERATION_MODEL"
)

var _ = Describe("RAG inference workflow", Ordered, func() {
	var user profileTestUser

	BeforeAll(func() {
		user = createVerifiedProfileAndLogin()
	})

	It("generates from materialized embedding context, records feedback, and exports preferences", func() {
		datasetID := createRAGInferenceDataset(user)
		uploadRAGInferenceDocument(user, datasetID)
		waitForRAGDatasetMaterialized(user, datasetID)

		modelID := uploadBaseModelThroughIngestion(user)
		selectedModel := assertModelSelectable(user, modelID, "UPLOAD", "rag-e2e-uploaded-base")

		client, closeClient := newInferenceClient()
		defer closeClient()

		var response *inferencepb.GenerateResponse
		requestID := uuid.NewString()
		Eventually(func(g Gomega) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			var err error
			response, err = client.Generate(ctx, &inferencepb.GenerateRequest{
				RequestId: requestID,
				UserId:    user.ID.String(),
				OrgId:     user.OrgID.String(),
				DatasetId: datasetID,
				ModelId:   modelID.String(),
				QueryText: "What phrase identifies the embedded knowledge base?",
				TopK:      3,
			})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(response.GetAnswer())).NotTo(BeEmpty())
			expectRAGVerificationContext(g, response)
		}, 45*time.Second, 1*time.Second).Should(Succeed())

		Expect(response.GetDatasetId()).To(Equal(datasetID))
		Expect(response.GetModelId()).To(Equal(modelID.String()))
		Expect(response.GetGenerationProtocol()).To(Equal(stringField(selectedModel, "serving_protocol")))
		Expect(response.GetGenerationModel()).To(Equal(stringField(selectedModel, "serving_model")))
		Expect(response.GetPromptStrategyVersion()).To(Equal("rag-prompt-v1"))
		Expect(response.GetContexts()[0].GetEmbeddingRecordId()).NotTo(BeEmpty())
		Expect(response.GetContexts()[0].GetEmbeddingSnapshotId()).NotTo(BeEmpty())
		Expect(ragResponseContextText(response)).To(ContainSubstring("RAG e2e verification phrase"))

		feedbackID := uuid.NewString()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		feedback, err := client.RecordFeedback(ctx, &inferencepb.RecordFeedbackRequest{
			FeedbackId:      feedbackID,
			RequestId:       response.GetRequestId(),
			UserId:          user.ID.String(),
			OrgId:           user.OrgID.String(),
			Accepted:        false,
			Rating:          -1,
			Comment:         "Prefer the explicit verification phrase.",
			PreferredAnswer: "RAG e2e verification phrase: the citadel index stores normalized feature context.",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(feedback.GetFeedbackId()).To(Equal(feedbackID))
		Expect(feedback.GetRequestId()).To(Equal(response.GetRequestId()))

		export, err := client.ExportPreferenceDataset(ctx, &inferencepb.ExportPreferenceDatasetRequest{
			RequestId:   response.GetRequestId(),
			UserId:      user.ID.String(),
			OrgId:       user.OrgID.String(),
			DatasetId:   datasetID,
			ModelId:     modelID.String(),
			OutputUri:   "s3://local-dev-bucket/preference-datasets/rag-e2e/{preference_dataset_id}.jsonl",
			MinExamples: 1,
			Limit:       10,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(export.GetRequestId()).To(Equal(response.GetRequestId()))
		Expect(export.GetDatasetId()).To(Equal(datasetID))
		Expect(export.GetModelId()).To(Equal(modelID.String()))
		Expect(export.GetExampleCount()).To(BeNumerically(">=", 1))
		Expect(export.GetExported()).To(BeTrue())
		Expect(export.GetOutputUri()).To(MatchRegexp(`^s3://local-dev-bucket/preference-datasets/rag-e2e/.+\.jsonl$`))
	})

	It("uploads a base model artifact and uses the served model for RAG", func() {
		datasetID := createRAGInferenceDataset(user)
		uploadRAGInferenceDocument(user, datasetID)
		waitForRAGDatasetMaterialized(user, datasetID)

		modelID := uploadBaseModelThroughIngestion(user)
		selectedModel := assertModelSelectable(user, modelID, "UPLOAD", "rag-e2e-uploaded-base")
		client, closeClient := newInferenceClient()
		defer closeClient()

		var response *inferencepb.GenerateResponse
		Eventually(func(g Gomega) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			var err error
			response, err = client.Generate(ctx, &inferencepb.GenerateRequest{
				RequestId: uuid.NewString(),
				UserId:    user.ID.String(),
				OrgId:     user.OrgID.String(),
				DatasetId: datasetID,
				ModelId:   modelID.String(),
				QueryText: "Which phrase proves the uploaded base model can serve RAG?",
				TopK:      3,
			})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(response.GetAnswer())).NotTo(BeEmpty())
			expectRAGVerificationContext(g, response)
		}, 75*time.Second, 1*time.Second).Should(Succeed())

		Expect(response.GetDatasetId()).To(Equal(datasetID))
		Expect(response.GetModelId()).To(Equal(modelID.String()))
		Expect(response.GetGenerationProtocol()).To(Equal(stringField(selectedModel, "serving_protocol")))
		Expect(response.GetGenerationModel()).To(Equal(stringField(selectedModel, "serving_model")))
	})

	It("selects a base model and starts an idempotent training run for a materialized dataset", func() {
		datasetID := createRAGInferenceDataset(user)
		uploadRAGInferenceDocument(user, datasetID)
		waitForRAGDatasetMaterialized(user, datasetID)

		modelID := uploadBaseModelThroughIngestion(user)
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

	It("onboards a Hugging Face base model, lists it for selection, and uses it for RAG", func() {
		datasetID := createRAGInferenceDataset(user)
		uploadRAGInferenceDocument(user, datasetID)
		waitForRAGDatasetMaterialized(user, datasetID)

		replaceHuggingFaceToken(user, "hf_api_e2e_token")
		modelID := onboardHuggingFaceBaseModel(user)
		selectedModel := assertModelSelectable(user, modelID, "HUGGING_FACE", "rag-e2e-huggingface-base")

		client, closeClient := newInferenceClient()
		defer closeClient()

		var response *inferencepb.GenerateResponse
		Eventually(func(g Gomega) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			var err error
			response, err = client.Generate(ctx, &inferencepb.GenerateRequest{
				RequestId: uuid.NewString(),
				UserId:    user.ID.String(),
				OrgId:     user.OrgID.String(),
				DatasetId: datasetID,
				ModelId:   modelID.String(),
				QueryText: "Which phrase proves the Hugging Face base model can serve RAG?",
				TopK:      3,
			})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(response.GetAnswer())).NotTo(BeEmpty())
			expectRAGVerificationContext(g, response)
		}, 75*time.Second, 1*time.Second).Should(Succeed())

		Expect(response.GetDatasetId()).To(Equal(datasetID))
		Expect(response.GetModelId()).To(Equal(modelID.String()))
		Expect(response.GetGenerationProtocol()).To(Equal(stringField(selectedModel, "serving_protocol")))
		Expect(response.GetGenerationModel()).To(Equal(stringField(selectedModel, "serving_model")))
	})

	It("rejects invalid model archive uploads before they become selectable", func() {
		archive := invalidHFModelArchive()
		initiatePayload := map[string]any{
			"file_name":           "invalid-model.zip",
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

	status, body := doJSON(http.MethodPost, "/v1/private/data/registry", createPayload, user.Token, uuid.New())
	Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
	created := decodeObject(body)
	return stringField(created, "id")
}

func uploadRAGInferenceDocument(user profileTestUser, datasetID string) {
	html := []byte("<!doctype html><html><body><main><h1>RAG verification</h1><p>RAG e2e verification phrase: the citadel index stores normalized feature context.</p></main></body></html>")
	Eventually(func(g Gomega) {
		status, body := doMultipartFile(http.MethodPost, "/v1/private/data/store/"+datasetID, "file", "rag-inference.html", html, user.Token, uuid.New())
		g.Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
	}, 30*time.Second, 1*time.Second).Should(Succeed())
}

func waitForRAGDatasetMaterialized(user profileTestUser, datasetID string) {
	Eventually(func(g Gomega) {
		status, body := doJSON(http.MethodGet, "/v1/private/data/registry/"+datasetID, nil, user.Token, uuid.Nil)
		g.Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))

		read := decodeObject(body)
		g.Expect(read["processingState"]).To(Equal("EMBEDDINGS_MATERIALIZED"))
		metadata := schemaMetadataObject(g, read)
		g.Expect(metadata["source_format"]).To(Equal("html"))
		g.Expect(metadata["rows"]).To(BeNumerically(">=", 1))
		expectSchemaField(g, metadata, "source_text")
	}, 45*time.Second, 1*time.Second).Should(Succeed())
}

func expectRAGVerificationContext(g Gomega, response *inferencepb.GenerateResponse) {
	g.Expect(response.GetContexts()).NotTo(BeEmpty())
	g.Expect(ragResponseContextText(response)).To(ContainSubstring("RAG e2e verification phrase"))
}

func ragResponseContextText(response *inferencepb.GenerateResponse) string {
	contexts := make([]string, 0, len(response.GetContexts()))
	for _, retrieved := range response.GetContexts() {
		contexts = append(contexts, retrieved.GetSourceText())
	}
	return strings.Join(contexts, "\n")
}

func expectLocalOllamaModelAvailable(modelName string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost:11434/api/tags", nil)
	Expect(err).NotTo(HaveOccurred())
	resp, err := http.DefaultClient.Do(req)
	Expect(err).NotTo(HaveOccurred(), "local Ollama must be running for full-stack RAG e2e")
	defer resp.Body.Close()
	Expect(resp.StatusCode).To(Equal(http.StatusOK))
	var payload map[string]any
	Expect(json.NewDecoder(resp.Body).Decode(&payload)).To(Succeed())
	models, ok := payload["models"].([]any)
	Expect(ok).To(BeTrue(), "ollama tags payload: %#v", payload)
	for _, candidate := range models {
		object, ok := candidate.(map[string]any)
		if ok && ollamaTagMatches(fmt.Sprint(object["name"]), modelName) {
			return
		}
	}
	Fail(fmt.Sprintf("local Ollama model %q is not available; run `ollama pull %s`", modelName, modelName))
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

func uploadBaseModelThroughIngestion(user profileTestUser) uuid.UUID {
	archive := minimalHFModelArchive()
	baseModel := ragE2EGenerationModel()
	initiatePayload := map[string]any{
		"file_name":           "rag-base-model.zip",
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
	Eventually(func(g Gomega) {
		status, body := doJSON(http.MethodPost, "/v1/private/models/uploads", initiatePayload, user.Token, uuid.New())
		g.Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
		initiated := decodeObject(body)
		uploadID = stringField(initiated, "upload_id")
		parsedResourceID, err := uuid.Parse(stringField(initiated, "resource_id"))
		g.Expect(err).NotTo(HaveOccurred())
		resourceID = parsedResourceID
		g.Expect(stringField(initiated, "url")).To(Equal("local-s3://local-dev-bucket"))
		var ok bool
		fields, ok = initiated["fields"].(map[string]any)
		g.Expect(ok).To(BeTrue(), "fields: %#v", initiated["fields"])
		g.Expect(fields).To(HaveKeyWithValue("key", MatchRegexp(`^staging/model_artifact/`)))
		g.Expect(fields).To(HaveKeyWithValue("Content-Type", "application/zip"))
	}, 30*time.Second, 1*time.Second).Should(Succeed())

	writeLocalS3Object("local-dev-bucket", fields["key"].(string), "application/zip", archive)

	status, body := doJSON(http.MethodPost, "/v1/private/models/uploads/"+uploadID+"/complete", nil, user.Token, uuid.New())
	Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
	completed := decodeObject(body)
	Expect(completed["status"]).To(Equal("PROMOTED"))
	Expect(completed["resource_id"]).To(Equal(resourceID.String()))
	Expect(completed["artifact_type"]).To(Equal("BASE_MODEL"))
	Expect(completed["artifact_format"]).To(Equal("hf_model"))
	Expect(completed["model_name"]).To(Equal("rag-e2e-uploaded-base"))
	Expect(completed["model_version"]).To(Equal("1"))
	Expect(completed["base_model"]).To(Equal(baseModel))
	Expect(completed["storage_location"]).To(MatchRegexp(`^s3://local-dev-bucket/models/artifacts/`))
	Expect(completed["checksum"]).To(MatchRegexp(`^sha256:[0-9a-f]{64}$`))
	Expect(completed["actual_size_bytes"]).To(BeNumerically("==", len(archive)))
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

func onboardHuggingFaceBaseModel(user profileTestUser) uuid.UUID {
	baseModel := ragE2EGenerationModel()
	payload := map[string]any{
		"repo_id":       "bighill/rag-e2e-huggingface-base",
		"revision":      "main",
		"client_nonce":  "rag-hf-base-" + uuid.NewString(),
		"model_name":    "rag-e2e-huggingface-base",
		"model_version": "1",
		"base_model":    baseModel,
	}

	var resourceID uuid.UUID
	Eventually(func(g Gomega) {
		status, body := doJSON(http.MethodPost, "/v1/private/models/onboard/huggingface", payload, user.Token, uuid.New())
		g.Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
		completed := decodeObject(body)
		parsedResourceID, err := uuid.Parse(stringField(completed, "resource_id"))
		g.Expect(err).NotTo(HaveOccurred())
		resourceID = parsedResourceID
		g.Expect(completed["status"]).To(Equal("PROMOTED"))
		g.Expect(completed["artifact_type"]).To(Equal("BASE_MODEL"))
		g.Expect(completed["artifact_format"]).To(Equal("HF_MODEL"))
		g.Expect(completed["model_name"]).To(Equal("rag-e2e-huggingface-base"))
		g.Expect(completed["storage_location"]).To(MatchRegexp(`^s3://local-dev-bucket/models/huggingface/`))
		g.Expect(completed["checksum"]).To(MatchRegexp(`^sha256:[0-9a-f]{64}$`))
	}, 45*time.Second, 1*time.Second).Should(Succeed())
	return resourceID
}

func assertModelSelectable(user profileTestUser, modelID uuid.UUID, source string, name string) map[string]any {
	var selected map[string]any
	Eventually(func(g Gomega) {
		status, body := doJSON(http.MethodGet, "/v1/private/models?source="+source+"&kind=BASE&status=READY&limit=25&page=1", nil, user.Token, uuid.Nil)
		g.Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))
		list := decodeObject(body)
		resources, ok := list["resources"].([]any)
		g.Expect(ok).To(BeTrue(), "resources: %#v", list["resources"])
		g.Expect(resources).To(ContainElement(SatisfyAll(
			HaveKeyWithValue("id", modelID.String()),
			HaveKeyWithValue("source", source),
			HaveKeyWithValue("model_kind", "BASE"),
			HaveKeyWithValue("status", "READY"),
			HaveKeyWithValue("serving_load_status", "LOADED"),
			HaveKeyWithValue("serving_model", ragE2EGenerationModel()),
			HaveKey("serving_protocol"),
			HaveKeyWithValue("name", name),
		)))

		status, body = doJSON(http.MethodGet, "/v1/private/models/"+modelID.String(), nil, user.Token, uuid.Nil)
		g.Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))
		selected = decodeObject(body)
		g.Expect(selected).To(SatisfyAll(
			HaveKeyWithValue("id", modelID.String()),
			HaveKeyWithValue("source", source),
			HaveKeyWithValue("model_kind", "BASE"),
			HaveKeyWithValue("status", "READY"),
			HaveKeyWithValue("serving_load_status", "LOADED"),
			HaveKeyWithValue("serving_model", ragE2EGenerationModel()),
			HaveKey("serving_protocol"),
			HaveKeyWithValue("name", name),
		))
	}, 75*time.Second, 1*time.Second).Should(Succeed())
	return selected
}

func ragE2EGenerationModel() string {
	return envOrDefault(e2eGenerationModelEnv, defaultRAGE2EGenerationModel)
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

func newInferenceClient() (inferencepb.InferenceServiceClient, func()) {
	conn, err := grpc.NewClient(inferenceGRPCAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	Expect(err).NotTo(HaveOccurred())
	return inferencepb.NewInferenceServiceClient(conn), func() {
		Expect(conn.Close()).To(Succeed())
	}
}

func inferenceGRPCAddress() string {
	host := strings.TrimSpace(os.Getenv("INFERENCE_SERVICE_API_GRPC_HOST"))
	if host == "" {
		host = "localhost"
	}
	port := strings.TrimSpace(os.Getenv("INFERENCE_SERVICE_API_GRPC_PORT"))
	if port == "" {
		return defaultInferenceGRPCAddress
	}
	if _, err := strconv.Atoi(port); err != nil {
		return defaultInferenceGRPCAddress
	}
	return fmt.Sprintf("%s:%s", host, port)
}
