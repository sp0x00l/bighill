package test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	dataregistrypb "lib/data_contracts_lib/data_registry"
	featurepb "lib/data_contracts_lib/feature_materializer"
	profilepb "lib/data_contracts_lib/profile"
	env "lib/shared_lib/env"
	msgConn "lib/shared_lib/messaging"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	realHuggingFaceE2EFlag           = "BIGHILL_E2E_HUGGINGFACE_REAL_DOWNLOAD"
	realHuggingFaceTokenEnv          = "BIGHILL_E2E_HUGGINGFACE_TOKEN"
	realHuggingFaceRepoIDEnv         = "BIGHILL_E2E_HUGGINGFACE_REPO_ID"
	realHuggingFaceRevisionEnv       = "BIGHILL_E2E_HUGGINGFACE_REVISION"
	realHuggingFaceFileEnv           = "BIGHILL_E2E_HUGGINGFACE_FILE"
	realHuggingFaceBaseModelEnv      = "BIGHILL_E2E_HUGGINGFACE_BASE_MODEL"
	realHuggingFaceArtifactFormatEnv = "BIGHILL_E2E_HUGGINGFACE_ARTIFACT_FORMAT"
	realHuggingFaceTimeoutEnv        = "BIGHILL_E2E_HUGGINGFACE_TIMEOUT_SECONDS"
	defaultRealHuggingFaceRepoID     = "QuantFactory/Meta-Llama-3-8B-Instruct-GGUF"
	defaultRealHuggingFaceFile       = "Meta-Llama-3-8B-Instruct.Q4_K_M.gguf"
	defaultRealHuggingFaceTimeout    = 90 * time.Minute
	techniqueRAGPaperPath            = "data/papers/techniquerag-2505.11988v1.pdf"
	techniqueRAGPaperSHA256          = "2d123f7850c99302b030b2809fd76a1fc689964f0cca8c608a21cfd1115bff42"
	techniqueRAGPaperPageCount       = 14
)

var _ = Describe("Hugging Face real model onboarding", Label("real-huggingface"), func() {
	It("downloads and provisions a real GGUF model, then grounds RAG over a pinned TechniqueRAG paper", func() {
		if !strings.EqualFold(strings.TrimSpace(os.Getenv(realHuggingFaceE2EFlag)), "true") {
			Fail("set BIGHILL_E2E_HUGGINGFACE_REAL_DOWNLOAD=true to run the real Hugging Face download e2e")
		}
		token := strings.TrimSpace(os.Getenv(realHuggingFaceTokenEnv))
		Expect(token).NotTo(BeEmpty(), "set BIGHILL_E2E_HUGGINGFACE_TOKEN to run the real Hugging Face download e2e")

		repoID := envOrDefault(realHuggingFaceRepoIDEnv, defaultRealHuggingFaceRepoID)
		revision := envOrDefault(realHuggingFaceRevisionEnv, "main")
		baseModel := envOrDefault(realHuggingFaceBaseModelEnv, repoID)
		hfFile := envOrDefault(realHuggingFaceFileEnv, defaultRealHuggingFaceFile)
		artifactFormat := envOrDefault(realHuggingFaceArtifactFormatEnv, "GGUF_MODEL")
		timeout := durationEnvOrDefault(realHuggingFaceTimeoutEnv, defaultRealHuggingFaceTimeout)
		modelName := "hf-real-e2e-" + sanitizeModelName(repoID) + "-" + uuid.NewString()[:8]
		clientNonce := "hf-real-" + uuid.NewString()

		tenantTopic := env.WithDefaultString("TENANT_SERVICE_KAFKA_PUBLISHER_TOPIC", "tenant")
		profileSubscriber, startProfileSubscriber, stopProfileSubscriber := newKafkaAssertsSubscriber(context.Background(), topicList(tenantTopic))
		defer stopProfileSubscriber()
		profileCreatedEvents := newKafkaEventCollector(msgConn.MsgTypeUserCreated, func() *profilepb.UserCreatedEvent {
			return &profilepb.UserCreatedEvent{}
		})
		profileUpdatedEvents := newKafkaEventCollector(msgConn.MsgTypeUserUpdated, func() *profilepb.UserUpdatedEvent {
			return &profilepb.UserUpdatedEvent{}
		})
		msgConn.AddListener(profileSubscriber, profileCreatedEvents)
		msgConn.AddListener(profileSubscriber, profileUpdatedEvents)
		startProfileSubscriber()

		dataTopic := env.WithDefaultString("DATA_REGISTRY_SERVICE_TOPIC", "data_registry")
		featureTopic := env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_TOPIC", "feature_materializer")
		materializationSubscriber, startMaterializationSubscriber, stopMaterializationSubscriber := newKafkaAssertsSubscriber(context.Background(), topicList(dataTopic+","+featureTopic))
		defer stopMaterializationSubscriber()
		datasetUpdatedEvents := newKafkaEventCollector(msgConn.MsgTypeDatasetUpdated, func() *dataregistrypb.DatasetUpdatedEvent {
			return &dataregistrypb.DatasetUpdatedEvent{}
		})
		embeddingSnapshotReadyEvents := newKafkaEventCollector(msgConn.MsgTypeEmbeddingSnapshotReady, func() *featurepb.EmbeddingSnapshotReadyEvent {
			return &featurepb.EmbeddingSnapshotReadyEvent{}
		})
		msgConn.AddListener(materializationSubscriber, datasetUpdatedEvents)
		msgConn.AddListener(materializationSubscriber, embeddingSnapshotReadyEvents)
		startMaterializationSubscriber()

		user := createVerifiedProfileAndLogin()
		profileCreatedEvents.waitFor(user.ID, 30*time.Second, nil)
		replaceHuggingFaceToken(user, token)
		profileUpdatedEvents.waitFor(user.ID, 30*time.Second, func(event *profilepb.UserUpdatedEvent) bool {
			return strings.TrimSpace(event.GetHuggingfaceTokenCiphertext()) != ""
		})

		datasetID := createTechniqueRAGPaperDataset(user)
		uploadTechniqueRAGPaper(user, datasetID)
		materialized := waitForTechniqueRAGPaperMaterialized(user, datasetID, datasetUpdatedEvents, embeddingSnapshotReadyEvents)

		status, body := doJSONWithTimeout(http.MethodPost, "/v1/private/models/onboard/huggingface", map[string]any{
			"dataset_id":      datasetID,
			"repo_id":         repoID,
			"revision":        revision,
			"hf_file":         hfFile,
			"client_nonce":    clientNonce,
			"model_name":      modelName,
			"model_version":   "1",
			"base_model":      baseModel,
			"artifact_type":   "BASE_MODEL",
			"artifact_format": artifactFormat,
		}, user.Token, uuid.New(), timeout)
		Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))

		completed := decodeObject(body)
		modelID := stringField(completed, "resource_id")
		Expect(completed).To(HaveKeyWithValue("status", "PROMOTED"))
		Expect(completed).To(HaveKeyWithValue("artifact_type", "BASE_MODEL"))
		Expect(completed).To(HaveKeyWithValue("artifact_format", artifactFormat))
		Expect(completed).To(HaveKeyWithValue("source", "HUGGING_FACE"))
		Expect(completed).To(HaveKeyWithValue("source_uri", "https://huggingface.co/"+repoID))
		Expect(completed).To(HaveKeyWithValue("dataset_id", datasetID))
		Expect(completed).To(HaveKeyWithValue("hf_repo_id", repoID))
		Expect(completed).To(HaveKeyWithValue("hf_revision", revision))
		Expect(completed).To(HaveKeyWithValue("model_name", modelName))
		Expect(completed).To(HaveKeyWithValue("base_model", baseModel))
		Expect(completed["actual_size_bytes"]).To(BeNumerically(">", 0))
		Expect(stringField(completed, "checksum")).To(MatchRegexp(`^sha256:[0-9a-f]{64}$`))
		Expect(stringField(completed, "storage_location")).To(MatchRegexp(`^s3://local-dev-bucket/models/huggingface/`))
		Expect(stringField(completed, "storage_location")).To(ContainSubstring(hfFile))
		Expect(stringField(completed, "manifest_location")).To(MatchRegexp(`^s3://local-dev-bucket/models/huggingface/.+/manifest\.json$`))

		commit := stringField(completed, "hf_commit_sha")
		Expect(commit).To(MatchRegexp(`^[0-9a-f]{40}$`), "expected a real Hugging Face commit sha, got %q", commit)

		loadedModel := waitForRealHuggingFaceModelLoaded(user, modelID, modelName, stringField(completed, "checksum"))
		servingModel := stringField(loadedModel, "serving_model")
		expectLocalOllamaModelAvailable(servingModel)
		expectLocalOllamaChatTemplate(servingModel)
		endpointID := waitForPublishedEndpoint(user.Token, modelName)

		var response map[string]any
		Eventually(func() bool {
			status, body := doJSONWithTimeout(http.MethodPost, "/v1/private/inference/endpoints/"+endpointID.String()+"/generations", map[string]any{
				"query_text": "Using the TechniqueRAG paper, what problem is addressed by reranking noisy candidates for adversarial technique annotation?",
				"top_k":      5,
			}, user.Token, uuid.New(), 90*time.Second)
			if status != http.StatusOK {
				return false
			}
			response = decodeObject(body)
			return strings.TrimSpace(stringField(response, "answer")) != "" &&
				hasNoChatSpecialTokenLeakageObject(response) &&
				hasTechniqueRAGPaperContextObject(response, materialized)
		}, 10*time.Minute, 5*time.Second).Should(BeTrue())

		Expect(response["dataset_id"]).To(Equal(datasetID))
		Expect(response["generation_protocol"]).To(Equal("OPENAI_CHAT_COMPLETIONS"))
		Expect(response["generation_model"]).To(Equal(servingModel))
	})
})

type techniqueRAGMaterialization struct {
	datasetID           string
	embeddingSnapshotID string
	embeddingCount      int64
}

func createTechniqueRAGPaperDataset(user profileTestUser) string {
	createPayload := map[string]any{
		"title":             "TechniqueRAG Paper Corpus",
		"description":       "Pinned arXiv TechniqueRAG paper uploaded through the gateway and materialized by the RAG feature pipeline",
		"category":          "cyber-threat-intelligence",
		"tableNamespace":    "features",
		"tableName":         "techniquerag_paper_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:8],
		"tableFormat":       "PARQUET",
		"catalogProvider":   "LOCAL",
		"processingProfile": "TEXT_RAG_PROCESSING_PROFILE",
	}

	created := createDataRegistryDataset(user, createPayload)
	return stringField(created, "id")
}

func uploadTechniqueRAGPaper(user profileTestUser, datasetID string) {
	pdf := readTechniqueRAGPaperFixture()
	Eventually(func() bool {
		status, _ := doMultipartFile(http.MethodPost, "/v1/private/data/store/"+datasetID, "file", "techniquerag-2505.11988v1.pdf", pdf, user.Token, uuid.New())
		return status == http.StatusCreated
	}, 30*time.Second, 1*time.Second).Should(BeTrue())
}

func readTechniqueRAGPaperFixture() []byte {
	pdf, err := os.ReadFile(techniqueRAGPaperPath)
	Expect(err).NotTo(HaveOccurred())
	Expect(pdf).NotTo(BeEmpty())
	actual := fmt.Sprintf("%x", sha256.Sum256(pdf))
	Expect(actual).To(Equal(techniqueRAGPaperSHA256), "TechniqueRAG fixture checksum changed")
	return pdf
}

func waitForTechniqueRAGPaperMaterialized(
	user profileTestUser,
	datasetID string,
	datasetUpdatedEvents *kafkaEventCollector[*dataregistrypb.DatasetUpdatedEvent],
	embeddingSnapshotReadyEvents *kafkaEventCollector[*featurepb.EmbeddingSnapshotReadyEvent],
) techniqueRAGMaterialization {
	datasetUUID, err := uuid.Parse(datasetID)
	Expect(err).NotTo(HaveOccurred())

	embeddingEvent := embeddingSnapshotReadyEvents.waitFor(datasetUUID, 5*time.Minute, func(event *featurepb.EmbeddingSnapshotReadyEvent) bool {
		return event.GetDatasetId() == datasetID &&
			strings.TrimSpace(event.GetEmbeddingSnapshotId()) != "" &&
			event.GetEmbeddingCount() > 0
	})
	datasetEvent := datasetUpdatedEvents.waitFor(datasetUUID, 5*time.Minute, func(event *dataregistrypb.DatasetUpdatedEvent) bool {
		return event.GetDatasetId() == datasetID &&
			event.GetProcessingState() == "EMBEDDINGS_MATERIALIZED" &&
			strings.TrimSpace(event.GetEmbeddingSnapshotId()) == embeddingEvent.GetEmbeddingSnapshotId()
	})

	Eventually(func() bool {
		status, body := doJSON(http.MethodGet, "/v1/private/data/registry/"+datasetID, nil, user.Token, uuid.Nil)
		if status != http.StatusOK {
			return false
		}

		read := decodeObject(body)
		metadata, ok := schemaMetadataObjectOK(read)
		return ok &&
			read["processingState"] == "EMBEDDINGS_MATERIALIZED" &&
			isFeatureParquetLocation(read["storageLocation"]) &&
			read["tableFormat"] == "PARQUET" &&
			read["catalogProvider"] == "LOCAL" &&
			read["processingProfile"] == "TEXT_RAG_PROCESSING_PROFILE" &&
			metadata["source_format"] == "pdf" &&
			numericEqual(metadata["source_page_count"], techniqueRAGPaperPageCount) &&
			metadata["extractor_name"] == "poppler-cpp-pdf-extractor" &&
			metadata["extractor_version"] == "v1" &&
			numericAtLeast(metadata["rows"], 1) &&
			dataMaterializationSchemaMetadataHasField(metadata, "source_text")
	}, 2*time.Minute, 1*time.Second).Should(BeTrue())

	return techniqueRAGMaterialization{
		datasetID:           datasetID,
		embeddingSnapshotID: datasetEvent.GetEmbeddingSnapshotId(),
		embeddingCount:      embeddingEvent.GetEmbeddingCount(),
	}
}

func expectTechniqueRAGPaperContextObject(response map[string]any, materialized techniqueRAGMaterialization) {
	Expect(hasTechniqueRAGPaperContextObject(response, materialized)).To(BeTrue())
}

func hasTechniqueRAGPaperContextObject(response map[string]any, materialized techniqueRAGMaterialization) bool {
	if response["dataset_id"] != materialized.datasetID {
		return false
	}
	rawContexts, ok := response["contexts"].([]any)
	if !ok || len(rawContexts) == 0 || int64(len(rawContexts)) > materialized.embeddingCount {
		return false
	}
	for _, raw := range rawContexts {
		retrieved, ok := raw.(map[string]any)
		if !ok || strings.TrimSpace(fmt.Sprint(retrieved["source_text"])) == "" {
			return false
		}
	}

	contextText := strings.ToLower(ragResponseContextTextObject(response))
	return strings.Contains(compactPaperText(contextText), "techniquerag") &&
		strings.Contains(contextText, "adversarial technique") &&
		strings.Contains(contextText, "re-ranker") &&
		strings.Contains(contextText, "candidate set") &&
		strings.Contains(contextText, "data scarcity")
}

func compactPaperText(value string) string {
	return strings.Join(strings.Fields(strings.ToLower(value)), "")
}

func expectNoChatSpecialTokenLeakageObject(response map[string]any) {
	Expect(hasNoChatSpecialTokenLeakageObject(response)).To(BeTrue())
}

func hasNoChatSpecialTokenLeakageObject(response map[string]any) bool {
	if hasChatSpecialTokenLeak(stringField(response, "answer")) {
		return false
	}
	rawContexts, ok := response["contexts"].([]any)
	if !ok {
		return false
	}
	for _, raw := range rawContexts {
		retrieved, ok := raw.(map[string]any)
		if !ok || hasChatSpecialTokenLeak(strings.TrimSpace(fmt.Sprint(retrieved["source_text"]))) {
			return false
		}
	}
	return true
}

func hasChatSpecialTokenLeak(value string) bool {
	return strings.Contains(value, "<|eot_id|>") ||
		strings.Contains(value, "<|im_end|>") ||
		strings.Contains(value, "<end_of_turn>") ||
		strings.Contains(value, "<|end|>")
}

func waitForRealHuggingFaceModelLoaded(user profileTestUser, modelID string, modelName string, checksum string) map[string]any {
	var loaded map[string]any
	checksumPart := checksumServingTagPart(checksum)
	Eventually(func() bool {
		status, body := doJSON(http.MethodGet, "/v1/private/models/"+modelID, nil, user.Token, uuid.Nil)
		if status != http.StatusOK {
			return false
		}
		read := decodeObject(body)
		servingModel := stringField(read, "serving_model")
		if read["id"] != modelID ||
			read["source"] != "HUGGING_FACE" ||
			read["model_kind"] != "BASE" ||
			read["status"] != "READY" ||
			read["serving_load_status"] != "LOADED" ||
			read["serving_protocol"] != "OPENAI_CHAT_COMPLETIONS" ||
			read["name"] != modelName ||
			!strings.Contains(servingModel, checksumPart) ||
			servingModel == stringField(read, "base_model") {
			return false
		}
		loaded = read
		return true
	}, 20*time.Minute, 5*time.Second).Should(BeTrue())
	return loaded
}

func checksumServingTagPart(checksum string) string {
	trimmed := strings.TrimPrefix(strings.TrimSpace(checksum), "sha256:")
	Expect(trimmed).To(MatchRegexp(`^[0-9a-f]{64}$`))
	return trimmed[:12]
}

func expectLocalOllamaChatTemplate(modelName string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	payload, err := json.Marshal(map[string]any{"model": modelName})
	Expect(err).NotTo(HaveOccurred())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost:11434/api/show", bytes.NewReader(payload))
	Expect(err).NotTo(HaveOccurred())
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	Expect(err).NotTo(HaveOccurred(), "local Ollama must be running for full-stack Hugging Face GGUF e2e")
	defer resp.Body.Close()
	Expect(resp.StatusCode).To(Equal(http.StatusOK))
	var body map[string]any
	Expect(json.NewDecoder(resp.Body).Decode(&body)).To(Succeed())
	template := strings.TrimSpace(fmt.Sprint(body["template"]))
	Expect(template).NotTo(BeEmpty(), "Ollama chat model %q must expose a chat template", modelName)
	Expect(template).To(Or(ContainSubstring(".Prompt"), ContainSubstring(".Messages")), "Ollama chat model %q must expose an Ollama-compatible template", modelName)
	Expect(template).NotTo(ContainSubstring("{%"), "Ollama chat model %q must not store the raw Hugging Face/Jinja chat template", modelName)
	Expect(fmt.Sprint(body["parameters"])).To(ContainSubstring("stop"), "Ollama chat model %q must expose stop parameters", modelName)
}

func envOrDefault(name string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	return value
}

func durationEnvOrDefault(name string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	seconds, err := time.ParseDuration(value + "s")
	Expect(err).NotTo(HaveOccurred(), "%s must be a whole number of seconds", name)
	return seconds
}

func sanitizeModelName(repoID string) string {
	replacer := strings.NewReplacer("/", "-", "_", "-", ".", "-")
	return strings.Trim(replacer.Replace(strings.ToLower(repoID)), "-")
}
