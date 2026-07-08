package test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	inferencepb "lib/data_contracts_lib/inference"
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
)

var _ = Describe("Hugging Face real model onboarding", func() {
	It("validates the user's Hugging Face login, downloads, provisions, and invokes a real GGUF model when explicitly enabled", func() {
		if !strings.EqualFold(strings.TrimSpace(os.Getenv(realHuggingFaceE2EFlag)), "true") {
			Skip("set BIGHILL_E2E_HUGGINGFACE_REAL_DOWNLOAD=true to run the real Hugging Face download e2e")
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

		profileTopic := env.WithDefaultString("PROFILE_SERVICE_KAFKA_PUBLISHER_TOPIC", "profile")
		profileSubscriber, startProfileSubscriber, stopProfileSubscriber := newKafkaAssertsSubscriber(context.Background(), topicList(profileTopic))
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

		user := createVerifiedProfileAndLogin()
		profileCreatedEvents.waitFor(user.ID, 30*time.Second, nil)
		replaceHuggingFaceToken(user, token)
		profileUpdatedEvents.waitFor(user.ID, 30*time.Second, func(event *profilepb.UserUpdatedEvent) bool {
			return strings.TrimSpace(event.GetHuggingfaceTokenCiphertext()) != ""
		})

		status, body := doJSONWithTimeout(http.MethodPost, "/v1/private/models/onboard/huggingface", map[string]any{
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

		datasetID := createRAGInferenceDataset(user)
		uploadRAGInferenceDocument(user, datasetID)
		waitForRAGDatasetMaterialized(user, datasetID)

		loadedModel := waitForRealHuggingFaceModelLoaded(user, modelID, modelName, stringField(completed, "checksum"))
		servingModel := stringField(loadedModel, "serving_model")
		expectLocalOllamaModelAvailable(servingModel)
		expectLocalOllamaChatTemplate(servingModel)

		client, closeClient := newInferenceClient()
		defer closeClient()

		var response *inferencepb.GenerateResponse
		Eventually(func(g Gomega) {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()

			var err error
			response, err = client.Generate(ctx, &inferencepb.GenerateRequest{
				RequestId: uuid.NewString(),
				UserId:    user.ID.String(),
				OrgId:     user.OrgID.String(),
				DatasetId: datasetID,
				ModelId:   modelID,
				QueryText: "Which phrase proves the real Hugging Face GGUF model can serve RAG?",
				TopK:      3,
			})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(response.GetAnswer())).NotTo(BeEmpty())
			g.Expect(response.GetAnswer()).NotTo(MatchRegexp(`(<\|eot_id\|>|<\|im_end\|>|<end_of_turn>|<\|end\|>)`))
			expectRAGVerificationContext(g, response)
		}, 10*time.Minute, 5*time.Second).Should(Succeed())

		Expect(response.GetDatasetId()).To(Equal(datasetID))
		Expect(response.GetModelId()).To(Equal(modelID))
		Expect(response.GetGenerationProtocol()).To(Equal("OPENAI_CHAT_COMPLETIONS"))
		Expect(response.GetGenerationModel()).To(Equal(servingModel))
	})
})

func waitForRealHuggingFaceModelLoaded(user profileTestUser, modelID string, modelName string, checksum string) map[string]any {
	var loaded map[string]any
	checksumPart := checksumServingTagPart(checksum)
	Eventually(func(g Gomega) {
		status, body := doJSON(http.MethodGet, "/v1/private/models/"+modelID, nil, user.Token, uuid.Nil)
		g.Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))
		read := decodeObject(body)
		g.Expect(read).To(SatisfyAll(
			HaveKeyWithValue("id", modelID),
			HaveKeyWithValue("source", "HUGGING_FACE"),
			HaveKeyWithValue("model_kind", "BASE"),
			HaveKeyWithValue("status", "READY"),
			HaveKeyWithValue("serving_load_status", "LOADED"),
			HaveKeyWithValue("serving_protocol", "OPENAI_CHAT_COMPLETIONS"),
			HaveKeyWithValue("name", modelName),
		))
		servingModel := stringField(read, "serving_model")
		g.Expect(servingModel).To(ContainSubstring(checksumPart))
		g.Expect(servingModel).NotTo(Equal(stringField(read, "base_model")))
		loaded = read
	}, 20*time.Minute, 5*time.Second).Should(Succeed())
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

func doJSONWithTimeout(method, path string, payload any, bearerToken string, requestID uuid.UUID, timeout time.Duration) (int, []byte) {
	var body io.Reader
	if payload != nil {
		payloadBytes, err := json.Marshal(payload)
		Expect(err).NotTo(HaveOccurred())
		body = bytes.NewReader(payloadBytes)
	}

	req, err := http.NewRequest(method, gatewayBaseURL()+path, body)
	Expect(err).NotTo(HaveOccurred())
	req.Header.Set("Accept", "application/json")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if requestID != uuid.Nil {
		req.Header.Set("X-Request-ID", requestID.String())
	}
	if bearerToken != "" {
		if strings.HasPrefix(strings.ToLower(bearerToken), "bearer ") {
			req.Header.Set("Authorization", bearerToken)
		} else {
			req.Header.Set("Authorization", "Bearer "+bearerToken)
		}
	}

	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	Expect(err).NotTo(HaveOccurred())
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	Expect(err).NotTo(HaveOccurred())
	return resp.StatusCode, respBody
}
