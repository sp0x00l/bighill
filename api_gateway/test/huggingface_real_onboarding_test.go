package test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
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
	realHuggingFaceE2EFlag        = "BIGHILL_E2E_HUGGINGFACE_REAL_DOWNLOAD"
	realHuggingFaceTokenEnv       = "BIGHILL_E2E_HUGGINGFACE_TOKEN"
	realHuggingFaceRepoIDEnv      = "BIGHILL_E2E_HUGGINGFACE_REPO_ID"
	realHuggingFaceRevisionEnv    = "BIGHILL_E2E_HUGGINGFACE_REVISION"
	realHuggingFaceBaseModelEnv   = "BIGHILL_E2E_HUGGINGFACE_BASE_MODEL"
	realHuggingFaceTimeoutEnv     = "BIGHILL_E2E_HUGGINGFACE_TIMEOUT_SECONDS"
	defaultRealHuggingFaceRepoID  = "meta-llama/Meta-Llama-3-8B"
	defaultRealHuggingFaceTimeout = 90 * time.Minute
)

var _ = Describe("Hugging Face real model onboarding", func() {
	It("validates the user's Hugging Face login and downloads a real model snapshot when explicitly enabled", func() {
		if !strings.EqualFold(strings.TrimSpace(os.Getenv(realHuggingFaceE2EFlag)), "true") {
			Skip("set BIGHILL_E2E_HUGGINGFACE_REAL_DOWNLOAD=true to run the real Hugging Face download e2e")
		}
		token := strings.TrimSpace(os.Getenv(realHuggingFaceTokenEnv))
		Expect(token).NotTo(BeEmpty(), "set BIGHILL_E2E_HUGGINGFACE_TOKEN to run the real Hugging Face download e2e")

		repoID := envOrDefault(realHuggingFaceRepoIDEnv, defaultRealHuggingFaceRepoID)
		revision := envOrDefault(realHuggingFaceRevisionEnv, "main")
		baseModel := envOrDefault(realHuggingFaceBaseModelEnv, repoID)
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
			"repo_id":       repoID,
			"revision":      revision,
			"client_nonce":  clientNonce,
			"model_name":    modelName,
			"model_version": "1",
			"base_model":    baseModel,
		}, user.Token, uuid.New(), timeout)
		Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))

		completed := decodeObject(body)
		Expect(completed).To(HaveKeyWithValue("status", "PROMOTED"))
		Expect(completed).To(HaveKeyWithValue("artifact_type", "BASE_MODEL"))
		Expect(completed).To(HaveKeyWithValue("artifact_format", "HF_MODEL"))
		Expect(completed).To(HaveKeyWithValue("source", "HUGGING_FACE"))
		Expect(completed).To(HaveKeyWithValue("source_uri", "https://huggingface.co/"+repoID))
		Expect(completed).To(HaveKeyWithValue("hf_repo_id", repoID))
		Expect(completed).To(HaveKeyWithValue("hf_revision", revision))
		Expect(completed).To(HaveKeyWithValue("model_name", modelName))
		Expect(completed).To(HaveKeyWithValue("base_model", baseModel))
		Expect(completed["actual_size_bytes"]).To(BeNumerically(">", 0))
		Expect(stringField(completed, "checksum")).To(MatchRegexp(`^sha256:[0-9a-f]{64}$`))
		Expect(stringField(completed, "storage_location")).To(MatchRegexp(`^s3://local-dev-bucket/models/huggingface/`))
		Expect(stringField(completed, "manifest_location")).To(MatchRegexp(`^s3://local-dev-bucket/models/huggingface/.+/manifest\.json$`))

		commit := stringField(completed, "hf_commit_sha")
		Expect(commit).To(MatchRegexp(`^[0-9a-f]{40}$`), "expected a real Hugging Face commit sha, got %q; local fixtures use local-* and the API stub uses api-e2e-*", commit)
	})
})

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
