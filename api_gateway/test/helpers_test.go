package test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	dataregistrypb "lib/data_contracts_lib/data_registry"
	ingestionpb "lib/data_contracts_lib/ingestion"
	profilepb "lib/data_contracts_lib/profile"
	env "lib/shared_lib/env"
	msgConn "lib/shared_lib/messaging"
	"mime/multipart"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/gomega"
)

const (
	defaultGatewayURL      = "http://127.0.0.1:3000"
	emailVerificationToken = "staging-test-email-verify-token"
)

var (
	apiClient   = &http.Client{Timeout: 30 * time.Second}
	gbPhoneSeq  int64
	runSaltOnce sync.Once
	runSalt     int64
)

type profileTestUser struct {
	ID       uuid.UUID
	OrgID    uuid.UUID
	Email    string
	Password string
	Phone    string
	Token    string
}

func gatewayBaseURL() string {
	baseURL := strings.TrimRight(os.Getenv("API_GATEWAY_URL"), "/")
	if baseURL == "" {
		return defaultGatewayURL
	}
	return baseURL
}

func doJSON(method, path string, payload any, bearerToken string, requestID uuid.UUID) (int, []byte) {
	return doJSONWithTimeout(method, path, payload, bearerToken, requestID, apiClient.Timeout)
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

func doMultipartFile(method, path string, fieldName string, filename string, content []byte, bearerToken string, requestID uuid.UUID) (int, []byte) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile(fieldName, filename)
	Expect(err).NotTo(HaveOccurred())
	_, err = part.Write(content)
	Expect(err).NotTo(HaveOccurred())
	Expect(writer.Close()).To(Succeed())

	req, err := http.NewRequest(method, gatewayBaseURL()+path, bytes.NewReader(body.Bytes()))
	Expect(err).NotTo(HaveOccurred())
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", writer.FormDataContentType())
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

	resp, err := apiClient.Do(req)
	Expect(err).NotTo(HaveOccurred())
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	Expect(err).NotTo(HaveOccurred())
	return resp.StatusCode, respBody
}

func decodeObject(body []byte) map[string]any {
	var decoded map[string]any
	err := json.Unmarshal(body, &decoded)
	Expect(err).NotTo(HaveOccurred(), "body: %s", string(body))
	return decoded
}

func stringField(body map[string]any, key string) string {
	value, ok := body[key].(string)
	Expect(ok).To(BeTrue(), "expected %q string in %#v", key, body)
	return value
}

func createVerifiedProfileAndLogin() profileTestUser {
	tenantEvents, stopTenantEvents := newTenantUserCreatedEventCollector()
	defer stopTenantEvents()

	password := "SecurePass123!"
	email := fmt.Sprintf("gateway-%s@test.com", strings.ReplaceAll(uuid.NewString(), "-", "")[:12])
	phone := uniqueGBPhone()

	createPayload := map[string]any{
		"email":       email,
		"phoneNumber": phone,
		"countryCode": "GB",
		"password":    password,
	}
	status, body := doJSON(http.MethodPost, "/public/v1/profiles", createPayload, "", uuid.New())
	Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))

	created := decodeObject(body)
	userID, err := uuid.Parse(stringField(created, "id"))
	Expect(err).NotTo(HaveOccurred())
	tenantEvents.waitFor(userID, 30*time.Second, func(event *profilepb.UserCreatedEvent) bool {
		return event.GetUserId() == userID.String()
	})

	verifyPayload := map[string]any{"token": testEmailVerificationToken(email)}
	status, body = doJSON(http.MethodPost, "/public/v1/profiles/email/verify", verifyPayload, "", uuid.Nil)
	Expect(status).To(Equal(http.StatusNoContent), "body: %s", string(body))

	loginPayload := map[string]any{
		"email":    email,
		"password": password,
	}
	status, body = doJSON(http.MethodPost, "/public/v1/profiles/password/verify", loginPayload, "", uuid.Nil)
	Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))

	login := decodeObject(body)
	Expect(login["isValid"]).To(Equal(true))
	token := stringField(login, "token")
	Expect(token).NotTo(BeEmpty())

	status, body = doJSON(http.MethodGet, "/v1/private/orgs/current", nil, token, uuid.Nil)
	Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))
	currentOrg := decodeObject(body)
	orgID, err := uuid.Parse(stringField(currentOrg, "orgId"))
	Expect(err).NotTo(HaveOccurred())

	user := profileTestUser{
		ID:       userID,
		OrgID:    orgID,
		Email:    email,
		Password: password,
		Phone:    phone,
		Token:    token,
	}
	return user
}

func newTenantUserCreatedEventCollector() (*kafkaEventCollector[*profilepb.UserCreatedEvent], context.CancelFunc) {
	tenantTopic := env.WithDefaultString("TENANT_SERVICE_KAFKA_PUBLISHER_TOPIC", "tenant")
	subscriber, start, cancel := newKafkaAssertsSubscriber(context.Background(), topicList(tenantTopic))
	collector := newKafkaEventCollector(msgConn.MsgTypeUserCreated, func() *profilepb.UserCreatedEvent {
		return &profilepb.UserCreatedEvent{}
	})
	msgConn.AddListener(subscriber, collector)
	start()
	return collector, cancel
}

func newModelArtifactIngestedEventCollector() (*kafkaEventCollector[*ingestionpb.ModelArtifactIngestedEvent], context.CancelFunc) {
	ingestionTopic := env.WithDefaultString("INGESTION_SERVICE_TOPIC", "ingestion")
	subscriber, start, cancel := newKafkaAssertsSubscriber(context.Background(), topicList(ingestionTopic))
	collector := newKafkaEventCollector(msgConn.MsgTypeModelArtifactIngested, func() *ingestionpb.ModelArtifactIngestedEvent {
		return &ingestionpb.ModelArtifactIngestedEvent{}
	})
	msgConn.AddListener(subscriber, collector)
	start()
	return collector, cancel
}

func newDatasetCreatedEventCollector() (*kafkaEventCollector[*dataregistrypb.DatasetCreatedEvent], context.CancelFunc) {
	dataRegistryTopic := env.WithDefaultString("DATA_REGISTRY_SERVICE_TOPIC", "data_registry")
	subscriber, start, cancel := newKafkaAssertsSubscriber(context.Background(), topicList(dataRegistryTopic))
	collector := newKafkaEventCollector(msgConn.MsgTypeDatasetCreated, func() *dataregistrypb.DatasetCreatedEvent {
		return &dataregistrypb.DatasetCreatedEvent{}
	})
	msgConn.AddListener(subscriber, collector)
	start()
	return collector, cancel
}

func createDataRegistryDataset(user profileTestUser, payload map[string]any) map[string]any {
	datasetEvents, stopDatasetEvents := newDatasetCreatedEventCollector()
	defer stopDatasetEvents()

	var created map[string]any
	Eventually(func(g Gomega) {
		status, body := doJSON(http.MethodPost, "/v1/private/data/registry", payload, user.Token, uuid.New())
		g.Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
		created = decodeObject(body)
	}, 30*time.Second, 1*time.Second).Should(Succeed())
	datasetID, err := uuid.Parse(stringField(created, "id"))
	Expect(err).NotTo(HaveOccurred())
	datasetEvents.waitFor(datasetID, 30*time.Second, func(event *dataregistrypb.DatasetCreatedEvent) bool {
		return event.GetDatasetId() == datasetID.String() &&
			event.GetUserId() == user.ID.String() &&
			event.GetOrgId() == user.OrgID.String()
	})
	return created
}

func createDataRegistryConnector(user profileTestUser, connectorType string, payload map[string]any) map[string]any {
	var created map[string]any
	Eventually(func(g Gomega) {
		status, body := doJSON(http.MethodPost, "/v1/private/data/registry/connector/"+connectorType, payload, user.Token, uuid.New())
		g.Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
		created = decodeObject(body)
	}, 30*time.Second, 1*time.Second).Should(Succeed())
	return created
}

func testEmailVerificationToken(email string) string {
	emailHash := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(email))))
	return emailVerificationToken + "-" + hex.EncodeToString(emailHash[:8])
}

func initRunSalt() {
	runSaltOnce.Do(func() {
		n := uint64(time.Now().UTC().UnixNano())
		p := uint64(os.Getpid())
		runSalt = int64((n ^ (p * 0x9e3779b97f4a7c15)) % 1000000)
	})
}

func uniqueGBPhone() string {
	phone, err := uniqueGBPhoneFromStub("+447078")
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return phone
}

func uniqueGBPhoneFromStub(stub string) (string, error) {
	after := strings.TrimPrefix(stub, "+44")
	suffixLen := 10 - len(after)
	if suffixLen <= 0 {
		after = after[:9]
		suffixLen = 1
	}

	initRunSalt()

	epochMinutes := int64(time.Now().UTC().Unix() / 60)
	seq := atomic.AddInt64(&gbPhoneSeq, 1) - 1

	mod := int64(1)
	for i := 0; i < suffixLen; i++ {
		mod *= 10
	}

	const a int64 = 941083
	mixed := ((epochMinutes%mod)*a + (runSalt % mod) + (seq % mod)) % mod
	suffix := fmt.Sprintf("%0*d", suffixLen, mixed)
	return "+44" + after + suffix, nil
}
