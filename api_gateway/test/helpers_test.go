package test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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

	verifyPayload := map[string]any{"token": emailVerificationToken}
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

	return profileTestUser{
		ID:       userID,
		Email:    email,
		Password: password,
		Phone:    phone,
		Token:    token,
	}
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
	if err != nil {
		panic(err)
	}
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
