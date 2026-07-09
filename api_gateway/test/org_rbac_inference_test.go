package test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"lib/shared_lib/authz"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Organization RBAC inference facade", Ordered, func() {
	It("issues consumer tokens and denies non-consumer routes", func() {
		admin := createVerifiedProfileAndLogin()
		consumer := createVerifiedProfileAndLogin()

		addOrgMember(admin, admin.OrgID, consumer.ID, authz.RoleConsumer)
		consumer = loginExistingProfile(consumer)
		claims := decodeAccessTokenClaims(consumer.Token)
		Expect(claims["orgId"]).To(Equal(admin.OrgID.String()))
		Expect(claims["roles"]).To(Equal([]any{authz.RoleConsumer}))
		Expect(claims["permissions"]).To(ConsistOf(
			authz.PermissionInferenceEndpointsRead,
			authz.PermissionInferenceInvoke,
			authz.PermissionInferenceFeedback,
		))

		assertConsumerDeniedWriteRoutes(consumer)

		status, body := doJSONWithHeaders(http.MethodGet, "/v1/private/inference/endpoints", nil, consumer.Token, uuid.Nil, map[string]string{
			"X-Org-ID": uuid.NewString(),
		})
		Expect(status).To(Equal(http.StatusBadRequest), "body: %s", string(body))
		status, body = doJSONWithHeaders(http.MethodGet, "/v1/private/inference/endpoints", nil, consumer.Token, uuid.Nil, map[string]string{
			"X-User-ID": uuid.NewString(),
		})
		Expect(status).To(Equal(http.StatusBadRequest), "body: %s", string(body))
	})

	It("allows a consumer to invoke only published endpoints in their active org", func() {
		admin := createVerifiedProfileAndLogin()
		consumer := createVerifiedProfileAndLogin()
		addOrgMember(admin, admin.OrgID, consumer.ID, authz.RoleConsumer)
		consumer = loginExistingProfile(consumer)

		datasetID := createRAGInferenceDataset(admin)
		materializeRAGInferenceDataset(admin, datasetID)
		modelID := uploadBaseModelThroughIngestion(admin, datasetID)
		selectedModel := assertModelSelectable(admin, modelID, "UPLOAD", "rag-e2e-uploaded-base")
		endpointID := waitForPublishedEndpoint(consumer.Token, "rag-e2e-uploaded-base")

		status, body := doJSONWithTimeout(http.MethodPost, "/v1/private/inference/endpoints/"+endpointID.String()+"/generations", map[string]any{
			"query_text": "What phrase identifies the embedded knowledge base?",
			"top_k":      3,
		}, consumer.Token, uuid.New(), ragE2EGenerateCallTimeout)
		Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))
		generation := decodeObject(body)
		Expect(strings.TrimSpace(stringField(generation, "answer"))).NotTo(BeEmpty())
		Expect(generation["generation_model"]).To(Equal(stringField(selectedModel, "serving_model")))
		Expect(generation["generation_protocol"]).To(Equal(stringField(selectedModel, "serving_protocol")))

		otherAdmin := createVerifiedProfileAndLogin()
		otherDatasetID := createRAGInferenceDataset(otherAdmin)
		materializeRAGInferenceDataset(otherAdmin, otherDatasetID)
		otherModelID := uploadBaseModelThroughIngestion(otherAdmin, otherDatasetID)
		assertModelSelectable(otherAdmin, otherModelID, "UPLOAD", "rag-e2e-uploaded-base")
		otherEndpointID := waitForPublishedEndpoint(otherAdmin.Token, "rag-e2e-uploaded-base")

		status, body = doJSONWithTimeout(http.MethodPost, "/v1/private/inference/endpoints/"+otherEndpointID.String()+"/generations", map[string]any{
			"query_text": "What phrase identifies the embedded knowledge base?",
			"top_k":      3,
		}, consumer.Token, uuid.New(), ragE2EGenerateCallTimeout)
		Expect(status).To(Equal(http.StatusNotFound), "body: %s", string(body))
	})
})

func waitForPublishedEndpoint(token string, displayName string) uuid.UUID {
	var endpointID uuid.UUID
	Eventually(func(g Gomega) {
		status, body := doJSON(http.MethodGet, "/v1/private/inference/endpoints", nil, token, uuid.Nil)
		g.Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))
		var endpoints []map[string]any
		g.Expect(json.Unmarshal(body, &endpoints)).To(Succeed())
		for _, endpoint := range endpoints {
			if endpoint["display_name"] != displayName || endpoint["status"] != "ready" {
				continue
			}
			g.Expect(endpoint).To(SatisfyAll(
				HaveKey("endpoint_id"),
				Not(HaveKey("model_id")),
				Not(HaveKey("dataset_id")),
				Not(HaveKey("org_id")),
				Not(HaveKey("user_id")),
			))
			rawEndpointID, ok := endpoint["endpoint_id"].(string)
			g.Expect(ok).To(BeTrue())
			parsed, err := uuid.Parse(rawEndpointID)
			g.Expect(err).NotTo(HaveOccurred())
			endpointID = parsed
			return
		}
		g.Expect(endpoints).To(ContainElement(HaveKeyWithValue("display_name", displayName)))
	}, ragE2EGenerateWaitTimeout, 1*time.Second).Should(Succeed())
	return endpointID
}

func addOrgMember(admin profileTestUser, orgID uuid.UUID, memberID uuid.UUID, role string) {
	status, body := doJSON(http.MethodPost, "/v1/private/orgs/"+orgID.String()+"/members", map[string]any{
		"userId": memberID.String(),
		"role":   role,
		"status": "active",
	}, admin.Token, uuid.New())
	Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))
}

func loginExistingProfile(user profileTestUser) profileTestUser {
	status, body := doJSON(http.MethodPost, "/public/v1/profiles/password/verify", map[string]any{
		"email":    user.Email,
		"password": user.Password,
	}, "", uuid.Nil)
	Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))
	login := decodeObject(body)
	Expect(login["isValid"]).To(Equal(true))
	user.Token = stringField(login, "token")
	status, body = doJSON(http.MethodGet, "/v1/private/orgs/current", nil, user.Token, uuid.Nil)
	Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))
	currentOrg := decodeObject(body)
	orgID, err := uuid.Parse(stringField(currentOrg, "orgId"))
	Expect(err).NotTo(HaveOccurred())
	user.OrgID = orgID
	return user
}

func assertConsumerDeniedWriteRoutes(consumer profileTestUser) {
	status, body := doJSON(http.MethodPost, "/v1/private/data/registry", map[string]any{
		"title": "Denied",
	}, consumer.Token, uuid.New())
	Expect(status).To(Equal(http.StatusForbidden), "body: %s", string(body))

	status, body = doJSON(http.MethodPost, "/v1/private/models/uploads", map[string]any{
		"file_name": "denied.zip",
	}, consumer.Token, uuid.New())
	Expect(status).To(Equal(http.StatusForbidden), "body: %s", string(body))

	status, body = doJSON(http.MethodPost, "/v1/private/training-runs", map[string]any{
		"dataset_id":      uuid.NewString(),
		"source_model_id": uuid.NewString(),
	}, consumer.Token, uuid.New())
	Expect(status).To(Equal(http.StatusForbidden), "body: %s", string(body))
}

func decodeAccessTokenClaims(token string) map[string]any {
	parts := strings.Split(token, ".")
	Expect(parts).To(HaveLen(3))
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	Expect(err).NotTo(HaveOccurred())
	var claims map[string]any
	Expect(json.Unmarshal(payload, &claims)).To(Succeed())
	return claims
}

func doJSONWithHeaders(method, path string, payload any, bearerToken string, requestID uuid.UUID, headers map[string]string) (int, []byte) {
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
		req.Header.Set("Authorization", "Bearer "+strings.TrimPrefix(bearerToken, "Bearer "))
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	resp, err := apiClient.Do(req)
	Expect(err).NotTo(HaveOccurred())
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	Expect(err).NotTo(HaveOccurred())
	return resp.StatusCode, respBody
}
