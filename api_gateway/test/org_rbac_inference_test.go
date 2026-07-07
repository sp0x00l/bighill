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
	It("allows a consumer to invoke only published endpoints in their active org", func() {
		admin := createVerifiedProfileAndLogin()
		consumer := createVerifiedProfileAndLogin()

		datasetID := createRAGInferenceDataset(admin)
		uploadRAGInferenceDocument(admin, datasetID)
		waitForRAGDatasetMaterialized(admin, datasetID)

		modelID := uuid.New()
		publishReadyModelForInference(modelID, admin.ID, admin.OrgID, uuid.MustParse(datasetID))
		endpointID := publishedEndpointID(admin.OrgID, modelID, uuid.MustParse(datasetID))

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

		Eventually(func(g Gomega) {
			status, body := doJSON(http.MethodGet, "/v1/private/inference/endpoints", nil, consumer.Token, uuid.Nil)
			g.Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))
			var endpoints []map[string]any
			g.Expect(json.Unmarshal(body, &endpoints)).To(Succeed())
			g.Expect(endpoints).To(ContainElement(SatisfyAll(
				HaveKeyWithValue("endpoint_id", endpointID.String()),
				HaveKeyWithValue("display_name", "rag-e2e-generator"),
				HaveKeyWithValue("status", "ready"),
				Not(HaveKey("model_id")),
				Not(HaveKey("dataset_id")),
				Not(HaveKey("org_id")),
				Not(HaveKey("user_id")),
			)))
		}, 45*time.Second, 1*time.Second).Should(Succeed())

		generationPayload := map[string]any{
			"query_text":       "What phrase identifies the embedded knowledge base?",
			"top_k":            3,
			"metadata_filters": map[string]string{},
			"model_id":         uuid.NewString(),
			"dataset_id":       uuid.NewString(),
			"org_id":           uuid.NewString(),
			"user_id":          uuid.NewString(),
		}
		status, body := doJSON(http.MethodPost, "/v1/private/inference/endpoints/"+endpointID.String()+"/generations", generationPayload, consumer.Token, uuid.New())
		Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))
		generated := decodeObject(body)
		Expect(generated["answer"]).To(ContainSubstring("Based on the retrieved context:"))
		Expect(generated["contexts"]).NotTo(BeEmpty())

		feedbackRequestID := uuid.MustParse(stringField(generated, "request_id"))
		status, body = doJSON(http.MethodPost, "/v1/private/inference/feedback", map[string]any{
			"request_id":       feedbackRequestID.String(),
			"accepted":         false,
			"rating":           -1,
			"preferred_answer": "RAG e2e verification phrase: the citadel index stores normalized feature context.",
		}, consumer.Token, uuid.New())
		Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))

		otherOrgOwner := createVerifiedProfileAndLogin()
		otherDatasetID := uuid.New()
		otherModelID := uuid.New()
		publishReadyModelForInference(otherModelID, otherOrgOwner.ID, otherOrgOwner.OrgID, otherDatasetID)
		otherEndpointID := publishedEndpointID(otherOrgOwner.OrgID, otherModelID, otherDatasetID)
		Eventually(func(g Gomega) {
			status, body := doJSON(http.MethodGet, "/v1/private/inference/endpoints", nil, otherOrgOwner.Token, uuid.Nil)
			g.Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))
			var endpoints []map[string]any
			g.Expect(json.Unmarshal(body, &endpoints)).To(Succeed())
			g.Expect(endpoints).To(ContainElement(HaveKeyWithValue("endpoint_id", otherEndpointID.String())))
		}, 45*time.Second, 1*time.Second).Should(Succeed())

		status, body = doJSON(http.MethodPost, "/v1/private/inference/endpoints/"+otherEndpointID.String()+"/generations", map[string]any{
			"query_text": "try another org",
		}, consumer.Token, uuid.New())
		Expect(status).To(Equal(http.StatusNotFound), "body: %s", string(body))

		status, body = doJSONWithHeaders(http.MethodGet, "/v1/private/inference/endpoints", nil, consumer.Token, uuid.Nil, map[string]string{
			"X-Org-ID": uuid.NewString(),
		})
		Expect(status).To(Equal(http.StatusBadRequest), "body: %s", string(body))
		status, body = doJSONWithHeaders(http.MethodGet, "/v1/private/inference/endpoints", nil, consumer.Token, uuid.Nil, map[string]string{
			"X-User-ID": uuid.NewString(),
		})
		Expect(status).To(Equal(http.StatusBadRequest), "body: %s", string(body))
	})
})

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

func publishedEndpointID(orgID uuid.UUID, modelID uuid.UUID, datasetID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("published-inference-endpoint:"+orgID.String()+":"+modelID.String()+":"+datasetID.String()))
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
