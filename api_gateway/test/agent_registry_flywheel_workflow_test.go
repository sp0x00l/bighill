package test

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Agent registry flywheel control plane", Label("agent", "flywheel"), func() {
	It("projects champion spec decisions into inference and ignores stale champion events", func() {
		user := createVerifiedProfileAndLogin()
		datasetID := createRAGInferenceDataset(user)
		materializeRAGInferenceDataset(user, datasetID)
		modelID := uploadBaseModelThroughIngestion(user, datasetID)
		selectedModel := assertModelSelectable(user, modelID, "UPLOAD", "rag-e2e-uploaded-base")
		if stringField(selectedModel, "serving_protocol") != "OPENAI_CHAT_COMPLETIONS" {
			Skip("agent spec publication requires a model with tool-call support")
		}

		lineage := "agent-registry-champion-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:8]
		specAHash, supported := tryPublishAgentSpecWithPayload(user, agentSpecPayloadForTool(
			modelID,
			lineage,
			"search_knowledge",
			"Use search_knowledge first. This is the first champion candidate.",
		))
		if !supported {
			Skip("agent spec publication was capability-gated for this model")
		}
		specBHash, supported := tryPublishAgentSpecWithPayload(user, agentSpecPayloadForTool(
			modelID,
			lineage,
			"search_knowledge",
			"Use search_knowledge first. This is the promoted champion candidate.",
		))
		if !supported {
			Skip("agent spec publication was capability-gated for this model")
		}
		Expect(specBHash).NotTo(Equal(specAHash))

		endpointID := publishAgentEndpointWithDisplayName(user, modelID, datasetID, specAHash, "agent-registry-e2e-"+uuid.NewString()[:8])
		registerAgentSpecVersion(user, lineage, specAHash)
		registerAgentSpecVersion(user, lineage, specBHash)
		registerAgentEndpointBinding(user, lineage, endpointID)

		decisionB := uuid.New()
		decidedAtB := time.Now().UTC()
		stateB := promoteAgentSpecChampion(user, lineage, specBHash, decisionB, decidedAtB)
		Expect(stateB["champion_agent_spec_hash"]).To(Equal(specBHash))
		Expect(stateB["decision_id"]).To(Equal(decisionB.String()))

		Eventually(func() string {
			endpoint := readInferenceEndpoint(user, endpointID)
			return fmt.Sprint(endpoint["agent_spec_hash"])
		}, 30*time.Second, time.Second).Should(Equal(specBHash))

		olderDecision := uuid.New()
		stateA := promoteAgentSpecChampion(user, lineage, specAHash, olderDecision, decidedAtB.Add(-time.Hour))
		Expect(stateA["champion_agent_spec_hash"]).To(Equal(specAHash))
		Expect(stateA["decision_id"]).To(Equal(olderDecision.String()))

		Consistently(func() string {
			endpoint := readInferenceEndpoint(user, endpointID)
			return fmt.Sprint(endpoint["agent_spec_hash"])
		}, 5*time.Second, time.Second).Should(Equal(specBHash))
	})
})

func registerAgentSpecVersion(user profileTestUser, lineage string, specHash string) map[string]any {
	status, body := doJSON(http.MethodPost, "/v1/private/agent-registry/spec-versions", map[string]any{
		"agent_lineage":   lineage,
		"agent_spec_hash": specHash,
	}, user.Token, uuid.New())
	Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
	version := decodeSingleObject(body)
	Expect(version["agent_lineage"]).To(Equal(lineage))
	Expect(version["agent_spec_hash"]).To(Equal(specHash))
	return version
}

func registerAgentEndpointBinding(user profileTestUser, lineage string, endpointID uuid.UUID) map[string]any {
	status, body := doJSON(http.MethodPost, "/v1/private/agent-registry/endpoint-bindings", map[string]any{
		"agent_lineage": lineage,
		"endpoint_id":   endpointID.String(),
	}, user.Token, uuid.New())
	Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
	binding := decodeSingleObject(body)
	Expect(binding["agent_lineage"]).To(Equal(lineage))
	Expect(binding["endpoint_id"]).To(Equal(endpointID.String()))
	return binding
}

func promoteAgentSpecChampion(user profileTestUser, lineage string, specHash string, decisionID uuid.UUID, decidedAt time.Time) map[string]any {
	status, body := doJSON(http.MethodPost, "/v1/private/agent-registry/champions", map[string]any{
		"agent_lineage":   lineage,
		"agent_spec_hash": specHash,
		"decision_id":     decisionID.String(),
		"decided_at":      decidedAt.UTC().Format(time.RFC3339Nano),
	}, user.Token, uuid.New())
	Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))
	return decodeSingleObject(body)
}

func readInferenceEndpoint(user profileTestUser, endpointID uuid.UUID) map[string]any {
	status, body := doJSON(http.MethodGet, "/v1/private/inference/endpoints/"+endpointID.String(), nil, user.Token, uuid.Nil)
	Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))
	endpoint := decodeSingleObject(body)
	Expect(endpoint["endpoint_id"]).To(Equal(endpointID.String()))
	return endpoint
}
