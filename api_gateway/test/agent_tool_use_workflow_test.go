package test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	toolspb "lib/data_contracts_lib/tools"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcstatus "google.golang.org/grpc/status"
)

const (
	defaultSocketURL                = "ws://127.0.0.1:8089/v1/socket"
	defaultToolServiceGRPCAddress   = "127.0.0.1:7084"
	toolServiceSecurityAllowedOrgID = "11111111-1111-1111-1111-111111111111"
	agentE2ESocketTimeout           = 20 * time.Second
	toolServiceE2ERequestTimeout    = 5 * time.Second
)

var _ = Describe("Agent tool-use workflow", Label("agent"), func() {
	It("fails closed without tool-call support or records a forced search_knowledge trajectory", func() {
		user := createVerifiedProfileAndLogin()
		datasetID := createRAGInferenceDataset(user)
		materializeRAGInferenceDataset(user, datasetID)
		modelID := uploadBaseModelThroughIngestion(user, datasetID)
		selectedModel := assertModelSelectable(user, modelID, "UPLOAD", "rag-e2e-uploaded-base")
		oneStepSpec := agentSpecPayload(modelID, "agent-tool-use-one-step-rejected")
		oneStepSpec["budgets"].(map[string]any)["max_steps"] = 1
		assertAgentSpecPublishBadRequest(user, oneStepSpec, "max_steps")
		unknownToolSpec := agentSpecPayload(modelID, "agent-tool-use-unknown-tool-rejected")
		unknownToolSpec["tools"].([]map[string]any)[0]["name"] = "write_database"
		assertAgentSpecPublishBadRequest(user, unknownToolSpec, "tool")
		if stringField(selectedModel, "serving_protocol") != "OPENAI_CHAT_COMPLETIONS" {
			assertAgentSpecPublishRejected(user, modelID, "agent-tool-use-e2e-unsupported")
			return
		}

		agentSpecHash, supported := tryPublishAgentSpec(user, modelID, "agent-tool-use-e2e")
		if !supported {
			return
		}
		endpointID := publishAgentEndpoint(user, modelID, datasetID, agentSpecHash)

		status, body := doJSONWithTimeout(http.MethodPost, "/v1/private/inference/endpoints/"+endpointID.String()+"/generations", map[string]any{
			"query_text": "Use the knowledge search tool first. What phrase identifies the embedded knowledge base?",
			"top_k":      3,
		}, user.Token, uuid.New(), ragE2EGenerateCallTimeout)
		Expect(status).To(Equal(http.StatusAccepted), "body: %s", string(body))
		generation := decodeObject(body)
		runID := stringField(generation, "agent_run_id")
		Expect(strings.TrimSpace(runID)).NotTo(BeEmpty())
		Expect(generation["status"]).To(Equal("RUNNING"))
		Expect(generation["agent_run_href"]).To(Equal("/v1/inference/agent-runs/" + runID))

		trajectory := waitForAgentRunTerminal(user, runID)
		run := trajectoryObject(trajectory, "run")
		Expect(run["agent_spec_hash"]).To(Equal(agentSpecHash))
		Expect(run["status"]).To(Equal("COMPLETED"))
		Expect(run["decoding_params"]).To(HaveKey("seed"))
		Expect(run["decoding_params"]).To(HaveKeyWithValue("temperature", BeNumerically("==", 0)))

		steps := trajectoryArray(trajectory, "steps")
		Expect(steps).NotTo(BeEmpty())
		Expect(steps[0].(map[string]any)["presented_tool_schemas"]).NotTo(BeEmpty())
		invocations := trajectoryArray(trajectory, "tool_invocations")
		Expect(invocations).NotTo(BeEmpty())
		Expect(invocations[0].(map[string]any)["tool_name"]).To(Equal("search_knowledge"))

		eventTypes := replayAgentRunSocketEvents(user, runID)
		Expect(eventTypes).To(ContainElement("agent.step.completed"))
		Expect(eventTypes).To(ContainElement("agent.tool.result"))
		Expect(eventTypes).To(ContainElement("agent.run.completed"))

		otherUser := createVerifiedProfileAndLogin()
		otherEventTypes := replayAgentRunSocketEvents(otherUser, runID)
		Expect(otherEventTypes).NotTo(ContainElement("agent.step.completed"))
		Expect(otherEventTypes).NotTo(ContainElement("agent.run.completed"))
	})

	It("records a graph_search trajectory after graph materialization is projected", Label("graph"), func() {
		user := createVerifiedProfileAndLogin()
		datasetID := createGraphRAGInferenceDataset(user)
		graphSnapshotID := materializeGraphRAGInferenceDataset(user, datasetID)
		Expect(strings.TrimSpace(graphSnapshotID)).NotTo(BeEmpty())
		modelID := uploadBaseModelThroughIngestion(user, datasetID)
		selectedModel := assertModelSelectable(user, modelID, "UPLOAD", "rag-e2e-uploaded-base")
		if stringField(selectedModel, "serving_protocol") != "OPENAI_CHAT_COMPLETIONS" {
			assertAgentSpecPublishRejectedWithPayload(user, agentGraphSpecPayload(modelID, "agent-graph-rag-e2e-unsupported"))
			return
		}

		agentSpecHash, supported := tryPublishAgentSpecWithPayload(user, agentGraphSpecPayload(modelID, "agent-graph-rag-e2e"))
		if !supported {
			return
		}
		endpointID := publishAgentEndpoint(user, modelID, datasetID, agentSpecHash)

		status, body := doJSONWithTimeout(http.MethodPost, "/v1/private/inference/endpoints/"+endpointID.String()+"/generations", map[string]any{
			"query_text": "Call graph_search first with query_text Aurora Relay, top_k 3, and max_hops 2. Then answer from the returned context.",
			"top_k":      3,
		}, user.Token, uuid.New(), ragE2EGenerateCallTimeout)
		Expect(status).To(Equal(http.StatusAccepted), "body: %s", string(body))
		generation := decodeObject(body)
		runID := stringField(generation, "agent_run_id")
		Expect(strings.TrimSpace(runID)).NotTo(BeEmpty())
		Expect(generation["status"]).To(Equal("RUNNING"))
		Expect(generation["agent_run_href"]).To(Equal("/v1/inference/agent-runs/" + runID))

		trajectory := waitForAgentRunTerminal(user, runID)
		run := trajectoryObject(trajectory, "run")
		Expect(run["agent_spec_hash"]).To(Equal(agentSpecHash))
		Expect(run["status"]).To(Equal("COMPLETED"))

		invocations := trajectoryArray(trajectory, "tool_invocations")
		Expect(invocations).NotTo(BeEmpty())
		firstInvocation := invocations[0].(map[string]any)
		Expect(firstInvocation["tool_name"]).To(Equal("graph_search"))
		Expect(fmt.Sprint(firstInvocation["result"])).To(ContainSubstring("Graph e2e verification phrase"))

		eventTypes := replayAgentRunSocketEvents(user, runID)
		Expect(eventTypes).To(ContainElement("agent.tool.result"))
		Expect(eventTypes).To(ContainElement("agent.run.completed"))
	})

	It("denies unallowlisted tenants at the tool service boundary", func() {
		err := invokeHTTPGetTool(uuid.NewString(), "http://example.com")

		expectToolServiceError(err, codes.PermissionDenied, "allowlisted")
	})

	It("blocks SSRF targets at the tool service boundary", func() {
		err := invokeHTTPGetTool(toolServiceSecurityAllowedOrgID, "http://127.0.0.1:11434/api/tags")

		expectToolServiceError(err, codes.PermissionDenied, "blocked")
	})

	It("rejects malformed tool arguments at the tool service boundary", func() {
		err := invokeToolRaw(toolServiceSecurityAllowedOrgID, "http_get", []byte(`{}`))

		expectToolServiceError(err, codes.InvalidArgument, "validation")
	})
})

func tryPublishAgentSpec(user profileTestUser, modelID uuid.UUID, lineage string) (string, bool) {
	return tryPublishAgentSpecWithPayload(user, agentSpecPayload(modelID, lineage))
}

func tryPublishAgentSpecWithPayload(user profileTestUser, payload map[string]any) (string, bool) {
	status, body := doJSON(http.MethodPost, "/v1/private/inference/agent-specs", payload, user.Token, uuid.New())
	if status == http.StatusConflict {
		Expect(agentSpecPublishRejectedReason(body)).To(BeTrue(), "body: %s", string(body))
		return "", false
	}
	Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
	spec := decodeSingleObject(body)
	hash := stringField(spec, "content_hash")
	Expect(hash).To(MatchRegexp(`^[0-9a-f]{64}$`))
	return hash, true
}

func assertAgentSpecPublishRejected(user profileTestUser, modelID uuid.UUID, lineage string) {
	assertAgentSpecPublishRejectedWithPayload(user, agentSpecPayload(modelID, lineage))
}

func assertAgentSpecPublishRejectedWithPayload(user profileTestUser, payload map[string]any) {
	status, body := doJSON(http.MethodPost, "/v1/private/inference/agent-specs", payload, user.Token, uuid.New())
	Expect(status).To(Equal(http.StatusConflict), "body: %s", string(body))
	Expect(agentSpecPublishRejectedReason(body)).To(BeTrue(), "body: %s", string(body))
}

func assertAgentSpecPublishBadRequest(user profileTestUser, payload map[string]any, reason string) {
	status, body := doJSON(http.MethodPost, "/v1/private/inference/agent-specs", payload, user.Token, uuid.New())
	Expect(status).To(Equal(http.StatusBadRequest), "body: %s", string(body))
	lower := strings.ToLower(string(body))
	if strings.TrimSpace(reason) != "" && strings.Contains(lower, strings.ToLower(reason)) {
		return
	}
	Expect(lower).To(ContainSubstring("invalid agent spec"), "body: %s", string(body))
}

func agentSpecPublishRejectedReason(body []byte) bool {
	lower := strings.ToLower(string(body))
	return strings.Contains(lower, "tool") || strings.Contains(lower, "capability")
}

func agentSpecPayload(modelID uuid.UUID, lineage string) map[string]any {
	return agentSpecPayloadForTool(modelID, lineage, "search_knowledge", "Use tools when they are available. Do not answer until the required tool result has been observed.")
}

func agentGraphSpecPayload(modelID uuid.UUID, lineage string) map[string]any {
	return agentSpecPayloadForTool(modelID, lineage, "graph_search", "You must call graph_search first. Use query_text Aurora Relay, top_k 3, and max_hops 2. Do not answer until the graph_search result has been observed.")
}

func agentSpecPayloadForTool(modelID uuid.UUID, lineage string, toolName string, systemPrompt string) map[string]any {
	return map[string]any{
		"schema_version": "agent_spec_v1",
		"agent_lineage":  lineage,
		"system_prompt":  systemPrompt,
		"model_binding": map[string]any{
			"model_id": modelID.String(),
		},
		"tools": []map[string]any{
			{
				"name":        toolName,
				"required":    true,
				"tool_choice": "required",
				"config":      map[string]any{},
			},
		},
		"retrieval_config": map[string]any{},
		"budgets": map[string]any{
			"max_steps": 3,
			"token":     128,
			"wall_ms":   60000,
		},
		"stop_conditions": map[string]any{},
		"guardrails":      map[string]any{},
	}
}

func publishAgentEndpoint(user profileTestUser, modelID uuid.UUID, datasetID string, agentSpecHash string) uuid.UUID {
	status, body := doJSON(http.MethodPost, "/v1/private/inference/endpoints", map[string]any{
		"model_id":        modelID.String(),
		"dataset_ids":     []string{datasetID},
		"mode":            "agent",
		"agent_spec_hash": agentSpecHash,
		"display_name":    "agent-tool-use-e2e-" + uuid.NewString()[:8],
		"merge_strategy":  "reranker",
	}, user.Token, uuid.New())
	Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
	endpoint := decodeSingleObject(body)
	Expect(endpoint["mode"]).To(Equal("agent"))
	Expect(endpoint["agent_spec_hash"]).To(Equal(agentSpecHash))
	endpointID, err := uuid.Parse(stringField(endpoint, "endpoint_id"))
	Expect(err).NotTo(HaveOccurred())
	return endpointID
}

func readAgentRun(user profileTestUser, runID string) map[string]any {
	status, body := doJSON(http.MethodGet, "/v1/private/inference/agent-runs/"+runID, nil, user.Token, uuid.Nil)
	Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))
	return decodeSingleObject(body)
}

func waitForAgentRunTerminal(user profileTestUser, runID string) map[string]any {
	var trajectory map[string]any
	Eventually(func() string {
		trajectory = readAgentRun(user, runID)
		run := trajectoryObject(trajectory, "run")
		return fmt.Sprint(run["status"])
	}, 2*time.Minute, 2*time.Second).Should(SatisfyAny(Equal("COMPLETED"), Equal("FAILED")))
	return trajectory
}

func replayAgentRunSocketEvents(user profileTestUser, runID string) []string {
	status, body := doJSON(http.MethodPost, "/v1/private/socket-token", nil, user.Token, uuid.New())
	Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
	ticket := decodeObject(body)
	socketToken := stringField(ticket, "token")
	socketURL := socketTestURL(socketToken)

	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.Dial(socketURL, http.Header{"Origin": []string{"http://localhost:3000"}})
	Expect(err).NotTo(HaveOccurred())
	defer conn.Close()
	Expect(conn.SetReadDeadline(time.Now().Add(agentE2ESocketTimeout))).To(Succeed())

	_, readyPayload, err := conn.ReadMessage()
	Expect(err).NotTo(HaveOccurred())
	Expect(socketMessageType(readyPayload)).To(Equal("ready"))
	Expect(conn.WriteJSON(map[string]any{
		"type": "hello",
		"filters": []map[string]any{
			{
				"resource_type": "agent_run",
				"resource_id":   runID,
			},
		},
	})).To(Succeed())

	eventTypes := []string{}
	for {
		_, payload, err := conn.ReadMessage()
		Expect(err).NotTo(HaveOccurred())
		messageType := socketMessageType(payload)
		if messageType == "replay_complete" {
			return eventTypes
		}
		if messageType != "event" {
			continue
		}
		event := socketMessageEvent(payload)
		if fmt.Sprint(event["resource"].(map[string]any)["id"]) != runID {
			continue
		}
		eventTypes = append(eventTypes, fmt.Sprint(event["event_type"]))
	}
}

func socketTestURL(token string) string {
	base := strings.TrimRight(os.Getenv("SOCKET_SERVICE_WS_URL"), "/")
	if base == "" {
		base = defaultSocketURL
	}
	parsed, err := url.Parse(base)
	Expect(err).NotTo(HaveOccurred())
	query := parsed.Query()
	query.Set("token", token)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func socketMessageType(payload []byte) string {
	var message map[string]any
	Expect(json.Unmarshal(payload, &message)).To(Succeed(), "payload: %s", string(payload))
	return fmt.Sprint(message["type"])
}

func socketMessageEvent(payload []byte) map[string]any {
	var message map[string]any
	Expect(json.Unmarshal(payload, &message)).To(Succeed(), "payload: %s", string(payload))
	event, ok := message["event"].(map[string]any)
	Expect(ok).To(BeTrue(), "payload: %s", string(payload))
	return event
}

func invokeHTTPGetTool(orgID string, targetURL string) error {
	arguments, err := json.Marshal(map[string]string{"url": targetURL})
	Expect(err).NotTo(HaveOccurred())
	return invokeToolRaw(orgID, "http_get", arguments)
}

func invokeToolRaw(orgID string, toolName string, arguments []byte) error {
	client, cleanup := newToolServiceClient()
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), toolServiceE2ERequestTimeout)
	defer cancel()
	_, err := client.Invoke(ctx, &toolspb.InvokeToolRequest{
		ToolName:      toolName,
		ArgumentsJson: arguments,
		OrgId:         orgID,
		UserId:        uuid.NewString(),
		TraceId:       uuid.NewString(),
		InvocationId:  uuid.NewString(),
	})
	return err
}

func newToolServiceClient() (toolspb.ToolServiceClient, func()) {
	address := strings.TrimSpace(os.Getenv("TOOL_SERVICE_GRPC_ADDRESS"))
	if address == "" {
		address = defaultToolServiceGRPCAddress
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	Expect(err).NotTo(HaveOccurred())
	return toolspb.NewToolServiceClient(conn), func() {
		Expect(conn.Close()).To(Succeed())
	}
}

func expectToolServiceError(err error, code codes.Code, messageFragment string) {
	Expect(err).To(HaveOccurred())
	status, ok := grpcstatus.FromError(err)
	Expect(ok).To(BeTrue())
	Expect(status.Code()).To(Equal(code), status.Message())
	Expect(strings.ToLower(status.Message())).To(ContainSubstring(messageFragment))
}

func trajectoryObject(payload map[string]any, key string) map[string]any {
	value, ok := payload[key].(map[string]any)
	Expect(ok).To(BeTrue(), "expected %s object in %#v", key, payload)
	return value
}

func trajectoryArray(payload map[string]any, key string) []any {
	value, ok := payload[key].([]any)
	Expect(ok).To(BeTrue(), "expected %s array in %#v", key, payload)
	return value
}
