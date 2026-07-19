package client_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"agent_registry_service/pkg/domain"
	"agent_registry_service/pkg/domain/model"
	agentclient "agent_registry_service/pkg/infra/network/client"
	"lib/shared_lib/authz"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestInferenceVerifierClient(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Agent registry inference verifier client unit test suite")
}

var _ = Describe("InferenceVerifier", func() {
	It("reads agent specs with actor and org scope headers", func() {
		orgID := uuid.New()
		userID := uuid.New()
		modelID := uuid.New()
		var gotOrg string
		var gotUser string
		client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			gotOrg = r.Header.Get(authz.HeaderOrgID)
			gotUser = r.Header.Get(authz.HeaderUserID)
			Expect(r.URL.Path).To(Equal("/v1/inference/agent-specs/sha256-spec"))
			return jsonResponse(http.StatusOK, `[{"agent_lineage":"support-agent","content_hash":"sha256-spec","model_id":"`+modelID.String()+`"}]`), nil
		})}
		verifier := agentclient.NewInferenceVerifierWithClient(agentclient.InferenceVerifierConfig{
			BaseURL:        "http://inference.local",
			RequestTimeout: time.Second,
		}, client)

		spec, err := verifier.ReadAgentSpec(context.Background(), orgID, userID, "sha256-spec")

		Expect(err).NotTo(HaveOccurred())
		Expect(spec.AgentLineage).To(Equal("support-agent"))
		Expect(spec.ModelID).To(Equal(modelID))
		Expect(gotOrg).To(Equal(orgID.String()))
		Expect(gotUser).To(Equal(userID.String()))
	})

	It("maps missing inference specs to the registry dependency error", func() {
		client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return jsonResponse(http.StatusNotFound, `{}`), nil
		})}
		verifier := agentclient.NewInferenceVerifierWithClient(agentclient.InferenceVerifierConfig{
			BaseURL:        "http://inference.local",
			RequestTimeout: time.Second,
		}, client)

		spec, err := verifier.ReadAgentSpec(context.Background(), uuid.New(), uuid.New(), "sha256-missing")

		Expect(spec).To(BeNil())
		Expect(errors.Is(err, domain.ErrAgentSpecUnavailable)).To(BeTrue())
	})

	It("maps inference transport failures to the operation dependency error", func() {
		client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("dial failed")
		})}
		verifier := agentclient.NewInferenceVerifierWithClient(agentclient.InferenceVerifierConfig{
			BaseURL:        "http://inference.local",
			RequestTimeout: time.Second,
		}, client)

		spec, err := verifier.ReadAgentSpec(context.Background(), uuid.New(), uuid.New(), "sha256-missing")

		Expect(spec).To(BeNil())
		Expect(errors.Is(err, domain.ErrAgentSpecUnavailable)).To(BeTrue())
		Expect(err).To(MatchError(ContainSubstring("dial failed")))
	})

	It("reads endpoint references with actor and org scope headers", func() {
		orgID := uuid.New()
		userID := uuid.New()
		endpointID := uuid.New()
		client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			Expect(r.Header.Get(authz.HeaderOrgID)).To(Equal(orgID.String()))
			Expect(r.Header.Get(authz.HeaderUserID)).To(Equal(userID.String()))
			Expect(r.URL.Path).To(Equal("/v1/inference/endpoints/" + endpointID.String()))
			return jsonResponse(http.StatusOK, `[{"endpoint_id":"`+endpointID.String()+`"}]`), nil
		})}
		verifier := agentclient.NewInferenceVerifierWithClient(agentclient.InferenceVerifierConfig{
			BaseURL:        "http://inference.local",
			RequestTimeout: time.Second,
		}, client)

		endpoint, err := verifier.ReadEndpoint(context.Background(), orgID, userID, endpointID)

		Expect(err).NotTo(HaveOccurred())
		Expect(endpoint.EndpointID).To(Equal(endpointID))
	})

	It("starts pinned spec eval runs and polls the persisted trajectory", func() {
		orgID := uuid.New()
		userID := uuid.New()
		endpointID := uuid.New()
		taskID := uuid.New()
		servingModelID := uuid.New()
		runID := uuid.New()
		paths := []string{}
		var requestID string
		var postedBody string
		client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			paths = append(paths, r.Method+" "+r.URL.Path)
			Expect(r.Header.Get(authz.HeaderOrgID)).To(Equal(orgID.String()))
			Expect(r.Header.Get(authz.HeaderUserID)).To(Equal(userID.String()))
			switch r.Method + " " + r.URL.Path {
			case http.MethodPost + " /v1/inference/endpoints/" + endpointID.String() + "/agent-eval-runs/sha256-spec":
				requestID = r.Header.Get("X-Request-ID")
				raw, err := io.ReadAll(r.Body)
				Expect(err).NotTo(HaveOccurred())
				postedBody = string(raw)
				return jsonResponse(http.StatusAccepted, `{"agent_run_id":"`+runID.String()+`","status":"ACCEPTED"}`), nil
			case http.MethodGet + " /v1/inference/agent-runs/" + runID.String():
				return jsonResponse(http.StatusOK, `{
					"run":{"run_id":"`+runID.String()+`","status":"COMPLETED","stop_reason":"FINAL_ANSWER"},
					"steps":[{"step_index":0,"generation_result":{"content":"Alice signed the agreement."}}],
					"tool_invocations":[{"tool_name":"search_knowledge","error_type":"UNKNOWN","result":{"contexts":[{"source_text":"Alice signed."}]}}]
				}`), nil
			default:
				return jsonResponse(http.StatusNotFound, `{}`), nil
			}
		})}
		verifier := agentclient.NewInferenceVerifierWithClient(agentclient.InferenceVerifierConfig{
			BaseURL:        "http://inference.local",
			RequestTimeout: time.Second,
			PollInterval:   time.Millisecond,
			PollAttempts:   1,
		}, client)

		result, err := verifier.RunAgentTask(context.Background(), model.AgentTaskRunCommand{
			OrgID:          orgID,
			UserID:         userID,
			EndpointID:     endpointID,
			AgentSpecHash:  "sha256-spec",
			ServingModelID: servingModelID,
			TaskID:         taskID,
			QueryText:      "Who signed the agreement?",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(paths).To(Equal([]string{
			http.MethodPost + " /v1/inference/endpoints/" + endpointID.String() + "/agent-eval-runs/sha256-spec",
			http.MethodGet + " /v1/inference/agent-runs/" + runID.String(),
		}))
		Expect(requestID).NotTo(BeEmpty())
		Expect(postedBody).To(MatchJSON(`{"query_text":"Who signed the agreement?","serving_model_id":"` + servingModelID.String() + `"}`))
		Expect(result.RunID).To(Equal(runID))
		Expect(result.Status).To(Equal("COMPLETED"))
		Expect(result.StopReason).To(Equal("FINAL_ANSWER"))
		Expect(result.Answer).To(Equal("Alice signed the agreement."))
		Expect(result.GroundedContextCount).To(Equal(1))
		Expect(result.GroundedContextTexts).To(Equal([]string{"Alice signed."}))
		Expect(result.ToolInvocations).To(HaveLen(1))
		Expect(result.ToolInvocations[0].ToolName).To(Equal("search_knowledge"))
	})
})

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func jsonResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}
