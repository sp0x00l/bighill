package rest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"
	"inference_service/pkg/infra/network/adapter"

	"lib/shared_lib/ctxutil"
	serializers "lib/shared_lib/serializer"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestRest(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Inference service REST unit test suite")
}

type inferenceUsecaseStub struct {
	endpoints         []*model.PublishedEndpoint
	preferenceDataset *model.PreferenceDataset
	agentTrajectory   *model.AgentTrajectory
	generateRequest   model.GenerateRequest
	endpointID        uuid.UUID
	feedback          *model.InferenceFeedback
	idempotencyKey    uuid.UUID
	actor             uuid.UUID
	org               uuid.UUID
	err               error
}

func (s *inferenceUsecaseStub) RecordModelUpdated(context.Context, *model.InferenceModel, uuid.UUID) (*model.InferenceModel, error) {
	return nil, nil
}

func (s *inferenceUsecaseStub) RecordDatasetUpdated(context.Context, *model.InferenceDataset, uuid.UUID) (*model.InferenceDataset, error) {
	return nil, nil
}

func (s *inferenceUsecaseStub) ReadModel(context.Context, uuid.UUID, uuid.UUID) (*model.InferenceModel, error) {
	return nil, nil
}

func (s *inferenceUsecaseStub) PublishAgentSpec(context.Context, model.AgentSpecPublication) (*model.AgentSpec, error) {
	return nil, s.err
}

func (s *inferenceUsecaseStub) ListEndpoints(ctx context.Context, orgID uuid.UUID) ([]*model.PublishedEndpoint, error) {
	s.org = orgID
	if actor, ok := ctxutil.TenantID(ctx); ok {
		s.actor = actor
	}
	if err := s.err; err != nil {
		return nil, err
	}
	return s.endpoints, nil
}

func (s *inferenceUsecaseStub) PublishEndpoint(context.Context, model.EndpointPublication) (*model.PublishedEndpoint, error) {
	return nil, s.err
}

func (s *inferenceUsecaseStub) SetEndpointDatasets(context.Context, model.EndpointDatasetBinding) (*model.PublishedEndpoint, error) {
	return nil, s.err
}

func (s *inferenceUsecaseStub) SetEndpointMergeStrategy(context.Context, model.EndpointMergeConfiguration) (*model.PublishedEndpoint, error) {
	return nil, s.err
}

func (s *inferenceUsecaseStub) GenerateForEndpoint(ctx context.Context, endpointID uuid.UUID, request model.GenerateRequest) (*model.GenerateResponse, error) {
	s.endpointID = endpointID
	s.generateRequest = request
	if actor, ok := ctxutil.TenantID(ctx); ok {
		s.actor = actor
	}
	if org, ok := ctxutil.OrgID(ctx); ok {
		s.org = org
	}
	if err := s.err; err != nil {
		return nil, err
	}
	return &model.GenerateResponse{
		RequestID: request.RequestID,
		OrgID:     request.OrgID,
		QueryText: request.QueryText,
		Answer:    "answer",
		Contexts: []model.RetrievedContext{{
			ChunkIndex: 2,
			SourceText: "source",
			Similarity: 0.8,
		}},
	}, nil
}

func (s *inferenceUsecaseStub) Generate(context.Context, model.GenerateRequest) (*model.GenerateResponse, error) {
	return nil, nil
}

func (s *inferenceUsecaseStub) RecordFeedback(ctx context.Context, feedback *model.InferenceFeedback, idempotencyKey uuid.UUID) (*model.InferenceFeedback, error) {
	s.feedback = feedback
	s.idempotencyKey = idempotencyKey
	if actor, ok := ctxutil.TenantID(ctx); ok {
		s.actor = actor
	}
	if org, ok := ctxutil.OrgID(ctx); ok {
		s.org = org
	}
	if err := s.err; err != nil {
		return nil, err
	}
	return feedback, nil
}

func (s *inferenceUsecaseStub) BuildPreferenceDatasetForEndpoint(ctx context.Context, endpointID uuid.UUID, request model.PreferenceDatasetBuildRequest) (*model.PreferenceDataset, error) {
	s.endpointID = endpointID
	if actor, ok := ctxutil.TenantID(ctx); ok {
		s.actor = actor
	}
	if org, ok := ctxutil.OrgID(ctx); ok {
		s.org = org
	}
	if err := s.err; err != nil {
		return nil, err
	}
	if s.preferenceDataset != nil {
		return s.preferenceDataset, nil
	}
	return &model.PreferenceDataset{
		PreferenceDatasetID: uuid.New(),
		EndpointID:          endpointID,
		UserID:              request.UserID,
		OrgID:               request.OrgID,
		ModelID:             uuid.New(),
		OutputURI:           request.OutputURI,
	}, nil
}

func (s *inferenceUsecaseStub) ReadPreferenceDataset(context.Context, uuid.UUID, uuid.UUID) (*model.PreferenceDataset, error) {
	return s.preferenceDataset, s.err
}

func (s *inferenceUsecaseStub) ListPreferenceDatasets(context.Context, uuid.UUID, model.PreferenceDatasetFilter) ([]*model.PreferenceDataset, error) {
	if s.preferenceDataset == nil {
		return nil, s.err
	}
	return []*model.PreferenceDataset{s.preferenceDataset}, s.err
}

func (s *inferenceUsecaseStub) BuildPreferenceDataset(context.Context, model.PreferenceDatasetBuildRequest) (*model.PreferenceDataset, error) {
	return nil, nil
}

func (s *inferenceUsecaseStub) ReadAgentTrajectory(context.Context, uuid.UUID, uuid.UUID) (*model.AgentTrajectory, error) {
	return s.agentTrajectory, s.err
}

func (s *inferenceUsecaseStub) ReapExpiredAgentRuns(context.Context, int) (int64, error) {
	return 0, nil
}

var _ = Describe("InferenceHandlers", func() {
	var (
		usecase    *inferenceUsecaseStub
		handlers   *InferenceHandlers
		userID     uuid.UUID
		orgID      uuid.UUID
		requestID  uuid.UUID
		endpointID uuid.UUID
	)

	BeforeEach(func() {
		userID = uuid.New()
		orgID = uuid.New()
		requestID = uuid.New()
		endpointID = uuid.New()
		usecase = &inferenceUsecaseStub{
			endpoints: []*model.PublishedEndpoint{{
				EndpointID:    endpointID,
				OrgID:         orgID,
				ModelID:       uuid.New(),
				DatasetIDs:    []uuid.UUID{uuid.New()},
				MergeStrategy: model.RAGMergeStrategyReranker,
				Status:        model.PublishedEndpointStatusReady,
				DisplayName:   "Support bot",
			}},
		}
		handlers = NewInferenceHandlers(
			usecase,
			adapter.NewEndpointDTOAdapter(serializers.NewJSONSerializer()),
			adapter.NewGenerationDTOAdapter(serializers.NewJSONSerializer()),
			adapter.NewFeedbackDTOAdapter(serializers.NewJSONSerializer()),
			adapter.NewAgentSpecDTOAdapter(serializers.NewJSONSerializer()),
			adapter.NewPreferenceDatasetDTOAdapter(serializers.NewJSONSerializer()),
			adapter.NewAgentTrajectoryDTOAdapter(serializers.NewJSONSerializer()),
		)
	})

	It("lists safe endpoint projections for the trusted org", func() {
		res, err := handlers.ListEndpoints(context.Background(), requestWithAuth(http.MethodGet, "/v1/inference/endpoints", "", userID, orgID, uuid.Nil))

		Expect(err).NotTo(HaveOccurred())
		Expect(res.StatusCode()).To(Equal(http.StatusOK))
		Expect(usecase.actor).To(Equal(userID))
		Expect(usecase.org).To(Equal(orgID))
		var dtos []adapter.PublishedEndpointDTO
		Expect(json.Unmarshal(res.Payload(), &dtos)).To(Succeed())
		Expect(dtos).To(HaveLen(1))
		Expect(dtos[0].EndpointID).To(Equal(endpointID.String()))
		Expect(dtos[0].DisplayName).To(Equal("Support bot"))
	})

	It("generates through an endpoint and ignores raw resource ids in the body", func() {
		body := `{"query_text":"what now?","top_k":4,"metadata_filters":{"team":"ml"},"model_id":"` + uuid.NewString() + `","dataset_id":"` + uuid.NewString() + `"}`
		req := requestWithAuth(http.MethodPost, "/v1/inference/endpoints/"+endpointID.String()+"/generations", body, userID, orgID, requestID)
		req = mux.SetURLVars(req, map[string]string{"endpointId": endpointID.String()})

		res, err := handlers.Generate(context.Background(), req)

		Expect(err).NotTo(HaveOccurred())
		Expect(res.StatusCode()).To(Equal(http.StatusOK))
		Expect(usecase.endpointID).To(Equal(endpointID))
		Expect(usecase.generateRequest.RequestID).To(Equal(requestID))
		Expect(usecase.generateRequest.UserID).To(Equal(userID))
		Expect(usecase.generateRequest.OrgID).To(Equal(orgID))
		Expect(usecase.generateRequest.ModelID).To(Equal(uuid.Nil))
		Expect(usecase.generateRequest.DatasetID).To(Equal(uuid.Nil))
		Expect(usecase.generateRequest.TopK).To(Equal(4))
		var dto adapter.GenerateResponseDTO
		Expect(json.Unmarshal(res.Payload(), &dto)).To(Succeed())
		Expect(dto.Answer).To(Equal("answer"))
	})

	It("rejects generation requests without idempotency", func() {
		req := requestWithAuth(http.MethodPost, "/v1/inference/endpoints/"+endpointID.String()+"/generations", `{"query_text":"hello"}`, userID, orgID, uuid.Nil)
		req = mux.SetURLVars(req, map[string]string{"endpointId": endpointID.String()})

		_, err := handlers.Generate(context.Background(), req)

		var httpErr *HTTPError
		Expect(errors.As(err, &httpErr)).To(BeTrue())
		Expect(httpErr.statusCode).To(Equal(http.StatusBadRequest))
	})

	It("records feedback with trusted actor/org and X-Request-ID idempotency", func() {
		targetRequestID := uuid.New()
		req := requestWithAuth(http.MethodPost, "/v1/inference/feedback", `{"request_id":"`+targetRequestID.String()+`","accepted":true,"rating":1}`, userID, orgID, requestID)

		res, err := handlers.RecordFeedback(context.Background(), req)

		Expect(err).NotTo(HaveOccurred())
		Expect(res.StatusCode()).To(Equal(http.StatusCreated))
		Expect(usecase.idempotencyKey).To(Equal(requestID))
		Expect(usecase.feedback.FeedbackID).To(Equal(requestID))
		Expect(usecase.feedback.RequestID).To(Equal(targetRequestID))
		Expect(usecase.feedback.UserID).To(Equal(userID))
		Expect(usecase.feedback.OrgID).To(Equal(orgID))
	})

	It("reads an agent run trajectory for the trusted org", func() {
		runID := uuid.New()
		stepID := uuid.New()
		usecase.agentTrajectory = &model.AgentTrajectory{
			Run: &model.AgentRun{
				RunID:                   runID,
				UserID:                  userID,
				OrgID:                   orgID,
				EndpointID:              endpointID,
				AgentSpecHash:           "spec-hash",
				ToolsetHash:             "toolset-hash",
				TrajectorySchemaVersion: "trajectory-v1",
				DecodingParams:          []byte(`{"temperature":0}`),
				Status:                  model.AgentRunStatusCompleted,
				StopReason:              model.AgentStopReasonFinalAnswer,
			},
			Steps: []*model.AgentStep{{
				StepID:               stepID,
				RunID:                runID,
				OrgID:                orgID,
				StepIndex:            0,
				PresentedToolSchemas: []byte(`[]`),
				GenerationResult:     []byte(`{"content":"answer"}`),
				FinishReason:         model.GenerationFinishReasonStop,
			}},
		}
		req := requestWithAuth(http.MethodGet, "/v1/inference/agent-runs/"+runID.String(), "", userID, orgID, uuid.Nil)
		req = mux.SetURLVars(req, map[string]string{"runId": runID.String()})

		res, err := handlers.ReadAgentRun(context.Background(), req)

		Expect(err).NotTo(HaveOccurred())
		Expect(res.StatusCode()).To(Equal(http.StatusOK))
		var dto adapter.AgentTrajectoryDTO
		Expect(json.Unmarshal(res.Payload(), &dto)).To(Succeed())
		Expect(dto.Run.RunID).To(Equal(runID.String()))
		Expect(dto.Run.AgentSpecHash).To(Equal("spec-hash"))
		Expect(dto.Steps).To(HaveLen(1))
		Expect(dto.Steps[0].StepID).To(Equal(stepID.String()))
	})

	It("maps domain errors to HTTP status codes", func() {
		usecase.err = domain.ErrModelNotReady.Extend("endpoint is not ready")
		req := requestWithAuth(http.MethodPost, "/v1/inference/endpoints/"+endpointID.String()+"/generations", `{"query_text":"hello"}`, userID, orgID, requestID)
		req = mux.SetURLVars(req, map[string]string{"endpointId": endpointID.String()})

		_, err := handlers.Generate(context.Background(), req)

		var httpErr *HTTPError
		Expect(errors.As(err, &httpErr)).To(BeTrue())
		Expect(httpErr.statusCode).To(Equal(http.StatusConflict))

		usecase.err = domain.ErrModelNotFound
		req = requestWithAuth(http.MethodPost, "/v1/inference/endpoints/"+endpointID.String()+"/generations", `{"query_text":"hello"}`, userID, orgID, requestID)
		req = mux.SetURLVars(req, map[string]string{"endpointId": endpointID.String()})
		_, err = handlers.Generate(context.Background(), req)
		Expect(errors.As(err, &httpErr)).To(BeTrue())
		Expect(httpErr.statusCode).To(Equal(http.StatusNotFound))
	})

	It("rejects missing trusted org/user headers", func() {
		req := httptest.NewRequest(http.MethodGet, "/v1/inference/endpoints", nil)
		req.Header.Set("X-User-ID", userID.String())

		_, err := handlers.ListEndpoints(context.Background(), req)

		var httpErr *HTTPError
		Expect(errors.As(err, &httpErr)).To(BeTrue())
		Expect(httpErr.statusCode).To(Equal(http.StatusBadRequest))
	})
})

func requestWithAuth(method, path, body string, userID, orgID, requestID uuid.UUID) *http.Request {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if userID != uuid.Nil {
		req.Header.Set("X-User-ID", userID.String())
	}
	if orgID != uuid.Nil {
		req.Header.Set("X-Org-ID", orgID.String())
	}
	if requestID != uuid.Nil {
		req.Header.Set("X-Request-ID", requestID.String())
	}
	return req
}
