package app_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"inference_service/pkg/app"
	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"
	"lib/shared_lib/ctxutil"
	msgConn "lib/shared_lib/messaging"
	shareduow "lib/shared_lib/uow"
	"lib/shared_lib/userevents"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestApp(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Inference service app unit test suite")
}

var _ = Describe("AgentRunWorkflow", func() {
	var suite testsuite.WorkflowTestSuite

	It("keeps large payloads out of workflow state", func() {
		stateType := reflect.TypeOf(app.AgentRunWorkflowState{})
		_, hasMessages := stateType.FieldByName("Messages")
		_, hasContexts := stateType.FieldByName("Contexts")
		_, hasResolvedToolSpecs := stateType.FieldByName("ResolvedToolSpecs")

		Expect(hasMessages).To(BeFalse())
		Expect(hasContexts).To(BeFalse())
		Expect(hasResolvedToolSpecs).To(BeFalse())
	})

	It("orchestrates prepare, generate, record step, and completion as separate activities", func() {
		env := suite.NewTestWorkflowEnvironment()
		input := app.AgentRunWorkflowInput{
			EndpointID: uuid.New(),
			WallMs:     60000,
			Request: model.GenerateRequest{
				RequestID:  uuid.New(),
				AgentRunID: uuid.New(),
				OrgID:      uuid.New(),
			},
		}
		order := []string{}
		env.RegisterActivityWithOptions(func(app.PrepareAgentRunActivityInput) (app.AgentRunWorkflowState, error) {
			order = append(order, "prepare")
			return app.AgentRunWorkflowState{
				Request: input.Request,
				Budgets: model.AgentBudgets{
					MaxSteps: 1,
					Token:    1000,
					WallMs:   60000,
				},
				TransientToolFailureCount: map[string]int{},
			}, nil
		}, activity.RegisterOptions{Name: app.PrepareAgentRunActivityName})
		env.RegisterActivityWithOptions(func(app.GenerateAgentStepActivityInput) (app.GenerateAgentStepActivityOutput, error) {
			order = append(order, "generate")
			return app.GenerateAgentStepActivityOutput{Result: model.GenerationResult{
				Content:      "answer",
				FinishReason: model.GenerationFinishReasonStop,
			}, PromptTokenEstimate: 8, TokenUsage: 12}, nil
		}, activity.RegisterOptions{Name: app.GenerateAgentStepActivityName})
		env.RegisterActivityWithOptions(func(app.RecordAgentStepActivityInput) (uuid.UUID, error) {
			order = append(order, "record-step")
			return uuid.New(), nil
		}, activity.RegisterOptions{Name: app.RecordAgentStepActivityName})
		env.RegisterActivityWithOptions(func(app.CompleteAgentRunActivityInput) error {
			order = append(order, "complete")
			return nil
		}, activity.RegisterOptions{Name: app.CompleteAgentRunActivityName})

		env.ExecuteWorkflow(app.AgentRunWorkflow, input)

		Expect(env.IsWorkflowCompleted()).To(BeTrue())
		Expect(env.GetWorkflowError()).NotTo(HaveOccurred())
		Expect(order).To(Equal([]string{"prepare", "generate", "record-step", "complete"}))
	})

	It("does not retry non-idempotent generation activities", func() {
		env := suite.NewTestWorkflowEnvironment()
		input := app.AgentRunWorkflowInput{
			EndpointID: uuid.New(),
			WallMs:     60000,
			Request: model.GenerateRequest{
				RequestID:  uuid.New(),
				AgentRunID: uuid.New(),
				OrgID:      uuid.New(),
			},
		}
		generateAttempts := 0
		env.RegisterActivityWithOptions(func(app.PrepareAgentRunActivityInput) (app.AgentRunWorkflowState, error) {
			return app.AgentRunWorkflowState{
				Request: input.Request,
				Budgets: model.AgentBudgets{
					MaxSteps: 1,
					Token:    1000,
					WallMs:   60000,
				},
				TransientToolFailureCount: map[string]int{},
			}, nil
		}, activity.RegisterOptions{Name: app.PrepareAgentRunActivityName})
		env.RegisterActivityWithOptions(func(app.GenerateAgentStepActivityInput) (app.GenerateAgentStepActivityOutput, error) {
			generateAttempts++
			return app.GenerateAgentStepActivityOutput{}, errors.New("generation failed")
		}, activity.RegisterOptions{Name: app.GenerateAgentStepActivityName})
		env.RegisterActivityWithOptions(func(app.FailAgentRunActivityInput) error {
			return nil
		}, activity.RegisterOptions{Name: app.FailAgentRunActivityName})

		env.ExecuteWorkflow(app.AgentRunWorkflow, input)

		Expect(env.IsWorkflowCompleted()).To(BeTrue())
		Expect(env.GetWorkflowError()).To(HaveOccurred())
		Expect(generateAttempts).To(Equal(1))
	})
})

type inferenceModelRepositoryStub struct {
	model          *model.InferenceModel
	models         []*model.InferenceModel
	upsertedModel  *model.InferenceModel
	upsertCtx      context.Context
	idempotencyKey uuid.UUID
	readUserID     uuid.UUID
	readID         uuid.UUID
	readCount      int
	err            error
}

func (s *inferenceModelRepositoryStub) UpsertModel(ctx context.Context, inferenceModel *model.InferenceModel, idempotencyKey uuid.UUID) (*model.InferenceModel, error) {
	s.upsertCtx = ctx
	s.upsertedModel = inferenceModel
	s.idempotencyKey = idempotencyKey
	if s.err != nil {
		return nil, s.err
	}
	return inferenceModel, nil
}

func (s *inferenceModelRepositoryStub) ReadByID(_ context.Context, userID uuid.UUID, modelID uuid.UUID) (*model.InferenceModel, error) {
	s.readUserID = userID
	s.readID = modelID
	if s.err != nil {
		return nil, s.err
	}
	if len(s.models) > 0 {
		index := s.readCount
		if index >= len(s.models) {
			index = len(s.models) - 1
		}
		s.readCount++
		return s.models[index], nil
	}
	return s.model, nil
}

type inferenceDatasetRepositoryStub struct {
	dataset        *model.InferenceDataset
	datasets       map[uuid.UUID]*model.InferenceDataset
	upserted       *model.InferenceDataset
	idempotencyKey uuid.UUID
	readUserID     uuid.UUID
	readID         uuid.UUID
	err            error
}

func (s *inferenceDatasetRepositoryStub) UpsertDataset(_ context.Context, dataset *model.InferenceDataset, idempotencyKey uuid.UUID) (*model.InferenceDataset, error) {
	s.upserted = dataset
	s.idempotencyKey = idempotencyKey
	if s.err != nil {
		return nil, s.err
	}
	return dataset, nil
}

func (s *inferenceDatasetRepositoryStub) ReadDataset(_ context.Context, userID uuid.UUID, datasetID uuid.UUID) (*model.InferenceDataset, error) {
	s.readUserID = userID
	s.readID = datasetID
	if s.err != nil {
		return nil, s.err
	}
	if s.datasets != nil {
		if dataset, ok := s.datasets[datasetID]; ok {
			return dataset, nil
		}
		return nil, domain.ErrDatasetNotFound
	}
	return s.dataset, nil
}

type retrievalClientStub struct {
	mu                sync.Mutex
	userID            uuid.UUID
	datasetID         uuid.UUID
	queryText         string
	topK              int
	metadataFilters   map[string]string
	contexts          []model.RetrievedContext
	contextsByDataset map[uuid.UUID][]model.RetrievedContext
	err               error
	errorsByDataset   map[uuid.UUID]error
	calls             []uuid.UUID
}

func (s *retrievalClientStub) SearchEmbeddings(_ context.Context, userID uuid.UUID, datasetID uuid.UUID, queryText string, topK int, metadataFilters map[string]string) ([]model.RetrievedContext, error) {
	s.mu.Lock()
	s.userID = userID
	s.datasetID = datasetID
	s.queryText = queryText
	s.topK = topK
	s.metadataFilters = metadataFilters
	s.calls = append(s.calls, datasetID)
	s.mu.Unlock()
	if s.errorsByDataset != nil {
		if err, ok := s.errorsByDataset[datasetID]; ok {
			return nil, err
		}
	}
	if s.err != nil {
		return nil, s.err
	}
	contexts := s.contexts
	if s.contextsByDataset != nil {
		contexts = s.contextsByDataset[datasetID]
	}
	if topK < len(contexts) {
		return contexts[:topK], nil
	}
	return contexts, nil
}

func (s *retrievalClientStub) SearchGraph(_ context.Context, userID uuid.UUID, datasetID uuid.UUID, queryText string, topK int, _ int) ([]model.RetrievedContext, error) {
	return s.SearchEmbeddings(context.Background(), userID, datasetID, queryText, topK, nil)
}

func (s *retrievalClientStub) Close() error {
	return nil
}

type rerankerStub struct {
	query      string
	candidates []model.RetrievedContext
	topK       int
	contexts   []model.RetrievedContext
	err        error
}

func (s *rerankerStub) Rerank(_ context.Context, query string, candidates []model.RetrievedContext, topK int) ([]model.RetrievedContext, error) {
	s.query = query
	s.candidates = candidates
	s.topK = topK
	if s.err != nil {
		return nil, s.err
	}
	return s.contexts, nil
}

type generationAdapterStub struct {
	request   model.GenerationRequest
	requests  []model.GenerationRequest
	results   []model.GenerationResult
	answer    string
	toolCalls []model.ToolCall
	err       error
}

func (s *generationAdapterStub) Generate(_ context.Context, request model.GenerationRequest) (model.GenerationResult, error) {
	s.request = request
	s.requests = append(s.requests, request)
	if s.err != nil {
		return model.GenerationResult{}, s.err
	}
	if len(s.results) > 0 {
		result := s.results[0]
		s.results = s.results[1:]
		return result, nil
	}
	if len(request.Tools) > 0 && len(s.toolCalls) > 0 {
		return model.GenerationResult{ToolCalls: s.toolCalls, FinishReason: model.GenerationFinishReasonToolCalls}, nil
	}
	if s.answer != "" {
		return model.GenerationResult{Content: s.answer, FinishReason: model.GenerationFinishReasonStop}, nil
	}
	return model.GenerationResult{Content: "generated answer", FinishReason: model.GenerationFinishReasonStop}, nil
}

type modelServingLoadTriggerStub struct {
	orgID   uuid.UUID
	modelID uuid.UUID
	calls   int
	err     error
}

func (s *modelServingLoadTriggerStub) TriggerModelLoad(_ context.Context, orgID uuid.UUID, modelID uuid.UUID) error {
	s.orgID = orgID
	s.modelID = modelID
	s.calls++
	return s.err
}

type inferenceRequestRepositoryStub struct {
	request *model.InferenceRequest
	err     error
}

func (s *inferenceRequestRepositoryStub) RecordInferenceRequest(_ context.Context, request *model.InferenceRequest) error {
	s.request = request
	return s.err
}

type capabilityReportRepositoryStub struct {
	report              *model.CapabilityReport
	recorded            *model.CapabilityReport
	readEffectiveBaseID string
	readErr             error
	err                 error
}

func (s *capabilityReportRepositoryStub) RecordCapabilityReport(_ context.Context, report *model.CapabilityReport) (*model.CapabilityReport, error) {
	s.recorded = report
	if s.err != nil {
		return nil, s.err
	}
	return report, nil
}

func (s *capabilityReportRepositoryStub) ReadCapabilityReportForEffectiveBase(_ context.Context, effectiveBaseID string) (*model.CapabilityReport, error) {
	s.readEffectiveBaseID = effectiveBaseID
	if s.readErr != nil {
		return nil, s.readErr
	}
	if s.err != nil {
		return nil, s.err
	}
	if s.report != nil {
		return s.report, nil
	}
	return &model.CapabilityReport{EffectiveBaseID: effectiveBaseID, SupportsToolCalls: true}, nil
}

type agentSpecRepositoryStub struct {
	upserted *model.AgentSpec
	spec     *model.AgentSpec
	err      error
}

type agentTrajectoryRepositoryStub struct {
	runs        []*model.AgentRun
	steps       []*model.AgentStep
	invocations []*model.AgentToolInvocation
	err         error
}

func (s *agentTrajectoryRepositoryStub) RecordAgentRun(_ context.Context, run *model.AgentRun) (*model.AgentRun, error) {
	if s.err != nil {
		return nil, s.err
	}
	recorded := *run
	if recorded.RunID == uuid.Nil {
		recorded.RunID = uuid.New()
	}
	if recorded.StartedAt.IsZero() {
		recorded.StartedAt = time.Now().UTC()
	}
	s.runs = append(s.runs, &recorded)
	return &recorded, nil
}

func (s *agentTrajectoryRepositoryStub) RecordAgentStep(_ context.Context, step *model.AgentStep) (*model.AgentStep, error) {
	if s.err != nil {
		return nil, s.err
	}
	recorded := *step
	if recorded.StepID == uuid.Nil {
		recorded.StepID = uuid.New()
	}
	if recorded.CreatedAt.IsZero() {
		recorded.CreatedAt = time.Now().UTC()
	}
	s.steps = append(s.steps, &recorded)
	return &recorded, nil
}

func (s *agentTrajectoryRepositoryStub) RecordToolInvocation(_ context.Context, invocation *model.AgentToolInvocation) (*model.AgentToolInvocation, error) {
	if s.err != nil {
		return nil, s.err
	}
	recorded := *invocation
	if recorded.InvocationID == uuid.Nil {
		recorded.InvocationID = uuid.New()
	}
	if recorded.CreatedAt.IsZero() {
		recorded.CreatedAt = time.Now().UTC()
	}
	s.invocations = append(s.invocations, &recorded)
	return &recorded, nil
}

func (s *agentTrajectoryRepositoryStub) ReadAgentTrajectory(_ context.Context, _ uuid.UUID, _ uuid.UUID) (*model.AgentTrajectory, error) {
	if s.err != nil {
		return nil, s.err
	}
	return &model.AgentTrajectory{}, nil
}

func (s *agentTrajectoryRepositoryStub) FailExpiredAgentRuns(_ context.Context, _ time.Duration) (int64, error) {
	if s.err != nil {
		return 0, s.err
	}
	return 0, nil
}

type agentRunWorkflowStarterStub struct {
	inputs []app.AgentRunWorkflowInput
	err    error
}

func (s *agentRunWorkflowStarterStub) StartAgentRunWorkflow(_ context.Context, input app.AgentRunWorkflowInput) error {
	s.inputs = append(s.inputs, input)
	return s.err
}

type userEventPublisherStub struct {
	events []userevents.Event
	err    error
}

func (s *userEventPublisherStub) Publish(_ context.Context, event userevents.Event) error {
	s.events = append(s.events, event)
	return s.err
}

func (s *userEventPublisherStub) lastEventOfType(eventType string) *userevents.Event {
	for index := len(s.events) - 1; index >= 0; index-- {
		if s.events[index].EventType == eventType {
			return &s.events[index]
		}
	}
	return nil
}

type toolInvokerStub struct {
	tools  []model.ToolSpec
	result model.ToolResult
	err    error
}

func (s *toolInvokerStub) Available(_ context.Context, _ app.ToolResolutionContext, bindings []model.ToolBinding) ([]model.ToolSpec, error) {
	if s.err != nil {
		return nil, s.err
	}
	if len(bindings) == 0 {
		return s.tools, nil
	}
	available := map[string]struct{}{}
	for _, tool := range s.tools {
		available[tool.Name] = struct{}{}
	}
	for _, binding := range bindings {
		if _, ok := available[binding.Name]; !ok {
			return nil, domain.ErrValidationFailed.Extend("agent spec references unavailable tool " + binding.Name)
		}
	}
	return s.tools, s.err
}

func (s *toolInvokerStub) Invoke(_ context.Context, _ app.ToolInvocationContext, call model.ToolCall) (model.ToolResult, error) {
	if s.result.CallID == "" {
		s.result.CallID = call.ID
	}
	if s.result.Name == "" {
		s.result.Name = call.Name
	}
	return s.result, s.err
}

func (s *agentSpecRepositoryStub) UpsertAgentSpec(_ context.Context, spec *model.AgentSpec) (*model.AgentSpec, error) {
	s.upserted = spec
	if s.err != nil {
		return nil, s.err
	}
	return spec, nil
}

func (s *agentSpecRepositoryStub) ReadAgentSpecByHash(_ context.Context, _ uuid.UUID, _ string) (*model.AgentSpec, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.spec, nil
}

type publishedEndpointRepositoryStub struct {
	endpoint       *model.PublishedEndpoint
	upserted       *model.PublishedEndpoint
	datasetIDs     []uuid.UUID
	readOrgID      uuid.UUID
	readEndpointID uuid.UUID
	err            error
}

func (s *publishedEndpointRepositoryStub) UpsertEndpoint(_ context.Context, endpoint *model.PublishedEndpoint) (*model.PublishedEndpoint, error) {
	s.upserted = endpoint
	if s.err != nil {
		return nil, s.err
	}
	return endpoint, nil
}

func (s *publishedEndpointRepositoryStub) SetEndpointDatasets(_ context.Context, _ uuid.UUID, _ uuid.UUID, datasetIDs []uuid.UUID) (*model.PublishedEndpoint, error) {
	s.datasetIDs = datasetIDs
	if s.err != nil {
		return nil, s.err
	}
	endpoint := s.endpoint
	if endpoint == nil {
		endpoint = &model.PublishedEndpoint{}
	}
	endpoint.DatasetIDs = datasetIDs
	return endpoint, nil
}

func (s *publishedEndpointRepositoryStub) ListEndpoints(_ context.Context, _ uuid.UUID) ([]*model.PublishedEndpoint, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.endpoint == nil {
		return nil, nil
	}
	return []*model.PublishedEndpoint{s.endpoint}, nil
}

func (s *publishedEndpointRepositoryStub) ReadEndpoint(_ context.Context, orgID uuid.UUID, endpointID uuid.UUID) (*model.PublishedEndpoint, error) {
	s.readOrgID = orgID
	s.readEndpointID = endpointID
	if s.err != nil {
		return nil, s.err
	}
	return s.endpoint, nil
}

type inferenceFeedbackRepositoryStub struct {
	feedback          *model.InferenceFeedback
	idempotencyKey    uuid.UUID
	preferenceRequest model.PreferenceDatasetBuildRequest
	preferenceDataset *model.PreferenceDataset
	recordedSnapshot  *model.PreferenceDataset
	snapshotRequest   model.PreferenceDatasetBuildRequest
	err               error
}

func (s *inferenceFeedbackRepositoryStub) RecordFeedback(_ context.Context, _ pgx.Tx, feedback *model.InferenceFeedback, idempotencyKey uuid.UUID) (*model.InferenceFeedback, error) {
	s.feedback = feedback
	s.idempotencyKey = idempotencyKey
	return feedback, s.err
}

func (s *inferenceFeedbackRepositoryStub) ReadPreferenceDataset(_ context.Context, request model.PreferenceDatasetBuildRequest) (*model.PreferenceDataset, error) {
	s.preferenceRequest = request
	if s.preferenceDataset != nil {
		return s.preferenceDataset, s.err
	}
	return &model.PreferenceDataset{UserID: request.UserID, DatasetID: request.DatasetID, ModelID: request.ModelID}, s.err
}

func (s *inferenceFeedbackRepositoryStub) RecordPreferenceDatasetSnapshot(_ context.Context, _ pgx.Tx, dataset *model.PreferenceDataset, request model.PreferenceDatasetBuildRequest) (*model.PreferenceDataset, error) {
	s.recordedSnapshot = dataset
	s.snapshotRequest = request
	return dataset, s.err
}

func (s *inferenceFeedbackRepositoryStub) ReadPreferenceDatasetSnapshot(_ context.Context, _ uuid.UUID, _ uuid.UUID) (*model.PreferenceDataset, error) {
	if s.preferenceDataset != nil {
		return s.preferenceDataset, s.err
	}
	return nil, s.err
}

func (s *inferenceFeedbackRepositoryStub) ListPreferenceDatasetSnapshots(_ context.Context, _ uuid.UUID, _ model.PreferenceDatasetFilter) ([]*model.PreferenceDataset, error) {
	if s.preferenceDataset != nil {
		return []*model.PreferenceDataset{s.preferenceDataset}, s.err
	}
	return nil, s.err
}

type lineageEvalSetRepositoryStub struct {
	activeEvalSet *model.LineageEvalSet
	readOrgID     uuid.UUID
	readLineage   string
	frozenSet     *model.LineageEvalSet
	frozenIDs     []uuid.UUID
	curatedSet    *model.LineageEvalSet
	curatedIDs    []uuid.UUID
	err           error
}

func (s *lineageEvalSetRepositoryStub) ReadActiveEvalSet(_ context.Context, orgID uuid.UUID, lineageName string) (*model.LineageEvalSet, error) {
	s.readOrgID = orgID
	s.readLineage = lineageName
	if s.err != nil {
		return nil, s.err
	}
	if s.activeEvalSet == nil {
		return nil, domain.ErrEvalSetNotFound
	}
	return s.activeEvalSet, nil
}

func (s *lineageEvalSetRepositoryStub) FreezeEvalSet(_ context.Context, _ pgx.Tx, evalSet *model.LineageEvalSet, exampleIDs []uuid.UUID) (*model.LineageEvalSet, error) {
	s.frozenSet = evalSet
	s.frozenIDs = append([]uuid.UUID(nil), exampleIDs...)
	return evalSet, s.err
}

func (s *lineageEvalSetRepositoryStub) RegisterCuratedEvalSet(_ context.Context, _ pgx.Tx, evalSet *model.LineageEvalSet, exampleIDs []uuid.UUID) (*model.LineageEvalSet, error) {
	s.curatedSet = evalSet
	s.curatedIDs = append([]uuid.UUID(nil), exampleIDs...)
	return evalSet, s.err
}

type inferenceUnitOfWorkStub struct {
	messages []msgConn.OutboundMessage
	err      error
}

func (s *inferenceUnitOfWorkStub) Do(ctx context.Context, fn shareduow.TxFunc) error {
	if s.err != nil {
		return s.err
	}
	return fn(ctx, nil, func(msg msgConn.OutboundMessage) error {
		s.messages = append(s.messages, msg)
		return nil
	})
}

type preferenceDatasetWriterStub struct {
	dataset *model.PreferenceDataset
	err     error
}

func (s *preferenceDatasetWriterStub) WritePreferenceDataset(_ context.Context, dataset *model.PreferenceDataset) (*model.PreferenceDataset, error) {
	s.dataset = dataset
	if s.err != nil {
		return nil, s.err
	}
	dataset.Exported = true
	return dataset, nil
}

type queryTransformerStub struct {
	request     model.QueryTransformRequest
	result      *model.QueryTransformResult
	err         error
	deadline    time.Time
	deadlineSet bool
}

func (s *queryTransformerStub) TransformQuery(ctx context.Context, request model.QueryTransformRequest) (*model.QueryTransformResult, error) {
	s.deadline, s.deadlineSet = ctx.Deadline()
	s.request = request
	return s.result, s.err
}

var _ = Describe("InferenceUsecase", func() {
	It("records a complete model update", func() {
		repository := &inferenceModelRepositoryStub{}
		endpointRepository := &publishedEndpointRepositoryStub{}
		capabilityRepository := &capabilityReportRepositoryStub{}
		uc := app.NewInferenceUsecase(
			repository,
			app.WithPublishedEndpointRepository(endpointRepository),
			app.WithCapabilityReportRepository(capabilityRepository),
		)
		idempotencyKey := uuid.New()
		inferenceModel := validInferenceModel()

		recorded, err := uc.RecordModelUpdated(context.Background(), inferenceModel, idempotencyKey)

		Expect(err).NotTo(HaveOccurred())
		Expect(recorded.ModelID).To(Equal(repository.upsertedModel.ModelID))
		Expect(repository.idempotencyKey).To(Equal(idempotencyKey))
		Expect(endpointRepository.upserted).NotTo(BeNil())
		Expect(endpointRepository.upserted.OrgID).To(Equal(inferenceModel.OrgID))
		Expect(endpointRepository.upserted.ModelID).To(Equal(inferenceModel.ModelID))
		Expect(endpointRepository.upserted.DatasetIDs).To(Equal([]uuid.UUID{inferenceModel.DatasetID}))
		Expect(endpointRepository.upserted.Status).To(Equal(model.PublishedEndpointStatusReady))
		Expect(capabilityRepository.recorded).To(BeNil())
	})

	It("does not fabricate capability reports during model projection", func() {
		repository := &inferenceModelRepositoryStub{}
		capabilityRepository := &capabilityReportRepositoryStub{}
		uc := app.NewInferenceUsecase(
			repository,
			app.WithCapabilityReportRepository(capabilityRepository),
		)
		inferenceModel := validInferenceModel()
		inferenceModel.DatasetID = uuid.Nil

		_, err := uc.RecordModelUpdated(context.Background(), inferenceModel, uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Expect(capabilityRepository.recorded).To(BeNil())
	})

	It("does not run live capability probes or fail model updates when capability projection fails", func() {
		repository := &inferenceModelRepositoryStub{}
		endpointRepository := &publishedEndpointRepositoryStub{}
		capabilityRepository := &capabilityReportRepositoryStub{err: errors.New("capability database unavailable")}
		generator := &generationAdapterStub{answer: "ok", toolCalls: []model.ToolCall{{
			ID:        "probe-call",
			Name:      "capability_probe",
			Arguments: []byte(`{}`),
		}}}
		uc := app.NewInferenceUsecase(
			repository,
			app.WithPublishedEndpointRepository(endpointRepository),
			app.WithCapabilityReportRepository(capabilityRepository),
			app.WithToolInvoker(&toolInvokerStub{tools: []model.ToolSpec{{
				Name:       "search_knowledge",
				Parameters: []byte(`{"type":"object"}`),
			}}}),
			app.WithGenerationAdapters(map[string]app.GenerationAdapter{
				model.ServingProtocolOpenAIChatCompletions.String(): generator,
			}),
		)

		recorded, err := uc.RecordModelUpdated(context.Background(), validInferenceModel(), uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Expect(recorded).NotTo(BeNil())
		Expect(generator.requests).To(BeEmpty())
		Expect(capabilityRepository.recorded).To(BeNil())
	})

	It("lazily probes and records model capability when publishing a tool-using agent spec", func() {
		inferenceModel := validInferenceModel()
		agentSpecRepository := &agentSpecRepositoryStub{}
		capabilityRepository := &capabilityReportRepositoryStub{
			report: &model.CapabilityReport{SupportsToolCalls: false},
		}
		generator := &generationAdapterStub{answer: "ok", toolCalls: []model.ToolCall{{
			ID:        "probe-call",
			Name:      "capability_probe",
			Arguments: []byte(`{}`),
		}}}
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{model: inferenceModel},
			app.WithAgentSpecRepository(agentSpecRepository),
			app.WithCapabilityReportRepository(capabilityRepository),
			app.WithToolInvoker(&toolInvokerStub{tools: []model.ToolSpec{{
				Name:       "search_knowledge",
				Parameters: []byte(`{"type":"object"}`),
			}}}),
			app.WithGenerationAdapters(map[string]app.GenerationAdapter{
				model.ServingProtocolOpenAIChatCompletions.String(): generator,
			}),
		)

		spec, err := uc.PublishAgentSpec(context.Background(), model.AgentSpecPublication{
			UserID: inferenceModel.UserID,
			OrgID:  inferenceModel.OrgID,
			Spec:   validToolUsingAgentSpec(inferenceModel),
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(spec).To(Equal(agentSpecRepository.upserted))
		Expect(capabilityRepository.recorded).NotTo(BeNil())
		Expect(capabilityRepository.recorded.EffectiveBaseID).To(Equal(inferenceModel.EffectiveBaseID))
		Expect(capabilityRepository.recorded.SupportsToolCalls).To(BeTrue())
		Expect(capabilityRepository.readEffectiveBaseID).To(Equal(inferenceModel.EffectiveBaseID))
		Expect(generator.requests).To(HaveLen(3))
		Expect(generator.requests).To(ContainElement(SatisfyAll(
			HaveField("ToolChoice", "required"),
			HaveField("Tools", HaveLen(1)),
		)))
	})

	It("rejects tool-using agent specs when the capability report lacks tool-call support", func() {
		inferenceModel := validInferenceModel()
		agentSpecRepository := &agentSpecRepositoryStub{}
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{model: inferenceModel},
			app.WithAgentSpecRepository(agentSpecRepository),
			app.WithCapabilityReportRepository(&capabilityReportRepositoryStub{
				report: &model.CapabilityReport{SupportsToolCalls: false},
			}),
			app.WithToolInvoker(&toolInvokerStub{tools: []model.ToolSpec{{
				Name:       "search_knowledge",
				Parameters: []byte(`{"type":"object"}`),
			}}}),
		)

		_, err := uc.PublishAgentSpec(context.Background(), model.AgentSpecPublication{
			UserID: inferenceModel.UserID,
			OrgID:  inferenceModel.OrgID,
			Spec:   validToolUsingAgentSpec(inferenceModel),
		})

		Expect(errors.Is(err, domain.ErrModelNotReady)).To(BeTrue())
		Expect(agentSpecRepository.upserted).To(BeNil())
	})

	It("rejects tool-using agent specs when the model has no effective base identity", func() {
		inferenceModel := validInferenceModel()
		inferenceModel.EffectiveBaseID = ""
		agentSpecRepository := &agentSpecRepositoryStub{}
		capabilityRepository := &capabilityReportRepositoryStub{
			report: &model.CapabilityReport{SupportsToolCalls: true},
		}
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{model: inferenceModel},
			app.WithAgentSpecRepository(agentSpecRepository),
			app.WithCapabilityReportRepository(capabilityRepository),
			app.WithToolInvoker(&toolInvokerStub{tools: []model.ToolSpec{{
				Name:       "search_knowledge",
				Parameters: []byte(`{"type":"object"}`),
			}}}),
		)

		_, err := uc.PublishAgentSpec(context.Background(), model.AgentSpecPublication{
			UserID: inferenceModel.UserID,
			OrgID:  inferenceModel.OrgID,
			Spec:   validToolUsingAgentSpec(inferenceModel),
		})

		Expect(errors.Is(err, domain.ErrModelNotReady)).To(BeTrue())
		Expect(err).To(MatchError(ContainSubstring("effective base")))
		Expect(capabilityRepository.readEffectiveBaseID).To(BeEmpty())
		Expect(agentSpecRepository.upserted).To(BeNil())
	})

	It("rejects publishing a spec whose tool is not available to the org", func() {
		inferenceModel := validInferenceModel()
		agentSpecRepository := &agentSpecRepositoryStub{}
		spec := validToolUsingAgentSpec(inferenceModel)
		spec.ToolBindings = []model.ToolBinding{{
			Name:       "write_database",
			Required:   true,
			ToolChoice: "required",
			Config:     []byte(`{}`),
		}}
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{model: inferenceModel},
			app.WithAgentSpecRepository(agentSpecRepository),
			app.WithCapabilityReportRepository(&capabilityReportRepositoryStub{
				report: &model.CapabilityReport{SupportsToolCalls: true},
			}),
			app.WithToolInvoker(&toolInvokerStub{tools: []model.ToolSpec{{
				Name:       "search_knowledge",
				Parameters: []byte(`{"type":"object"}`),
			}}}),
		)

		_, err := uc.PublishAgentSpec(context.Background(), model.AgentSpecPublication{
			UserID: inferenceModel.UserID,
			OrgID:  inferenceModel.OrgID,
			Spec:   spec,
		})

		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
		Expect(agentSpecRepository.upserted).To(BeNil())
	})

	It("starts agent endpoints asynchronously through the workflow starter", func() {
		dataset := validInferenceDataset()
		inferenceModel := validInferenceModel()
		inferenceModel.UserID = dataset.UserID
		inferenceModel.OrgID = dataset.OrgID
		inferenceModel.ModelKind = model.ModelKindBase
		inferenceModel.DatasetID = uuid.Nil
		spec := validToolUsingAgentSpec(inferenceModel)
		endpointID := uuid.New()
		endpoint := &model.PublishedEndpoint{
			EndpointID:    endpointID,
			OrgID:         dataset.OrgID,
			ModelID:       inferenceModel.ModelID,
			Mode:          model.AgentEndpointModeAgent,
			AgentSpecHash: spec.ContentHash,
			DatasetIDs:    []uuid.UUID{dataset.DatasetID},
			MergeStrategy: model.RAGMergeStrategyScoreNormalized,
			Status:        model.PublishedEndpointStatusReady,
		}
		trajectoryRepository := &agentTrajectoryRepositoryStub{}
		workflowStarter := &agentRunWorkflowStarterStub{}
		requestID := uuid.New()
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{model: inferenceModel},
			app.WithPublishedEndpointRepository(&publishedEndpointRepositoryStub{endpoint: endpoint}),
			app.WithAgentSpecRepository(&agentSpecRepositoryStub{spec: spec}),
			app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{datasets: map[uuid.UUID]*model.InferenceDataset{dataset.DatasetID: dataset}}),
			app.WithInferenceRequestRepository(&inferenceRequestRepositoryStub{}),
			app.WithAgentTrajectoryRepository(trajectoryRepository),
			app.WithToolInvoker(&toolInvokerStub{tools: []model.ToolSpec{{
				Name:                  "search_knowledge",
				Description:           "Search knowledge",
				Parameters:            []byte(`{"type":"object"}`),
				ImplementationVersion: "search_knowledge:v1",
				Locality:              "local",
			}}}),
			app.WithAgentRunWorkflowStarter(workflowStarter),
		)

		response, err := uc.GenerateForEndpoint(context.Background(), endpointID, model.GenerateRequest{
			RequestID: requestID,
			UserID:    dataset.UserID,
			OrgID:     dataset.OrgID,
			QueryText: "answer with tools",
			TopK:      2,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(response.Accepted).To(BeTrue())
		Expect(response.AgentRunID).To(Equal(requestID))
		Expect(workflowStarter.inputs).To(HaveLen(1))
		Expect(workflowStarter.inputs[0].Request.AgentRunID).To(Equal(requestID))
		Expect(workflowStarter.inputs[0].WallMs).To(Equal(spec.Budgets.WallMs))
		Expect(trajectoryRepository.runs).To(HaveLen(1))
		Expect(trajectoryRepository.runs[0].RunID).To(Equal(requestID))
		Expect(trajectoryRepository.runs[0].Status).To(Equal(model.AgentRunStatusRunning))
		Expect(trajectoryRepository.runs[0].EffectiveBaseID).To(Equal(inferenceModel.EffectiveBaseID))
		Expect(trajectoryRepository.runs[0].DataSnapshotSet).To(Equal([]model.DatasetSnapshotRef{{
			DatasetID:           dataset.DatasetID,
			EmbeddingSnapshotID: dataset.EmbeddingSnapshotID,
			GraphSnapshotID:     uuid.Nil,
		}}))
		Expect(trajectoryRepository.runs[0].DataSnapshotHash).NotTo(BeEmpty())
		Expect(trajectoryRepository.steps).To(BeEmpty())
	})

	It("enforces agent token budget even when the backend omits usage", func() {
		dataset := validInferenceDataset()
		inferenceModel := validInferenceModel()
		inferenceModel.UserID = dataset.UserID
		inferenceModel.OrgID = dataset.OrgID
		inferenceModel.ModelKind = model.ModelKindBase
		inferenceModel.DatasetID = uuid.Nil
		spec := validToolUsingAgentSpec(inferenceModel)
		spec.ToolBindings = nil
		spec.SystemPrompt = ""
		spec.Budgets.Token = 15
		endpointID := uuid.New()
		endpoint := &model.PublishedEndpoint{
			EndpointID:    endpointID,
			OrgID:         dataset.OrgID,
			ModelID:       inferenceModel.ModelID,
			Mode:          model.AgentEndpointModeAgent,
			AgentSpecHash: spec.ContentHash,
			DatasetIDs:    []uuid.UUID{dataset.DatasetID},
			MergeStrategy: model.RAGMergeStrategyScoreNormalized,
			Status:        model.PublishedEndpointStatusReady,
		}
		trajectoryRepository := &agentTrajectoryRepositoryStub{}
		generator := &generationAdapterStub{answer: "this answer has no backend usage block but it is not free"}
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{model: inferenceModel},
			app.WithPublishedEndpointRepository(&publishedEndpointRepositoryStub{endpoint: endpoint}),
			app.WithAgentSpecRepository(&agentSpecRepositoryStub{spec: spec}),
			app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{datasets: map[uuid.UUID]*model.InferenceDataset{dataset.DatasetID: dataset}}),
			app.WithInferenceRequestRepository(&inferenceRequestRepositoryStub{}),
			app.WithAgentTrajectoryRepository(trajectoryRepository),
			app.WithToolInvoker(&toolInvokerStub{}),
			app.WithGenerationAdapters(map[string]app.GenerationAdapter{
				model.ServingProtocolOpenAIChatCompletions.String(): generator,
			}),
		)

		response, err := uc.GenerateForEndpoint(context.Background(), endpointID, model.GenerateRequest{
			RequestID: uuid.New(),
			UserID:    dataset.UserID,
			OrgID:     dataset.OrgID,
			QueryText: "please spend some tokens",
			TopK:      2,
		})

		Expect(err).To(HaveOccurred())
		Expect(response).To(BeNil())
		Expect(trajectoryRepository.runs).NotTo(BeEmpty())
		terminal := trajectoryRepository.runs[len(trajectoryRepository.runs)-1]
		Expect(terminal.Status).To(Equal(model.AgentRunStatusFailed))
		Expect(terminal.StopReason).To(Equal(model.AgentStopReasonBudget))
		Expect(terminal.TotalTokens).To(BeNumerically(">", 0))
	})

	It("records the canonical empty resolved toolset hash", func() {
		hash := runAgentAndReturnFirstToolsetHash(nil, nil)

		Expect(hash).NotTo(BeEmpty())
		Expect(hash).NotTo(Equal(userevents.SHA256String("")))
	})

	It("records toolset_hash from resolved tool definitions", func() {
		bindings := []model.ToolBinding{{Name: "http_get"}}
		first := runAgentAndReturnFirstToolsetHash([]model.ToolSpec{{
			Name:                  "http_get",
			Description:           "Fetch an HTTP resource",
			Parameters:            []byte(`{"type":"object","properties":{"url":{"type":"string"}}}`),
			ImplementationVersion: "http_get:v1",
			Locality:              "remote",
		}}, bindings)
		second := runAgentAndReturnFirstToolsetHash([]model.ToolSpec{{
			Name:                  "http_get",
			Description:           "Fetch an HTTP resource",
			Parameters:            []byte(`{"type":"object","properties":{"url":{"type":"string"}}}`),
			ImplementationVersion: "http_get:v2",
			Locality:              "remote",
		}}, bindings)

		Expect(first).NotTo(Equal(second))
	})

	It("records data_snapshot_hash from the resolved ready dataset snapshots", func() {
		dataset := validInferenceDataset()
		withoutGraph := runAgentAndReturnFirstRunForDataset(dataset, nil, nil)
		datasetWithGraph := *dataset
		datasetWithGraph.ProcessingState = model.DatasetProcessingGraphMaterialized
		datasetWithGraph.GraphSnapshotID = uuid.New()
		datasetWithGraph.GraphProvenanceHash = "sha256-graph-provenance"
		withGraph := runAgentAndReturnFirstRunForDataset(&datasetWithGraph, nil, nil)

		Expect(withoutGraph.EffectiveBaseID).To(Equal("sha256-effective-base"))
		Expect(withoutGraph.DataSnapshotSet).To(Equal([]model.DatasetSnapshotRef{{
			DatasetID:           dataset.DatasetID,
			EmbeddingSnapshotID: dataset.EmbeddingSnapshotID,
			GraphSnapshotID:     uuid.Nil,
		}}))
		Expect(withGraph.DataSnapshotSet).To(Equal([]model.DatasetSnapshotRef{{
			DatasetID:           dataset.DatasetID,
			EmbeddingSnapshotID: dataset.EmbeddingSnapshotID,
			GraphSnapshotID:     datasetWithGraph.GraphSnapshotID,
		}}))
		Expect(withoutGraph.DataSnapshotHash).NotTo(BeEmpty())
		Expect(withGraph.DataSnapshotHash).NotTo(Equal(withoutGraph.DataSnapshotHash))
	})

	It("does not record an agent run when tool resolution fails before the run starts", func() {
		dataset := validInferenceDataset()
		inferenceModel := validInferenceModel()
		inferenceModel.UserID = dataset.UserID
		inferenceModel.OrgID = dataset.OrgID
		inferenceModel.ModelKind = model.ModelKindBase
		inferenceModel.DatasetID = uuid.Nil
		spec := validToolUsingAgentSpec(inferenceModel)
		endpointID := uuid.New()
		endpoint := &model.PublishedEndpoint{
			EndpointID:    endpointID,
			OrgID:         dataset.OrgID,
			ModelID:       inferenceModel.ModelID,
			Mode:          model.AgentEndpointModeAgent,
			AgentSpecHash: spec.ContentHash,
			DatasetIDs:    []uuid.UUID{dataset.DatasetID},
			MergeStrategy: model.RAGMergeStrategyScoreNormalized,
			Status:        model.PublishedEndpointStatusReady,
		}
		trajectoryRepository := &agentTrajectoryRepositoryStub{}
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{model: inferenceModel},
			app.WithPublishedEndpointRepository(&publishedEndpointRepositoryStub{endpoint: endpoint}),
			app.WithAgentSpecRepository(&agentSpecRepositoryStub{spec: spec}),
			app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{datasets: map[uuid.UUID]*model.InferenceDataset{dataset.DatasetID: dataset}}),
			app.WithInferenceRequestRepository(&inferenceRequestRepositoryStub{}),
			app.WithAgentTrajectoryRepository(trajectoryRepository),
			app.WithToolInvoker(&toolInvokerStub{err: domain.ErrValidationFailed.Extend("tool resolution failed")}),
			app.WithGenerationAdapters(map[string]app.GenerationAdapter{
				model.ServingProtocolOpenAIChatCompletions.String(): &generationAdapterStub{answer: "not reached"},
			}),
		)

		response, err := uc.GenerateForEndpoint(context.Background(), endpointID, model.GenerateRequest{
			RequestID: uuid.New(),
			UserID:    dataset.UserID,
			OrgID:     dataset.OrgID,
			QueryText: "answer with tools",
			TopK:      2,
		})

		Expect(err).To(HaveOccurred())
		Expect(response).To(BeNil())
		Expect(trajectoryRepository.runs).To(BeEmpty())
		Expect(trajectoryRepository.steps).To(BeEmpty())
		Expect(trajectoryRepository.invocations).To(BeEmpty())
	})

	It("records only model-facing tool schemas in the presented_tool_schemas trajectory field", func() {
		dataset := validInferenceDataset()
		inferenceModel := validInferenceModel()
		inferenceModel.UserID = dataset.UserID
		inferenceModel.OrgID = dataset.OrgID
		inferenceModel.ModelKind = model.ModelKindBase
		inferenceModel.DatasetID = uuid.Nil
		spec := validToolUsingAgentSpec(inferenceModel)
		spec.ToolBindings = []model.ToolBinding{{Name: "http_get"}}
		endpointID := uuid.New()
		endpoint := &model.PublishedEndpoint{
			EndpointID:    endpointID,
			OrgID:         dataset.OrgID,
			ModelID:       inferenceModel.ModelID,
			Mode:          model.AgentEndpointModeAgent,
			AgentSpecHash: spec.ContentHash,
			DatasetIDs:    []uuid.UUID{dataset.DatasetID},
			MergeStrategy: model.RAGMergeStrategyScoreNormalized,
			Status:        model.PublishedEndpointStatusReady,
		}
		trajectoryRepository := &agentTrajectoryRepositoryStub{}
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{model: inferenceModel},
			app.WithPublishedEndpointRepository(&publishedEndpointRepositoryStub{endpoint: endpoint}),
			app.WithAgentSpecRepository(&agentSpecRepositoryStub{spec: spec}),
			app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{datasets: map[uuid.UUID]*model.InferenceDataset{dataset.DatasetID: dataset}}),
			app.WithInferenceRequestRepository(&inferenceRequestRepositoryStub{}),
			app.WithAgentTrajectoryRepository(trajectoryRepository),
			app.WithToolInvoker(&toolInvokerStub{tools: []model.ToolSpec{{
				Name:                  "http_get",
				Description:           "Fetch an HTTP resource",
				Parameters:            []byte(`{"type":"object","properties":{"url":{"type":"string"}}}`),
				ImplementationVersion: "http_get:v1",
				Locality:              "remote",
			}}}),
			app.WithGenerationAdapters(map[string]app.GenerationAdapter{
				model.ServingProtocolOpenAIChatCompletions.String(): &generationAdapterStub{answer: "done"},
			}),
		)

		response, err := uc.GenerateForEndpoint(context.Background(), endpointID, model.GenerateRequest{
			RequestID: uuid.New(),
			UserID:    dataset.UserID,
			OrgID:     dataset.OrgID,
			QueryText: "answer directly",
			TopK:      2,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(response).NotTo(BeNil())
		Expect(trajectoryRepository.steps).To(HaveLen(1))
		Expect(string(trajectoryRepository.steps[0].PresentedToolSchemas)).NotTo(ContainSubstring("implementation_version"))
		Expect(string(trajectoryRepository.steps[0].PresentedToolSchemas)).NotTo(ContainSubstring("locality"))
		Expect(string(trajectoryRepository.steps[0].PresentedToolSchemas)).To(ContainSubstring("http_get"))
		Expect(string(trajectoryRepository.steps[0].PresentedToolSchemas)).To(ContainSubstring("parameters"))
	})

	It("marks the agent run failed when a permanent tool invocation fails", func() {
		dataset := validInferenceDataset()
		inferenceModel := validInferenceModel()
		inferenceModel.UserID = dataset.UserID
		inferenceModel.OrgID = dataset.OrgID
		inferenceModel.ModelKind = model.ModelKindBase
		inferenceModel.DatasetID = uuid.Nil
		spec := validToolUsingAgentSpec(inferenceModel)
		spec.ToolBindings = []model.ToolBinding{{
			Name:       "http_get",
			Required:   true,
			ToolChoice: "required",
			Config:     []byte(`{}`),
		}}
		endpointID := uuid.New()
		endpoint := &model.PublishedEndpoint{
			EndpointID:    endpointID,
			OrgID:         dataset.OrgID,
			ModelID:       inferenceModel.ModelID,
			Mode:          model.AgentEndpointModeAgent,
			AgentSpecHash: spec.ContentHash,
			DatasetIDs:    []uuid.UUID{dataset.DatasetID},
			MergeStrategy: model.RAGMergeStrategyScoreNormalized,
			Status:        model.PublishedEndpointStatusReady,
		}
		trajectoryRepository := &agentTrajectoryRepositoryStub{}
		requestRepository := &inferenceRequestRepositoryStub{}
		userEvents := &userEventPublisherStub{}
		generator := &generationAdapterStub{toolCalls: []model.ToolCall{{
			ID:        "call-http",
			Name:      "http_get",
			Arguments: []byte(`{"url":"https://example.com"}`),
		}}}
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{model: inferenceModel},
			app.WithPublishedEndpointRepository(&publishedEndpointRepositoryStub{endpoint: endpoint}),
			app.WithAgentSpecRepository(&agentSpecRepositoryStub{spec: spec}),
			app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{datasets: map[uuid.UUID]*model.InferenceDataset{dataset.DatasetID: dataset}}),
			app.WithInferenceRequestRepository(requestRepository),
			app.WithAgentTrajectoryRepository(trajectoryRepository),
			app.WithUserEventPublisher(userEvents),
			app.WithToolInvoker(&toolInvokerStub{
				tools: []model.ToolSpec{{
					Name:       "http_get",
					Parameters: []byte(`{"type":"object"}`),
				}},
				result: model.ToolResult{
					Content:         `{"status":400}`,
					IsError:         true,
					ErrorType:       model.ToolErrorTypePermanent,
					ToolImplVersion: "http_get:test",
				},
			}),
			app.WithGenerationAdapters(map[string]app.GenerationAdapter{
				model.ServingProtocolOpenAIChatCompletions.String(): generator,
			}),
		)

		response, err := uc.GenerateForEndpoint(context.Background(), endpointID, model.GenerateRequest{
			RequestID: uuid.New(),
			UserID:    dataset.UserID,
			OrgID:     dataset.OrgID,
			QueryText: "fetch the tool",
			TopK:      2,
		})

		Expect(err).To(HaveOccurred())
		Expect(response).To(BeNil())
		Expect(requestRepository.request).NotTo(BeNil())
		Expect(requestRepository.request.Status).To(Equal(model.InferenceRequestStatusFailed))
		Expect(trajectoryRepository.invocations).To(HaveLen(1))
		Expect(trajectoryRepository.invocations[0].ErrorType).To(Equal(model.ToolErrorTypePermanent))
		terminal := trajectoryRepository.runs[len(trajectoryRepository.runs)-1]
		Expect(terminal.Status).To(Equal(model.AgentRunStatusFailed))
		Expect(terminal.StopReason).To(Equal(model.AgentStopReasonToolError))
		toolEvent := userEvents.lastEventOfType(userevents.EventTypeAgentToolResult)
		Expect(toolEvent).NotTo(BeNil())
		Expect(toolEvent.Severity).To(Equal(userevents.SeverityError))
		Expect(toolEvent.Status.State).To(Equal("FAILED"))
	})

	It("feeds transient tool errors back so the agent can recover", func() {
		dataset := validInferenceDataset()
		inferenceModel := validInferenceModel()
		inferenceModel.UserID = dataset.UserID
		inferenceModel.OrgID = dataset.OrgID
		inferenceModel.ModelKind = model.ModelKindBase
		inferenceModel.DatasetID = uuid.Nil
		spec := validToolUsingAgentSpec(inferenceModel)
		spec.ToolBindings = []model.ToolBinding{{
			Name:       "http_get",
			Required:   true,
			ToolChoice: "required",
			Config:     []byte(`{}`),
		}}
		endpointID := uuid.New()
		endpoint := &model.PublishedEndpoint{
			EndpointID:    endpointID,
			OrgID:         dataset.OrgID,
			ModelID:       inferenceModel.ModelID,
			Mode:          model.AgentEndpointModeAgent,
			AgentSpecHash: spec.ContentHash,
			DatasetIDs:    []uuid.UUID{dataset.DatasetID},
			MergeStrategy: model.RAGMergeStrategyScoreNormalized,
			Status:        model.PublishedEndpointStatusReady,
		}
		trajectoryRepository := &agentTrajectoryRepositoryStub{}
		requestRepository := &inferenceRequestRepositoryStub{}
		userEvents := &userEventPublisherStub{}
		generator := &generationAdapterStub{results: []model.GenerationResult{
			{
				ToolCalls: []model.ToolCall{{
					ID:        "call-http",
					Name:      "http_get",
					Arguments: []byte(`{"url":"https://example.com"}`),
				}},
				FinishReason: model.GenerationFinishReasonToolCalls,
			},
			{
				Content:      "recovered after the transient tool failure",
				FinishReason: model.GenerationFinishReasonStop,
			},
		}}
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{model: inferenceModel},
			app.WithPublishedEndpointRepository(&publishedEndpointRepositoryStub{endpoint: endpoint}),
			app.WithAgentSpecRepository(&agentSpecRepositoryStub{spec: spec}),
			app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{datasets: map[uuid.UUID]*model.InferenceDataset{dataset.DatasetID: dataset}}),
			app.WithInferenceRequestRepository(requestRepository),
			app.WithAgentTrajectoryRepository(trajectoryRepository),
			app.WithUserEventPublisher(userEvents),
			app.WithToolInvoker(&toolInvokerStub{
				tools: []model.ToolSpec{{
					Name:       "http_get",
					Parameters: []byte(`{"type":"object"}`),
				}},
				result: model.ToolResult{
					Content:         `{"status":503}`,
					IsError:         true,
					ErrorType:       model.ToolErrorTypeTransient,
					ToolImplVersion: "http_get:test",
				},
			}),
			app.WithGenerationAdapters(map[string]app.GenerationAdapter{
				model.ServingProtocolOpenAIChatCompletions.String(): generator,
			}),
		)

		response, err := uc.GenerateForEndpoint(context.Background(), endpointID, model.GenerateRequest{
			RequestID: uuid.New(),
			UserID:    dataset.UserID,
			OrgID:     dataset.OrgID,
			QueryText: "fetch the tool",
			TopK:      2,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(response.Answer).To(Equal("recovered after the transient tool failure"))
		Expect(requestRepository.request).NotTo(BeNil())
		Expect(requestRepository.request.Status).To(Equal(model.InferenceRequestStatusCompleted))
		Expect(trajectoryRepository.invocations).To(HaveLen(1))
		Expect(trajectoryRepository.invocations[0].ErrorType).To(Equal(model.ToolErrorTypeTransient))
		terminal := trajectoryRepository.runs[len(trajectoryRepository.runs)-1]
		Expect(terminal.Status).To(Equal(model.AgentRunStatusCompleted))
		Expect(terminal.StopReason).To(Equal(model.AgentStopReasonFinalAnswer))
		Expect(generator.requests).To(HaveLen(2))
		Expect(generator.requests[1].Messages).To(ContainElement(SatisfyAll(
			HaveField("Role", model.ChatMessageRoleTool),
			HaveField("Content", `{"status":503}`),
		)))
		toolEvent := userEvents.lastEventOfType(userevents.EventTypeAgentToolResult)
		Expect(toolEvent).NotTo(BeNil())
		Expect(toolEvent.Severity).To(Equal(userevents.SeverityWarning))
		Expect(toolEvent.Status.State).To(Equal("FAILED"))
	})

	It("uses the per-step call key for deterministic invocation IDs when tool call IDs are empty", func() {
		dataset := validInferenceDataset()
		inferenceModel := validInferenceModel()
		inferenceModel.UserID = dataset.UserID
		inferenceModel.OrgID = dataset.OrgID
		inferenceModel.ModelKind = model.ModelKindBase
		inferenceModel.DatasetID = uuid.Nil
		spec := validToolUsingAgentSpec(inferenceModel)
		spec.ToolBindings = []model.ToolBinding{{
			Name:       "http_get",
			Required:   true,
			ToolChoice: "required",
			Config:     []byte(`{}`),
		}}
		endpointID := uuid.New()
		endpoint := &model.PublishedEndpoint{
			EndpointID:    endpointID,
			OrgID:         dataset.OrgID,
			ModelID:       inferenceModel.ModelID,
			Mode:          model.AgentEndpointModeAgent,
			AgentSpecHash: spec.ContentHash,
			DatasetIDs:    []uuid.UUID{dataset.DatasetID},
			MergeStrategy: model.RAGMergeStrategyScoreNormalized,
			Status:        model.PublishedEndpointStatusReady,
		}
		trajectoryRepository := &agentTrajectoryRepositoryStub{}
		generator := &generationAdapterStub{results: []model.GenerationResult{
			{
				ToolCalls: []model.ToolCall{
					{Name: "http_get", Arguments: []byte(`{"url":"https://example.com/one"}`)},
					{Name: "http_get", Arguments: []byte(`{"url":"https://example.com/two"}`)},
				},
				FinishReason: model.GenerationFinishReasonToolCalls,
			},
			{
				Content:      "done",
				FinishReason: model.GenerationFinishReasonStop,
			},
		}}
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{model: inferenceModel},
			app.WithPublishedEndpointRepository(&publishedEndpointRepositoryStub{endpoint: endpoint}),
			app.WithAgentSpecRepository(&agentSpecRepositoryStub{spec: spec}),
			app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{datasets: map[uuid.UUID]*model.InferenceDataset{dataset.DatasetID: dataset}}),
			app.WithInferenceRequestRepository(&inferenceRequestRepositoryStub{}),
			app.WithAgentTrajectoryRepository(trajectoryRepository),
			app.WithToolInvoker(&toolInvokerStub{
				tools: []model.ToolSpec{{
					Name:       "http_get",
					Parameters: []byte(`{"type":"object"}`),
				}},
				result: model.ToolResult{
					Content:         `{"status":200}`,
					ToolImplVersion: "http_get:test",
				},
			}),
			app.WithGenerationAdapters(map[string]app.GenerationAdapter{
				model.ServingProtocolOpenAIChatCompletions.String(): generator,
			}),
		)

		response, err := uc.GenerateForEndpoint(context.Background(), endpointID, model.GenerateRequest{
			RequestID: uuid.New(),
			UserID:    dataset.UserID,
			OrgID:     dataset.OrgID,
			QueryText: "fetch twice",
			TopK:      2,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(response.Answer).To(Equal("done"))
		Expect(trajectoryRepository.invocations).To(HaveLen(2))
		Expect(trajectoryRepository.invocations[0].InvocationID).NotTo(Equal(trajectoryRepository.invocations[1].InvocationID))
	})

	It("does not grant system context when a model update is missing actor and org", func() {
		repository := &inferenceModelRepositoryStub{}
		uc := app.NewInferenceUsecase(repository, app.WithCapabilityReportRepository(&capabilityReportRepositoryStub{}))
		inferenceModel := validInferenceModel()
		inferenceModel.UserID = uuid.Nil
		inferenceModel.OrgID = uuid.Nil
		inferenceModel.DatasetID = uuid.Nil

		recorded, err := uc.RecordModelUpdated(context.Background(), inferenceModel, uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Expect(recorded).To(Equal(inferenceModel))
		Expect(ctxutil.IsSystemContext(repository.upsertCtx)).To(BeFalse())
		_, hasOrg := ctxutil.OrgID(repository.upsertCtx)
		Expect(hasOrg).To(BeFalse())
	})

	It("reads a model by id", func() {
		expected := validInferenceModel()
		repository := &inferenceModelRepositoryStub{model: expected}
		uc := app.NewInferenceUsecase(repository)

		readModel, err := uc.ReadModel(context.Background(), expected.OrgID, expected.ModelID)

		Expect(err).NotTo(HaveOccurred())
		Expect(readModel).To(Equal(expected))
		Expect(repository.readUserID).To(Equal(expected.OrgID))
		Expect(repository.readID).To(Equal(expected.ModelID))
	})

	It("records a registry dataset update", func() {
		datasetRepository := &inferenceDatasetRepositoryStub{}
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{},
			app.WithInferenceDatasetRepository(datasetRepository),
		)
		idempotencyKey := uuid.New()

		recorded, err := uc.RecordDatasetUpdated(context.Background(), validInferenceDataset(), idempotencyKey)

		Expect(err).NotTo(HaveOccurred())
		Expect(recorded.DatasetID).To(Equal(datasetRepository.upserted.DatasetID))
		Expect(datasetRepository.idempotencyKey).To(Equal(idempotencyKey))
	})

	It("records inference feedback through the repository", func() {
		feedbackRepository := &inferenceFeedbackRepositoryStub{}
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{},
			app.WithInferenceFeedbackRepository(feedbackRepository),
			app.WithInferenceUnitOfWork(&inferenceUnitOfWorkStub{}),
		)
		idempotencyKey := uuid.New()
		feedback := &model.InferenceFeedback{
			FeedbackID: uuid.New(),
			RequestID:  uuid.New(),
			UserID:     uuid.New(),
			Accepted:   false,
			Rating:     -1,
			Comment:    "not grounded",
		}

		recorded, err := uc.RecordFeedback(context.Background(), feedback, idempotencyKey)

		Expect(err).NotTo(HaveOccurred())
		Expect(recorded).To(Equal(feedback))
		Expect(feedbackRepository.feedback).To(Equal(feedback))
		Expect(feedbackRepository.idempotencyKey).To(Equal(idempotencyKey))
	})

	It("does not export a preference dataset while recording feedback", func() {
		requestID := uuid.New()
		feedbackRepository := &inferenceFeedbackRepositoryStub{preferenceDataset: &model.PreferenceDataset{
			RequestID: requestID,
			DatasetID: uuid.New(),
			ModelID:   uuid.New(),
			Examples:  []model.PreferenceExample{},
		}}
		writer := &preferenceDatasetWriterStub{}
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{},
			app.WithInferenceFeedbackRepository(feedbackRepository),
			app.WithInferenceUnitOfWork(&inferenceUnitOfWorkStub{}),
			app.WithPreferenceDatasetWriter(writer),
		)

		_, err := uc.RecordFeedback(context.Background(), &model.InferenceFeedback{
			FeedbackID: uuid.New(),
			RequestID:  requestID,
			UserID:     uuid.New(),
			Accepted:   true,
			Rating:     1,
		}, uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Consistently(func() *model.PreferenceDataset { return writer.dataset }).Should(BeNil())
		Expect(feedbackRepository.preferenceRequest).To(Equal(model.PreferenceDatasetBuildRequest{}))
	})

	It("builds a preference dataset when explicitly requested with enough complete pairs", func() {
		requestID := uuid.New()
		userID := uuid.New()
		datasetID := uuid.New()
		modelID := uuid.New()
		feedbackRepository := &inferenceFeedbackRepositoryStub{preferenceDataset: &model.PreferenceDataset{
			UserID:                 userID,
			DatasetID:              datasetID,
			ModelID:                modelID,
			ParentModelKind:        model.ModelKindFineTuned,
			ParentArtifactURI:      "s3://models/parent-artifact",
			ParentArtifactChecksum: "sha256:parent",
			ParentAdapterURI:       "s3://models/parent",
			ParentBaseModel:        "mistral-7b",
			ParentModelName:        "dpo-" + modelID.String(),
			ParentLineageName:      "fraud-rag-ranker",
			ParentModelVersion:     7,
			Examples: []model.PreferenceExample{{
				PreferenceExampleID: uuid.New(),
				RequestID:           requestID,
				UserID:              userID,
				DatasetID:           datasetID,
				ModelID:             modelID,
				PromptText:          "prompt",
				AcceptedAnswer:      "chosen",
				RejectedAnswer:      "rejected",
			}},
		}}
		writer := &preferenceDatasetWriterStub{}
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{},
			app.WithInferenceFeedbackRepository(feedbackRepository),
			app.WithInferenceUnitOfWork(&inferenceUnitOfWorkStub{}),
			app.WithPreferenceDatasetWriter(writer),
		)
		dataset, err := uc.BuildPreferenceDataset(context.Background(), model.PreferenceDatasetBuildRequest{
			UserID:      userID,
			OutputURI:   "s3://local-dev-bucket/preferences/{dataset_id}/preference_dataset.jsonl",
			MinExamples: 1,
			Limit:       100,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(dataset.Exported).To(BeTrue())
		Expect(feedbackRepository.preferenceRequest.UserID).To(Equal(userID))
		Expect(feedbackRepository.preferenceRequest.MinExamples).To(Equal(1))
		Expect(feedbackRepository.preferenceRequest.Limit).To(Equal(100))
		Expect(writer.dataset).NotTo(BeNil())
		Expect(writer.dataset.OutputURI).To(ContainSubstring("s3://local-dev-bucket/preferences/" + datasetID.String() + "/preference_dataset-"))
		Expect(writer.dataset.OutputURI).To(HaveSuffix(".jsonl"))
		Expect(writer.dataset.Exported).To(BeTrue())
	})

	It("records a preference dataset snapshot after write succeeds", func() {
		requestID := uuid.New()
		userID := uuid.New()
		datasetID := uuid.New()
		modelID := uuid.New()
		feedbackRepository := &inferenceFeedbackRepositoryStub{preferenceDataset: &model.PreferenceDataset{
			UserID:                 userID,
			DatasetID:              datasetID,
			ModelID:                modelID,
			ParentModelKind:        model.ModelKindFineTuned,
			ParentArtifactURI:      "s3://models/parent-artifact",
			ParentArtifactChecksum: "sha256:parent",
			ParentAdapterURI:       "s3://models/parent",
			ParentBaseModel:        "mistral-7b",
			ParentModelName:        "dpo-" + modelID.String(),
			ParentLineageName:      "fraud-rag-ranker",
			ParentModelVersion:     7,
			Examples: []model.PreferenceExample{{
				PreferenceExampleID: uuid.New(),
				RequestID:           requestID,
				UserID:              userID,
				DatasetID:           datasetID,
				ModelID:             modelID,
				PromptText:          "prompt",
				AcceptedAnswer:      "chosen",
				RejectedAnswer:      "rejected",
			}},
		}}
		writer := &preferenceDatasetWriterStub{}
		unitOfWork := &inferenceUnitOfWorkStub{}
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{},
			app.WithInferenceFeedbackRepository(feedbackRepository),
			app.WithInferenceUnitOfWork(unitOfWork),
			app.WithPreferenceDatasetWriter(writer),
		)

		_, err := uc.BuildPreferenceDataset(context.Background(), model.PreferenceDatasetBuildRequest{
			UserID:      userID,
			OutputURI:   "s3://local-dev-bucket/preferences/{dataset_id}/{preference_dataset_id}.jsonl",
			MinExamples: 1,
			Limit:       100,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(feedbackRepository.recordedSnapshot).NotTo(BeNil())
		Expect(feedbackRepository.recordedSnapshot.PreferenceDatasetID).NotTo(Equal(uuid.Nil))
		Expect(feedbackRepository.recordedSnapshot.UserID).To(Equal(userID))
		Expect(feedbackRepository.recordedSnapshot.OutputURI).To(ContainSubstring("s3://local-dev-bucket/preferences/" + datasetID.String() + "/" + feedbackRepository.recordedSnapshot.PreferenceDatasetID.String()))
		Expect(feedbackRepository.recordedSnapshot.OutputURI).To(HaveSuffix(".jsonl"))
		Expect(feedbackRepository.recordedSnapshot.EvaluationOutputURI).To(ContainSubstring("-eval.jsonl"))
		Expect(feedbackRepository.recordedSnapshot.Format).To(Equal("DPO_JSONL"))
		Expect(feedbackRepository.recordedSnapshot.EligibilityPolicy).To(Equal("complete_rejected_pairs_train_eval_split_v1"))
		Expect(feedbackRepository.snapshotRequest.MinExamples).To(Equal(1))
		Expect(feedbackRepository.snapshotRequest.UserID).To(Equal(userID))
		Expect(feedbackRepository.snapshotRequest.Limit).To(Equal(100))
		Expect(unitOfWork.messages).To(BeEmpty())
	})

	It("freezes the gen-0 eval split in the preference dataset snapshot transaction", func() {
		requestID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
		datasetID := uuid.New()
		modelID := uuid.New()
		evalExampleID := uuid.New()
		trainExampleID := uuid.New()
		feedbackRepository := &inferenceFeedbackRepositoryStub{preferenceDataset: &model.PreferenceDataset{
			RequestID:              requestID,
			UserID:                 userID,
			OrgID:                  orgID,
			DatasetID:              datasetID,
			ModelID:                modelID,
			ParentModelKind:        model.ModelKindFineTuned,
			ParentArtifactURI:      "s3://models/parent-artifact",
			ParentArtifactChecksum: "sha256:parent",
			ParentAdapterURI:       "s3://models/parent",
			ParentBaseModel:        "mistral-7b",
			ParentModelName:        "dpo-" + modelID.String(),
			ParentLineageName:      "fraud-rag-ranker",
			ParentModelVersion:     7,
			Examples: []model.PreferenceExample{{
				PreferenceExampleID: evalExampleID,
				RequestID:           requestID,
				UserID:              userID,
				OrgID:               orgID,
				DatasetID:           datasetID,
				ModelID:             modelID,
				Split:               "EVAL",
				PromptText:          "eval prompt",
				AcceptedAnswer:      "chosen",
				RejectedAnswer:      "rejected",
			}, {
				PreferenceExampleID: trainExampleID,
				RequestID:           requestID,
				UserID:              userID,
				OrgID:               orgID,
				DatasetID:           datasetID,
				ModelID:             modelID,
				Split:               "TRAIN",
				PromptText:          "train prompt",
				AcceptedAnswer:      "chosen",
				RejectedAnswer:      "rejected",
			}},
		}}
		lineageRepository := &lineageEvalSetRepositoryStub{}
		writer := &preferenceDatasetWriterStub{}
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{},
			app.WithInferenceFeedbackRepository(feedbackRepository),
			app.WithLineageEvalSetRepository(lineageRepository),
			app.WithInferenceUnitOfWork(&inferenceUnitOfWorkStub{}),
			app.WithPreferenceDatasetWriter(writer),
		)

		dataset, err := uc.BuildPreferenceDataset(context.Background(), model.PreferenceDatasetBuildRequest{
			UserID:      userID,
			OrgID:       orgID,
			OutputURI:   "s3://local-dev-bucket/preferences/{dataset_id}/{preference_dataset_id}.jsonl",
			MinExamples: 1,
			Limit:       100,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(dataset.Exported).To(BeTrue())
		Expect(lineageRepository.readOrgID).To(Equal(orgID))
		Expect(lineageRepository.readLineage).To(Equal("fraud-rag-ranker"))
		Expect(lineageRepository.frozenSet).NotTo(BeNil())
		Expect(lineageRepository.frozenSet.OrgID).To(Equal(orgID))
		Expect(lineageRepository.frozenSet.LineageName).To(Equal("fraud-rag-ranker"))
		Expect(lineageRepository.frozenSet.EvalDatasetURI).To(Equal(feedbackRepository.recordedSnapshot.EvaluationOutputURI))
		Expect(lineageRepository.frozenSet.Source).To(Equal(model.LineageEvalSetSourceFrozenGen0))
		Expect(lineageRepository.frozenSet.ExampleCount).To(Equal(1))
		Expect(lineageRepository.frozenSet.Checksum).To(HavePrefix("sha256:"))
		Expect(lineageRepository.frozenIDs).To(Equal([]uuid.UUID{evalExampleID}))
	})

	It("does not freeze a gen-0 eval set when the split has no eval examples", func() {
		requestID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
		datasetID := uuid.New()
		modelID := uuid.New()
		feedbackRepository := &inferenceFeedbackRepositoryStub{preferenceDataset: &model.PreferenceDataset{
			RequestID:              requestID,
			UserID:                 userID,
			OrgID:                  orgID,
			DatasetID:              datasetID,
			ModelID:                modelID,
			ParentModelKind:        model.ModelKindFineTuned,
			ParentArtifactURI:      "s3://models/parent-artifact",
			ParentArtifactChecksum: "sha256:parent",
			ParentAdapterURI:       "s3://models/parent",
			ParentBaseModel:        "mistral-7b",
			ParentModelName:        "dpo-" + modelID.String(),
			ParentLineageName:      "fraud-rag-ranker",
			ParentModelVersion:     7,
			Examples: []model.PreferenceExample{{
				PreferenceExampleID: uuid.New(),
				RequestID:           requestID,
				UserID:              userID,
				OrgID:               orgID,
				DatasetID:           datasetID,
				ModelID:             modelID,
				Split:               "TRAIN",
				PromptText:          "train prompt",
				AcceptedAnswer:      "chosen",
				RejectedAnswer:      "rejected",
			}},
		}}
		lineageRepository := &lineageEvalSetRepositoryStub{}
		writer := &preferenceDatasetWriterStub{}
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{},
			app.WithInferenceFeedbackRepository(feedbackRepository),
			app.WithLineageEvalSetRepository(lineageRepository),
			app.WithInferenceUnitOfWork(&inferenceUnitOfWorkStub{}),
			app.WithPreferenceDatasetWriter(writer),
		)

		dataset, err := uc.BuildPreferenceDataset(context.Background(), model.PreferenceDatasetBuildRequest{
			UserID:      userID,
			OrgID:       orgID,
			OutputURI:   "s3://local-dev-bucket/preferences/{dataset_id}/{preference_dataset_id}.jsonl",
			MinExamples: 1,
			Limit:       100,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(dataset.Exported).To(BeTrue())
		Expect(lineageRepository.readLineage).To(Equal("fraud-rag-ranker"))
		Expect(lineageRepository.frozenSet).To(BeNil())
		Expect(feedbackRepository.recordedSnapshot).NotTo(BeNil())
		Expect(feedbackRepository.recordedSnapshot.EvaluationOutputURI).To(HaveSuffix("-eval.jsonl"))
	})

	It("reuses an active frozen eval set and routes new examples to train", func() {
		requestID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
		datasetID := uuid.New()
		modelID := uuid.New()
		feedbackRepository := &inferenceFeedbackRepositoryStub{preferenceDataset: &model.PreferenceDataset{
			RequestID:              requestID,
			UserID:                 userID,
			OrgID:                  orgID,
			DatasetID:              datasetID,
			ModelID:                modelID,
			ParentModelKind:        model.ModelKindFineTuned,
			ParentArtifactURI:      "s3://models/parent-artifact",
			ParentArtifactChecksum: "sha256:parent",
			ParentAdapterURI:       "s3://models/parent",
			ParentBaseModel:        "mistral-7b",
			ParentModelName:        "dpo-" + modelID.String(),
			ParentLineageName:      "fraud-rag-ranker",
			ParentModelVersion:     7,
			Examples: []model.PreferenceExample{{
				PreferenceExampleID: uuid.New(),
				RequestID:           requestID,
				UserID:              userID,
				OrgID:               orgID,
				DatasetID:           datasetID,
				ModelID:             modelID,
				Split:               "EVAL",
				PromptText:          "new prompt",
				AcceptedAnswer:      "chosen",
				RejectedAnswer:      "rejected",
			}},
		}}
		lineageRepository := &lineageEvalSetRepositoryStub{activeEvalSet: &model.LineageEvalSet{
			OrgID:          orgID,
			LineageName:    "fraud-rag-ranker",
			Version:        1,
			EvalDatasetURI: "s3://local-dev-bucket/preferences/frozen-eval.jsonl",
			Source:         model.LineageEvalSetSourceFrozenGen0,
			Active:         true,
		}}
		writer := &preferenceDatasetWriterStub{}
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{},
			app.WithInferenceFeedbackRepository(feedbackRepository),
			app.WithLineageEvalSetRepository(lineageRepository),
			app.WithInferenceUnitOfWork(&inferenceUnitOfWorkStub{}),
			app.WithPreferenceDatasetWriter(writer),
		)

		dataset, err := uc.BuildPreferenceDataset(context.Background(), model.PreferenceDatasetBuildRequest{
			UserID:      userID,
			OrgID:       orgID,
			OutputURI:   "s3://local-dev-bucket/preferences/{dataset_id}/{preference_dataset_id}.jsonl",
			MinExamples: 1,
			Limit:       100,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(dataset.PreferenceDatasetID).NotTo(Equal(uuid.Nil))
		Expect(dataset.IntegrityKey).To(MatchRegexp(`^sha256:[0-9a-f]{64}$`))
		Expect(dataset.IntegrityKey).NotTo(Equal(dataset.PreferenceDatasetID.String()))
		Expect(dataset.OutputURI).To(ContainSubstring(dataset.PreferenceDatasetID.String()))
		Expect(dataset.EvaluationOutputURI).To(Equal("s3://local-dev-bucket/preferences/frozen-eval.jsonl"))
		Expect(writer.dataset).NotTo(BeNil())
		Expect(writer.dataset.EvaluationExampleCount()).To(Equal(0))
		Expect(writer.dataset.TrainingExampleCount()).To(Equal(1))
		Expect(lineageRepository.frozenSet).To(BeNil())
		Expect(feedbackRepository.recordedSnapshot.PreferenceDatasetID).To(Equal(dataset.PreferenceDatasetID))
		Expect(feedbackRepository.recordedSnapshot.IntegrityKey).To(Equal(dataset.IntegrityKey))
		Expect(feedbackRepository.recordedSnapshot.OutputURI).To(ContainSubstring(dataset.PreferenceDatasetID.String()))
		Expect(feedbackRepository.recordedSnapshot.EvaluationOutputURI).To(Equal("s3://local-dev-bucket/preferences/frozen-eval.jsonl"))
	})

	It("reuses a curated eval set when one is active", func() {
		requestID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
		datasetID := uuid.New()
		modelID := uuid.New()
		feedbackRepository := &inferenceFeedbackRepositoryStub{preferenceDataset: &model.PreferenceDataset{
			RequestID:              requestID,
			UserID:                 userID,
			OrgID:                  orgID,
			DatasetID:              datasetID,
			ModelID:                modelID,
			ParentModelKind:        model.ModelKindBase,
			ParentArtifactURI:      "s3://models/base",
			ParentArtifactChecksum: "sha256:base",
			ParentBaseModel:        "llama3",
			ParentModelName:        "shared-base",
			ParentModelVersion:     1,
			Examples: []model.PreferenceExample{{
				PreferenceExampleID: uuid.New(),
				RequestID:           requestID,
				UserID:              userID,
				OrgID:               orgID,
				DatasetID:           datasetID,
				ModelID:             modelID,
				Split:               "EVAL",
				PromptText:          "new prompt",
				AcceptedAnswer:      "chosen",
				RejectedAnswer:      "rejected",
			}},
		}}
		lineageRepository := &lineageEvalSetRepositoryStub{activeEvalSet: &model.LineageEvalSet{
			OrgID:          orgID,
			LineageName:    "shared-base",
			Version:        3,
			EvalDatasetURI: "s3://curated/held-out.jsonl",
			Source:         model.LineageEvalSetSourceCurated,
			Active:         true,
		}}
		writer := &preferenceDatasetWriterStub{}
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{},
			app.WithInferenceFeedbackRepository(feedbackRepository),
			app.WithLineageEvalSetRepository(lineageRepository),
			app.WithInferenceUnitOfWork(&inferenceUnitOfWorkStub{}),
			app.WithPreferenceDatasetWriter(writer),
		)

		dataset, err := uc.BuildPreferenceDataset(context.Background(), model.PreferenceDatasetBuildRequest{
			UserID:      userID,
			OrgID:       orgID,
			OutputURI:   "s3://local-dev-bucket/preferences/{dataset_id}/{preference_dataset_id}.jsonl",
			MinExamples: 1,
			Limit:       100,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(dataset.EvaluationOutputURI).To(Equal("s3://curated/held-out.jsonl"))
		Expect(writer.dataset.EvaluationExampleCount()).To(Equal(0))
		Expect(writer.dataset.TrainingExampleCount()).To(Equal(1))
		Expect(lineageRepository.readLineage).To(Equal("shared-base"))
		Expect(lineageRepository.frozenSet).To(BeNil())
	})

	It("does not write a preference dataset before the configured threshold is met", func() {
		userID := uuid.New()
		feedbackRepository := &inferenceFeedbackRepositoryStub{preferenceDataset: &model.PreferenceDataset{
			UserID:    userID,
			DatasetID: uuid.New(),
			ModelID:   uuid.New(),
			Examples:  []model.PreferenceExample{},
		}}
		writer := &preferenceDatasetWriterStub{}
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{},
			app.WithInferenceFeedbackRepository(feedbackRepository),
			app.WithInferenceUnitOfWork(&inferenceUnitOfWorkStub{}),
			app.WithPreferenceDatasetWriter(writer),
		)

		dataset, err := uc.BuildPreferenceDataset(context.Background(), model.PreferenceDatasetBuildRequest{
			UserID:      userID,
			OutputURI:   "s3://local-dev-bucket/preferences/{dataset_id}/preference_dataset.jsonl",
			MinExamples: 1,
			Limit:       100,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(dataset.ExampleCount()).To(Equal(0))
		Expect(writer.dataset).To(BeNil())
	})

	It("uses total eligible examples for the preference export threshold", func() {
		requestID := uuid.New()
		userID := uuid.New()
		datasetID := uuid.New()
		modelID := uuid.New()
		feedbackRepository := &inferenceFeedbackRepositoryStub{preferenceDataset: &model.PreferenceDataset{
			RequestID:              requestID,
			UserID:                 userID,
			DatasetID:              datasetID,
			ModelID:                modelID,
			ParentModelKind:        model.ModelKindFineTuned,
			ParentArtifactURI:      "s3://models/parent-artifact",
			ParentArtifactChecksum: "sha256:parent",
			ParentAdapterURI:       "s3://models/parent",
			ParentBaseModel:        "mistral-7b",
			ParentModelVersion:     7,
			Examples: []model.PreferenceExample{{
				PreferenceExampleID: uuid.New(),
				RequestID:           requestID,
				UserID:              userID,
				DatasetID:           datasetID,
				ModelID:             modelID,
				Split:               "EVAL",
				PromptText:          "prompt",
				AcceptedAnswer:      "chosen",
				RejectedAnswer:      "rejected",
			}},
		}}
		writer := &preferenceDatasetWriterStub{}
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{},
			app.WithInferenceFeedbackRepository(feedbackRepository),
			app.WithInferenceUnitOfWork(&inferenceUnitOfWorkStub{}),
			app.WithPreferenceDatasetWriter(writer),
		)

		dataset, err := uc.BuildPreferenceDataset(context.Background(), model.PreferenceDatasetBuildRequest{
			UserID:      userID,
			OutputURI:   "s3://local-dev-bucket/preferences/{dataset_id}/preference_dataset.jsonl",
			MinExamples: 1,
			Limit:       100,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(dataset.ExampleCount()).To(Equal(1))
		Expect(dataset.TrainingExampleCount()).To(Equal(0))
		Expect(writer.dataset).NotTo(BeNil())
		Expect(writer.dataset.ExampleCount()).To(Equal(1))
	})

	It("generates from retrieved RAG contexts", func() {
		dataset := validInferenceDataset()
		inferenceModel := validInferenceModel()
		inferenceModel.UserID = dataset.UserID
		inferenceModel.OrgID = dataset.OrgID
		inferenceModel.DatasetID = dataset.DatasetID
		modelRepository := &inferenceModelRepositoryStub{model: inferenceModel}
		datasetRepository := &inferenceDatasetRepositoryStub{dataset: dataset}
		requestRepository := &inferenceRequestRepositoryStub{}
		retrieval := &retrievalClientStub{contexts: []model.RetrievedContext{{
			EmbeddingRecordID:   uuid.New(),
			EmbeddingSnapshotID: dataset.EmbeddingSnapshotID,
			ChunkIndex:          2,
			SourceText:          "retrieved context",
			Similarity:          0.87,
		}}}
		generator := &generationAdapterStub{}
		promptStrategy := testPromptStrategy()
		uc := app.NewInferenceUsecase(
			modelRepository,
			app.WithInferenceDatasetRepository(datasetRepository),
			app.WithInferenceRequestRepository(requestRepository),
			app.WithRetrievalClient(retrieval),
			app.WithGenerationAdapters(map[string]app.GenerationAdapter{
				model.ServingProtocolOpenAIChatCompletions.String(): generator,
			}),
			app.WithPromptStrategy(promptStrategy),
			app.WithContextPacker(app.NewContextWindowPacker(promptStrategy)),
			app.WithPromptBuilder(app.NewDefaultPromptBuilder(promptStrategy)),
		)
		requestID := uuid.New()

		response, err := uc.Generate(context.Background(), model.GenerateRequest{
			RequestID:       requestID,
			UserID:          dataset.UserID,
			OrgID:           dataset.OrgID,
			DatasetID:       dataset.DatasetID,
			ModelID:         inferenceModel.ModelID,
			QueryText:       "what happened?",
			TopK:            8,
			MetadataFilters: map[string]string{"source": "manual"},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(datasetRepository.readUserID).To(Equal(dataset.OrgID))
		Expect(datasetRepository.readID).To(Equal(dataset.DatasetID))
		Expect(modelRepository.readUserID).To(Equal(dataset.OrgID))
		Expect(modelRepository.readID).To(Equal(inferenceModel.ModelID))
		Expect(retrieval.userID).To(Equal(dataset.UserID))
		Expect(retrieval.datasetID).To(Equal(dataset.DatasetID))
		Expect(retrieval.queryText).To(Equal("what happened?"))
		Expect(retrieval.topK).To(Equal(8))
		Expect(retrieval.metadataFilters).To(Equal(map[string]string{"source": "manual"}))
		Expect(generator.request.Dataset).To(Equal(dataset))
		Expect(generator.request.Model).To(Equal(inferenceModel))
		Expect(generator.request.RequestID).To(Equal(requestID))
		Expect(generator.request.Prompt).To(ContainSubstring("Retrieved context"))
		Expect(generator.request.PromptStrategyVersion).To(Equal("test-rag-prompt-v1"))
		Expect(response.Answer).To(Equal("generated answer"))
		Expect(response.RequestID).To(Equal(requestID))
		Expect(response.PromptStrategyVersion).To(Equal("test-rag-prompt-v1"))
		Expect(response.GenerationProtocol).To(Equal(model.ServingProtocolOpenAIChatCompletions.String()))
		Expect(response.GenerationModel).To(Equal(inferenceModel.ServingModel))
		Expect(response.Contexts).To(HaveLen(1))
		Expect(requestRepository.request).NotTo(BeNil())
		Expect(requestRepository.request.UserID).To(Equal(dataset.UserID))
		Expect(requestRepository.request.OrgID).To(Equal(dataset.OrgID))
		Expect(requestRepository.request.RequestID).To(Equal(requestID))
		Expect(requestRepository.request.Status).To(Equal(model.InferenceRequestStatusCompleted))
		Expect(requestRepository.request.GenerationProtocol).To(Equal(model.ServingProtocolOpenAIChatCompletions.String()))
		Expect(requestRepository.request.GenerationModel).To(Equal(inferenceModel.ServingModel))
		Expect(requestRepository.request.PromptText).To(ContainSubstring("Retrieved context"))
		Expect(requestRepository.request.PromptText).To(ContainSubstring("retrieved context"))
		Expect(requestRepository.request.AnswerText).To(Equal("generated answer"))
		Expect(requestRepository.request.RetrievedContexts).NotTo(BeEmpty())
		var auditedContexts []struct {
			EmbeddingRecordID   string `json:"embedding_record_id"`
			EmbeddingSnapshotID string `json:"embedding_snapshot_id"`
			SourceText          string `json:"source_text"`
		}
		Expect(json.Unmarshal([]byte(requestRepository.request.RetrievedContexts), &auditedContexts)).To(Succeed())
		Expect(auditedContexts).To(HaveLen(1))
		Expect(auditedContexts[0].SourceText).To(Equal("retrieved context"))
		Expect(auditedContexts[0].EmbeddingSnapshotID).To(Equal(dataset.EmbeddingSnapshotID.String()))
	})

	It("uses query transformer output for retrieval without changing the generated question", func() {
		dataset := validInferenceDataset()
		inferenceModel := validInferenceModel()
		inferenceModel.UserID = dataset.UserID
		inferenceModel.DatasetID = dataset.DatasetID
		retrieval := &retrievalClientStub{contexts: []model.RetrievedContext{{
			EmbeddingRecordID:   uuid.New(),
			EmbeddingSnapshotID: dataset.EmbeddingSnapshotID,
			ChunkIndex:          1,
			SourceText:          "retrieved context",
			Similarity:          0.91,
		}}}
		transformer := &queryTransformerStub{result: &model.QueryTransformResult{
			QueryText:       "semantic query",
			MetadataFilters: map[string]string{"section": "risk", "source": "generated"},
		}}
		generator := &generationAdapterStub{}
		promptStrategy := testPromptStrategy()
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{model: inferenceModel},
			app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{dataset: dataset}),
			app.WithInferenceRequestRepository(&inferenceRequestRepositoryStub{}),
			app.WithRetrievalClient(retrieval),
			app.WithQueryTransformer(transformer),
			app.WithQueryTransformerTimeout(17*time.Second),
			app.WithGenerationAdapters(map[string]app.GenerationAdapter{
				model.ServingProtocolOpenAIChatCompletions.String(): generator,
			}),
			app.WithPromptStrategy(promptStrategy),
			app.WithContextPacker(app.NewContextWindowPacker(promptStrategy)),
			app.WithPromptBuilder(app.NewDefaultPromptBuilder(promptStrategy)),
		)

		response, err := uc.Generate(context.Background(), model.GenerateRequest{
			RequestID:       uuid.New(),
			UserID:          dataset.UserID,
			DatasetID:       dataset.DatasetID,
			ModelID:         inferenceModel.ModelID,
			QueryText:       "original question",
			TopK:            3,
			MetadataFilters: map[string]string{"source": "manual"},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(transformer.request.QueryText).To(Equal("original question"))
		Expect(transformer.request.UserID).To(Equal(dataset.UserID))
		Expect(transformer.deadlineSet).To(BeTrue())
		Expect(time.Until(transformer.deadline)).To(BeNumerically(">", 15*time.Second))
		Expect(retrieval.userID).To(Equal(dataset.UserID))
		Expect(retrieval.queryText).To(Equal("semantic query"))
		Expect(retrieval.metadataFilters).To(Equal(map[string]string{"section": "risk", "source": "generated"}))
		Expect(generator.request.Query).To(Equal("original question"))
		Expect(response.QueryText).To(Equal("original question"))
	})

	It("uses the ready endpoint dataset that supplied retrieved context for prompt, response, and audit", func() {
		notReadyDataset := validInferenceDataset()
		notReadyDataset.ProcessingState = model.DatasetProcessingFeatureMaterialized
		readyDataset := validInferenceDataset()
		readyDataset.UserID = notReadyDataset.UserID
		readyDataset.OrgID = notReadyDataset.OrgID
		inferenceModel := validInferenceModel()
		inferenceModel.UserID = readyDataset.UserID
		inferenceModel.OrgID = readyDataset.OrgID
		inferenceModel.ModelKind = model.ModelKindBase
		inferenceModel.DatasetID = uuid.Nil
		endpointID := uuid.New()
		endpoint := &model.PublishedEndpoint{
			EndpointID:    endpointID,
			OrgID:         readyDataset.OrgID,
			ModelID:       inferenceModel.ModelID,
			DatasetIDs:    []uuid.UUID{notReadyDataset.DatasetID, readyDataset.DatasetID},
			MergeStrategy: model.RAGMergeStrategyScoreNormalized,
			Status:        model.PublishedEndpointStatusReady,
		}
		requestRepository := &inferenceRequestRepositoryStub{}
		generator := &generationAdapterStub{}
		retrieval := &retrievalClientStub{contextsByDataset: map[uuid.UUID][]model.RetrievedContext{
			readyDataset.DatasetID: {{
				EmbeddingRecordID:   uuid.New(),
				EmbeddingSnapshotID: readyDataset.EmbeddingSnapshotID,
				DatasetID:           readyDataset.DatasetID,
				ChunkIndex:          1,
				SourceText:          "ready endpoint context",
				Similarity:          0.91,
			}},
		}}
		promptStrategy := testPromptStrategy()
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{model: inferenceModel},
			app.WithPublishedEndpointRepository(&publishedEndpointRepositoryStub{endpoint: endpoint}),
			app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{datasets: map[uuid.UUID]*model.InferenceDataset{
				notReadyDataset.DatasetID: notReadyDataset,
				readyDataset.DatasetID:    readyDataset,
			}}),
			app.WithInferenceRequestRepository(requestRepository),
			app.WithRetrievalClient(retrieval),
			app.WithDefaultRAGMergeStrategy(model.RAGMergeStrategyScoreNormalized),
			app.WithGenerationAdapters(map[string]app.GenerationAdapter{
				model.ServingProtocolOpenAIChatCompletions.String(): generator,
			}),
			app.WithPromptStrategy(promptStrategy),
			app.WithContextPacker(app.NewContextWindowPacker(promptStrategy)),
			app.WithPromptBuilder(app.NewDefaultPromptBuilder(promptStrategy)),
		)

		response, err := uc.GenerateForEndpoint(context.Background(), endpointID, model.GenerateRequest{
			RequestID: uuid.New(),
			UserID:    readyDataset.UserID,
			OrgID:     readyDataset.OrgID,
			ModelID:   inferenceModel.ModelID,
			QueryText: "query",
			TopK:      2,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(response.DatasetID).To(Equal(readyDataset.DatasetID))
		Expect(response.DatasetIDs).To(Equal([]uuid.UUID{readyDataset.DatasetID}))
		Expect(generator.request.Dataset).To(Equal(readyDataset))
		Expect(generator.request.Prompt).To(ContainSubstring(readyDataset.DatasetID.String()))
		Expect(generator.request.Prompt).NotTo(ContainSubstring(notReadyDataset.DatasetID.String()))
		Expect(requestRepository.request).NotTo(BeNil())
		Expect(requestRepository.request.DatasetID).To(Equal(readyDataset.DatasetID))
		Expect(requestRepository.request.EmbeddingSnapshotID).To(Equal(readyDataset.EmbeddingSnapshotID))
		Expect(retrieval.calls).To(Equal([]uuid.UUID{readyDataset.DatasetID}))
	})

	It("continues endpoint generation when one ready dataset retrieval fails and another succeeds", func() {
		failedDataset := validInferenceDataset()
		successDataset := validInferenceDataset()
		successDataset.UserID = failedDataset.UserID
		successDataset.OrgID = failedDataset.OrgID
		inferenceModel := validInferenceModel()
		inferenceModel.UserID = successDataset.UserID
		inferenceModel.OrgID = successDataset.OrgID
		inferenceModel.ModelKind = model.ModelKindBase
		inferenceModel.DatasetID = uuid.Nil
		endpointID := uuid.New()
		endpoint := &model.PublishedEndpoint{
			EndpointID:    endpointID,
			OrgID:         successDataset.OrgID,
			ModelID:       inferenceModel.ModelID,
			DatasetIDs:    []uuid.UUID{failedDataset.DatasetID, successDataset.DatasetID},
			MergeStrategy: model.RAGMergeStrategyScoreNormalized,
			Status:        model.PublishedEndpointStatusReady,
		}
		retrieval := &retrievalClientStub{
			errorsByDataset: map[uuid.UUID]error{failedDataset.DatasetID: errors.New("vector store unavailable")},
			contextsByDataset: map[uuid.UUID][]model.RetrievedContext{
				successDataset.DatasetID: {{
					EmbeddingRecordID:   uuid.New(),
					EmbeddingSnapshotID: successDataset.EmbeddingSnapshotID,
					DatasetID:           successDataset.DatasetID,
					ChunkIndex:          7,
					SourceText:          "successful dataset context",
					Similarity:          0.88,
				}},
			},
		}
		promptStrategy := testPromptStrategy()
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{model: inferenceModel},
			app.WithPublishedEndpointRepository(&publishedEndpointRepositoryStub{endpoint: endpoint}),
			app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{datasets: map[uuid.UUID]*model.InferenceDataset{
				failedDataset.DatasetID:  failedDataset,
				successDataset.DatasetID: successDataset,
			}}),
			app.WithInferenceRequestRepository(&inferenceRequestRepositoryStub{}),
			app.WithRetrievalClient(retrieval),
			app.WithDefaultRAGMergeStrategy(model.RAGMergeStrategyScoreNormalized),
			app.WithGenerationAdapters(map[string]app.GenerationAdapter{
				model.ServingProtocolOpenAIChatCompletions.String(): &generationAdapterStub{},
			}),
			app.WithPromptStrategy(promptStrategy),
			app.WithContextPacker(app.NewContextWindowPacker(promptStrategy)),
			app.WithPromptBuilder(app.NewDefaultPromptBuilder(promptStrategy)),
		)

		response, err := uc.GenerateForEndpoint(context.Background(), endpointID, model.GenerateRequest{
			RequestID: uuid.New(),
			UserID:    successDataset.UserID,
			OrgID:     successDataset.OrgID,
			ModelID:   inferenceModel.ModelID,
			QueryText: "query",
			TopK:      2,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(response.Contexts).To(HaveLen(1))
		Expect(response.Contexts[0].DatasetID).To(Equal(successDataset.DatasetID))
		Expect(retrieval.calls).To(ConsistOf(failedDataset.DatasetID, successDataset.DatasetID))
	})

	It("fails endpoint generation when all ready dataset retrievals fail", func() {
		dataset := validInferenceDataset()
		inferenceModel := validInferenceModel()
		inferenceModel.UserID = dataset.UserID
		inferenceModel.OrgID = dataset.OrgID
		inferenceModel.ModelKind = model.ModelKindBase
		inferenceModel.DatasetID = uuid.Nil
		endpointID := uuid.New()
		endpoint := &model.PublishedEndpoint{
			EndpointID:    endpointID,
			OrgID:         dataset.OrgID,
			ModelID:       inferenceModel.ModelID,
			DatasetIDs:    []uuid.UUID{dataset.DatasetID},
			MergeStrategy: model.RAGMergeStrategyScoreNormalized,
			Status:        model.PublishedEndpointStatusReady,
		}
		requestRepository := &inferenceRequestRepositoryStub{}
		promptStrategy := testPromptStrategy()
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{model: inferenceModel},
			app.WithPublishedEndpointRepository(&publishedEndpointRepositoryStub{endpoint: endpoint}),
			app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{datasets: map[uuid.UUID]*model.InferenceDataset{dataset.DatasetID: dataset}}),
			app.WithInferenceRequestRepository(requestRepository),
			app.WithRetrievalClient(&retrievalClientStub{errorsByDataset: map[uuid.UUID]error{dataset.DatasetID: errors.New("vector store unavailable")}}),
			app.WithDefaultRAGMergeStrategy(model.RAGMergeStrategyScoreNormalized),
			app.WithGenerationAdapters(map[string]app.GenerationAdapter{
				model.ServingProtocolOpenAIChatCompletions.String(): &generationAdapterStub{},
			}),
			app.WithPromptStrategy(promptStrategy),
			app.WithContextPacker(app.NewContextWindowPacker(promptStrategy)),
			app.WithPromptBuilder(app.NewDefaultPromptBuilder(promptStrategy)),
		)

		_, err := uc.GenerateForEndpoint(context.Background(), endpointID, model.GenerateRequest{
			RequestID: uuid.New(),
			UserID:    dataset.UserID,
			OrgID:     dataset.OrgID,
			ModelID:   inferenceModel.ModelID,
			QueryText: "query",
			TopK:      2,
		})

		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrRetrievalFailed)).To(BeTrue())
		Expect(requestRepository.request).NotTo(BeNil())
		Expect(requestRepository.request.Status).To(Equal(model.InferenceRequestStatusFailed))
	})

	It("score-normalized endpoint merge uses a global candidate scale instead of dataset order", func() {
		firstDataset := validInferenceDataset()
		secondDataset := validInferenceDataset()
		secondDataset.UserID = firstDataset.UserID
		secondDataset.OrgID = firstDataset.OrgID
		inferenceModel := validInferenceModel()
		inferenceModel.UserID = firstDataset.UserID
		inferenceModel.OrgID = firstDataset.OrgID
		inferenceModel.ModelKind = model.ModelKindBase
		inferenceModel.DatasetID = uuid.Nil
		endpointID := uuid.New()
		endpoint := &model.PublishedEndpoint{
			EndpointID:    endpointID,
			OrgID:         firstDataset.OrgID,
			ModelID:       inferenceModel.ModelID,
			DatasetIDs:    []uuid.UUID{firstDataset.DatasetID, secondDataset.DatasetID},
			MergeStrategy: model.RAGMergeStrategyScoreNormalized,
			Status:        model.PublishedEndpointStatusReady,
		}
		retrieval := &retrievalClientStub{contextsByDataset: map[uuid.UUID][]model.RetrievedContext{
			firstDataset.DatasetID: {
				{EmbeddingRecordID: uuid.New(), EmbeddingSnapshotID: firstDataset.EmbeddingSnapshotID, DatasetID: firstDataset.DatasetID, ChunkIndex: 1, SourceText: "low relevance first", Similarity: 0.10},
				{EmbeddingRecordID: uuid.New(), EmbeddingSnapshotID: firstDataset.EmbeddingSnapshotID, DatasetID: firstDataset.DatasetID, ChunkIndex: 2, SourceText: "lower relevance first", Similarity: 0.09},
			},
			secondDataset.DatasetID: {
				{EmbeddingRecordID: uuid.New(), EmbeddingSnapshotID: secondDataset.EmbeddingSnapshotID, DatasetID: secondDataset.DatasetID, ChunkIndex: 3, SourceText: "high relevance second", Similarity: 0.95},
			},
		}}
		promptStrategy := testPromptStrategy()
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{model: inferenceModel},
			app.WithPublishedEndpointRepository(&publishedEndpointRepositoryStub{endpoint: endpoint}),
			app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{datasets: map[uuid.UUID]*model.InferenceDataset{
				firstDataset.DatasetID:  firstDataset,
				secondDataset.DatasetID: secondDataset,
			}}),
			app.WithInferenceRequestRepository(&inferenceRequestRepositoryStub{}),
			app.WithRetrievalClient(retrieval),
			app.WithDefaultRAGMergeStrategy(model.RAGMergeStrategyScoreNormalized),
			app.WithGenerationAdapters(map[string]app.GenerationAdapter{
				model.ServingProtocolOpenAIChatCompletions.String(): &generationAdapterStub{},
			}),
			app.WithPromptStrategy(promptStrategy),
			app.WithContextPacker(app.NewContextWindowPacker(promptStrategy)),
			app.WithPromptBuilder(app.NewDefaultPromptBuilder(promptStrategy)),
		)

		response, err := uc.GenerateForEndpoint(context.Background(), endpointID, model.GenerateRequest{
			RequestID: uuid.New(),
			UserID:    firstDataset.UserID,
			OrgID:     firstDataset.OrgID,
			ModelID:   inferenceModel.ModelID,
			QueryText: "query",
			TopK:      1,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(response.Contexts).To(HaveLen(1))
		Expect(response.Contexts[0].DatasetID).To(Equal(secondDataset.DatasetID))
		Expect(response.Contexts[0].RerankScore).To(Equal(1.0))
	})

	It("falls back to raw retrieval when query transformation fails", func() {
		dataset := validInferenceDataset()
		inferenceModel := validInferenceModel()
		inferenceModel.UserID = dataset.UserID
		inferenceModel.DatasetID = dataset.DatasetID
		retrieval := &retrievalClientStub{contexts: []model.RetrievedContext{{
			EmbeddingRecordID:   uuid.New(),
			EmbeddingSnapshotID: dataset.EmbeddingSnapshotID,
			ChunkIndex:          1,
			SourceText:          "retrieved context",
			Similarity:          0.91,
		}}}
		transformer := &queryTransformerStub{err: errors.New("planner unavailable")}
		generator := &generationAdapterStub{}
		promptStrategy := testPromptStrategy()
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{model: inferenceModel},
			app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{dataset: dataset}),
			app.WithInferenceRequestRepository(&inferenceRequestRepositoryStub{}),
			app.WithRetrievalClient(retrieval),
			app.WithQueryTransformer(transformer),
			app.WithGenerationAdapters(map[string]app.GenerationAdapter{
				model.ServingProtocolOpenAIChatCompletions.String(): generator,
			}),
			app.WithPromptStrategy(promptStrategy),
			app.WithContextPacker(app.NewContextWindowPacker(promptStrategy)),
			app.WithPromptBuilder(app.NewDefaultPromptBuilder(promptStrategy)),
		)

		response, err := uc.Generate(context.Background(), model.GenerateRequest{
			RequestID: uuid.New(),
			UserID:    dataset.UserID,
			DatasetID: dataset.DatasetID,
			ModelID:   inferenceModel.ModelID,
			QueryText: "original question",
			TopK:      3,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(retrieval.queryText).To(Equal("original question"))
		Expect(response.QueryText).To(Equal("original question"))
	})

	DescribeTable("reranking",
		func(rerankerEnabled bool, expectedRetrievalTopK int, expectedResponseChunks []int) {
			dataset := validInferenceDataset()
			inferenceModel := validInferenceModel()
			inferenceModel.UserID = dataset.UserID
			inferenceModel.DatasetID = dataset.DatasetID
			retrieved := []model.RetrievedContext{
				{EmbeddingRecordID: uuid.New(), EmbeddingSnapshotID: dataset.EmbeddingSnapshotID, ChunkIndex: 1, SourceText: "first", Similarity: 0.70},
				{EmbeddingRecordID: uuid.New(), EmbeddingSnapshotID: dataset.EmbeddingSnapshotID, ChunkIndex: 2, SourceText: "second", Similarity: 0.68},
				{EmbeddingRecordID: uuid.New(), EmbeddingSnapshotID: dataset.EmbeddingSnapshotID, ChunkIndex: 3, SourceText: "third", Similarity: 0.65},
				{EmbeddingRecordID: uuid.New(), EmbeddingSnapshotID: dataset.EmbeddingSnapshotID, ChunkIndex: 4, SourceText: "fourth", Similarity: 0.60},
			}
			retrieval := &retrievalClientStub{contexts: retrieved}
			reranker := &rerankerStub{contexts: []model.RetrievedContext{
				withRerankScore(retrieved[2], 0.99),
				withRerankScore(retrieved[0], 0.90),
			}}
			promptStrategy := testPromptStrategy()
			options := []app.InferenceOption{
				app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{dataset: dataset}),
				app.WithInferenceRequestRepository(&inferenceRequestRepositoryStub{}),
				app.WithRetrievalClient(retrieval),
				app.WithGenerationAdapters(map[string]app.GenerationAdapter{
					model.ServingProtocolOpenAIChatCompletions.String(): &generationAdapterStub{},
				}),
				app.WithPromptStrategy(promptStrategy),
				app.WithContextPacker(app.NewContextWindowPacker(promptStrategy)),
				app.WithPromptBuilder(app.NewDefaultPromptBuilder(promptStrategy)),
			}
			if rerankerEnabled {
				options = append(options, app.WithReranker(reranker), app.WithRerankCandidateMultiplier(3))
			}
			uc := app.NewInferenceUsecase(&inferenceModelRepositoryStub{model: inferenceModel}, options...)

			response, err := uc.Generate(context.Background(), model.GenerateRequest{
				RequestID: uuid.New(),
				UserID:    dataset.UserID,
				DatasetID: dataset.DatasetID,
				ModelID:   inferenceModel.ModelID,
				QueryText: "query",
				TopK:      2,
			})

			Expect(err).NotTo(HaveOccurred())
			Expect(retrieval.topK).To(Equal(expectedRetrievalTopK))
			if rerankerEnabled {
				Expect(reranker.query).To(Equal("query"))
				Expect(reranker.topK).To(Equal(2))
				Expect(reranker.candidates).To(Equal(retrieved))
			}
			Expect(response.Contexts).To(HaveLen(len(expectedResponseChunks)))
			for i, chunkIndex := range expectedResponseChunks {
				Expect(response.Contexts[i].ChunkIndex).To(Equal(chunkIndex))
			}
			if rerankerEnabled {
				Expect(response.Contexts[0].RerankScore).To(Equal(0.99))
				Expect(response.Contexts[0].Similarity).To(Equal(0.65))
			}
		},
		Entry("uses request topK when reranker is not configured", false, 2, []int{1, 2}),
		Entry("over-fetches, reranks, then packs when reranker is configured", true, 6, []int{3, 1}),
	)

	It("rejects generation before embeddings are ready", func() {
		dataset := validInferenceDataset()
		dataset.ProcessingState = model.DatasetProcessingFeatureMaterialized
		inferenceModel := validInferenceModel()
		inferenceModel.UserID = dataset.UserID
		inferenceModel.DatasetID = dataset.DatasetID
		inferenceModel.DatasetID = dataset.DatasetID
		promptStrategy := testPromptStrategy()
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{model: inferenceModel},
			app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{dataset: dataset}),
			app.WithInferenceRequestRepository(&inferenceRequestRepositoryStub{}),
			app.WithRetrievalClient(&retrievalClientStub{}),
			app.WithGenerationAdapters(map[string]app.GenerationAdapter{
				model.ServingProtocolOpenAIChatCompletions.String(): &generationAdapterStub{},
			}),
			app.WithPromptStrategy(promptStrategy),
			app.WithContextPacker(app.NewContextWindowPacker(promptStrategy)),
			app.WithPromptBuilder(app.NewDefaultPromptBuilder(promptStrategy)),
		)

		_, err := uc.Generate(context.Background(), model.GenerateRequest{
			RequestID: uuid.New(),
			UserID:    dataset.UserID,
			DatasetID: dataset.DatasetID,
			ModelID:   inferenceModel.ModelID,
			QueryText: "query",
			TopK:      4,
		})

		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrDatasetNotReady)).To(BeTrue())
	})

	It("rejects generation when the dataset embedding provider is unsupported", func() {
		dataset := validInferenceDataset()
		dataset.EmbeddingProvider = "unknown"
		inferenceModel := validInferenceModel()
		inferenceModel.UserID = dataset.UserID
		inferenceModel.OrgID = dataset.OrgID
		inferenceModel.DatasetID = dataset.DatasetID
		requestRepository := &inferenceRequestRepositoryStub{}
		retrieval := &retrievalClientStub{}
		promptStrategy := testPromptStrategy()
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{model: inferenceModel},
			app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{dataset: dataset}),
			app.WithInferenceRequestRepository(requestRepository),
			app.WithRetrievalClient(retrieval),
			app.WithGenerationAdapters(map[string]app.GenerationAdapter{
				model.ServingProtocolOpenAIChatCompletions.String(): &generationAdapterStub{},
			}),
			app.WithPromptStrategy(promptStrategy),
			app.WithContextPacker(app.NewContextWindowPacker(promptStrategy)),
			app.WithPromptBuilder(app.NewDefaultPromptBuilder(promptStrategy)),
		)

		_, err := uc.Generate(context.Background(), model.GenerateRequest{
			RequestID: uuid.New(),
			UserID:    dataset.UserID,
			OrgID:     dataset.OrgID,
			DatasetID: dataset.DatasetID,
			ModelID:   inferenceModel.ModelID,
			QueryText: "query",
			TopK:      4,
		})

		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrDatasetNotReady)).To(BeTrue())
		Expect(err.Error()).To(ContainSubstring("embedding provider"))
		Expect(retrieval.datasetID).To(Equal(uuid.Nil))
		Expect(requestRepository.request).NotTo(BeNil())
		Expect(requestRepository.request.Status).To(Equal(model.InferenceRequestStatusFailed))
	})

	It("rejects generation when the model is not ready", func() {
		dataset := validInferenceDataset()
		inferenceModel := validInferenceModel()
		inferenceModel.UserID = dataset.UserID
		inferenceModel.DatasetID = dataset.DatasetID
		inferenceModel.Status = model.ModelStatusFailed
		requestRepository := &inferenceRequestRepositoryStub{}
		promptStrategy := testPromptStrategy()
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{model: inferenceModel},
			app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{dataset: dataset}),
			app.WithInferenceRequestRepository(requestRepository),
			app.WithRetrievalClient(&retrievalClientStub{}),
			app.WithGenerationAdapters(map[string]app.GenerationAdapter{
				model.ServingProtocolOpenAIChatCompletions.String(): &generationAdapterStub{},
			}),
			app.WithPromptStrategy(promptStrategy),
			app.WithContextPacker(app.NewContextWindowPacker(promptStrategy)),
			app.WithPromptBuilder(app.NewDefaultPromptBuilder(promptStrategy)),
		)

		_, err := uc.Generate(context.Background(), model.GenerateRequest{
			RequestID: uuid.New(),
			UserID:    dataset.UserID,
			DatasetID: dataset.DatasetID,
			ModelID:   inferenceModel.ModelID,
			QueryText: "query",
			TopK:      4,
		})

		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrModelNotReady)).To(BeTrue())
		Expect(requestRepository.request).NotTo(BeNil())
		Expect(requestRepository.request.Status).To(Equal(model.InferenceRequestStatusFailed))
	})

	It("rejects generation when the ready model is not loaded by the serving layer", func() {
		dataset := validInferenceDataset()
		inferenceModel := validInferenceModel()
		inferenceModel.UserID = dataset.UserID
		inferenceModel.DatasetID = dataset.DatasetID
		inferenceModel.ServingLoadStatus = model.ModelLoadStatusNotLoaded
		requestRepository := &inferenceRequestRepositoryStub{}
		promptStrategy := testPromptStrategy()
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{model: inferenceModel},
			app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{dataset: dataset}),
			app.WithInferenceRequestRepository(requestRepository),
			app.WithRetrievalClient(&retrievalClientStub{}),
			app.WithGenerationAdapters(map[string]app.GenerationAdapter{
				model.ServingProtocolOpenAIChatCompletions.String(): &generationAdapterStub{},
			}),
			app.WithPromptStrategy(promptStrategy),
			app.WithContextPacker(app.NewContextWindowPacker(promptStrategy)),
			app.WithPromptBuilder(app.NewDefaultPromptBuilder(promptStrategy)),
		)

		_, err := uc.Generate(context.Background(), model.GenerateRequest{
			RequestID: uuid.New(),
			UserID:    dataset.UserID,
			DatasetID: dataset.DatasetID,
			ModelID:   inferenceModel.ModelID,
			QueryText: "query",
			TopK:      4,
		})

		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrModelNotReady)).To(BeTrue())
		Expect(requestRepository.request).NotTo(BeNil())
		Expect(requestRepository.request.Status).To(Equal(model.InferenceRequestStatusFailed))
	})

	It("triggers a cold model load and generates after the model registry projects loaded status", func() {
		dataset := validInferenceDataset()
		notLoadedModel := validInferenceModel()
		notLoadedModel.UserID = dataset.UserID
		notLoadedModel.OrgID = dataset.OrgID
		notLoadedModel.DatasetID = dataset.DatasetID
		notLoadedModel.ServingLoadStatus = model.ModelLoadStatusNotLoaded
		loadedModel := *notLoadedModel
		loadedModel.ServingLoadStatus = model.ModelLoadStatusLoaded
		modelRepository := &inferenceModelRepositoryStub{models: []*model.InferenceModel{notLoadedModel, &loadedModel}}
		requestRepository := &inferenceRequestRepositoryStub{}
		trigger := &modelServingLoadTriggerStub{}
		retrieval := &retrievalClientStub{contexts: []model.RetrievedContext{{
			EmbeddingRecordID:   uuid.New(),
			EmbeddingSnapshotID: dataset.EmbeddingSnapshotID,
			DatasetID:           dataset.DatasetID,
			ChunkIndex:          1,
			SourceText:          "reloaded adapter context",
			Similarity:          0.91,
		}}}
		generator := &generationAdapterStub{answer: "generated after reload"}
		promptStrategy := testPromptStrategy()
		uc := app.NewInferenceUsecase(
			modelRepository,
			app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{dataset: dataset}),
			app.WithInferenceRequestRepository(requestRepository),
			app.WithRetrievalClient(retrieval),
			app.WithModelServingLoadTrigger(trigger, 250*time.Millisecond, time.Millisecond),
			app.WithGenerationAdapters(map[string]app.GenerationAdapter{
				model.ServingProtocolOpenAIChatCompletions.String(): generator,
			}),
			app.WithPromptStrategy(promptStrategy),
			app.WithContextPacker(app.NewContextWindowPacker(promptStrategy)),
			app.WithPromptBuilder(app.NewDefaultPromptBuilder(promptStrategy)),
		)

		response, err := uc.Generate(context.Background(), model.GenerateRequest{
			RequestID: uuid.New(),
			UserID:    dataset.UserID,
			OrgID:     dataset.OrgID,
			DatasetID: dataset.DatasetID,
			ModelID:   notLoadedModel.ModelID,
			QueryText: "query",
			TopK:      4,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(trigger.calls).To(Equal(1))
		Expect(trigger.orgID).To(Equal(dataset.OrgID))
		Expect(trigger.modelID).To(Equal(notLoadedModel.ModelID))
		Expect(modelRepository.readCount).To(Equal(2))
		Expect(response.Answer).To(Equal("generated after reload"))
		Expect(response.GenerationModel).To(Equal(loadedModel.ServingModel))
		Expect(generator.request.Model.ServingLoadStatus).To(Equal(model.ModelLoadStatusLoaded))
		Expect(requestRepository.request).NotTo(BeNil())
		Expect(requestRepository.request.Status).To(Equal(model.InferenceRequestStatusCompleted))
	})

	It("rejects generation when the model belongs to a different dataset", func() {
		dataset := validInferenceDataset()
		inferenceModel := validInferenceModel()
		inferenceModel.UserID = dataset.UserID
		requestRepository := &inferenceRequestRepositoryStub{}
		promptStrategy := testPromptStrategy()
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{model: inferenceModel},
			app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{dataset: dataset}),
			app.WithInferenceRequestRepository(requestRepository),
			app.WithRetrievalClient(&retrievalClientStub{}),
			app.WithGenerationAdapters(map[string]app.GenerationAdapter{
				model.ServingProtocolOpenAIChatCompletions.String(): &generationAdapterStub{},
			}),
			app.WithPromptStrategy(promptStrategy),
			app.WithContextPacker(app.NewContextWindowPacker(promptStrategy)),
			app.WithPromptBuilder(app.NewDefaultPromptBuilder(promptStrategy)),
		)

		_, err := uc.Generate(context.Background(), model.GenerateRequest{
			RequestID: uuid.New(),
			UserID:    dataset.UserID,
			DatasetID: dataset.DatasetID,
			ModelID:   inferenceModel.ModelID,
			QueryText: "query",
			TopK:      4,
		})

		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrModelMismatch)).To(BeTrue())
		Expect(requestRepository.request).NotTo(BeNil())
		Expect(requestRepository.request.Status).To(Equal(model.InferenceRequestStatusFailed))
	})

	It("allows a base model to generate over any requested dataset", func() {
		dataset := validInferenceDataset()
		inferenceModel := validInferenceModel()
		inferenceModel.UserID = dataset.UserID
		inferenceModel.ModelKind = model.ModelKindBase
		inferenceModel.DatasetID = uuid.Nil
		inferenceModel.TrainingRunID = uuid.Nil
		inferenceModel.AdapterURI = ""
		requestRepository := &inferenceRequestRepositoryStub{}
		retrieval := &retrievalClientStub{contexts: []model.RetrievedContext{{
			EmbeddingRecordID:   uuid.New(),
			EmbeddingSnapshotID: dataset.EmbeddingSnapshotID,
			ChunkIndex:          1,
			SourceText:          "base model context",
			Similarity:          0.91,
		}}}
		promptStrategy := testPromptStrategy()
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{model: inferenceModel},
			app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{dataset: dataset}),
			app.WithInferenceRequestRepository(requestRepository),
			app.WithRetrievalClient(retrieval),
			app.WithGenerationAdapters(map[string]app.GenerationAdapter{
				model.ServingProtocolOpenAIChatCompletions.String(): &generationAdapterStub{},
			}),
			app.WithPromptStrategy(promptStrategy),
			app.WithContextPacker(app.NewContextWindowPacker(promptStrategy)),
			app.WithPromptBuilder(app.NewDefaultPromptBuilder(promptStrategy)),
		)

		response, err := uc.Generate(context.Background(), model.GenerateRequest{
			RequestID: uuid.New(),
			UserID:    dataset.UserID,
			DatasetID: dataset.DatasetID,
			ModelID:   inferenceModel.ModelID,
			QueryText: "query",
			TopK:      4,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(response.Answer).To(Equal("generated answer"))
		Expect(retrieval.datasetID).To(Equal(dataset.DatasetID))
		Expect(requestRepository.request).NotTo(BeNil())
		Expect(requestRepository.request.Status).To(Equal(model.InferenceRequestStatusCompleted))
	})

	It("returns audit recording errors for otherwise successful generations", func() {
		dataset := validInferenceDataset()
		inferenceModel := validInferenceModel()
		inferenceModel.UserID = dataset.UserID
		inferenceModel.DatasetID = dataset.DatasetID
		auditErr := errors.New("audit table unavailable")
		requestRepository := &inferenceRequestRepositoryStub{err: auditErr}
		retrieval := &retrievalClientStub{contexts: []model.RetrievedContext{{
			EmbeddingRecordID:   uuid.New(),
			EmbeddingSnapshotID: dataset.EmbeddingSnapshotID,
			ChunkIndex:          1,
			SourceText:          "retrieved context",
			Similarity:          0.92,
		}}}
		promptStrategy := testPromptStrategy()
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{model: inferenceModel},
			app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{dataset: dataset}),
			app.WithInferenceRequestRepository(requestRepository),
			app.WithRetrievalClient(retrieval),
			app.WithGenerationAdapters(map[string]app.GenerationAdapter{
				model.ServingProtocolOpenAIChatCompletions.String(): &generationAdapterStub{},
			}),
			app.WithPromptStrategy(promptStrategy),
			app.WithContextPacker(app.NewContextWindowPacker(promptStrategy)),
			app.WithPromptBuilder(app.NewDefaultPromptBuilder(promptStrategy)),
		)

		response, err := uc.Generate(context.Background(), model.GenerateRequest{
			RequestID: uuid.New(),
			UserID:    dataset.UserID,
			DatasetID: dataset.DatasetID,
			ModelID:   inferenceModel.ModelID,
			QueryText: "query",
			TopK:      4,
		})

		Expect(response).To(BeNil())
		Expect(errors.Is(err, auditErr)).To(BeTrue())
		Expect(requestRepository.request).NotTo(BeNil())
		Expect(requestRepository.request.Status).To(Equal(model.InferenceRequestStatusCompleted))
		Expect(requestRepository.request.PromptText).To(ContainSubstring("retrieved context"))
		Expect(requestRepository.request.AnswerText).To(Equal("generated answer"))
	})
})

func testPromptStrategy() model.PromptStrategy {
	return model.PromptStrategy{
		Version:          "test-rag-prompt-v1",
		SystemPrompt:     "Use context only.",
		MaxContextTokens: 200,
		MaxContextChunks: 4,
	}
}

func withRerankScore(context model.RetrievedContext, score float64) model.RetrievedContext {
	context.RerankScore = score
	return context
}

func validInferenceModel() *model.InferenceModel {
	return &model.InferenceModel{
		ModelID:           uuid.New(),
		UserID:            uuid.New(),
		OrgID:             uuid.New(),
		TrainingRunID:     uuid.New(),
		DatasetID:         uuid.New(),
		ModelKind:         model.ModelKindFineTuned,
		Source:            model.ModelSourceTraining,
		SourceMetadata:    "{}",
		Name:              "sentence-transformer",
		ModelVersion:      1,
		BaseModel:         "base-model",
		ArtifactLocation:  "s3://local-dev-bucket/models/model-1",
		ArtifactFormat:    "HF_PEFT_ADAPTER",
		ArtifactChecksum:  "checksum",
		ArtifactSizeBytes: 10,
		AdapterURI:        "s3://local-dev-bucket/models/model-1",
		ServingTarget:     "vllm-local",
		ServingModel:      "sentence-transformer-v1",
		ServingProtocol:   model.ServingProtocolOpenAIChatCompletions,
		ServingLoadStatus: model.ModelLoadStatusLoaded,
		EffectiveBaseID:   "sha256-effective-base",
		MetricsMetadata:   "{}",
		Status:            model.ModelStatusReady,
	}
}

func validToolUsingAgentSpec(inferenceModel *model.InferenceModel) *model.AgentSpec {
	return &model.AgentSpec{
		OrgID:            inferenceModel.OrgID,
		AgentLineage:     "support-agent",
		SystemPrompt:     "Use the available tools before answering.",
		SourceYAML:       "schema_version: agent_spec_v1",
		CanonicalJSON:    []byte(`{"schema_version":"agent_spec_v1"}`),
		SchemaVersion:    "agent_spec_v1",
		ContentHash:      "sha256:agent-spec",
		ValidationReport: "schema:passed;policy:passed",
		ModelID:          inferenceModel.ModelID,
		ToolBindings: []model.ToolBinding{{
			Name:       "search_knowledge",
			Required:   true,
			ToolChoice: "required",
			Config:     []byte(`{}`),
		}},
		RetrievalConfig: []byte(`{}`),
		Budgets: model.AgentBudgets{
			MaxSteps: 3,
			Token:    128,
			WallMs:   60000,
		},
		StopConditions: []byte(`{}`),
		Guardrails:     []byte(`{}`),
		Status:         model.AgentSpecStatusValidated,
	}
}

func runAgentAndReturnFirstToolsetHash(toolSpecs []model.ToolSpec, bindings []model.ToolBinding) string {
	return runAgentAndReturnFirstRunForDataset(validInferenceDataset(), toolSpecs, bindings).ToolsetHash
}

func runAgentAndReturnFirstRunForDataset(dataset *model.InferenceDataset, toolSpecs []model.ToolSpec, bindings []model.ToolBinding) *model.AgentRun {
	inferenceModel := validInferenceModel()
	inferenceModel.UserID = dataset.UserID
	inferenceModel.OrgID = dataset.OrgID
	inferenceModel.ModelKind = model.ModelKindBase
	inferenceModel.DatasetID = uuid.Nil
	spec := validToolUsingAgentSpec(inferenceModel)
	spec.ToolBindings = bindings
	endpointID := uuid.New()
	endpoint := &model.PublishedEndpoint{
		EndpointID:    endpointID,
		OrgID:         dataset.OrgID,
		ModelID:       inferenceModel.ModelID,
		Mode:          model.AgentEndpointModeAgent,
		AgentSpecHash: spec.ContentHash,
		DatasetIDs:    []uuid.UUID{dataset.DatasetID},
		MergeStrategy: model.RAGMergeStrategyScoreNormalized,
		Status:        model.PublishedEndpointStatusReady,
	}
	trajectoryRepository := &agentTrajectoryRepositoryStub{}
	uc := app.NewInferenceUsecase(
		&inferenceModelRepositoryStub{model: inferenceModel},
		app.WithPublishedEndpointRepository(&publishedEndpointRepositoryStub{endpoint: endpoint}),
		app.WithAgentSpecRepository(&agentSpecRepositoryStub{spec: spec}),
		app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{datasets: map[uuid.UUID]*model.InferenceDataset{dataset.DatasetID: dataset}}),
		app.WithInferenceRequestRepository(&inferenceRequestRepositoryStub{}),
		app.WithAgentTrajectoryRepository(trajectoryRepository),
		app.WithToolInvoker(&toolInvokerStub{tools: toolSpecs}),
		app.WithGenerationAdapters(map[string]app.GenerationAdapter{
			model.ServingProtocolOpenAIChatCompletions.String(): &generationAdapterStub{answer: "done"},
		}),
	)

	response, err := uc.GenerateForEndpoint(context.Background(), endpointID, model.GenerateRequest{
		RequestID: uuid.New(),
		UserID:    dataset.UserID,
		OrgID:     dataset.OrgID,
		QueryText: "answer directly",
		TopK:      2,
	})

	Expect(err).NotTo(HaveOccurred())
	Expect(response).NotTo(BeNil())
	Expect(trajectoryRepository.runs).NotTo(BeEmpty())
	return trajectoryRepository.runs[0]
}

func validInferenceDataset() *model.InferenceDataset {
	return &model.InferenceDataset{
		DatasetID:                uuid.New(),
		UserID:                   uuid.New(),
		OrgID:                    uuid.New(),
		DatasetVersion:           4,
		ProcessingState:          model.DatasetProcessingEmbeddingsMaterialized,
		StorageLocation:          "s3://local-dev-bucket/features/dataset.parquet",
		TableNamespace:           "features",
		TableName:                "movies",
		TableFormat:              "PARQUET",
		CatalogProvider:          "LOCAL",
		ProcessingProfile:        "TEXT_RAG_PROCESSING_PROFILE",
		SchemaVersion:            2,
		SchemaMetadata:           "{}",
		RawSnapshotID:            uuid.New(),
		FeatureSnapshotID:        uuid.New(),
		EmbeddingSnapshotID:      uuid.New(),
		VectorStore:              "pgvector",
		CollectionName:           "movies",
		EmbeddingDimensions:      384,
		EmbeddingCount:           12,
		EmbeddingStrategyVersion: "rag-v1",
		EmbeddingChunkerName:     "go-token-window",
		EmbeddingChunkerVersion:  "v1",
		EmbeddingChunkSize:       384,
		EmbeddingChunkOverlap:    64,
		EmbeddingProvider:        "ollama",
		EmbeddingModel:           "bge-small-en-v1.5",
	}
}
