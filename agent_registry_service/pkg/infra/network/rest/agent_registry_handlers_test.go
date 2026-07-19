package rest_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"agent_registry_service/pkg/domain/model"
	agentadapter "agent_registry_service/pkg/infra/network/adapter"
	agentrest "agent_registry_service/pkg/infra/network/rest"
	"lib/shared_lib/authz"
	serializers "lib/shared_lib/serializer"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestAgentRegistryHandlers(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Agent registry REST handler unit test suite")
}

type usecaseStub struct {
	registerSpecCommand     model.RegisterAgentSpecVersionCommand
	registerEndpointCommand model.RegisterEndpointBindingCommand
	promoteCommand          model.PromoteSpecChampionCommand
	importGoldenCommand     model.ImportGoldenTasksCommand
	listGoldenCommand       model.ListGoldenTasksCommand
	labelCommand            model.LabelAgentRunCommand
	listLabelsCommand       model.ListAgentRunLabelsCommand
	buildDatasetCommand     model.BuildTrajectoryDatasetCommand
	trainAdapterCommand     model.DispatchAgentAdapterTrainingCommand
	evaluateAdapterCommand  model.EvaluateAdapterCandidateCommand
	promoteAdapterCommand   model.PromoteAgentAdapterCommand
	evaluateCommand         model.EvaluateSpecChampionCommand
	readDatasetOrgID        uuid.UUID
	readDatasetID           uuid.UUID
	readAdapterOrgID        uuid.UUID
	readAdapterID           uuid.UUID
	readEvalOrgID           uuid.UUID
	readEvalReportID        uuid.UUID
	version                 *model.AgentSpecVersion
	binding                 *model.AgentEndpointBinding
	state                   *model.AgentChampionState
	goldenTasks             []*model.GoldenTask
	labels                  []*model.AgentRunLabel
	label                   *model.AgentRunLabel
	dataset                 *model.AgentTrajectoryDataset
	adapter                 *model.AgentAdapter
	evalReport              *model.AgentEvalReport
	err                     error
}

func (s *usecaseStub) RegisterAgentSpecVersion(_ context.Context, command model.RegisterAgentSpecVersionCommand) (*model.AgentSpecVersion, error) {
	s.registerSpecCommand = command
	return s.version, s.err
}

func (s *usecaseStub) RegisterEndpointBinding(_ context.Context, command model.RegisterEndpointBindingCommand) (*model.AgentEndpointBinding, error) {
	s.registerEndpointCommand = command
	return s.binding, s.err
}

func (s *usecaseStub) PromoteSpecChampion(_ context.Context, command model.PromoteSpecChampionCommand) (*model.AgentChampionState, error) {
	s.promoteCommand = command
	return s.state, s.err
}

func (s *usecaseStub) ImportGoldenTasks(_ context.Context, command model.ImportGoldenTasksCommand) ([]*model.GoldenTask, error) {
	s.importGoldenCommand = command
	return s.goldenTasks, s.err
}

func (s *usecaseStub) ListGoldenTasks(_ context.Context, command model.ListGoldenTasksCommand) ([]*model.GoldenTask, error) {
	s.listGoldenCommand = command
	return s.goldenTasks, s.err
}

func (s *usecaseStub) LabelAgentRun(_ context.Context, command model.LabelAgentRunCommand) (*model.AgentRunLabel, error) {
	s.labelCommand = command
	return s.label, s.err
}

func (s *usecaseStub) ListAgentRunLabels(_ context.Context, command model.ListAgentRunLabelsCommand) ([]*model.AgentRunLabel, error) {
	s.listLabelsCommand = command
	return s.labels, s.err
}

func (s *usecaseStub) BuildTrajectoryDataset(_ context.Context, command model.BuildTrajectoryDatasetCommand) (*model.AgentTrajectoryDataset, error) {
	s.buildDatasetCommand = command
	return s.dataset, s.err
}

func (s *usecaseStub) ReadTrajectoryDataset(_ context.Context, orgID uuid.UUID, datasetID uuid.UUID) (*model.AgentTrajectoryDataset, error) {
	s.readDatasetOrgID = orgID
	s.readDatasetID = datasetID
	return s.dataset, s.err
}

func (s *usecaseStub) DispatchAgentAdapterTraining(_ context.Context, command model.DispatchAgentAdapterTrainingCommand) (*model.AgentAdapter, error) {
	s.trainAdapterCommand = command
	return s.adapter, s.err
}

func (s *usecaseStub) RecordAgentAdapterTrainingCompleted(context.Context, model.AgentAdapterTrainingCompletion) (*model.AgentAdapter, error) {
	return s.adapter, s.err
}

func (s *usecaseStub) RecordAgentAdapterTrainingFailed(context.Context, model.AgentAdapterTrainingFailure) (*model.AgentAdapter, error) {
	return s.adapter, s.err
}

func (s *usecaseStub) ReadAgentAdapter(_ context.Context, orgID uuid.UUID, adapterID uuid.UUID) (*model.AgentAdapter, error) {
	s.readAdapterOrgID = orgID
	s.readAdapterID = adapterID
	return s.adapter, s.err
}

func (s *usecaseStub) EvaluateAdapterCandidate(_ context.Context, command model.EvaluateAdapterCandidateCommand) (*model.AgentEvalReport, error) {
	s.evaluateAdapterCommand = command
	return s.evalReport, s.err
}

func (s *usecaseStub) PromoteAgentAdapter(_ context.Context, command model.PromoteAgentAdapterCommand) (*model.AgentAdapter, error) {
	s.promoteAdapterCommand = command
	return s.adapter, s.err
}

func (s *usecaseStub) EvaluateSpecChampion(_ context.Context, command model.EvaluateSpecChampionCommand) (*model.AgentEvalReport, error) {
	s.evaluateCommand = command
	return s.evalReport, s.err
}

func (s *usecaseStub) ReadAgentEvalReport(_ context.Context, orgID uuid.UUID, reportID uuid.UUID) (*model.AgentEvalReport, error) {
	s.readEvalOrgID = orgID
	s.readEvalReportID = reportID
	return s.evalReport, s.err
}

var _ = Describe("AgentRegistryHandlers", func() {
	var (
		orgID    uuid.UUID
		userID   uuid.UUID
		uc       *usecaseStub
		handlers *agentrest.AgentRegistryHandlers
	)

	BeforeEach(func() {
		orgID = uuid.New()
		userID = uuid.New()
		uc = &usecaseStub{}
		handlers = agentrest.NewAgentRegistryHandlers(uc, agentadapter.NewAgentRegistryDTOAdapter(serializers.NewJSONSerializer()))
	})

	It("registers agent spec versions with actor and org from headers", func() {
		uc.version = &model.AgentSpecVersion{
			OrgID:              orgID,
			AgentLineage:       "support-agent",
			AgentSpecHash:      "sha256-spec",
			ModelID:            uuid.New(),
			RegisteredByUserID: userID,
			RegisteredAt:       time.Now().UTC(),
		}
		req := httptest.NewRequest(http.MethodPost, "/v1/agent-registry/spec-versions", strings.NewReader(`{
			"agent_lineage":"support-agent",
			"agent_spec_hash":"sha256-spec"
		}`))
		req.Header.Set(authz.HeaderOrgID, orgID.String())
		req.Header.Set(authz.HeaderUserID, userID.String())

		response, err := handlers.RegisterAgentSpecVersion(context.Background(), req)

		Expect(err).NotTo(HaveOccurred())
		Expect(response.StatusCode()).To(Equal(http.StatusCreated))
		Expect(uc.registerSpecCommand.OrgID).To(Equal(orgID))
		Expect(uc.registerSpecCommand.UserID).To(Equal(userID))
		Expect(uc.registerSpecCommand.AgentSpecHash).To(Equal("sha256-spec"))
		Expect(string(response.Payload())).To(ContainSubstring("sha256-spec"))
	})

	It("rejects malformed endpoint binding DTOs before the usecase", func() {
		req := httptest.NewRequest(http.MethodPost, "/v1/agent-registry/endpoint-bindings", strings.NewReader(`{
			"agent_lineage":"support-agent",
			"endpoint_id":"not-a-uuid"
		}`))
		req.Header.Set(authz.HeaderOrgID, orgID.String())
		req.Header.Set(authz.HeaderUserID, userID.String())

		response, err := handlers.RegisterEndpointBinding(context.Background(), req)

		Expect(response).To(BeNil())
		Expect(err).To(HaveOccurred())
		Expect(uc.registerEndpointCommand.EndpointID).To(Equal(uuid.Nil))
	})

	It("imports golden tasks with actor and org from headers", func() {
		taskID := uuid.New()
		uc.goldenTasks = []*model.GoldenTask{{
			TaskID:                 taskID,
			OrgID:                  orgID,
			AgentLineage:           "support-agent",
			Split:                  model.GoldenTaskSplitPromotionHoldout,
			SplitVersion:           2,
			GroupKey:               "group-a",
			Prompt:                 "Who signed the agreement?",
			NormalizedPromptHash:   "hash-prompt",
			ContentFingerprint:     "fingerprint",
			ExpectedAnswer:         "Alice",
			ExpectedAnswerRubricID: "rubric-answer-v1",
			CreatedByUserID:        userID,
			CreatedAt:              time.Now().UTC(),
		}}
		req := httptest.NewRequest(http.MethodPost, "/v1/agent-registry/golden-tasks", strings.NewReader(`{
			"agent_lineage":"support-agent",
			"split":"promotion_holdout",
			"split_version":2,
			"tasks":[{"group_key":"group-a","prompt":"Who signed the agreement?","expected_answer":"Alice","expected_answer_rubric_id":"rubric-answer-v1"}]
		}`))
		req.Header.Set(authz.HeaderOrgID, orgID.String())
		req.Header.Set(authz.HeaderUserID, userID.String())

		response, err := handlers.ImportGoldenTasks(context.Background(), req)

		Expect(err).NotTo(HaveOccurred())
		Expect(response.StatusCode()).To(Equal(http.StatusCreated))
		Expect(uc.importGoldenCommand.OrgID).To(Equal(orgID))
		Expect(uc.importGoldenCommand.UserID).To(Equal(userID))
		Expect(uc.importGoldenCommand.Split).To(Equal(model.GoldenTaskSplitPromotionHoldout))
		Expect(string(response.Payload())).To(ContainSubstring(taskID.String()))
	})

	It("lists golden tasks through validated query params", func() {
		req := httptest.NewRequest(http.MethodGet, "/v1/agent-registry/golden-tasks?agent_lineage=support-agent&split=dev_eval&split_version=3", nil)
		req.Header.Set(authz.HeaderOrgID, orgID.String())
		req.Header.Set(authz.HeaderUserID, userID.String())

		response, err := handlers.ListGoldenTasks(context.Background(), req)

		Expect(err).NotTo(HaveOccurred())
		Expect(response.StatusCode()).To(Equal(http.StatusOK))
		Expect(uc.listGoldenCommand.OrgID).To(Equal(orgID))
		Expect(uc.listGoldenCommand.AgentLineage).To(Equal("support-agent"))
		Expect(uc.listGoldenCommand.Split).To(Equal(model.GoldenTaskSplitDevEval))
		Expect(uc.listGoldenCommand.SplitVersion).To(Equal(3))
	})

	It("labels agent runs with actor and org from headers", func() {
		runID := uuid.New()
		labelID := uuid.New()
		uc.label = &model.AgentRunLabel{
			LabelID:            labelID,
			RunID:              runID,
			AgentLineage:       "support-agent",
			AgentSpecHash:      "sha256-spec",
			ToolsetHash:        "sha256-tools",
			EffectiveBaseID:    "sha256-base",
			DataSnapshotHash:   "sha256-data",
			ContentFingerprint: "fingerprint",
			Evaluator:          "human:reviewer-a",
			TaskSuccess:        true,
			ToolSelectionScore: 1,
			Groundedness:       1,
			Confidence:         0.9,
			LabelSource:        "human",
			RubricVersion:      "trajectory_answer_contains_v1",
			CreatedByUserID:    userID,
			CreatedAt:          time.Now().UTC(),
		}
		req := httptest.NewRequest(http.MethodPost, "/v1/agent-registry/run-labels", strings.NewReader(`{
			"agent_lineage":"support-agent",
			"run_id":"`+runID.String()+`",
			"prompt":"Who signed the agreement?",
			"evaluator":"human:reviewer-a",
			"task_success":true,
			"tool_selection_score":1,
			"groundedness":1,
			"policy_violations":0,
			"confidence":0.9,
			"label_source":"human",
			"rubric_version":"trajectory_answer_contains_v1"
		}`))
		req.Header.Set(authz.HeaderOrgID, orgID.String())
		req.Header.Set(authz.HeaderUserID, userID.String())

		response, err := handlers.LabelAgentRun(context.Background(), req)

		Expect(err).NotTo(HaveOccurred())
		Expect(response.StatusCode()).To(Equal(http.StatusCreated))
		Expect(uc.labelCommand.OrgID).To(Equal(orgID))
		Expect(uc.labelCommand.UserID).To(Equal(userID))
		Expect(uc.labelCommand.RunID).To(Equal(runID))
		Expect(string(response.Payload())).To(ContainSubstring(labelID.String()))
	})

	It("rejects malformed label requests before the usecase", func() {
		req := httptest.NewRequest(http.MethodPost, "/v1/agent-registry/run-labels", strings.NewReader(`{
			"agent_lineage":"support-agent",
			"run_id":"not-a-uuid",
			"prompt":"Who signed the agreement?",
			"evaluator":"human:reviewer-a",
			"tool_selection_score":1,
			"groundedness":1,
			"confidence":1,
			"label_source":"human",
			"rubric_version":"trajectory_answer_contains_v1"
		}`))
		req.Header.Set(authz.HeaderOrgID, orgID.String())
		req.Header.Set(authz.HeaderUserID, userID.String())

		response, err := handlers.LabelAgentRun(context.Background(), req)

		Expect(response).To(BeNil())
		Expect(err).To(HaveOccurred())
		Expect(uc.labelCommand.RunID).To(Equal(uuid.Nil))
	})

	It("lists agent run labels through validated query params", func() {
		req := httptest.NewRequest(http.MethodGet, "/v1/agent-registry/run-labels?agent_lineage=support-agent", nil)
		req.Header.Set(authz.HeaderOrgID, orgID.String())
		req.Header.Set(authz.HeaderUserID, userID.String())

		response, err := handlers.ListAgentRunLabels(context.Background(), req)

		Expect(err).NotTo(HaveOccurred())
		Expect(response.StatusCode()).To(Equal(http.StatusOK))
		Expect(uc.listLabelsCommand.OrgID).To(Equal(orgID))
		Expect(uc.listLabelsCommand.AgentLineage).To(Equal("support-agent"))
	})

	It("builds trajectory datasets with actor and org from headers", func() {
		datasetID := uuid.New()
		uc.dataset = &model.AgentTrajectoryDataset{
			DatasetID:          datasetID,
			AgentLineage:       "support-agent",
			GoldenSplitVersion: 2,
			ContentHash:        "sha256-dataset",
			DatasetURI:         "agent-registry://trajectory-datasets/sha256-dataset",
			Format:             "AGENT_TRAJECTORY_JSON",
			LabelCount:         1,
			Manifest:           []byte(`{"schema_version":"agent_trajectory_dataset_v1"}`),
			EffectiveBaseID:    "sha256-base",
			AgentSpecHash:      "sha256-spec",
			ToolsetHash:        "sha256-tools",
			DataSnapshotHash:   "sha256-data",
			CreatedByUserID:    userID,
			CreatedAt:          time.Now().UTC(),
		}
		req := httptest.NewRequest(http.MethodPost, "/v1/agent-registry/trajectory-datasets", strings.NewReader(`{
			"agent_lineage":"support-agent",
			"golden_split_version":2
		}`))
		req.Header.Set(authz.HeaderOrgID, orgID.String())
		req.Header.Set(authz.HeaderUserID, userID.String())

		response, err := handlers.BuildTrajectoryDataset(context.Background(), req)

		Expect(err).NotTo(HaveOccurred())
		Expect(response.StatusCode()).To(Equal(http.StatusCreated))
		Expect(uc.buildDatasetCommand.OrgID).To(Equal(orgID))
		Expect(uc.buildDatasetCommand.UserID).To(Equal(userID))
		Expect(uc.buildDatasetCommand.GoldenSplitVersion).To(Equal(2))
		Expect(string(response.Payload())).To(ContainSubstring(datasetID.String()))
	})

	It("reads trajectory datasets scoped by the trusted org header", func() {
		datasetID := uuid.New()
		uc.dataset = &model.AgentTrajectoryDataset{
			DatasetID:          datasetID,
			AgentLineage:       "support-agent",
			GoldenSplitVersion: 2,
			ContentHash:        "sha256-dataset",
			DatasetURI:         "agent-registry://trajectory-datasets/sha256-dataset",
			Format:             "AGENT_TRAJECTORY_JSON",
			LabelCount:         1,
			Manifest:           []byte(`{"schema_version":"agent_trajectory_dataset_v1"}`),
			EffectiveBaseID:    "sha256-base",
			AgentSpecHash:      "sha256-spec",
			ToolsetHash:        "sha256-tools",
			DataSnapshotHash:   "sha256-data",
			CreatedByUserID:    userID,
			CreatedAt:          time.Now().UTC(),
		}
		req := httptest.NewRequest(http.MethodGet, "/v1/agent-registry/trajectory-datasets/"+datasetID.String(), nil)
		req = mux.SetURLVars(req, map[string]string{"datasetId": datasetID.String()})
		req.Header.Set(authz.HeaderOrgID, orgID.String())
		req.Header.Set(authz.HeaderUserID, userID.String())

		response, err := handlers.ReadTrajectoryDataset(context.Background(), req)

		Expect(err).NotTo(HaveOccurred())
		Expect(response.StatusCode()).To(Equal(http.StatusOK))
		Expect(uc.readDatasetOrgID).To(Equal(orgID))
		Expect(uc.readDatasetID).To(Equal(datasetID))
		Expect(string(response.Payload())).To(ContainSubstring("sha256-dataset"))
	})

	It("trains agent adapters with actor and org from headers", func() {
		datasetID := uuid.New()
		adapterID := uuid.New()
		uc.adapter = &model.AgentAdapter{
			AdapterID:                        adapterID,
			AgentLineage:                     "support-agent",
			DatasetID:                        datasetID,
			ServingModelID:                   uuid.New(),
			AdapterURI:                       "s3://bucket/adapter",
			AdapterChecksum:                  "sha256-adapter",
			TrainingProvider:                 "deterministic-agent-training",
			TrainedAgainstEffectiveBaseID:    "sha256-base",
			TrainedAgainstAgentSpecHash:      "sha256-spec",
			TrainedAgainstToolsetHash:        "sha256-tools",
			TrainedAgainstDataSnapshotHash:   "sha256-data",
			TrainedAgainstRubricVersion:      "trajectory_answer_contains_v1",
			TrainedAgainstGoldenSplitVersion: 2,
			Status:                           model.AgentAdapterStatusCandidate,
			CreatedByUserID:                  userID,
			CreatedAt:                        time.Now().UTC(),
			UpdatedAt:                        time.Now().UTC(),
		}
		req := httptest.NewRequest(http.MethodPost, "/v1/agent-registry/adapters", strings.NewReader(`{
			"agent_lineage":"support-agent",
			"dataset_id":"`+datasetID.String()+`"
		}`))
		req.Header.Set(authz.HeaderOrgID, orgID.String())
		req.Header.Set(authz.HeaderUserID, userID.String())

		response, err := handlers.DispatchAgentAdapterTraining(context.Background(), req)

		Expect(err).NotTo(HaveOccurred())
		Expect(response.StatusCode()).To(Equal(http.StatusCreated))
		Expect(uc.trainAdapterCommand.OrgID).To(Equal(orgID))
		Expect(uc.trainAdapterCommand.DatasetID).To(Equal(datasetID))
		Expect(string(response.Payload())).To(ContainSubstring(adapterID.String()))
	})

	It("reads agent adapters scoped by the trusted org header", func() {
		adapterID := uuid.New()
		uc.adapter = &model.AgentAdapter{
			AdapterID:                        adapterID,
			AgentLineage:                     "support-agent",
			DatasetID:                        uuid.New(),
			ServingModelID:                   uuid.New(),
			AdapterURI:                       "s3://bucket/adapter",
			AdapterChecksum:                  "sha256-adapter",
			TrainingProvider:                 "deterministic-agent-training",
			TrainedAgainstEffectiveBaseID:    "sha256-base",
			TrainedAgainstAgentSpecHash:      "sha256-spec",
			TrainedAgainstToolsetHash:        "sha256-tools",
			TrainedAgainstDataSnapshotHash:   "sha256-data",
			TrainedAgainstRubricVersion:      "trajectory_answer_contains_v1",
			TrainedAgainstGoldenSplitVersion: 2,
			Status:                           model.AgentAdapterStatusEvaluated,
			CreatedByUserID:                  userID,
			CreatedAt:                        time.Now().UTC(),
			UpdatedAt:                        time.Now().UTC(),
		}
		req := httptest.NewRequest(http.MethodGet, "/v1/agent-registry/adapters/"+adapterID.String(), nil)
		req = mux.SetURLVars(req, map[string]string{"adapterId": adapterID.String()})
		req.Header.Set(authz.HeaderOrgID, orgID.String())
		req.Header.Set(authz.HeaderUserID, userID.String())

		response, err := handlers.ReadAgentAdapter(context.Background(), req)

		Expect(err).NotTo(HaveOccurred())
		Expect(response.StatusCode()).To(Equal(http.StatusOK))
		Expect(uc.readAdapterOrgID).To(Equal(orgID))
		Expect(uc.readAdapterID).To(Equal(adapterID))
		Expect(string(response.Payload())).To(ContainSubstring(adapterID.String()))
	})

	It("evaluates adapter candidates with actor and org from headers", func() {
		adapterID := uuid.New()
		endpointID := uuid.New()
		reportID := uuid.New()
		uc.evalReport = &model.AgentEvalReport{
			ReportID:         reportID,
			AgentLineage:     "support-agent",
			AgentSpecHash:    "sha256-spec",
			AdapterID:        adapterID,
			EndpointID:       endpointID,
			Split:            model.GoldenTaskSplitPromotionHoldout,
			SplitVersion:     2,
			RubricVersion:    "trajectory_answer_contains_v1",
			TaskCount:        1,
			TaskSuccessRate:  1,
			ToolSuccessRate:  1,
			GroundednessRate: 1,
			Passed:           true,
			GateReason:       "candidate passes promotion holdout",
			EvaluatedBy:      userID,
			EvaluatedAt:      time.Now().UTC(),
		}
		req := httptest.NewRequest(http.MethodPost, "/v1/agent-registry/evaluations/adapter-candidates", strings.NewReader(`{
			"agent_lineage":"support-agent",
			"adapter_id":"`+adapterID.String()+`",
			"endpoint_id":"`+endpointID.String()+`",
			"split_version":2
		}`))
		req.Header.Set(authz.HeaderOrgID, orgID.String())
		req.Header.Set(authz.HeaderUserID, userID.String())

		response, err := handlers.EvaluateAdapterCandidate(context.Background(), req)

		Expect(err).NotTo(HaveOccurred())
		Expect(response.StatusCode()).To(Equal(http.StatusCreated))
		Expect(uc.evaluateAdapterCommand.OrgID).To(Equal(orgID))
		Expect(uc.evaluateAdapterCommand.UserID).To(Equal(userID))
		Expect(uc.evaluateAdapterCommand.AdapterID).To(Equal(adapterID))
		Expect(uc.evaluateAdapterCommand.EndpointID).To(Equal(endpointID))
		Expect(string(response.Payload())).To(ContainSubstring(reportID.String()))
	})

	It("promotes adapter candidates with actor and org from headers", func() {
		adapterID := uuid.New()
		reportID := uuid.New()
		uc.adapter = &model.AgentAdapter{
			AdapterID:                        adapterID,
			AgentLineage:                     "support-agent",
			DatasetID:                        uuid.New(),
			ServingModelID:                   uuid.New(),
			AdapterURI:                       "s3://bucket/adapter",
			AdapterChecksum:                  "sha256-adapter",
			TrainingProvider:                 "deterministic-agent-training",
			TrainedAgainstEffectiveBaseID:    "sha256-base",
			TrainedAgainstAgentSpecHash:      "sha256-spec",
			TrainedAgainstToolsetHash:        "sha256-tools",
			TrainedAgainstDataSnapshotHash:   "sha256-data",
			TrainedAgainstRubricVersion:      "trajectory_answer_contains_v1",
			TrainedAgainstGoldenSplitVersion: 2,
			Status:                           model.AgentAdapterStatusPromoted,
			PromotionPassed:                  true,
			CreatedByUserID:                  userID,
			CreatedAt:                        time.Now().UTC(),
			UpdatedAt:                        time.Now().UTC(),
		}
		req := httptest.NewRequest(http.MethodPost, "/v1/agent-registry/adapters/promotions", strings.NewReader(`{
			"agent_lineage":"support-agent",
			"adapter_id":"`+adapterID.String()+`",
			"report_id":"`+reportID.String()+`",
			"min_delta":0.05
		}`))
		req.Header.Set(authz.HeaderOrgID, orgID.String())
		req.Header.Set(authz.HeaderUserID, userID.String())

		response, err := handlers.PromoteAgentAdapter(context.Background(), req)

		Expect(err).NotTo(HaveOccurred())
		Expect(response.StatusCode()).To(Equal(http.StatusOK))
		Expect(uc.promoteAdapterCommand.OrgID).To(Equal(orgID))
		Expect(uc.promoteAdapterCommand.UserID).To(Equal(userID))
		Expect(uc.promoteAdapterCommand.AdapterID).To(Equal(adapterID))
		Expect(uc.promoteAdapterCommand.ReportID).To(Equal(reportID))
		Expect(uc.promoteAdapterCommand.MinDelta).To(Equal(0.05))
	})

	It("evaluates a spec champion with actor and org from headers", func() {
		reportID := uuid.New()
		endpointID := uuid.New()
		uc.evalReport = &model.AgentEvalReport{
			ReportID:         reportID,
			AgentLineage:     "support-agent",
			AgentSpecHash:    "sha256-spec",
			EndpointID:       endpointID,
			Split:            model.GoldenTaskSplitPromotionHoldout,
			SplitVersion:     2,
			RubricVersion:    "trajectory_answer_contains_v1",
			TaskCount:        1,
			TaskSuccessRate:  1,
			ToolSuccessRate:  1,
			GroundednessRate: 1,
			Passed:           true,
			GateReason:       "candidate passes promotion holdout",
			EvaluatedBy:      userID,
			EvaluatedAt:      time.Now().UTC(),
		}
		req := httptest.NewRequest(http.MethodPost, "/v1/agent-registry/evaluations/spec-champions", strings.NewReader(`{
			"agent_lineage":"support-agent",
			"agent_spec_hash":"sha256-spec",
			"endpoint_id":"`+endpointID.String()+`",
			"split_version":2,
			"min_task_success_rate":0.8
		}`))
		req.Header.Set(authz.HeaderOrgID, orgID.String())
		req.Header.Set(authz.HeaderUserID, userID.String())

		response, err := handlers.EvaluateSpecChampion(context.Background(), req)

		Expect(err).NotTo(HaveOccurred())
		Expect(response.StatusCode()).To(Equal(http.StatusCreated))
		Expect(uc.evaluateCommand.OrgID).To(Equal(orgID))
		Expect(uc.evaluateCommand.UserID).To(Equal(userID))
		Expect(uc.evaluateCommand.AgentSpecHash).To(Equal("sha256-spec"))
		Expect(uc.evaluateCommand.EndpointID).To(Equal(endpointID))
		Expect(uc.evaluateCommand.MinTaskSuccessRate).To(Equal(0.8))
		Expect(string(response.Payload())).To(ContainSubstring(reportID.String()))
	})

	It("reads eval reports scoped by the trusted org header", func() {
		reportID := uuid.New()
		uc.evalReport = &model.AgentEvalReport{
			ReportID:      reportID,
			AgentLineage:  "support-agent",
			AgentSpecHash: "sha256-spec",
			EndpointID:    uuid.New(),
			Split:         model.GoldenTaskSplitPromotionHoldout,
			SplitVersion:  2,
			RubricVersion: "trajectory_answer_contains_v1",
			TaskCount:     1,
			GateReason:    "candidate failed promotion holdout thresholds",
			EvaluatedBy:   userID,
			EvaluatedAt:   time.Now().UTC(),
		}
		req := httptest.NewRequest(http.MethodGet, "/v1/agent-registry/evaluations/"+reportID.String(), nil)
		req = mux.SetURLVars(req, map[string]string{"reportId": reportID.String()})
		req.Header.Set(authz.HeaderOrgID, orgID.String())
		req.Header.Set(authz.HeaderUserID, userID.String())

		response, err := handlers.ReadAgentEvalReport(context.Background(), req)

		Expect(err).NotTo(HaveOccurred())
		Expect(response.StatusCode()).To(Equal(http.StatusOK))
		Expect(uc.readEvalOrgID).To(Equal(orgID))
		Expect(uc.readEvalReportID).To(Equal(reportID))
		Expect(string(response.Payload())).To(ContainSubstring("support-agent"))
	})
})
