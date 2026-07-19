package adapter_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"agent_registry_service/pkg/domain"
	"agent_registry_service/pkg/domain/model"
	agentadapter "agent_registry_service/pkg/infra/network/adapter"
	serializers "lib/shared_lib/serializer"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestAgentRegistryDTOAdapter(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Agent registry DTO adapter unit test suite")
}

var _ = Describe("AgentRegistryDTOAdapter", func() {
	var adapter agentadapter.AgentRegistryDTOAdapter

	BeforeEach(func() {
		adapter = agentadapter.NewAgentRegistryDTOAdapter(serializers.NewJSONSerializer())
	})

	It("parses register spec requests at the boundary", func() {
		command, err := adapter.FromRegisterSpecVersionDTO(context.Background(), []byte(`{
			"agent_lineage":"support-agent",
			"agent_spec_hash":"sha256-spec"
		}`))

		Expect(err).NotTo(HaveOccurred())
		Expect(command.AgentLineage).To(Equal("support-agent"))
		Expect(command.AgentSpecHash).To(Equal("sha256-spec"))
	})

	It("rejects invalid endpoint IDs at the boundary", func() {
		_, err := adapter.FromRegisterEndpointBindingDTO(context.Background(), []byte(`{
			"agent_lineage":"support-agent",
			"endpoint_id":"not-a-uuid"
		}`))

		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrAgentRegistryValidation)).To(BeTrue())
	})

	It("parses explicit champion decision metadata", func() {
		decisionID := uuid.New()
		decidedAt := time.Date(2026, 7, 18, 12, 30, 0, 0, time.UTC)

		command, err := adapter.FromPromoteSpecChampionDTO(context.Background(), []byte(`{
			"agent_lineage":"support-agent",
			"agent_spec_hash":"sha256-spec",
			"decision_id":"`+decisionID.String()+`",
			"decided_at":"`+decidedAt.Format(time.RFC3339Nano)+`"
		}`))

		Expect(err).NotTo(HaveOccurred())
		Expect(command.AgentLineage).To(Equal("support-agent"))
		Expect(command.AgentSpecHash).To(Equal("sha256-spec"))
		Expect(command.DecisionID).To(Equal(decisionID))
		Expect(command.DecidedAt).To(Equal(decidedAt))
	})

	It("parses golden task imports at the boundary", func() {
		command, err := adapter.FromImportGoldenTasksDTO(context.Background(), []byte(`{
			"agent_lineage":"support-agent",
			"split":"seed_train",
			"split_version":4,
			"tasks":[{"group_key":"group-a","prompt":"Who signed the agreement?","expected_answer":"Alice","expected_answer_rubric_id":"rubric-answer-v1"}]
		}`))

		Expect(err).NotTo(HaveOccurred())
		Expect(command.AgentLineage).To(Equal("support-agent"))
		Expect(command.Split).To(Equal(model.GoldenTaskSplitSeedTrain))
		Expect(command.SplitVersion).To(Equal(4))
		Expect(command.Tasks).To(HaveLen(1))
		Expect(command.Tasks[0].Prompt).To(Equal("Who signed the agreement?"))
		Expect(command.Tasks[0].ExpectedAnswer).To(Equal("Alice"))
	})

	It("rejects invalid golden task splits at the boundary", func() {
		_, err := adapter.FromImportGoldenTasksDTO(context.Background(), []byte(`{
			"agent_lineage":"support-agent",
			"split":"training",
			"split_version":1,
			"tasks":[{"prompt":"Who signed the agreement?","expected_answer":"Alice","expected_answer_rubric_id":"rubric-answer-v1"}]
		}`))

		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrAgentRegistryValidation)).To(BeTrue())
	})

	It("parses golden task list filters from query values", func() {
		command, err := adapter.FromListGoldenTasksDTO(context.Background(), map[string]string{
			"agent_lineage": "support-agent",
			"split":         "promotion_holdout",
			"split_version": "2",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(command.AgentLineage).To(Equal("support-agent"))
		Expect(command.Split).To(Equal(model.GoldenTaskSplitPromotionHoldout))
		Expect(command.SplitVersion).To(Equal(2))
	})

	It("serializes champion adapter serving fields when present", func() {
		adapterID := uuid.New()
		servingModelID := uuid.New()
		raw, err := adapter.ToChampionStateDTO(context.Background(), &model.AgentChampionState{
			AgentLineage:          "support-agent",
			ChampionAgentSpecHash: "sha256-spec",
			ChampionAdapterID:     adapterID,
			ServingModelID:        servingModelID,
			DecisionID:            uuid.New(),
			DecidedBy:             uuid.New(),
			DecidedAt:             time.Date(2026, 7, 18, 11, 0, 0, 0, time.UTC),
		})

		Expect(err).NotTo(HaveOccurred())
		var dtos []agentadapter.AgentChampionStateDTO
		Expect(json.Unmarshal(raw, &dtos)).To(Succeed())
		Expect(dtos).To(HaveLen(1))
		Expect(dtos[0].ChampionAdapterID).To(Equal(adapterID.String()))
		Expect(dtos[0].ServingModelID).To(Equal(servingModelID.String()))
	})

	It("serializes golden tasks", func() {
		taskID := uuid.New()
		raw, err := adapter.ToGoldenTaskDTOs(context.Background(), []*model.GoldenTask{{
			TaskID:                 taskID,
			AgentLineage:           "support-agent",
			Split:                  model.GoldenTaskSplitDevEval,
			SplitVersion:           1,
			Prompt:                 "Who signed the agreement?",
			NormalizedPromptHash:   "hash-prompt",
			ContentFingerprint:     "fingerprint",
			ExpectedAnswer:         "Alice",
			ExpectedAnswerRubricID: "rubric-answer-v1",
			CreatedByUserID:        uuid.New(),
			CreatedAt:              time.Date(2026, 7, 18, 11, 0, 0, 0, time.UTC),
		}})

		Expect(err).NotTo(HaveOccurred())
		var dtos []agentadapter.GoldenTaskDTO
		Expect(json.Unmarshal(raw, &dtos)).To(Succeed())
		Expect(dtos).To(HaveLen(1))
		Expect(dtos[0].TaskID).To(Equal(taskID.String()))
		Expect(dtos[0].Split).To(Equal("DEV_EVAL"))
		Expect(dtos[0].ContentFingerprint).To(Equal("fingerprint"))
		Expect(dtos[0].ExpectedAnswer).To(Equal("Alice"))
	})

	It("parses eval gate commands at the boundary", func() {
		endpointID := uuid.New()

		command, err := adapter.FromEvaluateSpecChampionDTO(context.Background(), []byte(`{
			"agent_lineage":"support-agent",
			"agent_spec_hash":"sha256-spec",
			"endpoint_id":"`+endpointID.String()+`",
			"split_version":2,
			"min_task_success_rate":0.8,
			"min_tool_success_rate":0.9,
			"min_groundedness_rate":0.7
		}`))

		Expect(err).NotTo(HaveOccurred())
		Expect(command.AgentLineage).To(Equal("support-agent"))
		Expect(command.AgentSpecHash).To(Equal("sha256-spec"))
		Expect(command.EndpointID).To(Equal(endpointID))
		Expect(command.SplitVersion).To(Equal(2))
		Expect(command.MinTaskSuccessRate).To(Equal(0.8))
		Expect(command.MinToolSuccessRate).To(Equal(0.9))
		Expect(command.MinGroundednessRate).To(Equal(0.7))
	})

	It("parses agent run label commands at the boundary", func() {
		runID := uuid.New()

		command, err := adapter.FromLabelAgentRunDTO(context.Background(), []byte(`{
			"agent_lineage":"support-agent",
			"run_id":"`+runID.String()+`",
			"prompt":"Who signed the agreement?",
			"evaluator":"human:reviewer-a",
			"task_success":true,
			"tool_selection_score":0.75,
			"groundedness":0.9,
			"policy_violations":0,
			"confidence":0.8,
			"label_source":"human",
			"rubric_version":"trajectory_answer_contains_v1"
		}`))

		Expect(err).NotTo(HaveOccurred())
		Expect(command.AgentLineage).To(Equal("support-agent"))
		Expect(command.RunID).To(Equal(runID))
		Expect(command.ToolSelectionScore).To(Equal(0.75))
		Expect(command.RubricVersion).To(Equal("trajectory_answer_contains_v1"))
	})

	It("rejects invalid label scores at the boundary", func() {
		_, err := adapter.FromLabelAgentRunDTO(context.Background(), []byte(`{
			"agent_lineage":"support-agent",
			"run_id":"`+uuid.New().String()+`",
			"prompt":"Who signed the agreement?",
			"evaluator":"human:reviewer-a",
			"tool_selection_score":1.2,
			"groundedness":0.9,
			"policy_violations":0,
			"confidence":0.8,
			"label_source":"human",
			"rubric_version":"trajectory_answer_contains_v1"
		}`))

		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrAgentRegistryValidation)).To(BeTrue())
	})

	It("parses run label list filters from query values", func() {
		command, err := adapter.FromListAgentRunLabelsDTO(context.Background(), map[string]string{
			"agent_lineage": "support-agent",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(command.AgentLineage).To(Equal("support-agent"))
	})

	It("parses trajectory dataset build commands at the boundary", func() {
		command, err := adapter.FromBuildTrajectoryDatasetDTO(context.Background(), []byte(`{
			"agent_lineage":"support-agent",
			"golden_split_version":3
		}`))

		Expect(err).NotTo(HaveOccurred())
		Expect(command.AgentLineage).To(Equal("support-agent"))
		Expect(command.GoldenSplitVersion).To(Equal(3))
	})

	It("parses adapter training commands at the boundary", func() {
		datasetID := uuid.New()

		command, err := adapter.FromDispatchAgentAdapterTrainingDTO(context.Background(), []byte(`{
			"agent_lineage":"support-agent",
			"dataset_id":"`+datasetID.String()+`",
			"training_profile":"agent-sft-dpo-fast@v1"
		}`))

		Expect(err).NotTo(HaveOccurred())
		Expect(command.AgentLineage).To(Equal("support-agent"))
		Expect(command.DatasetID).To(Equal(datasetID))
		Expect(command.TrainingProfile).To(Equal("agent-sft-dpo-fast@v1"))
	})

	It("parses adapter candidate eval commands at the boundary", func() {
		adapterID := uuid.New()
		endpointID := uuid.New()

		command, err := adapter.FromEvaluateAdapterCandidateDTO(context.Background(), []byte(`{
			"agent_lineage":"support-agent",
			"adapter_id":"`+adapterID.String()+`",
			"endpoint_id":"`+endpointID.String()+`",
			"split_version":2,
			"min_task_success_rate":0.8
		}`))

		Expect(err).NotTo(HaveOccurred())
		Expect(command.AgentLineage).To(Equal("support-agent"))
		Expect(command.AdapterID).To(Equal(adapterID))
		Expect(command.EndpointID).To(Equal(endpointID))
		Expect(command.MinTaskSuccessRate).To(Equal(0.8))
	})

	It("parses adapter promotion commands at the boundary", func() {
		adapterID := uuid.New()
		reportID := uuid.New()

		command, err := adapter.FromPromoteAgentAdapterDTO(context.Background(), []byte(`{
			"agent_lineage":"support-agent",
			"adapter_id":"`+adapterID.String()+`",
			"report_id":"`+reportID.String()+`",
			"min_delta":0.05
		}`))

		Expect(err).NotTo(HaveOccurred())
		Expect(command.AgentLineage).To(Equal("support-agent"))
		Expect(command.AdapterID).To(Equal(adapterID))
		Expect(command.ReportID).To(Equal(reportID))
		Expect(command.MinDelta).To(Equal(0.05))
	})

	It("rejects invalid eval thresholds at the boundary", func() {
		_, err := adapter.FromEvaluateSpecChampionDTO(context.Background(), []byte(`{
			"agent_lineage":"support-agent",
			"agent_spec_hash":"sha256-spec",
			"endpoint_id":"`+uuid.New().String()+`",
			"split_version":2,
			"min_task_success_rate":1.5
		}`))

		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrAgentRegistryValidation)).To(BeTrue())
	})

	It("serializes trajectory datasets", func() {
		datasetID := uuid.New()
		raw, err := adapter.ToTrajectoryDatasetDTO(context.Background(), &model.AgentTrajectoryDataset{
			DatasetID:          datasetID,
			AgentLineage:       "support-agent",
			GoldenSplitVersion: 2,
			ContentHash:        "sha256-dataset",
			DatasetURI:         "agent-registry://trajectory-datasets/sha256-dataset",
			Format:             "AGENT_TRAJECTORY_JSON",
			LabelCount:         2,
			Manifest:           json.RawMessage(`{"schema_version":"agent_trajectory_dataset_v1"}`),
			EffectiveBaseID:    "sha256-base",
			AgentSpecHash:      "sha256-spec",
			ToolsetHash:        "sha256-tools",
			DataSnapshotHash:   "sha256-data",
			CreatedByUserID:    uuid.New(),
			CreatedAt:          time.Date(2026, 7, 18, 11, 0, 0, 0, time.UTC),
		})

		Expect(err).NotTo(HaveOccurred())
		var dtos []agentadapter.TrajectoryDatasetDTO
		Expect(json.Unmarshal(raw, &dtos)).To(Succeed())
		Expect(dtos).To(HaveLen(1))
		Expect(dtos[0].DatasetID).To(Equal(datasetID.String()))
		Expect(dtos[0].EffectiveBaseID).To(Equal("sha256-base"))
		Expect(dtos[0].Manifest).To(MatchJSON(`{"schema_version":"agent_trajectory_dataset_v1"}`))
	})

	It("serializes agent adapters", func() {
		adapterID := uuid.New()
		servingModelID := uuid.New()
		raw, err := adapter.ToAgentAdapterDTO(context.Background(), &model.AgentAdapter{
			AdapterID:                        adapterID,
			AgentLineage:                     "support-agent",
			DatasetID:                        uuid.New(),
			TrainingRunID:                    uuid.New(),
			ServingModelID:                   servingModelID,
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
			CreatedByUserID:                  uuid.New(),
			CreatedAt:                        time.Date(2026, 7, 18, 11, 0, 0, 0, time.UTC),
			UpdatedAt:                        time.Date(2026, 7, 18, 11, 5, 0, 0, time.UTC),
		})

		Expect(err).NotTo(HaveOccurred())
		var dtos []agentadapter.AgentAdapterDTO
		Expect(json.Unmarshal(raw, &dtos)).To(Succeed())
		Expect(dtos).To(HaveLen(1))
		Expect(dtos[0].AdapterID).To(Equal(adapterID.String()))
		Expect(dtos[0].ServingModelID).To(Equal(servingModelID.String()))
		Expect(dtos[0].Status).To(Equal("PROMOTED"))
		Expect(dtos[0].PromotionPassed).To(BeTrue())
	})

	It("serializes eval reports without fabricating missing run ids", func() {
		reportID := uuid.New()
		taskID := uuid.New()
		raw, err := adapter.ToAgentEvalReportDTO(context.Background(), &model.AgentEvalReport{
			ReportID:         reportID,
			AgentLineage:     "support-agent",
			AgentSpecHash:    "sha256-spec",
			EndpointID:       uuid.New(),
			Split:            model.GoldenTaskSplitPromotionHoldout,
			SplitVersion:     1,
			RubricVersion:    "trajectory_answer_contains_v1",
			TaskCount:        1,
			TaskSuccessRate:  0,
			ToolSuccessRate:  0,
			GroundednessRate: 0,
			Passed:           false,
			GateReason:       "runner failed",
			EvaluatedBy:      uuid.New(),
			EvaluatedAt:      time.Date(2026, 7, 18, 11, 0, 0, 0, time.UTC),
			TaskResults: []*model.AgentEvalTaskResult{{
				TaskID:        taskID,
				Status:        "FAILED",
				StopReason:    "RUNTIME_ERROR",
				FailureReason: "agent workflow failed",
			}},
		})

		Expect(err).NotTo(HaveOccurred())
		var dtos []agentadapter.AgentEvalReportDTO
		Expect(json.Unmarshal(raw, &dtos)).To(Succeed())
		Expect(dtos).To(HaveLen(1))
		Expect(dtos[0].ReportID).To(Equal(reportID.String()))
		Expect(dtos[0].TaskResults).To(HaveLen(1))
		Expect(dtos[0].TaskResults[0].TaskID).To(Equal(taskID.String()))
		Expect(dtos[0].TaskResults[0].RunID).To(BeEmpty())
		Expect(string(raw)).NotTo(ContainSubstring("00000000-0000-0000-0000-000000000000"))
	})
})
