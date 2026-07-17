package adapter

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"inference_service/pkg/domain/model"

	serializers "lib/shared_lib/serializer"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestAdapter(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Inference service adapter unit test suite")
}

var _ = Describe("Inference DTO adapters", func() {
	var (
		generationAdapter *generationDTOAdapter
		feedbackAdapter   *feedbackDTOAdapter
		preferenceAdapter *preferenceDatasetDTOAdapter
		endpointAdapter   *endpointDTOAdapter
		specAdapter       *agentSpecDTOAdapter
		trajectoryAdapter *agentTrajectoryDTOAdapter
	)

	BeforeEach(func() {
		generationAdapter = NewGenerationDTOAdapter(serializers.NewJSONSerializer())
		feedbackAdapter = NewFeedbackDTOAdapter(serializers.NewJSONSerializer())
		preferenceAdapter = NewPreferenceDatasetDTOAdapter(serializers.NewJSONSerializer())
		endpointAdapter = NewEndpointDTOAdapter(serializers.NewJSONSerializer())
		specAdapter = NewAgentSpecDTOAdapter(serializers.NewJSONSerializer())
		trajectoryAdapter = NewAgentTrajectoryDTOAdapter(serializers.NewJSONSerializer())
	})

	It("maps generation DTOs to domain requests", func() {
		request, err := generationAdapter.FromDTO(context.Background(), []byte(`{
			"query_text":"What did the report say?",
			"top_k":3,
			"metadata_filters":{"section":"summary"}
		}`))

		Expect(err).NotTo(HaveOccurred())
		Expect(request.QueryText).To(Equal("What did the report say?"))
		Expect(request.TopK).To(Equal(3))
		Expect(request.MetadataFilters).To(HaveKeyWithValue("section", "summary"))
	})

	It("defaults top_k and rejects invalid generation DTOs", func() {
		request, err := generationAdapter.FromDTO(context.Background(), []byte(`{"query_text":"hello"}`))
		Expect(err).NotTo(HaveOccurred())
		Expect(request.TopK).To(Equal(defaultTopK))

		_, err = generationAdapter.FromDTO(context.Background(), []byte(`{"top_k":1}`))
		Expect(err).To(HaveOccurred())

		_, err = generationAdapter.FromDTO(context.Background(), []byte(`{"query_text":"hello","top_k":0}`))
		Expect(err).To(HaveOccurred())
	})

	It("serializes generation responses with retrieval provenance", func() {
		datasetID := uuid.New()
		contextDatasetID := uuid.New()
		payload, err := generationAdapter.ToDTO(context.Background(), &model.GenerateResponse{
			RequestID:        uuid.New(),
			DatasetID:        datasetID,
			DatasetIDs:       []uuid.UUID{contextDatasetID},
			ModelID:          uuid.New(),
			QueryText:        "hello",
			Answer:           "world",
			RAGMergeStrategy: model.RAGMergeStrategyReranker,
			Contexts: []model.RetrievedContext{{
				DatasetID:  contextDatasetID,
				ChunkIndex: 1,
				SourceText: "source",
				Similarity: 0.9,
			}},
		})

		Expect(err).NotTo(HaveOccurred())
		var dto map[string]any
		Expect(json.Unmarshal(payload, &dto)).To(Succeed())
		Expect(dto).To(HaveKey("request_id"))
		Expect(dto).To(HaveKeyWithValue("answer", "world"))
		Expect(dto).To(HaveKeyWithValue("dataset_id", datasetID.String()))
		Expect(dto).To(HaveKeyWithValue("rag_merge_strategy", model.RAGMergeStrategyReranker.String()))
		Expect(dto).NotTo(HaveKey("model_id"))
	})

	It("maps feedback DTOs to domain feedback", func() {
		requestID := uuid.New()
		feedback, err := feedbackAdapter.FromDTO(context.Background(), []byte(`{
			"request_id":"`+requestID.String()+`",
			"accepted":true,
			"rating":1,
			"comment":"good",
			"preferred_answer":"better"
		}`))

		Expect(err).NotTo(HaveOccurred())
		Expect(feedback.RequestID).To(Equal(requestID))
		Expect(feedback.Accepted).To(BeTrue())
		Expect(feedback.Rating).To(Equal(1))
		Expect(feedback.Comment).To(Equal("good"))
	})

	It("rejects invalid feedback DTOs", func() {
		_, err := feedbackAdapter.FromDTO(context.Background(), []byte(`{"accepted":true}`))
		Expect(err).To(HaveOccurred())

		_, err = feedbackAdapter.FromDTO(context.Background(), []byte(`{"request_id":"not-a-uuid","rating":1}`))
		Expect(err).To(HaveOccurred())

		_, err = feedbackAdapter.FromDTO(context.Background(), []byte(`{"request_id":"`+uuid.NewString()+`","rating":2}`))
		Expect(err).To(HaveOccurred())
	})

	It("rejects request-scoped preference dataset output templates", func() {
		_, err := preferenceAdapter.FromDTO(context.Background(), []byte(`{
			"output_uri":"s3://local-dev-bucket/preferences/{request_id}.jsonl",
			"min_examples":1
		}`))

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("request_id"))
	})

	It("serializes preference datasets without a fake lifecycle status", func() {
		preferenceDatasetID := uuid.New()
		modelID := uuid.New()
		payload, err := preferenceAdapter.ToDTO(context.Background(), &model.PreferenceDataset{
			PreferenceDatasetID: preferenceDatasetID,
			ModelID:             modelID,
			ParentModelKind:     model.ModelKindBase,
			ParentArtifactURI:   "s3://models/base",
			ParentBaseModel:     "llama-3",
			ParentModelName:     "llama-3",
			ParentModelVersion:  1,
			OutputURI:           "s3://preferences/train.jsonl",
			Format:              "DPO_JSONL",
			EligibilityPolicy:   "complete_rejected_pairs_train_eval_split_v1",
			IntegrityKey:        "sha256:pref",
			ExampleTotal:        2,
		})

		Expect(err).NotTo(HaveOccurred())
		var dtos []map[string]any
		Expect(json.Unmarshal(payload, &dtos)).To(Succeed())
		Expect(dtos).To(HaveLen(1))
		Expect(dtos[0]).To(HaveKeyWithValue("preference_dataset_id", preferenceDatasetID.String()))
		Expect(dtos[0]).To(HaveKeyWithValue("integrity_key", "sha256:pref"))
		Expect(dtos[0]).NotTo(HaveKey("status"))
	})

	It("maps a schema-validated agent spec to the domain model", func() {
		modelID := uuid.New()
		spec, err := specAdapter.FromDTO(context.Background(), []byte(`
schema_version: agent_spec_v1
agent_lineage: support-agent
system_prompt: Use tools before answering.
model_binding:
  model_id: "`+modelID.String()+`"
tools:
  - name: search_knowledge
    required: true
    tool_choice: required
budgets:
  max_steps: 3
  token: 512
  wall_ms: 60000
`))

		Expect(err).NotTo(HaveOccurred())
		Expect(spec.Spec.ModelID).To(Equal(modelID))
		Expect(spec.Spec.SystemPrompt).To(Equal("Use tools before answering."))
		Expect(spec.Spec.ToolBindings).To(HaveLen(1))
		Expect(spec.Spec.ToolBindings[0].Name).To(Equal("search_knowledge"))
		Expect(spec.Spec.Budgets.WallMs).To(Equal(60000))
		Expect(spec.Spec.ContentHash).NotTo(BeEmpty())
		Expect(spec.Spec.ValidationReport).To(ContainSubstring("schema_status=validated"))
	})

	It("accepts implemented remote tool bindings through the agent spec schema", func() {
		modelID := uuid.New()
		spec, err := specAdapter.FromDTO(context.Background(), []byte(`
schema_version: agent_spec_v1
agent_lineage: support-agent
model_binding:
  model_id: "`+modelID.String()+`"
tools:
  - name: http_get
budgets:
  max_steps: 2
  token: 512
  wall_ms: 60000
`))

		Expect(err).NotTo(HaveOccurred())
		Expect(spec.Spec.ToolBindings).To(HaveLen(1))
		Expect(spec.Spec.ToolBindings[0].Name).To(Equal("http_get"))
	})

	It("accepts syntactically valid remote tool names through the schema", func() {
		modelID := uuid.New()
		spec, err := specAdapter.FromDTO(context.Background(), []byte(`
schema_version: agent_spec_v1
agent_lineage: support-agent
model_binding:
  model_id: "`+modelID.String()+`"
tools:
  - name: write_database
budgets:
  max_steps: 2
  token: 512
  wall_ms: 60000
`))

		Expect(err).NotTo(HaveOccurred())
		Expect(spec.Spec.ToolBindings).To(HaveLen(1))
		Expect(spec.Spec.ToolBindings[0].Name).To(Equal("write_database"))
	})

	It("rejects duplicate agent tool bindings at the DTO boundary", func() {
		modelID := uuid.New()

		_, err := specAdapter.FromDTO(context.Background(), []byte(`
schema_version: agent_spec_v1
agent_lineage: support-agent
model_binding:
  model_id: "`+modelID.String()+`"
tools:
  - name: search_knowledge
  - name: Search_Knowledge
budgets:
  max_steps: 3
  token: 512
  wall_ms: 60000
`))

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("tool names must be unique"))
	})

	It("rejects agent spec fields outside the JSON schema", func() {
		modelID := uuid.New()
		_, err := specAdapter.FromDTO(context.Background(), []byte(`
schema_version: agent_spec_v1
agent_lineage: support-agent
model_binding:
  model_id: "`+modelID.String()+`"
  requires_tool_calls: false
tools:
  - name: search_knowledge
budgets:
  max_steps: 2
  token: 256
  wall_ms: 60000
`))

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("requires_tool_calls"))
	})

	It("accepts agent specs without an effective base id at the DTO boundary", func() {
		modelID := uuid.New()
		spec, err := specAdapter.FromDTO(context.Background(), []byte(`
schema_version: agent_spec_v1
agent_lineage: support-agent
model_binding:
  model_id: "`+modelID.String()+`"
tools:
  - name: search_knowledge
budgets:
  max_steps: 2
  token: 256
  wall_ms: 60000
`))

		Expect(err).NotTo(HaveOccurred())
		Expect(spec.Spec.ModelID).To(Equal(modelID))
	})

	It("rejects agent specs that exceed deployment budget caps at the DTO boundary", func() {
		cappedAdapter := NewAgentSpecDTOAdapter(serializers.NewJSONSerializer(), WithAgentSpecBudgetCaps(2, 512, 60000))
		modelID := uuid.New()

		_, err := cappedAdapter.FromDTO(context.Background(), []byte(`
schema_version: agent_spec_v1
agent_lineage: support-agent
model_binding:
  model_id: "`+modelID.String()+`"
tools:
  - name: search_knowledge
budgets:
  max_steps: 3
  token: 256
  wall_ms: 60000
`))

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("max_steps exceeds deployment cap"))
	})

	It("rejects sub-second wall budgets at the DTO boundary", func() {
		modelID := uuid.New()

		_, err := specAdapter.FromDTO(context.Background(), []byte(`
schema_version: agent_spec_v1
agent_lineage: support-agent
model_binding:
  model_id: "`+modelID.String()+`"
tools: []
budgets:
  max_steps: 1
  token: 256
  wall_ms: 1
`))

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("/budgets/wall_ms"))
		Expect(err.Error()).To(ContainSubstring("minimum"))
	})

	It("rejects tool-bound agent specs with only one step at the DTO boundary", func() {
		modelID := uuid.New()

		_, err := specAdapter.FromDTO(context.Background(), []byte(`
schema_version: agent_spec_v1
agent_lineage: support-agent
model_binding:
  model_id: "`+modelID.String()+`"
tools:
  - name: search_knowledge
    required: true
budgets:
  max_steps: 1
  token: 256
  wall_ms: 60000
`))

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("max_steps must be at least 2"))
	})

	It("rejects unknown endpoint modes at the DTO boundary", func() {
		_, err := endpointAdapter.FromDTO(context.Background(), []byte(`{
			"model_id":"`+uuid.NewString()+`",
			"dataset_ids":["`+uuid.NewString()+`"],
			"mode":"maybe-agent",
			"display_name":"support endpoint"
		}`))

		Expect(err).To(HaveOccurred())
		Expect(strings.ToLower(err.Error())).To(ContainSubstring("mode"))
	})

	It("serializes safe endpoint projections", func() {
		endpointID := uuid.New()
		modelID := uuid.New()
		datasetID := uuid.New()
		payload, err := endpointAdapter.ToDTOs(context.Background(), []*model.PublishedEndpoint{{
			EndpointID:    endpointID,
			OrgID:         uuid.New(),
			ModelID:       modelID,
			DatasetIDs:    []uuid.UUID{datasetID},
			MergeStrategy: model.RAGMergeStrategyReranker,
			Status:        model.PublishedEndpointStatusReady,
			DisplayName:   "Support bot",
		}})

		Expect(err).NotTo(HaveOccurred())
		var dtos []map[string]any
		Expect(json.Unmarshal(payload, &dtos)).To(Succeed())
		Expect(dtos).To(HaveLen(1))
		Expect(dtos[0]).To(HaveKeyWithValue("endpoint_id", endpointID.String()))
		Expect(dtos[0]).To(HaveKeyWithValue("display_name", "Support bot"))
		Expect(dtos[0]).To(HaveKeyWithValue("merge_strategy", model.RAGMergeStrategyReranker.String()))
		Expect(dtos[0]).NotTo(HaveKey("model_id"))
		Expect(dtos[0]).NotTo(HaveKey("dataset_ids"))
		Expect(dtos[0]).NotTo(HaveKey("dataset_id"))
		Expect(dtos[0]).NotTo(HaveKey("created_by_user_id"))
	})

	It("serializes endpoint details for endpoint management responses", func() {
		endpointID := uuid.New()
		modelID := uuid.New()
		datasetID := uuid.New()
		createdBy := uuid.New()
		payload, err := endpointAdapter.ToDetailDTOs(context.Background(), []*model.PublishedEndpoint{{
			EndpointID:      endpointID,
			OrgID:           uuid.New(),
			ModelID:         modelID,
			DatasetIDs:      []uuid.UUID{datasetID},
			MergeStrategy:   model.RAGMergeStrategyReranker,
			Status:          model.PublishedEndpointStatusReady,
			DisplayName:     "Support bot",
			CreatedByUserID: createdBy,
		}})

		Expect(err).NotTo(HaveOccurred())
		var dtos []map[string]any
		Expect(json.Unmarshal(payload, &dtos)).To(Succeed())
		Expect(dtos).To(HaveLen(1))
		Expect(dtos[0]).To(HaveKeyWithValue("endpoint_id", endpointID.String()))
		Expect(dtos[0]).To(HaveKeyWithValue("model_id", modelID.String()))
		Expect(dtos[0]).To(HaveKeyWithValue("dataset_ids", []any{datasetID.String()}))
		Expect(dtos[0]).To(HaveKeyWithValue("created_by_user_id", createdBy.String()))
	})

	It("serializes captured agent trajectory JSON without losing tuple fields", func() {
		runID := uuid.New()
		orgID := uuid.New()
		userID := uuid.New()
		endpointID := uuid.New()
		stepID := uuid.New()
		invocationID := uuid.New()
		startedAt := time.Date(2026, 7, 15, 10, 11, 12, 0, time.UTC)
		deadlineAt := startedAt.Add(60 * time.Second)
		payload, err := trajectoryAdapter.ToDTO(context.Background(), &model.AgentTrajectory{
			Run: &model.AgentRun{
				RunID:                   runID,
				OrgID:                   orgID,
				UserID:                  userID,
				EndpointID:              endpointID,
				AgentSpecHash:           "sha256:agent-spec",
				ToolsetHash:             "sha256:toolset",
				TrajectorySchemaVersion: "agent_trajectory_v1",
				DecodingParams:          []byte(`{"temperature":0,"seed":123}`),
				Status:                  model.AgentRunStatusCompleted,
				StopReason:              model.AgentStopReasonFinalAnswer,
				StartedAt:               startedAt,
				DeadlineAt:              deadlineAt,
				TotalTokens:             19,
				WallMs:                  60000,
			},
			Steps: []*model.AgentStep{{
				StepID:               stepID,
				RunID:                runID,
				OrgID:                orgID,
				StepIndex:            0,
				PresentedToolSchemas: []byte(`[{"name":"search_knowledge"}]`),
				GenerationResult:     []byte(`{"content":"","tool_calls":[{"name":"search_knowledge"}]}`),
				FinishReason:         model.GenerationFinishReasonToolCalls,
				PromptTokens:         11,
				CompletionTokens:     8,
				CreatedAt:            startedAt.Add(time.Second),
			}},
			ToolInvocations: []*model.AgentToolInvocation{{
				InvocationID:    invocationID,
				StepID:          stepID,
				RunID:           runID,
				ToolName:        "search_knowledge",
				ToolImplVersion: "search_knowledge_v1",
				Arguments:       []byte(`{"query_text":"support"}`),
				Result:          []byte(`{"contexts":[]}`),
				ErrorType:       model.ToolErrorTypeTransient,
				LatencyMs:       17,
				CreatedAt:       startedAt.Add(2 * time.Second),
			}},
		})

		Expect(err).NotTo(HaveOccurred())
		var dto map[string]any
		Expect(json.Unmarshal(payload, &dto)).To(Succeed())
		run := dto["run"].(map[string]any)
		Expect(run).To(HaveKeyWithValue("run_id", runID.String()))
		Expect(run).To(HaveKeyWithValue("agent_spec_hash", "sha256:agent-spec"))
		Expect(run).To(HaveKeyWithValue("wall_ms", BeNumerically("==", 60000)))
		Expect(run).To(HaveKeyWithValue("deadline_at", deadlineAt.Format(time.RFC3339Nano)))
		Expect(run["decoding_params"]).To(HaveKeyWithValue("seed", BeNumerically("==", 123)))
		steps := dto["steps"].([]any)
		Expect(steps).To(HaveLen(1))
		Expect(steps[0].(map[string]any)["presented_tool_schemas"]).To(HaveLen(1))
		Expect(steps[0].(map[string]any)["generation_result"]).To(HaveKey("tool_calls"))
		invocations := dto["tool_invocations"].([]any)
		Expect(invocations).To(HaveLen(1))
		Expect(invocations[0].(map[string]any)).To(HaveKeyWithValue("error_type", model.ToolErrorTypeTransient.String()))
	})
})
