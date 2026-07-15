package db_test

import (
	"context"
	"errors"
	"time"

	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"
	inferencedb "inference_service/pkg/infra/repo/db"
	coreDB "lib/shared_lib/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("AgentTrajectoryRepository", func() {
	var (
		ctx        context.Context
		pool       *connectionPoolStub
		repository *inferencedb.AgentTrajectoryRepository
	)

	BeforeEach(func() {
		ctx = context.Background()
		pool = &connectionPoolStub{}
		repository = inferencedb.NewAgentTrajectoryRepository(coreDB.NewDatabase(pool, "test_db"))
	})

	It("records agent runs using database-owned ids and timestamps", func() {
		runID := uuid.New()
		startedAt := time.Now().UTC()
		run := validAgentRun()
		pool.nextRows = []pgx.Row{&repositoryRow{values: []any{runID.String(), startedAt}}}

		recorded, err := repository.RecordAgentRun(ctx, run)

		Expect(err).NotTo(HaveOccurred())
		Expect(recorded.RunID).To(Equal(runID))
		Expect(recorded.StartedAt).To(Equal(startedAt))
		Expect(pool.lastQuery).To(ContainSubstring("INSERT INTO test_db.agent_runs"))
		Expect(pool.lastQuery).To(ContainSubstring("RETURNING run_id::text, started_at"))
		Expect(pool.lastQuery).NotTo(ContainSubstring("started_at, finished_at"))
		args := namedArgs(pool.lastArgs)
		Expect(args).To(SatisfyAll(
			HaveKeyWithValue("org_id", pgtype.UUID{Bytes: run.OrgID, Valid: true}),
			HaveKeyWithValue("user_id", pgtype.UUID{Bytes: run.UserID, Valid: true}),
			HaveKeyWithValue("agent_spec_hash", run.AgentSpecHash),
			HaveKeyWithValue("status", model.AgentRunStatusRunning.String()),
		))
	})

	It("updates agent runs without resetting the database-owned started_at", func() {
		run := validAgentRun()
		run.RunID = uuid.New()
		startedAt := time.Now().UTC()
		run.Status = model.AgentRunStatusCompleted
		run.StopReason = model.AgentStopReasonFinalAnswer
		pool.nextRows = []pgx.Row{&repositoryRow{values: []any{run.RunID.String(), startedAt}}}

		recorded, err := repository.RecordAgentRun(ctx, run)

		Expect(err).NotTo(HaveOccurred())
		Expect(recorded.RunID).To(Equal(run.RunID))
		Expect(recorded.StartedAt).To(Equal(startedAt))
		Expect(pool.lastQuery).To(ContainSubstring("UPDATE test_db.agent_runs SET"))
		Expect(pool.lastQuery).To(ContainSubstring("finished_at = CASE"))
		Expect(pool.lastQuery).NotTo(ContainSubstring("started_at ="))
	})

	It("rejects agent runs without decoding params before querying", func() {
		run := validAgentRun()
		run.DecodingParams = nil

		_, err := repository.RecordAgentRun(ctx, run)

		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
		Expect(pool.queryRowCalled).To(BeFalse())
	})

	It("records agent steps using database-owned step ids", func() {
		stepID := uuid.New()
		createdAt := time.Now().UTC()
		step := &model.AgentStep{
			RunID:                uuid.New(),
			OrgID:                uuid.New(),
			StepIndex:            1,
			PresentedToolSchemas: []byte(`[]`),
			GenerationResult:     []byte(`{"content":"answer"}`),
			FinishReason:         model.GenerationFinishReasonStop,
			PromptTokens:         12,
			CompletionTokens:     4,
		}
		pool.nextRows = []pgx.Row{&repositoryRow{values: []any{stepID.String(), createdAt}}}

		recorded, err := repository.RecordAgentStep(ctx, step)

		Expect(err).NotTo(HaveOccurred())
		Expect(recorded.StepID).To(Equal(stepID))
		Expect(recorded.CreatedAt).To(Equal(createdAt))
		Expect(pool.lastQuery).To(ContainSubstring("INSERT INTO test_db.agent_steps"))
		Expect(pool.lastQuery).NotTo(ContainSubstring("step_id,"))
		Expect(pool.lastQuery).To(ContainSubstring("RETURNING step_id::text, created_at"))
	})

	It("rejects agent steps without generation payloads before querying", func() {
		step := validAgentStep()
		step.GenerationResult = nil

		_, err := repository.RecordAgentStep(ctx, step)

		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
		Expect(pool.queryRowCalled).To(BeFalse())
	})

	It("rejects agent steps with invalid presented tool schemas before querying", func() {
		step := validAgentStep()
		step.PresentedToolSchemas = []byte(`not-json`)

		_, err := repository.RecordAgentStep(ctx, step)

		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
		Expect(pool.queryRowCalled).To(BeFalse())
	})

	It("records tool invocations using database-owned invocation ids", func() {
		invocationID := uuid.New()
		createdAt := time.Now().UTC()
		invocation := &model.AgentToolInvocation{
			StepID:          uuid.New(),
			RunID:           uuid.New(),
			OrgID:           uuid.New(),
			ToolName:        "search_knowledge",
			ToolImplVersion: "search_knowledge_v1",
			Arguments:       []byte(`{"query_text":"hello","top_k":1}`),
			Result:          []byte(`{"contexts":[]}`),
		}
		pool.nextRows = []pgx.Row{&repositoryRow{values: []any{invocationID.String(), createdAt}}}

		recorded, err := repository.RecordToolInvocation(ctx, invocation)

		Expect(err).NotTo(HaveOccurred())
		Expect(recorded.InvocationID).To(Equal(invocationID))
		Expect(recorded.CreatedAt).To(Equal(createdAt))
		Expect(pool.lastQuery).To(ContainSubstring("INSERT INTO test_db.agent_tool_invocations"))
		Expect(pool.lastQuery).To(ContainSubstring("invocation_id,"))
		Expect(pool.lastQuery).To(ContainSubstring("RETURNING invocation_id::text, created_at"))
	})

	It("rejects tool invocations without arguments before querying", func() {
		invocation := validAgentToolInvocation()
		invocation.Arguments = nil

		_, err := repository.RecordToolInvocation(ctx, invocation)

		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
		Expect(pool.queryRowCalled).To(BeFalse())
	})

	It("reads a persisted agent trajectory aggregate", func() {
		runID := uuid.New()
		orgID := uuid.New()
		userID := uuid.New()
		endpointID := uuid.New()
		effectiveBaseID := uuid.New()
		stepID := uuid.New()
		invocationID := uuid.New()
		startedAt := time.Now().UTC()
		finishedAt := startedAt.Add(time.Second)
		createdAt := startedAt.Add(500 * time.Millisecond)
		pool.nextRows = []pgx.Row{&repositoryRow{values: []any{
			runID.String(),
			orgID.String(),
			userID.String(),
			endpointID.String(),
			"spec-hash",
			effectiveBaseID.String(),
			4,
			"toolset-hash",
			"rubric-v1",
			"trajectory-v1",
			"template-v1",
			`{"temperature":0}`,
			model.AgentRunStatusCompleted.String(),
			model.AgentStopReasonFinalAnswer.String(),
			startedAt,
			finishedAt,
			18,
			model.AgentTrainingEligibilityTenantOnly.String(),
		}}}
		pool.nextQueryRows = []pgx.Rows{
			&repositoryRows{rows: [][]any{{
				stepID.String(),
				runID.String(),
				orgID.String(),
				0,
				`[{"name":"search_knowledge"}]`,
				`{"content":"answer","finish_reason":"tool_calls"}`,
				string(model.GenerationFinishReasonToolCalls),
				12,
				6,
				createdAt,
			}}},
			&repositoryRows{rows: [][]any{{
				invocationID.String(),
				stepID.String(),
				runID.String(),
				orgID.String(),
				"search_knowledge",
				"search_knowledge_v1",
				`{"query_text":"support"}`,
				`{"contexts":[]}`,
				model.ToolErrorTypePermanent.String(),
				int64(15),
				createdAt,
			}}},
		}

		trajectory, err := repository.ReadAgentTrajectory(ctx, orgID, runID)

		Expect(err).NotTo(HaveOccurred())
		Expect(trajectory.Run.RunID).To(Equal(runID))
		Expect(trajectory.Run.OrgID).To(Equal(orgID))
		Expect(trajectory.Run.Status).To(Equal(model.AgentRunStatusCompleted))
		Expect(trajectory.Run.StopReason).To(Equal(model.AgentStopReasonFinalAnswer))
		Expect(trajectory.Run.DecodingParams).To(MatchJSON(`{"temperature":0}`))
		Expect(trajectory.Steps).To(HaveLen(1))
		Expect(trajectory.Steps[0].StepID).To(Equal(stepID))
		Expect(trajectory.Steps[0].FinishReason).To(Equal(model.GenerationFinishReasonToolCalls))
		Expect(trajectory.ToolInvocations).To(HaveLen(1))
		Expect(trajectory.ToolInvocations[0].InvocationID).To(Equal(invocationID))
		Expect(trajectory.ToolInvocations[0].ErrorType).To(Equal(model.ToolErrorTypePermanent))
		Expect(pool.queries).To(HaveLen(3))
		Expect(pool.queries[1]).To(ContainSubstring("FROM test_db.agent_steps"))
		Expect(pool.queries[2]).To(ContainSubstring("FROM test_db.agent_tool_invocations"))
	})

	It("maps a missing agent run to the domain not found error", func() {
		pool.nextRows = []pgx.Row{&repositoryRow{err: pgx.ErrNoRows}}

		_, err := repository.ReadAgentTrajectory(ctx, uuid.New(), uuid.New())

		Expect(errors.Is(err, domain.ErrAgentRunNotFound)).To(BeTrue())
		Expect(pool.queryCalled).To(BeFalse())
	})
})

func validAgentRun() *model.AgentRun {
	return &model.AgentRun{
		OrgID:                   uuid.New(),
		UserID:                  uuid.New(),
		EndpointID:              uuid.New(),
		AgentSpecHash:           "sha256:spec",
		ModelVersion:            3,
		ToolsetHash:             "sha256:toolset",
		TrajectorySchemaVersion: "agent_trajectory_v1",
		SystemTemplateVersion:   "sha256:template",
		DecodingParams:          []byte(`{"temperature":0}`),
		Status:                  model.AgentRunStatusRunning,
		TrainingEligibility:     model.AgentTrainingEligibilityTenantOnly,
	}
}

func validAgentStep() *model.AgentStep {
	return &model.AgentStep{
		RunID:                uuid.New(),
		OrgID:                uuid.New(),
		StepIndex:            1,
		PresentedToolSchemas: []byte(`[]`),
		GenerationResult:     []byte(`{"content":"answer"}`),
		FinishReason:         model.GenerationFinishReasonStop,
		PromptTokens:         12,
		CompletionTokens:     4,
	}
}

func validAgentToolInvocation() *model.AgentToolInvocation {
	return &model.AgentToolInvocation{
		StepID:          uuid.New(),
		RunID:           uuid.New(),
		OrgID:           uuid.New(),
		ToolName:        "search_knowledge",
		ToolImplVersion: "search_knowledge_v1",
		Arguments:       []byte(`{"query_text":"hello","top_k":1}`),
		Result:          []byte(`{"contexts":[]}`),
	}
}
