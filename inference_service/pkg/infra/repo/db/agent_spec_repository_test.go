package db_test

import (
	"context"
	"encoding/json"
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

var _ = Describe("AgentSpecRepository", func() {
	var (
		ctx        context.Context
		pool       *connectionPoolStub
		repository *inferencedb.AgentSpecRepository
		spec       *model.AgentSpec
	)

	BeforeEach(func() {
		ctx = context.Background()
		pool = &connectionPoolStub{}
		repository = inferencedb.NewAgentSpecRepository(coreDB.NewDatabase(pool, "test_db"))
		spec = validAgentSpec()
	})

	Describe("UpsertAgentSpec", func() {
		It("upserts the content-addressed spec with schema-qualified SQL and named args", func() {
			pool.nextRows = []pgx.Row{agentSpecRow(spec)}

			record, err := repository.UpsertAgentSpec(ctx, spec)

			Expect(err).NotTo(HaveOccurred())
			Expect(record).To(Equal(spec))
			Expect(pool.queryRowCalled).To(BeTrue())
			Expect(pool.lastQuery).To(ContainSubstring("INSERT INTO test_db.agent_specs"))
			Expect(pool.lastQuery).To(ContainSubstring("ON CONFLICT (org_id, content_hash) DO UPDATE SET"))
			Expect(pool.lastQuery).To(ContainSubstring("RETURNING agent_spec_id::text"))
			args := namedArgs(pool.lastArgs)
			Expect(args).To(SatisfyAll(
				HaveKeyWithValue("org_id", pgtype.UUID{Bytes: spec.OrgID, Valid: true}),
				HaveKeyWithValue("agent_lineage", spec.AgentLineage),
				HaveKeyWithValue("system_prompt", spec.SystemPrompt),
				HaveKeyWithValue("source_yaml", spec.SourceYAML),
				HaveKeyWithValue("canonical_json", string(spec.CanonicalJSON)),
				HaveKeyWithValue("schema_version", spec.SchemaVersion),
				HaveKeyWithValue("content_hash", spec.ContentHash),
				HaveKeyWithValue("validation_report", spec.ValidationReport),
				HaveKeyWithValue("model_id", pgtype.UUID{Bytes: spec.ModelID, Valid: true}),
				HaveKeyWithValue("retrieval_config", string(spec.RetrievalConfig)),
				HaveKeyWithValue("stop_conditions", string(spec.StopConditions)),
				HaveKeyWithValue("guardrails", string(spec.Guardrails)),
				HaveKeyWithValue("status", model.AgentSpecStatusValidated.String()),
			))
			Expect(args["tool_bindings"]).To(MatchJSON(`[{"name":"search_knowledge","required":true,"tool_choice":"auto","config":{"dataset":"support"}}]`))
			Expect(args["budgets"]).To(MatchJSON(`{"max_steps":8,"token":2048,"wall_ms":30000}`))
		})
	})

	Describe("ReadAgentSpecByHash", func() {
		It("reads a spec by tenant and content hash", func() {
			pool.nextRows = []pgx.Row{agentSpecRow(spec)}

			record, err := repository.ReadAgentSpecByHash(ctx, spec.OrgID, spec.ContentHash)

			Expect(err).NotTo(HaveOccurred())
			Expect(record).To(Equal(spec))
			Expect(pool.lastQuery).To(ContainSubstring("FROM test_db.agent_specs"))
			Expect(pool.lastQuery).To(ContainSubstring("WHERE org_id = @org_id AND content_hash = @content_hash"))
			Expect(namedArgs(pool.lastArgs)).To(SatisfyAll(
				HaveKeyWithValue("org_id", pgtype.UUID{Bytes: spec.OrgID, Valid: true}),
				HaveKeyWithValue("content_hash", spec.ContentHash),
			))
		})

		It("maps missing specs to the domain not-found error", func() {
			record, err := repository.ReadAgentSpecByHash(ctx, spec.OrgID, spec.ContentHash)

			Expect(record).To(BeNil())
			Expect(errors.Is(err, domain.ErrModelNotFound)).To(BeTrue())
		})

		It("surfaces invalid persisted status values", func() {
			row := agentSpecRow(spec).(*repositoryRow)
			row.values[15] = "BROKEN"
			pool.nextRows = []pgx.Row{row}

			record, err := repository.ReadAgentSpecByHash(ctx, spec.OrgID, spec.ContentHash)

			Expect(record).To(BeNil())
			Expect(err).To(MatchError(ContainSubstring("parse agent spec status")))
		})
	})
})

func validAgentSpec() *model.AgentSpec {
	return &model.AgentSpec{
		AgentSpecID:      uuid.New(),
		OrgID:            uuid.New(),
		AgentLineage:     "support-agent",
		SystemPrompt:     "Answer with cited company knowledge.",
		SourceYAML:       "name: support-agent\n",
		CanonicalJSON:    []byte(`{"name":"support-agent"}`),
		SchemaVersion:    "agent-spec/v1",
		ContentHash:      "sha256:agent-spec",
		ValidationReport: "{}",
		ModelID:          uuid.New(),
		ToolBindings: []model.ToolBinding{{
			Name:       "search_knowledge",
			Required:   true,
			ToolChoice: "auto",
			Config:     json.RawMessage(`{"dataset":"support"}`),
		}},
		RetrievalConfig: json.RawMessage(`{"top_k":5}`),
		Budgets: model.AgentBudgets{
			MaxSteps: 8,
			Token:    2048,
			WallMs:   30000,
		},
		StopConditions: json.RawMessage(`{"max_tool_errors":2}`),
		Guardrails:     json.RawMessage(`{"policy":"strict"}`),
		Status:         model.AgentSpecStatusValidated,
		CreatedAt:      time.Unix(1_700_000_000, 0).UTC(),
	}
}

func agentSpecRow(spec *model.AgentSpec) pgx.Row {
	return &repositoryRow{values: []any{
		spec.AgentSpecID.String(),
		spec.OrgID.String(),
		spec.AgentLineage,
		spec.SystemPrompt,
		spec.SourceYAML,
		string(spec.CanonicalJSON),
		spec.SchemaVersion,
		spec.ContentHash,
		spec.ValidationReport,
		spec.ModelID.String(),
		`[{"name":"search_knowledge","required":true,"tool_choice":"auto","config":{"dataset":"support"}}]`,
		string(spec.RetrievalConfig),
		`{"max_steps":8,"token":2048,"wall_ms":30000}`,
		string(spec.StopConditions),
		string(spec.Guardrails),
		spec.Status.String(),
		spec.CreatedAt,
	}}
}
