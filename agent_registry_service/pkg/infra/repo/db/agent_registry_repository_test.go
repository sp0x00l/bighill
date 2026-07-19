package db_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"agent_registry_service/pkg/domain"
	"agent_registry_service/pkg/domain/model"
	agentdb "agent_registry_service/pkg/infra/repo/db"
	coreDB "lib/shared_lib/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestAgentRegistryRepository(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Agent registry db unit test suite")
}

type testConnectionPool struct {
	CloseCalled         bool
	QueryRowCalledCount int
	QueryCalledCount    int
	QueryCalls          []string
	QueryArgs           [][]any
	ExecCalls           []string
	ExecArgs            [][]any
	NextRows            []pgx.Row
	NextQueryRows       pgx.Rows
	NextQueryError      error
	NextRowsAffected    int64
	NextError           error
	CommitCalled        bool
	RollbackCalled      bool
}

func (p *testConnectionPool) Close() {
	p.CloseCalled = true
}

func (p *testConnectionPool) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	p.QueryRowCalledCount++
	p.QueryCalls = append(p.QueryCalls, sql)
	p.QueryArgs = append(p.QueryArgs, args)
	if len(p.NextRows) > 0 {
		nextRow := p.NextRows[0]
		p.NextRows = p.NextRows[1:]
		return nextRow
	}
	return errorRow{err: pgx.ErrNoRows}
}

func (p *testConnectionPool) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	p.QueryCalledCount++
	p.QueryCalls = append(p.QueryCalls, sql)
	p.QueryArgs = append(p.QueryArgs, args)
	if p.NextQueryError != nil {
		return nil, p.NextQueryError
	}
	if p.NextQueryRows != nil {
		return p.NextQueryRows, nil
	}
	return &testRows{}, nil
}

func (p *testConnectionPool) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	p.ExecCalls = append(p.ExecCalls, sql)
	p.ExecArgs = append(p.ExecArgs, args)
	return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", p.NextRowsAffected)), p.NextError
}

func (p *testConnectionPool) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	if p.NextError != nil {
		return nil, p.NextError
	}
	return &testTx{pool: p}, nil
}

type testTx struct {
	pool *testConnectionPool
}

func (tx *testTx) Begin(context.Context) (pgx.Tx, error) {
	return tx, nil
}

func (tx *testTx) Commit(context.Context) error {
	tx.pool.CommitCalled = true
	return nil
}

func (tx *testTx) Rollback(context.Context) error {
	tx.pool.RollbackCalled = true
	return nil
}

func (tx *testTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, nil
}

func (tx *testTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults {
	return nil
}

func (tx *testTx) LargeObjects() pgx.LargeObjects {
	return pgx.LargeObjects{}
}

func (tx *testTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, nil
}

func (tx *testTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return tx.pool.Exec(ctx, sql, args...)
}

func (tx *testTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return tx.pool.Query(ctx, sql, args...)
}

func (tx *testTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return tx.pool.QueryRow(ctx, sql, args...)
}

func (tx *testTx) Conn() *pgx.Conn {
	return nil
}

type errorRow struct {
	err error
}

func (r errorRow) Scan(...any) error {
	return r.err
}

type testRows struct {
	rows   []pgx.Row
	index  int
	err    error
	closed bool
}

func (r *testRows) Close() {
	r.closed = true
}

func (r *testRows) Err() error {
	return r.err
}

func (r *testRows) CommandTag() pgconn.CommandTag {
	return pgconn.NewCommandTag("SELECT")
}

func (r *testRows) FieldDescriptions() []pgconn.FieldDescription {
	return nil
}

func (r *testRows) Next() bool {
	if r.index >= len(r.rows) {
		r.Close()
		return false
	}
	r.index++
	return true
}

func (r *testRows) Scan(dest ...any) error {
	if r.index == 0 || r.index > len(r.rows) {
		return pgx.ErrNoRows
	}
	return r.rows[r.index-1].Scan(dest...)
}

func (r *testRows) Values() ([]any, error) {
	return nil, nil
}

func (r *testRows) RawValues() [][]byte {
	return nil
}

func (r *testRows) Conn() *pgx.Conn {
	return nil
}

type specVersionRow struct {
	OrgID              uuid.UUID
	AgentLineage       string
	AgentSpecHash      string
	ModelID            uuid.UUID
	RegisteredByUserID uuid.UUID
	RegisteredAt       time.Time
}

func (r specVersionRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.OrgID.String()
	*(dest[1].(*string)) = r.AgentLineage
	*(dest[2].(*string)) = r.AgentSpecHash
	*(dest[3].(*string)) = r.ModelID.String()
	*(dest[4].(*string)) = r.RegisteredByUserID.String()
	*(dest[5].(*time.Time)) = r.RegisteredAt
	return nil
}

type endpointBindingRow struct {
	OrgID           uuid.UUID
	AgentLineage    string
	EndpointID      uuid.UUID
	CreatedByUserID uuid.UUID
	CreatedAt       time.Time
}

func (r endpointBindingRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.OrgID.String()
	*(dest[1].(*string)) = r.AgentLineage
	*(dest[2].(*string)) = r.EndpointID.String()
	*(dest[3].(*string)) = r.CreatedByUserID.String()
	*(dest[4].(*time.Time)) = r.CreatedAt
	return nil
}

type championStateRow struct {
	OrgID                 uuid.UUID
	AgentLineage          string
	ChampionAgentSpecHash string
	ChampionAdapterID     string
	ServingModelID        string
	PreviousAgentSpecHash string
	DecisionID            uuid.UUID
	DecidedBy             uuid.UUID
	DecidedAt             time.Time
}

func (r championStateRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.OrgID.String()
	*(dest[1].(*string)) = r.AgentLineage
	*(dest[2].(*string)) = r.ChampionAgentSpecHash
	*(dest[3].(*string)) = r.ChampionAdapterID
	*(dest[4].(*string)) = r.ServingModelID
	*(dest[5].(*string)) = r.PreviousAgentSpecHash
	*(dest[6].(*string)) = r.DecisionID.String()
	*(dest[7].(*string)) = r.DecidedBy.String()
	*(dest[8].(*time.Time)) = r.DecidedAt
	return nil
}

type goldenTaskRow struct {
	TaskID                   uuid.UUID
	OrgID                    uuid.UUID
	AgentLineage             string
	Split                    string
	SplitVersion             int
	GroupKey                 string
	Prompt                   string
	NormalizedPromptHash     string
	ContentFingerprint       string
	NearDuplicateFingerprint string
	ExpectedToolPlanHash     string
	ExpectedAnswer           string
	ExpectedAnswerRubricID   string
	LabelsHash               string
	CreatedByUserID          uuid.UUID
	CreatedAt                time.Time
}

func (r goldenTaskRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.TaskID.String()
	*(dest[1].(*string)) = r.OrgID.String()
	*(dest[2].(*string)) = r.AgentLineage
	*(dest[3].(*string)) = r.Split
	*(dest[4].(*int)) = r.SplitVersion
	*(dest[5].(*string)) = r.GroupKey
	*(dest[6].(*string)) = r.Prompt
	*(dest[7].(*string)) = r.NormalizedPromptHash
	*(dest[8].(*string)) = r.ContentFingerprint
	*(dest[9].(*string)) = r.NearDuplicateFingerprint
	*(dest[10].(*string)) = r.ExpectedToolPlanHash
	*(dest[11].(*string)) = r.ExpectedAnswer
	*(dest[12].(*string)) = r.ExpectedAnswerRubricID
	*(dest[13].(*string)) = r.LabelsHash
	*(dest[14].(*string)) = r.CreatedByUserID.String()
	*(dest[15].(*time.Time)) = r.CreatedAt
	return nil
}

type goldenTaskConflictRow struct {
	TaskID             uuid.UUID
	Split              string
	GroupKey           string
	ContentFingerprint string
}

func (r goldenTaskConflictRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.TaskID.String()
	*(dest[1].(*string)) = r.Split
	*(dest[2].(*string)) = r.GroupKey
	*(dest[3].(*string)) = r.ContentFingerprint
	return nil
}

type agentRunLabelRow struct {
	LabelID                  uuid.UUID
	OrgID                    uuid.UUID
	RunID                    uuid.UUID
	AgentLineage             string
	AgentSpecHash            string
	ToolsetHash              string
	EffectiveBaseID          string
	DataSnapshotHash         string
	ContentFingerprint       string
	NearDuplicateFingerprint string
	Evaluator                string
	TaskSuccess              bool
	ToolSelectionScore       float64
	Groundedness             float64
	PolicyViolations         int
	Confidence               float64
	LabelSource              string
	RubricVersion            string
	CreatedByUserID          uuid.UUID
	CreatedAt                time.Time
}

func (r agentRunLabelRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.LabelID.String()
	*(dest[1].(*string)) = r.OrgID.String()
	*(dest[2].(*string)) = r.RunID.String()
	*(dest[3].(*string)) = r.AgentLineage
	*(dest[4].(*string)) = r.AgentSpecHash
	*(dest[5].(*string)) = r.ToolsetHash
	*(dest[6].(*string)) = r.EffectiveBaseID
	*(dest[7].(*string)) = r.DataSnapshotHash
	*(dest[8].(*string)) = r.ContentFingerprint
	*(dest[9].(*string)) = r.NearDuplicateFingerprint
	*(dest[10].(*string)) = r.Evaluator
	*(dest[11].(*bool)) = r.TaskSuccess
	*(dest[12].(*float64)) = r.ToolSelectionScore
	*(dest[13].(*float64)) = r.Groundedness
	*(dest[14].(*int)) = r.PolicyViolations
	*(dest[15].(*float64)) = r.Confidence
	*(dest[16].(*string)) = r.LabelSource
	*(dest[17].(*string)) = r.RubricVersion
	*(dest[18].(*string)) = r.CreatedByUserID.String()
	*(dest[19].(*time.Time)) = r.CreatedAt
	return nil
}

type trajectoryDatasetRow struct {
	DatasetID          uuid.UUID
	OrgID              uuid.UUID
	AgentLineage       string
	GoldenSplitVersion int
	ContentHash        string
	DatasetURI         string
	Format             string
	LabelCount         int
	Manifest           json.RawMessage
	EffectiveBaseID    string
	AgentSpecHash      string
	ToolsetHash        string
	DataSnapshotHash   string
	CreatedByUserID    uuid.UUID
	CreatedAt          time.Time
}

func (r trajectoryDatasetRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.DatasetID.String()
	*(dest[1].(*string)) = r.OrgID.String()
	*(dest[2].(*string)) = r.AgentLineage
	*(dest[3].(*int)) = r.GoldenSplitVersion
	*(dest[4].(*string)) = r.ContentHash
	*(dest[5].(*string)) = r.DatasetURI
	*(dest[6].(*string)) = r.Format
	*(dest[7].(*int)) = r.LabelCount
	*(dest[8].(*json.RawMessage)) = r.Manifest
	*(dest[9].(*string)) = r.EffectiveBaseID
	*(dest[10].(*string)) = r.AgentSpecHash
	*(dest[11].(*string)) = r.ToolsetHash
	*(dest[12].(*string)) = r.DataSnapshotHash
	*(dest[13].(*string)) = r.CreatedByUserID.String()
	*(dest[14].(*time.Time)) = r.CreatedAt
	return nil
}

type agentAdapterRow struct {
	AdapterID                        uuid.UUID
	OrgID                            uuid.UUID
	AgentLineage                     string
	DatasetID                        uuid.UUID
	TrainingRunID                    string
	ServingModelID                   uuid.UUID
	AdapterURI                       string
	AdapterChecksum                  string
	TrainingProvider                 string
	TrainedAgainstEffectiveBaseID    string
	TrainedAgainstAgentSpecHash      string
	TrainedAgainstToolsetHash        string
	TrainedAgainstDataSnapshotHash   string
	TrainedAgainstRubricVersion      string
	TrainedAgainstGoldenSplitVersion int
	Status                           string
	PromotionPassed                  bool
	CreatedByUserID                  uuid.UUID
	CreatedAt                        time.Time
	UpdatedAt                        time.Time
}

func (r agentAdapterRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.AdapterID.String()
	*(dest[1].(*string)) = r.OrgID.String()
	*(dest[2].(*string)) = r.AgentLineage
	*(dest[3].(*string)) = r.DatasetID.String()
	*(dest[4].(*string)) = r.TrainingRunID
	*(dest[5].(*string)) = r.ServingModelID.String()
	*(dest[6].(*string)) = r.AdapterURI
	*(dest[7].(*string)) = r.AdapterChecksum
	*(dest[8].(*string)) = r.TrainingProvider
	*(dest[9].(*string)) = r.TrainedAgainstEffectiveBaseID
	*(dest[10].(*string)) = r.TrainedAgainstAgentSpecHash
	*(dest[11].(*string)) = r.TrainedAgainstToolsetHash
	*(dest[12].(*string)) = r.TrainedAgainstDataSnapshotHash
	*(dest[13].(*string)) = r.TrainedAgainstRubricVersion
	*(dest[14].(*int)) = r.TrainedAgainstGoldenSplitVersion
	*(dest[15].(*string)) = r.Status
	*(dest[16].(*bool)) = r.PromotionPassed
	*(dest[17].(*string)) = r.CreatedByUserID.String()
	*(dest[18].(*time.Time)) = r.CreatedAt
	*(dest[19].(*time.Time)) = r.UpdatedAt
	return nil
}

type agentEvalReportRow struct {
	ReportID           uuid.UUID
	OrgID              uuid.UUID
	AgentLineage       string
	AgentSpecHash      string
	AdapterID          string
	EndpointID         uuid.UUID
	Split              string
	SplitVersion       int
	RubricVersion      string
	TaskCount          int
	TaskSuccessRate    float64
	ToolSuccessRate    float64
	GroundednessRate   float64
	Passed             bool
	GateReason         string
	PromotedDecisionID string
	EvaluatedBy        uuid.UUID
	EvaluatedAt        time.Time
}

func (r agentEvalReportRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.ReportID.String()
	*(dest[1].(*string)) = r.OrgID.String()
	*(dest[2].(*string)) = r.AgentLineage
	*(dest[3].(*string)) = r.AgentSpecHash
	*(dest[4].(*string)) = r.AdapterID
	*(dest[5].(*string)) = r.EndpointID.String()
	*(dest[6].(*string)) = r.Split
	*(dest[7].(*int)) = r.SplitVersion
	*(dest[8].(*string)) = r.RubricVersion
	*(dest[9].(*int)) = r.TaskCount
	*(dest[10].(*float64)) = r.TaskSuccessRate
	*(dest[11].(*float64)) = r.ToolSuccessRate
	*(dest[12].(*float64)) = r.GroundednessRate
	*(dest[13].(*bool)) = r.Passed
	*(dest[14].(*string)) = r.GateReason
	*(dest[15].(*string)) = r.PromotedDecisionID
	*(dest[16].(*string)) = r.EvaluatedBy.String()
	*(dest[17].(*time.Time)) = r.EvaluatedAt
	return nil
}

type agentEvalTaskResultRow struct {
	OrgID         uuid.UUID
	ReportID      uuid.UUID
	TaskID        uuid.UUID
	RunID         string
	Status        string
	StopReason    string
	TaskSuccess   bool
	ToolSuccess   bool
	Groundedness  bool
	FailureReason string
}

func (r agentEvalTaskResultRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.OrgID.String()
	*(dest[1].(*string)) = r.ReportID.String()
	*(dest[2].(*string)) = r.TaskID.String()
	*(dest[3].(*string)) = r.RunID
	*(dest[4].(*string)) = r.Status
	*(dest[5].(*string)) = r.StopReason
	*(dest[6].(*bool)) = r.TaskSuccess
	*(dest[7].(*bool)) = r.ToolSuccess
	*(dest[8].(*bool)) = r.Groundedness
	*(dest[9].(*string)) = r.FailureReason
	return nil
}

func namedArgs(args []any) pgx.NamedArgs {
	Expect(args).To(HaveLen(1))
	return args[0].(pgx.NamedArgs)
}

var _ = Describe("AgentRegistryRepository", func() {
	var (
		ctx        context.Context
		pool       *testConnectionPool
		tx         pgx.Tx
		repository *agentdb.AgentRegistryRepository
		orgID      uuid.UUID
		userID     uuid.UUID
		modelID    uuid.UUID
		endpointID uuid.UUID
		now        time.Time
	)

	BeforeEach(func() {
		ctx = context.Background()
		pool = &testConnectionPool{NextRowsAffected: 1}
		tx = &testTx{pool: pool}
		repository = agentdb.NewAgentRegistryRepository(coreDB.NewDatabase(pool, "test_db"))
		orgID = uuid.New()
		userID = uuid.New()
		modelID = uuid.New()
		endpointID = uuid.New()
		now = time.Date(2026, 7, 18, 9, 0, 0, 0, time.UTC)
	})

	It("upserts agent spec versions without phantom lifecycle columns", func() {
		row := specVersionRow{
			OrgID:              orgID,
			AgentLineage:       "support-agent",
			AgentSpecHash:      "sha256-spec",
			ModelID:            modelID,
			RegisteredByUserID: userID,
			RegisteredAt:       now,
		}
		pool.NextRows = []pgx.Row{row}

		record, err := repository.UpsertAgentSpecVersion(ctx, tx, &model.AgentSpecVersion{
			OrgID:              orgID,
			AgentLineage:       row.AgentLineage,
			AgentSpecHash:      row.AgentSpecHash,
			ModelID:            modelID,
			RegisteredByUserID: userID,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(record.AgentSpecHash).To(Equal("sha256-spec"))
		Expect(pool.QueryCalls[0]).To(ContainSubstring("INSERT INTO test_db.agent_spec_versions"))
		Expect(pool.QueryCalls[0]).NotTo(ContainSubstring("effective_base_id"))
		Expect(pool.QueryCalls[0]).NotTo(ContainSubstring("status"))
		args := namedArgs(pool.QueryArgs[0])
		Expect(args).To(HaveKeyWithValue("org_id", pgtype.UUID{Bytes: orgID, Valid: true}))
		Expect(args).To(HaveKeyWithValue("agent_spec_hash", "sha256-spec"))
		Expect(args).To(HaveKeyWithValue("model_id", pgtype.UUID{Bytes: modelID, Valid: true}))
	})

	It("maps missing registered specs to the domain error", func() {
		pool.NextRows = []pgx.Row{errorRow{err: pgx.ErrNoRows}}

		record, err := repository.ReadSpecVersion(ctx, orgID, "sha256-missing")

		Expect(record).To(BeNil())
		Expect(errors.Is(err, domain.ErrAgentVersionNotFound)).To(BeTrue())
		Expect(pool.QueryCalls[0]).To(ContainSubstring("FROM test_db.agent_spec_versions"))
		Expect(namedArgs(pool.QueryArgs[0])).To(HaveKeyWithValue("agent_spec_hash", "sha256-missing"))
	})

	It("records champion state and returns the previous champion from the database", func() {
		decisionID := uuid.New()
		pool.NextRows = []pgx.Row{championStateRow{
			OrgID:                 orgID,
			AgentLineage:          "support-agent",
			ChampionAgentSpecHash: "sha256-new",
			PreviousAgentSpecHash: "sha256-old",
			DecisionID:            decisionID,
			DecidedBy:             userID,
			DecidedAt:             now,
		}}

		state, err := repository.RecordChampionState(ctx, tx, &model.AgentChampionState{
			OrgID:                 orgID,
			AgentLineage:          "support-agent",
			ChampionAgentSpecHash: "sha256-new",
			DecisionID:            decisionID,
			DecidedBy:             userID,
			DecidedAt:             now,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(state.PreviousAgentSpecHash).To(Equal("sha256-old"))
		Expect(pool.QueryCalls[0]).To(ContainSubstring("ON CONFLICT (org_id, agent_lineage) DO UPDATE"))
		Expect(namedArgs(pool.QueryArgs[0])).To(HaveKeyWithValue("champion_agent_spec_hash", "sha256-new"))
	})

	It("lists endpoint bindings for a lineage", func() {
		secondEndpoint := uuid.New()
		pool.NextQueryRows = &testRows{rows: []pgx.Row{
			endpointBindingRow{OrgID: orgID, AgentLineage: "support-agent", EndpointID: endpointID, CreatedByUserID: userID, CreatedAt: now},
			endpointBindingRow{OrgID: orgID, AgentLineage: "support-agent", EndpointID: secondEndpoint, CreatedByUserID: userID, CreatedAt: now},
		}}

		bindings, err := repository.ListEndpointBindings(ctx, orgID, "support-agent")

		Expect(err).NotTo(HaveOccurred())
		Expect(bindings).To(HaveLen(2))
		Expect(bindings[0].EndpointID).To(Equal(endpointID))
		Expect(bindings[1].EndpointID).To(Equal(secondEndpoint))
		Expect(namedArgs(pool.QueryArgs[0])).To(HaveKeyWithValue("agent_lineage", "support-agent"))
	})

	It("creates golden tasks with DB-generated IDs", func() {
		taskID := uuid.New()
		pool.NextRows = []pgx.Row{goldenTaskRow{
			TaskID:                 taskID,
			OrgID:                  orgID,
			AgentLineage:           "support-agent",
			Split:                  "PROMOTION_HOLDOUT",
			SplitVersion:           2,
			GroupKey:               "group-a",
			Prompt:                 "Who signed the agreement?",
			NormalizedPromptHash:   "hash-prompt",
			ContentFingerprint:     "fingerprint",
			ExpectedToolPlanHash:   "tool-plan",
			ExpectedAnswer:         "Alice",
			ExpectedAnswerRubricID: "rubric-answer-v1",
			LabelsHash:             "labels",
			CreatedByUserID:        userID,
			CreatedAt:              now,
		}}

		task, err := repository.CreateGoldenTask(ctx, tx, &model.GoldenTask{
			OrgID:                  orgID,
			AgentLineage:           "support-agent",
			Split:                  model.GoldenTaskSplitPromotionHoldout,
			SplitVersion:           2,
			GroupKey:               "group-a",
			Prompt:                 "Who signed the agreement?",
			NormalizedPromptHash:   "hash-prompt",
			ContentFingerprint:     "fingerprint",
			ExpectedToolPlanHash:   "tool-plan",
			ExpectedAnswer:         "Alice",
			ExpectedAnswerRubricID: "rubric-answer-v1",
			LabelsHash:             "labels",
			CreatedByUserID:        userID,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(task.TaskID).To(Equal(taskID))
		Expect(task.Split).To(Equal(model.GoldenTaskSplitPromotionHoldout))
		Expect(pool.QueryCalls[0]).To(ContainSubstring("INSERT INTO test_db.golden_tasks"))
		args := namedArgs(pool.QueryArgs[0])
		Expect(args).To(HaveKeyWithValue("split", "PROMOTION_HOLDOUT"))
		Expect(args).To(HaveKeyWithValue("content_fingerprint", "fingerprint"))
		Expect(args).To(HaveKeyWithValue("expected_answer", "Alice"))
	})

	It("finds golden task leakage conflicts across different splits", func() {
		conflictID := uuid.New()
		pool.NextQueryRows = &testRows{rows: []pgx.Row{
			goldenTaskConflictRow{TaskID: conflictID, Split: "PROMOTION_HOLDOUT", GroupKey: "group-a", ContentFingerprint: "fingerprint"},
		}}

		conflicts, err := repository.FindGoldenTaskLeakConflicts(ctx, tx, &model.GoldenTask{
			OrgID:              orgID,
			AgentLineage:       "support-agent",
			Split:              model.GoldenTaskSplitSeedTrain,
			SplitVersion:       2,
			GroupKey:           "group-a",
			ContentFingerprint: "fingerprint",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(conflicts).To(HaveLen(1))
		Expect(conflicts[0].TaskID).To(Equal(conflictID))
		Expect(conflicts[0].Split).To(Equal(model.GoldenTaskSplitPromotionHoldout))
		Expect(pool.QueryCalls[0]).To(ContainSubstring("split <> @split::test_db.golden_task_split_enum"))
	})

	It("lists golden tasks for a split version", func() {
		taskID := uuid.New()
		pool.NextQueryRows = &testRows{rows: []pgx.Row{
			goldenTaskRow{
				TaskID:                 taskID,
				OrgID:                  orgID,
				AgentLineage:           "support-agent",
				Split:                  "DEV_EVAL",
				SplitVersion:           3,
				Prompt:                 "Who signed the agreement?",
				NormalizedPromptHash:   "hash-prompt",
				ContentFingerprint:     "fingerprint",
				ExpectedAnswer:         "Alice",
				ExpectedAnswerRubricID: "rubric-answer-v1",
				CreatedByUserID:        userID,
				CreatedAt:              now,
			},
		}}

		tasks, err := repository.ListGoldenTasks(ctx, model.ListGoldenTasksCommand{
			OrgID:        orgID,
			AgentLineage: "support-agent",
			Split:        model.GoldenTaskSplitDevEval,
			SplitVersion: 3,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(tasks).To(HaveLen(1))
		Expect(tasks[0].TaskID).To(Equal(taskID))
		Expect(tasks[0].Split).To(Equal(model.GoldenTaskSplitDevEval))
		Expect(tasks[0].ExpectedAnswer).To(Equal("Alice"))
		Expect(namedArgs(pool.QueryArgs[0])).To(HaveKeyWithValue("split", "DEV_EVAL"))
		Expect(namedArgs(pool.QueryArgs[0])).To(HaveKeyWithValue("split_version", 3))
	})

	It("records agent run labels with trajectory tuple fields", func() {
		labelID := uuid.New()
		runID := uuid.New()
		pool.NextRows = []pgx.Row{agentRunLabelRow{
			LabelID:            labelID,
			OrgID:              orgID,
			RunID:              runID,
			AgentLineage:       "support-agent",
			AgentSpecHash:      "sha256-spec",
			ToolsetHash:        "sha256-tools",
			EffectiveBaseID:    "sha256-base",
			DataSnapshotHash:   "sha256-data",
			ContentFingerprint: "fingerprint",
			Evaluator:          "human:reviewer-a",
			TaskSuccess:        true,
			ToolSelectionScore: 0.9,
			Groundedness:       1,
			PolicyViolations:   0,
			Confidence:         0.95,
			LabelSource:        "human",
			RubricVersion:      "trajectory_answer_contains_v1",
			CreatedByUserID:    userID,
			CreatedAt:          now,
		}}

		label, err := repository.RecordAgentRunLabel(ctx, tx, &model.AgentRunLabel{
			OrgID:              orgID,
			RunID:              runID,
			AgentLineage:       "support-agent",
			AgentSpecHash:      "sha256-spec",
			ToolsetHash:        "sha256-tools",
			EffectiveBaseID:    "sha256-base",
			DataSnapshotHash:   "sha256-data",
			ContentFingerprint: "fingerprint",
			Evaluator:          "human:reviewer-a",
			TaskSuccess:        true,
			ToolSelectionScore: 0.9,
			Groundedness:       1,
			Confidence:         0.95,
			LabelSource:        "human",
			RubricVersion:      "trajectory_answer_contains_v1",
			CreatedByUserID:    userID,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(label.LabelID).To(Equal(labelID))
		Expect(label.EffectiveBaseID).To(Equal("sha256-base"))
		Expect(pool.QueryCalls[0]).To(ContainSubstring("INSERT INTO test_db.agent_run_labels"))
		args := namedArgs(pool.QueryArgs[0])
		Expect(args).To(HaveKeyWithValue("effective_base_id", "sha256-base"))
		Expect(args).To(HaveKeyWithValue("data_snapshot_hash", "sha256-data"))
	})

	It("lists agent run labels for a lineage", func() {
		labelID := uuid.New()
		runID := uuid.New()
		pool.NextQueryRows = &testRows{rows: []pgx.Row{
			agentRunLabelRow{
				LabelID:            labelID,
				OrgID:              orgID,
				RunID:              runID,
				AgentLineage:       "support-agent",
				AgentSpecHash:      "sha256-spec",
				ToolsetHash:        "sha256-tools",
				EffectiveBaseID:    "sha256-base",
				DataSnapshotHash:   "sha256-data",
				ContentFingerprint: "fingerprint",
				Evaluator:          "human:reviewer-a",
				TaskSuccess:        true,
				ToolSelectionScore: 0.9,
				Groundedness:       1,
				Confidence:         0.95,
				LabelSource:        "human",
				RubricVersion:      "trajectory_answer_contains_v1",
				CreatedByUserID:    userID,
				CreatedAt:          now,
			},
		}}

		labels, err := repository.ListAgentRunLabels(ctx, model.ListAgentRunLabelsCommand{
			OrgID:        orgID,
			AgentLineage: "support-agent",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(labels).To(HaveLen(1))
		Expect(labels[0].RunID).To(Equal(runID))
		Expect(namedArgs(pool.QueryArgs[0])).To(HaveKeyWithValue("agent_lineage", "support-agent"))
	})

	It("records trajectory datasets with manifest and tuple provenance", func() {
		datasetID := uuid.New()
		pool.NextRows = []pgx.Row{trajectoryDatasetRow{
			DatasetID:          datasetID,
			OrgID:              orgID,
			AgentLineage:       "support-agent",
			GoldenSplitVersion: 2,
			ContentHash:        "sha256-dataset",
			DatasetURI:         "agent-registry://trajectory-datasets/sha256-dataset",
			Format:             "AGENT_TRAJECTORY_JSON",
			LabelCount:         2,
			Manifest:           []byte(`{"schema_version":"agent_trajectory_dataset_v1"}`),
			EffectiveBaseID:    "sha256-base",
			AgentSpecHash:      "sha256-spec",
			ToolsetHash:        "sha256-tools",
			DataSnapshotHash:   "sha256-data",
			CreatedByUserID:    userID,
			CreatedAt:          now,
		}}

		dataset, err := repository.RecordTrajectoryDataset(ctx, tx, &model.AgentTrajectoryDataset{
			OrgID:              orgID,
			AgentLineage:       "support-agent",
			GoldenSplitVersion: 2,
			ContentHash:        "sha256-dataset",
			DatasetURI:         "agent-registry://trajectory-datasets/sha256-dataset",
			Format:             "AGENT_TRAJECTORY_JSON",
			LabelCount:         2,
			Manifest:           []byte(`{"schema_version":"agent_trajectory_dataset_v1"}`),
			EffectiveBaseID:    "sha256-base",
			AgentSpecHash:      "sha256-spec",
			ToolsetHash:        "sha256-tools",
			DataSnapshotHash:   "sha256-data",
			CreatedByUserID:    userID,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(dataset.DatasetID).To(Equal(datasetID))
		Expect(dataset.Manifest).To(MatchJSON(`{"schema_version":"agent_trajectory_dataset_v1"}`))
		Expect(pool.QueryCalls[0]).To(ContainSubstring("INSERT INTO test_db.agent_trajectory_datasets"))
		args := namedArgs(pool.QueryArgs[0])
		Expect(args).To(HaveKeyWithValue("content_hash", "sha256-dataset"))
		Expect(args).To(HaveKeyWithValue("data_snapshot_hash", "sha256-data"))
	})

	It("reads trajectory datasets by org and id", func() {
		datasetID := uuid.New()
		pool.NextRows = []pgx.Row{trajectoryDatasetRow{
			DatasetID:          datasetID,
			OrgID:              orgID,
			AgentLineage:       "support-agent",
			GoldenSplitVersion: 2,
			ContentHash:        "sha256-dataset",
			DatasetURI:         "agent-registry://trajectory-datasets/sha256-dataset",
			Format:             "AGENT_TRAJECTORY_JSON",
			LabelCount:         2,
			Manifest:           []byte(`{"schema_version":"agent_trajectory_dataset_v1"}`),
			EffectiveBaseID:    "sha256-base",
			AgentSpecHash:      "sha256-spec",
			ToolsetHash:        "sha256-tools",
			DataSnapshotHash:   "sha256-data",
			CreatedByUserID:    userID,
			CreatedAt:          now,
		}}

		dataset, err := repository.ReadTrajectoryDataset(ctx, orgID, datasetID)

		Expect(err).NotTo(HaveOccurred())
		Expect(dataset.DatasetID).To(Equal(datasetID))
		Expect(namedArgs(pool.QueryArgs[0])).To(HaveKeyWithValue("dataset_id", pgtype.UUID{Bytes: datasetID, Valid: true}))
	})

	It("records agent adapters as candidates with DB-generated IDs", func() {
		adapterID := uuid.New()
		datasetID := uuid.New()
		trainingRunID := uuid.New()
		servingModelID := uuid.New()
		pool.NextRows = []pgx.Row{agentAdapterRow{
			AdapterID:                        adapterID,
			OrgID:                            orgID,
			AgentLineage:                     "support-agent",
			DatasetID:                        datasetID,
			TrainingRunID:                    trainingRunID.String(),
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
			Status:                           "CANDIDATE",
			CreatedByUserID:                  userID,
			CreatedAt:                        now,
			UpdatedAt:                        now,
		}}

		record, err := repository.RecordAgentAdapter(ctx, tx, &model.AgentAdapter{
			OrgID:                            orgID,
			AgentLineage:                     "support-agent",
			DatasetID:                        datasetID,
			TrainingRunID:                    trainingRunID,
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
			Status:                           model.AgentAdapterStatusCandidate,
			CreatedByUserID:                  userID,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(record.AdapterID).To(Equal(adapterID))
		Expect(record.Status).To(Equal(model.AgentAdapterStatusCandidate))
		Expect(record.TrainingRunID).To(Equal(trainingRunID))
		Expect(pool.QueryCalls[0]).To(ContainSubstring("INSERT INTO test_db.agent_adapters"))
	})

	It("updates adapter promotion status", func() {
		adapterID := uuid.New()
		datasetID := uuid.New()
		servingModelID := uuid.New()
		pool.NextRows = []pgx.Row{agentAdapterRow{
			AdapterID:                        adapterID,
			OrgID:                            orgID,
			AgentLineage:                     "support-agent",
			DatasetID:                        datasetID,
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
			Status:                           "PROMOTED",
			PromotionPassed:                  true,
			CreatedByUserID:                  userID,
			CreatedAt:                        now,
			UpdatedAt:                        now,
		}}

		record, err := repository.UpdateAgentAdapterPromotion(ctx, tx, adapterID, model.AgentAdapterStatusPromoted, true)

		Expect(err).NotTo(HaveOccurred())
		Expect(record.Status).To(Equal(model.AgentAdapterStatusPromoted))
		Expect(record.PromotionPassed).To(BeTrue())
		Expect(pool.QueryCalls[0]).To(ContainSubstring("UPDATE test_db.agent_adapters"))
		args := namedArgs(pool.QueryArgs[0])
		Expect(args).To(HaveKeyWithValue("adapter_id", pgtype.UUID{Bytes: adapterID, Valid: true}))
		Expect(args).To(HaveKeyWithValue("status", "PROMOTED"))
	})

	It("records eval reports and task results without fabricating missing run ids", func() {
		reportID := uuid.New()
		taskID := uuid.New()
		pool.NextRows = []pgx.Row{agentEvalReportRow{
			ReportID:         reportID,
			OrgID:            orgID,
			AgentLineage:     "support-agent",
			AgentSpecHash:    "sha256-spec",
			EndpointID:       endpointID,
			Split:            "PROMOTION_HOLDOUT",
			SplitVersion:     1,
			RubricVersion:    "trajectory_answer_contains_v1",
			TaskCount:        1,
			TaskSuccessRate:  0,
			ToolSuccessRate:  0,
			GroundednessRate: 0,
			Passed:           false,
			GateReason:       "runner failed",
			EvaluatedBy:      userID,
			EvaluatedAt:      now,
		}}

		report, err := repository.RecordAgentEvalReport(ctx, tx, &model.AgentEvalReport{
			OrgID:            orgID,
			AgentLineage:     "support-agent",
			AgentSpecHash:    "sha256-spec",
			EndpointID:       endpointID,
			Split:            model.GoldenTaskSplitPromotionHoldout,
			SplitVersion:     1,
			RubricVersion:    "trajectory_answer_contains_v1",
			TaskCount:        1,
			TaskSuccessRate:  0,
			ToolSuccessRate:  0,
			GroundednessRate: 0,
			Passed:           false,
			GateReason:       "runner failed",
			EvaluatedBy:      userID,
			TaskResults: []*model.AgentEvalTaskResult{{
				TaskID:        taskID,
				Status:        "FAILED",
				StopReason:    "RUNTIME_ERROR",
				FailureReason: "agent workflow failed",
			}},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(report.ReportID).To(Equal(reportID))
		Expect(report.TaskResults).To(HaveLen(1))
		Expect(report.TaskResults[0].ReportID).To(Equal(reportID))
		Expect(pool.ExecCalls[0]).To(ContainSubstring("INSERT INTO test_db.agent_eval_task_results"))
		args := namedArgs(pool.ExecArgs[0])
		Expect(args).To(HaveKeyWithValue("run_id", pgtype.UUID{Valid: false}))
		Expect(args).To(HaveKeyWithValue("failure_reason", "agent workflow failed"))
	})

	It("reads eval reports with task results", func() {
		reportID := uuid.New()
		taskID := uuid.New()
		runID := uuid.New()
		pool.NextRows = []pgx.Row{agentEvalReportRow{
			ReportID:         reportID,
			OrgID:            orgID,
			AgentLineage:     "support-agent",
			AgentSpecHash:    "sha256-spec",
			EndpointID:       endpointID,
			Split:            "PROMOTION_HOLDOUT",
			SplitVersion:     1,
			RubricVersion:    "trajectory_answer_contains_v1",
			TaskCount:        1,
			TaskSuccessRate:  1,
			ToolSuccessRate:  1,
			GroundednessRate: 1,
			Passed:           true,
			GateReason:       "candidate passes promotion holdout",
			EvaluatedBy:      userID,
			EvaluatedAt:      now,
		}}
		pool.NextQueryRows = &testRows{rows: []pgx.Row{
			agentEvalTaskResultRow{
				ReportID:     reportID,
				TaskID:       taskID,
				RunID:        runID.String(),
				Status:       "COMPLETED",
				StopReason:   "FINAL_ANSWER",
				TaskSuccess:  true,
				ToolSuccess:  true,
				Groundedness: true,
			},
			agentEvalTaskResultRow{
				ReportID:      reportID,
				TaskID:        uuid.New(),
				RunID:         "",
				Status:        "FAILED",
				StopReason:    "RUNTIME_ERROR",
				FailureReason: "agent workflow failed",
			},
		}}

		report, err := repository.ReadAgentEvalReport(ctx, orgID, reportID)

		Expect(err).NotTo(HaveOccurred())
		Expect(report.ReportID).To(Equal(reportID))
		Expect(report.TaskResults).To(HaveLen(2))
		Expect(report.TaskResults[0].RunID).To(Equal(runID))
		Expect(report.TaskResults[1].RunID).To(Equal(uuid.Nil))
		Expect(pool.QueryCalls[1]).To(ContainSubstring("COALESCE(run_id::text, '')"))
	})
})
