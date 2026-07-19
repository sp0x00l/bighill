package integration_test

import (
	"context"
	"testing"
	"time"

	"agent_registry_service/pkg/app"
	"agent_registry_service/pkg/domain/model"
	agentmessaging "agent_registry_service/pkg/infra/network/messaging"
	agentdb "agent_registry_service/pkg/infra/repo/db"
	agentregistrypb "lib/data_contracts_lib/agent_registry"
	"lib/shared_lib/ctxutil"
	coreDB "lib/shared_lib/db"
	env "lib/shared_lib/env"
	sharedmessaging "lib/shared_lib/messaging"
	shareduow "lib/shared_lib/uow"
	"lib/shared_lib/userevents"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

func TestAgentRegistryIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Agent registry integration test suite")
}

type integrationInferenceVerifier struct {
	spec       *model.AgentSpecRef
	endpoint   *model.EndpointRef
	trajectory *model.AgentTrajectoryRef
	taskRuns   []model.AgentTaskRunResult
	taskErrors []error
}

func (v *integrationInferenceVerifier) ReadAgentSpec(context.Context, uuid.UUID, uuid.UUID, string) (*model.AgentSpecRef, error) {
	return v.spec, nil
}

func (v *integrationInferenceVerifier) ReadEndpoint(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) (*model.EndpointRef, error) {
	return v.endpoint, nil
}

func (v *integrationInferenceVerifier) ReadAgentTrajectory(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) (*model.AgentTrajectoryRef, error) {
	return v.trajectory, nil
}

func (v *integrationInferenceVerifier) RunAgentTask(context.Context, model.AgentTaskRunCommand) (model.AgentTaskRunResult, error) {
	result := model.AgentTaskRunResult{}
	if len(v.taskRuns) > 0 {
		result = v.taskRuns[0]
		v.taskRuns = v.taskRuns[1:]
	}
	if len(v.taskErrors) > 0 {
		err := v.taskErrors[0]
		v.taskErrors = v.taskErrors[1:]
		return result, err
	}
	return result, nil
}

type testAgentAdapterTrainingDispatcher struct{}

func (d *testAgentAdapterTrainingDispatcher) DispatchAgentAdapterTraining(_ context.Context, request model.AgentAdapterTrainingRequest) (*model.AgentAdapterTrainingResult, error) {
	key := request.OrgID.String() + ":" +
		request.AgentLineage + ":" +
		request.DatasetID.String() + ":" +
		request.ContentHash + ":" +
		request.TrainingProfile + ":" +
		request.EffectiveBaseID + ":" +
		request.AgentSpecHash + ":" +
		request.ToolsetHash + ":" +
		request.DataSnapshotHash
	trainingRunID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("test-agent-training:"+key))
	checksum := userevents.SHA256String("test-agent-adapter:" + key)
	return &model.AgentAdapterTrainingResult{
		TrainingRunID:    trainingRunID,
		ServingModelID:   trainingRunID,
		AdapterURI:       "agent-adapter://" + checksum,
		AdapterChecksum:  checksum,
		TrainingProvider: "test-agent-adapter-training",
	}, nil
}

var _ = Describe("Agent registry integration", Ordered, func() {
	var (
		ctx      context.Context
		cancel   context.CancelFunc
		database *coreDB.Database
		usecase  app.AgentRegistryUsecase
		verifier *integrationInferenceVerifier
	)

	BeforeAll(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 90*time.Second)
		dbConfig := coreDB.DatabaseConfig{}
		dbConfig.WithDbName("AGENT_REGISTRY_SERVICE_DB_NAME", "bighill_agent_registry_db")
		dbConfig.WithDbUser("AGENT_REGISTRY_SERVICE_DB_USER", "bighill_agent_registry_db_user")
		dbConfig.WithDbPassword("AGENT_REGISTRY_SERVICE_DB_PASSWORD", env.WithDefaultString("BIGHILL_DB_PASSWORD", "LrDwb53E7DmFc2j4qw77n4pUUfKtULDVh4vrHjWw"))
		dbConfig.WithDbMaxConnections("AGENT_REGISTRY_SERVICE_DB_MAX_CONNECTIONS", "20")
		dbConfig.WithDbHost("PGHOST", "127.0.0.1")
		dbConfig.WithDbPort("PGPORT", "5432")
		dbConfig.WithDbSSLMode("PGSSLMODE", "disable")
		var err error
		database, err = coreDB.InitDatabase(ctx, dbConfig.GetName(), dbConfig.GetConnectionString(), log.StandardLogger())
		Expect(err).NotTo(HaveOccurred())

		outboxWriter, err := sharedmessaging.NewPostgresOutbox(database.Pool, database.Name, "")
		Expect(err).NotTo(HaveOccurred())
		orderedOutbox, ok := outboxWriter.(sharedmessaging.OrderedOutbox)
		Expect(ok).To(BeTrue())
		verifier = &integrationInferenceVerifier{}
		usecase = app.NewAgentRegistryUsecase(
			agentdb.NewAgentRegistryRepository(database),
			shareduow.New(database.Pool, shareduow.WithTransactionalOutbox(orderedOutbox)),
			verifier,
			agentmessaging.NewAgentRegistryEventBuilder("agent_registry"),
			verifier,
			&testAgentAdapterTrainingDispatcher{},
		)
	})

	BeforeEach(func() {
		Expect(truncateAgentRegistry(ctx, database)).To(Succeed())
	})

	AfterAll(func() {
		if database != nil {
			database.Close()
		}
		if cancel != nil {
			cancel()
		}
	})

	It("registers a spec, binds an endpoint, promotes a champion, and writes the champion update outbox event", func() {
		orgID := uuid.New()
		userID := uuid.New()
		tenantCtx := ctxutil.WithActorOrg(ctx, userID, orgID)
		systemCtx := ctxutil.WithSystemContext(ctx)
		modelID := uuid.New()
		endpointID := uuid.New()
		decisionID := uuid.New()
		decidedAt := time.Date(2026, 7, 18, 13, 0, 0, 0, time.UTC)
		verifier.spec = &model.AgentSpecRef{
			OrgID:         orgID,
			AgentLineage:  "support-agent",
			AgentSpecHash: "sha256-spec-a",
			ModelID:       modelID,
		}
		verifier.endpoint = &model.EndpointRef{OrgID: orgID, EndpointID: endpointID}

		version, err := usecase.RegisterAgentSpecVersion(tenantCtx, model.RegisterAgentSpecVersionCommand{
			OrgID:         orgID,
			UserID:        userID,
			AgentLineage:  "support-agent",
			AgentSpecHash: "sha256-spec-a",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(version.ModelID).To(Equal(modelID))

		binding, err := usecase.RegisterEndpointBinding(tenantCtx, model.RegisterEndpointBindingCommand{
			OrgID:        orgID,
			UserID:       userID,
			AgentLineage: "support-agent",
			EndpointID:   endpointID,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(binding.EndpointID).To(Equal(endpointID))

		state, err := usecase.PromoteSpecChampion(tenantCtx, model.PromoteSpecChampionCommand{
			OrgID:         orgID,
			UserID:        userID,
			AgentLineage:  "support-agent",
			AgentSpecHash: "sha256-spec-a",
			DecisionID:    decisionID,
			DecidedAt:     decidedAt,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(state.ChampionAgentSpecHash).To(Equal("sha256-spec-a"))

		var champion string
		Expect(database.Pool.QueryRow(systemCtx, `
				SELECT champion_agent_spec_hash
				FROM `+database.Name+`.agent_champion_states
				WHERE org_id = $1 AND agent_lineage = $2
		`, orgID, "support-agent").Scan(&champion)).To(Succeed())
		Expect(champion).To(Equal("sha256-spec-a"))

		var raw []byte
		Expect(database.Pool.QueryRow(systemCtx, `
				SELECT payload
				FROM `+database.Name+`.outbox_messages
				WHERE resource_key = $1 AND event_type = $2
		`, endpointID, sharedmessaging.MsgTypeAgentChampionUpdated.String()).Scan(&raw)).To(Succeed())
		var envelope sharedmessaging.Message
		Expect(envelope.Deserialize(ctx, raw)).To(Succeed())
		Expect(envelope.ResourceKey).To(Equal(endpointID))
		Expect(envelope.MsgType).To(Equal(sharedmessaging.MsgTypeAgentChampionUpdated))
		payload := &agentregistrypb.AgentChampionUpdatedEvent{}
		Expect(envelope.DeserializePayload(payload)).To(Succeed())
		Expect(payload.GetEndpointId()).To(Equal(endpointID.String()))
		Expect(payload.GetAgentSpecHash()).To(Equal("sha256-spec-a"))
		Expect(payload.GetDecisionId()).To(Equal(decisionID.String()))
	})

	It("imports golden tasks, reads them back, and rejects cross-split leakage", func() {
		orgID := uuid.New()
		userID := uuid.New()
		tenantCtx := ctxutil.WithActorOrg(ctx, userID, orgID)
		lineage := "support-agent"

		holdout, err := usecase.ImportGoldenTasks(tenantCtx, model.ImportGoldenTasksCommand{
			OrgID:        orgID,
			UserID:       userID,
			AgentLineage: lineage,
			Split:        model.GoldenTaskSplitPromotionHoldout,
			SplitVersion: 1,
			Tasks: []model.GoldenTaskInput{{
				GroupKey:               "contract-42",
				Prompt:                 "Who signed the contract?",
				ExpectedAnswer:         "Alice",
				ExpectedAnswerRubricID: "rubric-answer-v1",
			}},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(holdout).To(HaveLen(1))
		Expect(holdout[0].TaskID).NotTo(Equal(uuid.Nil))
		Expect(holdout[0].ContentFingerprint).NotTo(BeEmpty())

		tasks, err := usecase.ListGoldenTasks(tenantCtx, model.ListGoldenTasksCommand{
			OrgID:        orgID,
			AgentLineage: lineage,
			Split:        model.GoldenTaskSplitPromotionHoldout,
			SplitVersion: 1,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(tasks).To(HaveLen(1))
		Expect(tasks[0].ContentFingerprint).To(Equal(holdout[0].ContentFingerprint))

		train, err := usecase.ImportGoldenTasks(tenantCtx, model.ImportGoldenTasksCommand{
			OrgID:        orgID,
			UserID:       userID,
			AgentLineage: lineage,
			Split:        model.GoldenTaskSplitSeedTrain,
			SplitVersion: 1,
			Tasks: []model.GoldenTaskInput{{
				GroupKey:               "contract-42",
				Prompt:                 "Who signed the contract?",
				ExpectedAnswer:         "Alice",
				ExpectedAnswerRubricID: "rubric-answer-v1",
			}},
		})
		Expect(train).To(BeEmpty())
		Expect(err).To(HaveOccurred())
	})

	It("evaluates holdout tasks, records the report, and promotes only when the gate passes", func() {
		orgID := uuid.New()
		userID := uuid.New()
		tenantCtx := ctxutil.WithActorOrg(ctx, userID, orgID)
		systemCtx := ctxutil.WithSystemContext(ctx)
		modelID := uuid.New()
		endpointID := uuid.New()
		runID := uuid.New()
		lineage := "support-agent"
		hash := "sha256-spec-eval"
		verifier.spec = &model.AgentSpecRef{
			OrgID:         orgID,
			AgentLineage:  lineage,
			AgentSpecHash: hash,
			ModelID:       modelID,
		}
		verifier.endpoint = &model.EndpointRef{OrgID: orgID, EndpointID: endpointID}
		verifier.taskRuns = []model.AgentTaskRunResult{{
			RunID:                runID,
			Status:               "COMPLETED",
			StopReason:           "FINAL_ANSWER",
			Answer:               "Alice signed the agreement.",
			GroundedContextCount: 1,
			GroundedContextTexts: []string{"Alice signed the agreement."},
			ToolInvocations: []model.AgentTaskToolInvocation{{
				ToolName: "search_knowledge",
			}},
		}}

		_, err := usecase.RegisterAgentSpecVersion(tenantCtx, model.RegisterAgentSpecVersionCommand{
			OrgID:         orgID,
			UserID:        userID,
			AgentLineage:  lineage,
			AgentSpecHash: hash,
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = usecase.RegisterEndpointBinding(tenantCtx, model.RegisterEndpointBindingCommand{
			OrgID:        orgID,
			UserID:       userID,
			AgentLineage: lineage,
			EndpointID:   endpointID,
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = usecase.ImportGoldenTasks(tenantCtx, model.ImportGoldenTasksCommand{
			OrgID:        orgID,
			UserID:       userID,
			AgentLineage: lineage,
			Split:        model.GoldenTaskSplitPromotionHoldout,
			SplitVersion: 1,
			Tasks: []model.GoldenTaskInput{{
				GroupKey:               "contract-42",
				Prompt:                 "Who signed the agreement?",
				ExpectedAnswer:         "Alice",
				ExpectedAnswerRubricID: "rubric-answer-v1",
			}},
		})
		Expect(err).NotTo(HaveOccurred())

		report, err := usecase.EvaluateSpecChampion(tenantCtx, model.EvaluateSpecChampionCommand{
			OrgID:               orgID,
			UserID:              userID,
			AgentLineage:        lineage,
			AgentSpecHash:       hash,
			EndpointID:          endpointID,
			SplitVersion:        1,
			MinGroundednessRate: 1,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(report.Passed).To(BeTrue())
		Expect(report.PromotedDecisionID).NotTo(Equal(uuid.Nil))
		read, err := usecase.ReadAgentEvalReport(tenantCtx, orgID, report.ReportID)
		Expect(err).NotTo(HaveOccurred())
		Expect(read.TaskResults).To(HaveLen(1))
		Expect(read.TaskResults[0].RunID).To(Equal(runID))

		var champion string
		Expect(database.Pool.QueryRow(systemCtx, `
				SELECT champion_agent_spec_hash
				FROM `+database.Name+`.agent_champion_states
			WHERE org_id = $1 AND agent_lineage = $2
		`, orgID, lineage).Scan(&champion)).To(Succeed())
		Expect(champion).To(Equal(hash))
	})

	It("builds and promotes one adapter flywheel turn from labeled trajectories", func() {
		orgID := uuid.New()
		userID := uuid.New()
		tenantCtx := ctxutil.WithActorOrg(ctx, userID, orgID)
		systemCtx := ctxutil.WithSystemContext(ctx)
		modelID := uuid.New()
		endpointID := uuid.New()
		runID := uuid.New()
		lineage := "support-agent"
		hash := "sha256-spec-flywheel"
		verifier.spec = &model.AgentSpecRef{
			OrgID:         orgID,
			AgentLineage:  lineage,
			AgentSpecHash: hash,
			ModelID:       modelID,
		}
		verifier.endpoint = &model.EndpointRef{OrgID: orgID, EndpointID: endpointID}
		verifier.trajectory = &model.AgentTrajectoryRef{
			RunID:            runID,
			OrgID:            orgID,
			UserID:           userID,
			EndpointID:       endpointID,
			AgentSpecHash:    hash,
			ToolsetHash:      "sha256-tools",
			EffectiveBaseID:  "sha256-base",
			DataSnapshotHash: "sha256-data",
			Status:           "COMPLETED",
			StopReason:       "FINAL_ANSWER",
		}

		_, err := usecase.RegisterAgentSpecVersion(tenantCtx, model.RegisterAgentSpecVersionCommand{
			OrgID:         orgID,
			UserID:        userID,
			AgentLineage:  lineage,
			AgentSpecHash: hash,
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = usecase.RegisterEndpointBinding(tenantCtx, model.RegisterEndpointBindingCommand{
			OrgID:        orgID,
			UserID:       userID,
			AgentLineage: lineage,
			EndpointID:   endpointID,
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = usecase.ImportGoldenTasks(tenantCtx, model.ImportGoldenTasksCommand{
			OrgID:        orgID,
			UserID:       userID,
			AgentLineage: lineage,
			Split:        model.GoldenTaskSplitPromotionHoldout,
			SplitVersion: 1,
			Tasks: []model.GoldenTaskInput{{
				GroupKey:               "holdout-contract",
				Prompt:                 "Who approved the holdout contract?",
				ExpectedAnswer:         "Alice",
				ExpectedAnswerRubricID: "rubric-answer-v1",
			}},
		})
		Expect(err).NotTo(HaveOccurred())
		label, err := usecase.LabelAgentRun(tenantCtx, model.LabelAgentRunCommand{
			OrgID:              orgID,
			UserID:             userID,
			RunID:              runID,
			AgentLineage:       lineage,
			Prompt:             "Who approved the training contract?",
			Evaluator:          "human-reviewer",
			TaskSuccess:        true,
			ToolSelectionScore: 1,
			Groundedness:       1,
			Confidence:         0.95,
			LabelSource:        "human",
			RubricVersion:      "trajectory_answer_contains_v1",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(label.EffectiveBaseID).To(Equal("sha256-base"))

		dataset, err := usecase.BuildTrajectoryDataset(tenantCtx, model.BuildTrajectoryDatasetCommand{
			OrgID:              orgID,
			UserID:             userID,
			AgentLineage:       lineage,
			GoldenSplitVersion: 1,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(dataset.LabelCount).To(Equal(1))
		Expect(dataset.AgentSpecHash).To(Equal(hash))

		adapter, err := usecase.DispatchAgentAdapterTraining(tenantCtx, model.DispatchAgentAdapterTrainingCommand{
			OrgID:        orgID,
			UserID:       userID,
			AgentLineage: lineage,
			DatasetID:    dataset.DatasetID,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(adapter.Status).To(Equal(model.AgentAdapterStatusCandidate))
		Expect(adapter.TrainedAgainstEffectiveBaseID).To(Equal("sha256-base"))

		verifier.taskRuns = []model.AgentTaskRunResult{{
			RunID:                uuid.New(),
			Status:               "COMPLETED",
			StopReason:           "FINAL_ANSWER",
			Answer:               "Alice approved the holdout contract.",
			GroundedContextCount: 1,
			GroundedContextTexts: []string{"Alice approved the holdout contract."},
			ToolInvocations: []model.AgentTaskToolInvocation{{
				ToolName: "search_knowledge",
			}},
		}}
		report, err := usecase.EvaluateAdapterCandidate(tenantCtx, model.EvaluateAdapterCandidateCommand{
			OrgID:               orgID,
			UserID:              userID,
			AgentLineage:        lineage,
			AdapterID:           adapter.AdapterID,
			EndpointID:          endpointID,
			SplitVersion:        1,
			MinGroundednessRate: 1,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Passed).To(BeTrue())

		promoted, err := usecase.PromoteAgentAdapter(tenantCtx, model.PromoteAgentAdapterCommand{
			OrgID:        orgID,
			UserID:       userID,
			AgentLineage: lineage,
			AdapterID:    adapter.AdapterID,
			ReportID:     report.ReportID,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(promoted.Status).To(Equal(model.AgentAdapterStatusPromoted))
		Expect(promoted.PromotionPassed).To(BeTrue())

		var championAdapterID string
		var servingModelID string
		Expect(database.Pool.QueryRow(systemCtx, `
				SELECT COALESCE(champion_adapter_id::text, ''), COALESCE(serving_model_id::text, '')
				FROM `+database.Name+`.agent_champion_states
			WHERE org_id = $1 AND agent_lineage = $2
		`, orgID, lineage).Scan(&championAdapterID, &servingModelID)).To(Succeed())
		Expect(championAdapterID).To(Equal(adapter.AdapterID.String()))
		Expect(servingModelID).To(Equal(adapter.ServingModelID.String()))
	})
})

func truncateAgentRegistry(ctx context.Context, database *coreDB.Database) error {
	ctx = ctxutil.WithSystemContext(ctx)
	for _, table := range []string{"outbox_messages", "agent_eval_task_results", "agent_eval_reports", "agent_adapters", "agent_trajectory_datasets", "agent_run_labels", "golden_tasks", "agent_champion_states", "agent_endpoint_bindings", "agent_spec_versions", "agent_lineages"} {
		if _, err := database.Pool.Exec(ctx, "DELETE FROM "+database.Name+"."+table); err != nil {
			return err
		}
	}
	return nil
}
