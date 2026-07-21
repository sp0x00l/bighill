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

type unexpectedAgentAdapterTrainingDispatcher struct{}

func (d *unexpectedAgentAdapterTrainingDispatcher) DispatchAgentAdapterTraining(context.Context, model.AgentAdapterTrainingRequest) (*model.AgentAdapterTrainingResult, error) {
	Fail("agent adapter training is covered by heuristic or real-infra suites, not the default registry integration suite")
	return nil, nil
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
			&unexpectedAgentAdapterTrainingDispatcher{},
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
