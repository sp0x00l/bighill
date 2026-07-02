package integration_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"model_registry_service/pkg/app"
	"model_registry_service/pkg/domain"
	"model_registry_service/pkg/domain/model"
	registrymessaging "model_registry_service/pkg/infra/network/messaging"
	repo "model_registry_service/pkg/infra/repo/db"

	modelregistrypb "lib/data_contracts_lib/model_registry"
	trainingpb "lib/data_contracts_lib/training"
	dbconn "lib/shared_lib/db"
	env "lib/shared_lib/env"
	sharedmessaging "lib/shared_lib/messaging"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

func TestModelRegistryIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Model registry integration test suite")
}

var _ = Describe("Model registry integration", Ordered, func() {
	var (
		ctx       context.Context
		cancel    context.CancelFunc
		database  *dbconn.Database
		models    app.ModelRepository
		modelsUse app.ModelRegistryUsecase
	)

	BeforeAll(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 90*time.Second)

		cfg := dbconn.DatabaseConfig{}
		cfg.WithDbName("MODEL_REGISTRY_DB_NAME", "bighill_model_registry_db")
		cfg.WithDbUser("MODEL_REGISTRY_DB_USER", "bighill_model_registry_db_user")
		cfg.WithDbPassword("MODEL_REGISTRY_DB_PASSWORD", "LrDwb53E7DmFc2j4qw77n4pUUfKtULDVh4vrHjWw")
		cfg.WithDbMaxConnections("MODEL_REGISTRY_DB_MAX_CONNECTIONS", "20")

		var err error
		database, err = dbconn.InitDatabase(ctx, cfg.GetName(), cfg.GetConnectionString(), log.StandardLogger())
		Expect(err).NotTo(HaveOccurred())

		models = repo.NewModelRepository(database)
		modelsUse = app.NewModelRegistryUsecase(models)
	})

	BeforeEach(func() {
		Expect(truncateModelRegistry(ctx, database)).To(Succeed())
	})

	AfterAll(func() {
		if models != nil {
			models.Close()
		}
		if cancel != nil {
			cancel()
		}
	})

	It("persists and updates model registry records", func() {
		registeredModel, err := modelsUse.RegisterModel(ctx, validIntegrationModel(), uuid.New())
		Expect(err).NotTo(HaveOccurred())
		Expect(registeredModel.ModelID).NotTo(Equal(uuid.Nil))
		Expect(registeredModel.Status).To(Equal(model.ModelStatusPending))

		readyModel, err := modelsUse.MarkModelReady(ctx, registeredModel.ModelID, "s3://local-dev-bucket/models/run/model")
		Expect(err).NotTo(HaveOccurred())
		Expect(readyModel.Status).To(Equal(model.ModelStatusReady))
		Expect(readyModel.ArtifactLocation).To(Equal("s3://local-dev-bucket/models/run/model"))

		readModel, err := modelsUse.ReadModel(ctx, registeredModel.ModelID)
		Expect(err).NotTo(HaveOccurred())
		Expect(readModel.ModelID).To(Equal(registeredModel.ModelID))
	})

	It("reports duplicate idempotency keys and missing models with domain errors", func() {
		idempotencyKey := uuid.New()
		_, err := modelsUse.RegisterModel(ctx, validIntegrationModel(), idempotencyKey)
		Expect(err).NotTo(HaveOccurred())

		_, err = modelsUse.RegisterModel(ctx, validIntegrationModel(), idempotencyKey)
		Expect(errors.Is(err, domain.ErrModelExists)).To(BeTrue())

		_, err = modelsUse.ReadModel(ctx, uuid.New())
		Expect(errors.Is(err, domain.ErrModelNotFound)).To(BeTrue())
	})

	It("records completed training from Kafka and publishes model updates through the Postgres outbox", func() {
		brokers := env.WithDefaultString("KAFKA_BROKER", "localhost:9092")
		suffix := fmt.Sprintf("%d", rand.Int64())
		topics := registrymessaging.ModelRegistryTopics{
			ModelRegistry: "model_registry",
			Training:      "training",
		}
		runCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		serviceMessenger := sharedmessaging.NewMessenger(sharedmessaging.MessengerConfig{
			Brokers:         brokers,
			GroupID:         "model-registry-integration-service-" + suffix,
			DlqURL:          "http://localhost:4566/model-registry-dev-env-queue/",
			AutoOffsetReset: "earliest",
		}, cancel)
		defer func() {
			_ = serviceMessenger.Close(runCtx)
		}()
		serviceSubscriber, err := serviceMessenger.Subscriber(runCtx)
		Expect(err).NotTo(HaveOccurred())
		relayPublisher, err := serviceMessenger.Publisher(runCtx)
		Expect(err).NotTo(HaveOccurred())

		outboxWriter, err := sharedmessaging.NewPostgresOutbox(database.Pool, database.Name, "")
		Expect(err).NotTo(HaveOccurred())
		orderedOutbox, ok := outboxWriter.(sharedmessaging.OrderedOutbox)
		Expect(ok).To(BeTrue())
		relayOutbox, ok := outboxWriter.(sharedmessaging.RelayOutbox)
		Expect(ok).To(BeTrue())
		modelRepositoryWithOutbox := repo.NewModelRepository(database, repo.WithTransactionalOutbox(orderedOutbox, topics.ModelRegistry))
		modelUsecase := app.NewModelRegistryUsecase(modelRepositoryWithOutbox)
		trainingEventSubscriber := registrymessaging.NewTrainingEventSubscriber(serviceSubscriber, modelUsecase, topics)

		relayRawPublisher, ok := relayPublisher.(sharedmessaging.RelayPublisher)
		Expect(ok).To(BeTrue())
		outboxRelay := sharedmessaging.NewOutboxRelay(relayOutbox, relayRawPublisher, sharedmessaging.OutboxRelayConfig{
			PollInterval:   100 * time.Millisecond,
			FailureBackoff: 250 * time.Millisecond,
			BatchSize:      10,
			InstanceID:     "model-registry-integration-" + suffix,
			LeaseDuration:  time.Second,
		})
		go func() {
			_ = outboxRelay.Run(runCtx)
		}()

		probe := newModelUpdatedProbe()
		outputMessenger := sharedmessaging.NewMessenger(sharedmessaging.MessengerConfig{
			Brokers:         brokers,
			GroupID:         "model-registry-integration-output-" + suffix,
			DlqURL:          "http://localhost:4566/model-registry-output-dev-env-queue/",
			AutoOffsetReset: "earliest",
		}, cancel)
		defer func() {
			_ = outputMessenger.Close(runCtx)
		}()
		outputSubscriber, err := outputMessenger.Subscriber(runCtx)
		Expect(err).NotTo(HaveOccurred())
		sharedmessaging.AddListener(outputSubscriber, probe)

		go func() {
			_ = outputSubscriber.Subscribe(runCtx, []string{topics.ModelRegistry})
		}()
		go func() {
			_ = trainingEventSubscriber.Start(runCtx)
		}()
		time.Sleep(750 * time.Millisecond)

		datasetID := uuid.New()
		trainingRunID := uuid.New()
		modelID := uuid.New()
		Expect(relayPublisher.Publish(runCtx, topics.Training, sharedmessaging.Message{
			ResourceKey: datasetID,
			MsgType:     sharedmessaging.MsgTypeModelTrainingCompleted,
		}, &trainingpb.ModelTrainingCompletedEvent{
			TrainingRunId:     trainingRunID.String(),
			DatasetId:         datasetID.String(),
			DatasetVersion:    "7",
			FeatureSnapshotId: uuid.NewString(),
			ModelId:           modelID.String(),
			ModelName:         "movie-ranker",
			ModelVersion:      "7",
			BaseModel:         "mistral-7b",
			ArtifactLocation:  "s3://local-dev-bucket/models/" + trainingRunID.String(),
			ArtifactFormat:    "HF_PEFT_ADAPTER",
			ArtifactChecksum:  "sha256:abc",
			ArtifactSizeBytes: 128,
			MetricsMetadata:   `{"eval_loss":0.12}`,
			ReportLocation:    "s3://local-dev-bucket/evals/" + trainingRunID.String() + ".json",
		})).To(Succeed())

		Eventually(func(g Gomega) {
			modelRecord, err := models.ReadByTrainingRunID(ctx, trainingRunID)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(modelRecord.DatasetID).To(Equal(datasetID))
			g.Expect(modelRecord.Status).To(Equal(model.ModelStatusReady))
			g.Expect(modelRecord.ModelVersion).To(Equal(7))
			g.Expect(modelRecord.ArtifactLocation).To(Equal("s3://local-dev-bucket/models/" + trainingRunID.String()))
			g.Expect(modelRecord.ArtifactFormat).To(Equal("HF_PEFT_ADAPTER"))
			g.Expect(outboxSentCount(ctx, database, modelRecord.ModelID)).To(Equal(1))
			g.Expect(probe.receivedTrainingRun(trainingRunID)).To(BeTrue())
		}, 30*time.Second, 500*time.Millisecond).Should(Succeed())
	})
})

func validIntegrationModel() *model.Model {
	return &model.Model{
		ModelID:           uuid.New(),
		TrainingRunID:     uuid.New(),
		DatasetID:         uuid.New(),
		Name:              "movie-ranker",
		ModelVersion:      1,
		BaseModel:         "mistral-7b",
		ArtifactLocation:  "s3://local-dev-bucket/models/pending",
		ArtifactFormat:    "HF_PEFT_ADAPTER",
		ArtifactChecksum:  "sha256:pending",
		ArtifactSizeBytes: 1,
		MetricsMetadata:   `{"eval_loss":0.12}`,
	}
}

func truncateModelRegistry(ctx context.Context, database *dbconn.Database) error {
	for _, table := range []string{"outbox_messages", "models"} {
		if _, err := database.Pool.Exec(ctx, "DELETE FROM "+database.Name+"."+table); err != nil {
			return err
		}
	}
	return nil
}

func outboxSentCount(ctx context.Context, database *dbconn.Database, modelID uuid.UUID) int {
	var count int
	err := database.Pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM `+database.Name+`.outbox_messages
		WHERE resource_key = $1 AND status = 'SENT'
	`, modelID).Scan(&count)
	Expect(err).NotTo(HaveOccurred())
	return count
}

type modelUpdatedProbe struct {
	events chan *modelregistrypb.ModelUpdatedEvent
	seen   map[uuid.UUID]struct{}
	mutex  sync.Mutex
}

func newModelUpdatedProbe() *modelUpdatedProbe {
	return &modelUpdatedProbe{
		events: make(chan *modelregistrypb.ModelUpdatedEvent, 16),
		seen:   map[uuid.UUID]struct{}{},
	}
}

func (p *modelUpdatedProbe) MsgType() sharedmessaging.MsgType {
	return sharedmessaging.MsgTypeModelUpdated
}

func (p *modelUpdatedProbe) NewMessage() *modelregistrypb.ModelUpdatedEvent {
	return &modelregistrypb.ModelUpdatedEvent{}
}

func (p *modelUpdatedProbe) Handle(_ context.Context, _ uuid.UUID, payload *modelregistrypb.ModelUpdatedEvent) error {
	trainingRunID, err := uuid.Parse(payload.GetTrainingRunId())
	if err == nil {
		p.mutex.Lock()
		p.seen[trainingRunID] = struct{}{}
		p.mutex.Unlock()
	}
	select {
	case p.events <- payload:
	default:
	}
	return nil
}

func (p *modelUpdatedProbe) receivedTrainingRun(trainingRunID uuid.UUID) bool {
	for {
		select {
		case event := <-p.events:
			parsed, err := uuid.Parse(event.GetTrainingRunId())
			if err == nil {
				p.mutex.Lock()
				p.seen[parsed] = struct{}{}
				p.mutex.Unlock()
			}
		default:
			p.mutex.Lock()
			defer p.mutex.Unlock()
			_, ok := p.seen[trainingRunID]
			return ok
		}
	}
}
