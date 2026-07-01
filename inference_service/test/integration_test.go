package integration_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"inference_service/pkg/app"
	"inference_service/pkg/domain/model"
	inferencemessaging "inference_service/pkg/infra/network/messaging"
	repo "inference_service/pkg/infra/repo/db"

	modelregistrypb "lib/data_contracts_lib/model_registry"
	dbconn "lib/shared_lib/db"
	env "lib/shared_lib/env"
	sharedmessaging "lib/shared_lib/messaging"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

func TestInferenceIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Inference service integration test suite")
}

var _ = Describe("Inference service integration", Ordered, func() {
	var (
		ctx       context.Context
		cancel    context.CancelFunc
		database  *dbconn.Database
		models    *repo.InferenceModelRepository
		modelsUse app.InferenceUsecase
	)

	BeforeAll(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 90*time.Second)

		dbName := env.WithDefaultString("INFERENCE_DB_NAME", "bighill_inference_db")
		connectionString := testPostgresConnectionString(dbName)

		var err error
		database, err = dbconn.InitDatabase(ctx, dbName, connectionString, log.StandardLogger())
		Expect(err).NotTo(HaveOccurred())

		models = repo.NewInferenceModelRepository(database)
		modelsUse = app.NewInferenceUsecase(models)
	})

	BeforeEach(func() {
		Expect(truncateInferenceModels(ctx, database)).To(Succeed())
	})

	AfterAll(func() {
		if database != nil {
			database.Close()
		}
		if cancel != nil {
			cancel()
		}
	})

	It("persists inference model updates from Kafka model registry facts", func() {
		brokers := env.WithDefaultString("KAFKA_BROKER", "localhost:9092")
		suffix := fmt.Sprintf("%d", rand.Int64())
		topics := inferencemessaging.InferenceTopics{
			ModelRegistry: "model_registry",
		}
		runCtx, runCancel := context.WithCancel(ctx)
		defer runCancel()

		serviceMessenger := sharedmessaging.NewMessenger(sharedmessaging.MessengerConfig{
			Brokers:         brokers,
			GroupID:         "inference-integration-service-" + suffix,
			DlqURL:          "http://localhost:4566/inference-dev-env-queue/",
			AutoOffsetReset: "earliest",
		}, runCancel)
		defer func() {
			_ = serviceMessenger.Close(runCtx)
		}()
		serviceSubscriber, err := serviceMessenger.Subscriber(runCtx)
		Expect(err).NotTo(HaveOccurred())
		publisher, err := serviceMessenger.Publisher(runCtx)
		Expect(err).NotTo(HaveOccurred())

		modelUpdatedSubscriber := inferencemessaging.NewModelUpdatedSubscriber(serviceSubscriber, modelsUse, topics)
		go func() {
			_ = modelUpdatedSubscriber.Start(runCtx)
		}()
		time.Sleep(750 * time.Millisecond)

		modelID := uuid.New()
		trainingRunID := uuid.New()
		datasetID := uuid.New()
		Expect(publisher.Publish(runCtx, topics.ModelRegistry, sharedmessaging.Message{
			ResourceKey: modelID,
			MsgType:     sharedmessaging.MsgTypeModelUpdated,
		}, &modelregistrypb.ModelUpdatedEvent{
			ModelId:           modelID.String(),
			TrainingRunId:     trainingRunID.String(),
			DatasetId:         datasetID.String(),
			Name:              "movie-ranker",
			ModelVersion:      3,
			BaseModel:         "mistral-7b",
			ArtifactLocation:  "s3://local-dev-bucket/models/" + modelID.String(),
			ArtifactFormat:    "HF_PEFT_ADAPTER",
			ArtifactChecksum:  "sha256:abc",
			ArtifactSizeBytes: 128,
			MetricsMetadata:   `{"eval_loss":0.12}`,
			Status:            "READY",
		})).To(Succeed())

		Eventually(func(g Gomega) {
			record, err := models.ReadByID(ctx, modelID)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(record.TrainingRunID).To(Equal(trainingRunID))
			g.Expect(record.DatasetID).To(Equal(datasetID))
			g.Expect(record.Status).To(Equal(model.ModelStatusReady))
			g.Expect(record.ArtifactLocation).To(Equal("s3://local-dev-bucket/models/" + modelID.String()))
			g.Expect(record.ArtifactFormat).To(Equal("HF_PEFT_ADAPTER"))
		}, 30*time.Second, 500*time.Millisecond).Should(Succeed())
	})
})

func truncateInferenceModels(ctx context.Context, database *dbconn.Database) error {
	_, err := database.Pool.Exec(ctx, "DELETE FROM "+database.Name+".inference_models")
	return err
}

func testPostgresConnectionString(dbName string) string {
	user := env.WithDefaultString("INFERENCE_DB_USER", "bighill_inference_db_user")
	password := env.WithDefaultString("INFERENCE_DB_PASSWORD", env.WithDefaultString("BIGHILL_DB_PASSWORD", "LrDwb53E7DmFc2j4qw77n4pUUfKtULDVh4vrHjWw"))
	host := env.WithDefaultString("INFERENCE_DB_HOST", env.WithDefaultString("PGHOST", "127.0.0.1"))
	if host == "" || host == "/private/tmp" {
		host = "127.0.0.1"
	}
	port := env.WithDefaultString("INFERENCE_DB_PORT", env.WithDefaultString("PGPORT", "5432"))
	sslMode := env.WithDefaultString("INFERENCE_DB_SSLMODE", env.WithDefaultString("PGSSLMODE", "disable"))
	maxConnections := env.WithDefaultInt("INFERENCE_DB_MAX_CONNECTIONS", "20")
	if value := os.Getenv("INFERENCE_DB_NAME"); value != "" {
		dbName = value
	}

	q := url.Values{}
	q.Set("sslmode", sslMode)
	q.Set("pool_max_conns", strconv.Itoa(maxConnections))
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?%s", url.QueryEscape(user), url.QueryEscape(password), host, port, dbName, q.Encode())
}
