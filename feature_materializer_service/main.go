package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	usecase "feature_materializer_service/pkg/app"
	"feature_materializer_service/pkg/domain/model"
	"feature_materializer_service/pkg/infra/materialization"
	featuregrpc "feature_materializer_service/pkg/infra/network/grpc"
	featuremessaging "feature_materializer_service/pkg/infra/network/messaging"
	featuredb "feature_materializer_service/pkg/infra/repo/db"
	featuretemporal "feature_materializer_service/pkg/infra/temporalworker"
	coreDB "lib/shared_lib/db"
	env "lib/shared_lib/env"
	coreHealthCheck "lib/shared_lib/healthcheck"
	logs "lib/shared_lib/logs"
	messagingConn "lib/shared_lib/messaging"
	trace "lib/shared_lib/trace"

	log "github.com/sirupsen/logrus"
	"go.temporal.io/sdk/client"
)

var Version string

type materializerConfig struct {
	ServiceName          string
	DBName               string
	DBConnectionString   string
	Messaging            messagingConn.MessengerConfig
	OutboxBackend        string
	OutboxRelay          messagingConn.OutboxRelayConfig
	ArtifactBucket       artifactBucketConfig
	Embedding            embeddingConfig
	Temporal             temporalConfig
	GRPCPort             int
	DatasetUploadedTopic string
	PublishTopics        featuremessaging.MaterializationTopics
	Health               healthConfig
}

type artifactBucketConfig struct {
	Name           string
	Region         string
	UploadPartSize int64
}

type embeddingConfig struct {
	Provider        string
	Endpoint        string
	Model           string
	Dimensions      int
	MaxRows         int
	StrategyVersion string
	ChunkerName     string
	ChunkerVersion  string
	ChunkSize       int
	ChunkOverlap    int
	RequestTimeout  time.Duration
}

type temporalConfig struct {
	Address   string
	Namespace string
	TaskQueue string
}

type healthConfig struct {
	CpuThresholdPercentage        int
	MemFreeThresholdPercentage    int
	HealthCheckPort               int
	DBConnectionString            string
	MessageBrokerConnectionString string
	DbLatencyThreshold            time.Duration
	MessageBrokerLatencyThreshold time.Duration
	ServiceLatencyThreshold       time.Duration
}

func init() {
	logs.Init()
}

func main() {
	ctx := context.Background()
	cancelCtx, cancelFtn := context.WithCancel(ctx)
	defer cancelFtn()

	cfg := readMaterializerConfig()
	serviceName := cfg.ServiceName

	log.Trace(fmt.Sprintf("starting the %s service", serviceName))
	traceShutdown := trace.Init(cancelCtx, serviceName, Version)
	defer traceShutdown()

	database, err := coreDB.InitDatabase(cancelCtx, cfg.DBName, cfg.DBConnectionString, log.StandardLogger())
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("database init failed")
	}
	defer database.Close()

	messagingFactory := messagingConn.NewMessenger(cfg.Messaging, cancelFtn)
	defer func() {
		_ = messagingFactory.Close(cancelCtx)
	}()

	outboxWriter, err := newPostgresOutbox(database, cfg.OutboxBackend)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create postgres outbox")
	}
	orderedOutbox, ok := outboxWriter.(messagingConn.OrderedOutbox)
	if !ok {
		log.Fatal("postgres outbox does not support ordered transactional enqueue")
	}
	relayOutbox, ok := outboxWriter.(messagingConn.RelayOutbox)
	if !ok {
		log.Fatal("postgres outbox does not support relay operations")
	}
	outboxPublisher, err := messagingConn.NewPublisher(cfg.Messaging.Brokers)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create outbox relay publisher")
	}
	defer outboxPublisher.Close()
	relayPublisher, ok := outboxPublisher.(messagingConn.RelayPublisher)
	if !ok {
		log.Fatal("publisher does not support outbox relay publishing")
	}
	outboxRelay := messagingConn.NewOutboxRelay(relayOutbox, relayPublisher, cfg.OutboxRelay)
	go func() {
		if relayErr := outboxRelay.Run(cancelCtx); relayErr != nil && !errors.Is(relayErr, context.Canceled) {
			log.WithContext(cancelCtx).WithError(relayErr).Error("outbox relay stopped unexpectedly")
		}
	}()

	subscriber, err := messagingFactory.Subscriber(cancelCtx)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create the subscriber")
	}

	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.Temporal.Address,
		Namespace: cfg.Temporal.Namespace,
	})
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to connect to Temporal")
	}
	defer temporalClient.Close()

	snapshotRepo := featuredb.NewSnapshotRepository(database, featuredb.WithTransactionalOutbox(orderedOutbox, cfg.PublishTopics.FeatureMaterializer))
	artifactStore, err := materialization.NewObjectArtifactStore(cancelCtx, cfg.ArtifactBucket.Name, cfg.ArtifactBucket.Region, cfg.ArtifactBucket.UploadPartSize)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create artifact store")
	}

	rawWriter := materialization.NewRawSnapshotWriter(artifactStore)
	featureBuilder := materialization.NewFeatureSnapshotBuilder(artifactStore)
	embeddingStrategy := embeddingStrategyFromConfig(cfg.Embedding)
	embeddingProvider, err := newEmbeddingProvider(cfg.Embedding)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create embedding provider")
	}
	embeddingChunker := materialization.NewTokenWindowChunker(embeddingStrategy)
	embeddingWriter := materialization.NewEmbeddingWriter(artifactStore, snapshotRepo, embeddingProvider, embeddingChunker, embeddingStrategy, "pgvector", cfg.Embedding.MaxRows)
	rawDispatcher := materialization.NewRawSnapshotWriterDispatcher(rawWriter)
	featureDispatcher := materialization.NewFeatureSnapshotBuilderDispatcher(featureBuilder)
	embeddingDispatcher := materialization.NewEmbeddingWriterDispatcher(embeddingWriter)

	rawSnapshotUsecase := usecase.NewRawSnapshotUsecase(snapshotRepo, rawDispatcher)
	featureSnapshotUsecase := usecase.NewFeatureSnapshotUsecase(snapshotRepo, snapshotRepo, featureDispatcher)
	embeddingUsecase := usecase.NewEmbeddingMaterializationUsecase(snapshotRepo, snapshotRepo, embeddingDispatcher)
	embeddingSearchUsecase := usecase.NewEmbeddingSearchUsecase(snapshotRepo, newQueryEmbeddingProviderFactory(cfg.Embedding))
	activities := featuretemporal.NewMaterializationActivities(rawSnapshotUsecase, featureSnapshotUsecase, embeddingUsecase)
	materializationWorker := featuretemporal.NewMaterializationWorker(temporalClient, cfg.Temporal.TaskQueue, activities)
	if err := materializationWorker.Start(); err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to start Temporal worker")
	}
	defer materializationWorker.Stop()

	workflowStarter := featuretemporal.NewMaterializationWorkflowStarter(temporalClient, cfg.Temporal.TaskQueue, embeddingStrategy)
	materializationSubscriber := featuremessaging.NewMaterializationSubscriber(
		subscriber,
		workflowStarter,
		[]string{cfg.DatasetUploadedTopic},
	)

	grpcService := featuregrpc.NewFeatureMaterializerGrpcServer(embeddingSearchUsecase)
	defer grpcService.Close()

	healthCheck := coreHealthCheck.NewMonitor(newHealthCheckConfig(cfg.Health))
	healthCheck = healthCheck.WithCpuCheck().WithDatabaseCheck().WithMemoryCheck().WithMessageBrokerCheck()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := materializationSubscriber.Start(cancelCtx); err != nil && !errors.Is(err, context.Canceled) {
			log.WithContext(cancelCtx).WithError(err).Fatal("materialization subscriber stopped unexpectedly")
		}
	}()

	go func() {
		if err := grpcService.Connect(cfg.GRPCPort); err != nil {
			log.Errorf("unable to start the %s grpc service: %v", serviceName, err)
			quit <- syscall.SIGTERM
		}
	}()

	go func() {
		if err := healthCheck.Connect(cancelCtx); err != nil {
			if err != http.ErrServerClosed {
				log.Fatalf("unable to start health check for the %s service: %v", serviceName, err)
			}
			quit <- syscall.SIGTERM
		}
	}()

	<-quit

	cancelFtn()
	healthCheck.Close()
	log.Trace(fmt.Sprintf("stopped the %s service", serviceName))
}

func readMaterializerConfig() materializerConfig {
	brokers := env.WithDefaultString("KAFKA_BROKER", "localhost:9092")
	dbName := env.WithDefaultString("FEATURE_MATERIALIZER_DB_NAME", "bighill_feature_materializer_db")
	dbConnectionString := postgresConnectionString(
		env.WithDefaultString("FEATURE_MATERIALIZER_DB_USER", "bighill_feature_materializer_db_user"),
		env.WithDefaultString("FEATURE_MATERIALIZER_DB_PASSWORD", ""),
		env.WithDefaultString("FEATURE_MATERIALIZER_DB_HOST", env.WithDefaultString("PGHOST", "127.0.0.1")),
		env.WithDefaultString("FEATURE_MATERIALIZER_DB_PORT", env.WithDefaultString("PGPORT", "5432")),
		dbName,
		env.WithDefaultString("FEATURE_MATERIALIZER_DB_SSLMODE", env.WithDefaultString("PGSSLMODE", "disable")),
		env.WithDefaultInt("FEATURE_MATERIALIZER_DB_MAX_CONNECTIONS", "20"),
	)
	uploadPartSizeMB := env.WithDefaultInt64("FEATURE_MATERIALIZER_ARTIFACT_UPLOAD_PART_SIZE_MB", "10")
	return materializerConfig{
		ServiceName:        env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_NAME", "feature-materializer-service"),
		DBName:             dbName,
		DBConnectionString: dbConnectionString,
		Messaging: messagingConn.MessengerConfig{
			DlqURL:  env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_DLQ", "http://localhost:4566/feature-materializer-dev-env-queue/"),
			GroupID: env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_KAFKA_GROUP_ID", "feature-materializer-group"),
			Brokers: brokers,
		},
		OutboxBackend: env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_OUTBOX", "postgres"),
		OutboxRelay: messagingConn.OutboxRelayConfig{
			PollInterval:   time.Duration(env.WithDefaultInt("FEATURE_MATERIALIZER_SERVICE_OUTBOX_RELAY_POLL_MS", "250")) * time.Millisecond,
			FailureBackoff: time.Duration(env.WithDefaultInt("FEATURE_MATERIALIZER_SERVICE_OUTBOX_RELAY_FAILURE_BACKOFF_MS", "2000")) * time.Millisecond,
			BatchSize:      int32(env.WithDefaultInt("FEATURE_MATERIALIZER_SERVICE_OUTBOX_RELAY_BATCH_SIZE", "100")),
		},
		ArtifactBucket: artifactBucketConfig{
			Name:           env.WithDefaultString("FEATURE_MATERIALIZER_ARTIFACT_BUCKET_NAME", "local-dev-bucket"),
			Region:         env.WithDefaultString("FEATURE_MATERIALIZER_ARTIFACT_BUCKET_REGION", "local-dev"),
			UploadPartSize: uploadPartSizeMB * 1024 * 1024,
		},
		Embedding: embeddingConfig{
			Provider:        env.WithDefaultString("FEATURE_MATERIALIZER_EMBEDDING_PROVIDER", "ollama"),
			Endpoint:        env.WithDefaultString("FEATURE_MATERIALIZER_EMBEDDING_ENDPOINT", "http://localhost:11434"),
			Model:           env.WithDefaultString("FEATURE_MATERIALIZER_EMBEDDING_MODEL", model.DefaultEmbeddingModel),
			Dimensions:      env.WithDefaultInt("FEATURE_MATERIALIZER_EMBEDDING_DIMENSIONS", strconv.Itoa(model.DefaultEmbeddingDimensions)),
			MaxRows:         env.WithDefaultInt("FEATURE_MATERIALIZER_EMBEDDING_MAX_ROWS", "1000"),
			StrategyVersion: env.WithDefaultString("FEATURE_MATERIALIZER_EMBEDDING_STRATEGY_VERSION", model.DefaultEmbeddingStrategyVersion),
			ChunkerName:     env.WithDefaultString("FEATURE_MATERIALIZER_EMBEDDING_CHUNKER_NAME", model.DefaultChunkerName),
			ChunkerVersion:  env.WithDefaultString("FEATURE_MATERIALIZER_EMBEDDING_CHUNKER_VERSION", model.DefaultChunkerVersion),
			ChunkSize:       env.WithDefaultInt("FEATURE_MATERIALIZER_EMBEDDING_CHUNK_SIZE", strconv.Itoa(model.DefaultChunkSize)),
			ChunkOverlap:    env.WithDefaultInt("FEATURE_MATERIALIZER_EMBEDDING_CHUNK_OVERLAP", strconv.Itoa(model.DefaultChunkOverlap)),
			RequestTimeout:  secondsFromEnv("FEATURE_MATERIALIZER_EMBEDDING_REQUEST_TIMEOUT_SECONDS", "30"),
		},
		Temporal: temporalConfig{
			Address:   env.WithDefaultString("FEATURE_MATERIALIZER_TEMPORAL_ADDRESS", env.WithDefaultString("TEMPORAL_ADDRESS", "localhost:7233")),
			Namespace: env.WithDefaultString("FEATURE_MATERIALIZER_TEMPORAL_NAMESPACE", env.WithDefaultString("TEMPORAL_NAMESPACE", "default")),
			TaskQueue: env.WithDefaultString("FEATURE_MATERIALIZER_TEMPORAL_TASK_QUEUE", usecase.DefaultMaterializeWorkflowTaskQueue),
		},
		GRPCPort:             env.WithDefaultInt("FEATURE_MATERIALIZER_API_GRPC_PORT", "7072"),
		DatasetUploadedTopic: env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_DATA_INGESTION_SUBSCRIBER_TOPIC", "data_ingestion"),
		PublishTopics: featuremessaging.MaterializationTopics{
			FeatureMaterializer: env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_TOPIC", "feature_materializer"),
		},
		Health: healthConfig{
			CpuThresholdPercentage:        env.WithDefaultInt("FEATURE_MATERIALIZER_HEALTHCHECK_CPU_THRESHOLD_PERCENT", "80"),
			MemFreeThresholdPercentage:    env.WithDefaultInt("FEATURE_MATERIALIZER_HEALTHCHECK_FREE_MEM_THRESHOLD_PERCENT", "20"),
			HealthCheckPort:               env.WithDefaultInt("FEATURE_MATERIALIZER_HEALTHCHECK_PORT", "5057"),
			DBConnectionString:            dbConnectionString,
			MessageBrokerConnectionString: brokers,
			DbLatencyThreshold:            secondsFromEnv("FEATURE_MATERIALIZER_HEALTHCHECK_DB_LATENCY_THRESHOLD_SECONDS", "5"),
			MessageBrokerLatencyThreshold: secondsFromEnv("FEATURE_MATERIALIZER_HEALTHCHECK_MSG_BROKER_LATENCY_THRESHOLD_SECONDS", "5"),
			ServiceLatencyThreshold:       secondsFromEnv("FEATURE_MATERIALIZER_HEALTHCHECK_SERVICE_LATENCY_THRESHOLD_SECONDS", "5"),
		},
	}
}

func newHealthCheckConfig(cfg healthConfig) coreHealthCheck.HealthCheckConfig {
	return coreHealthCheck.HealthCheckConfig{
		CpuThresholdPercentage:                       cfg.CpuThresholdPercentage,
		MemFreeThresholdPercentage:                   cfg.MemFreeThresholdPercentage,
		HealthCheckPort:                              cfg.HealthCheckPort,
		DBConnectionString:                           cfg.DBConnectionString,
		MessageBrokerConnectionString:                cfg.MessageBrokerConnectionString,
		DbLatencyThresholdSec:                        cfg.DbLatencyThreshold,
		MessageBrokerLatencyThresholdSec:             cfg.MessageBrokerLatencyThreshold,
		ServiceLatencyThresholdSec:                   cfg.ServiceLatencyThreshold,
		HttpCheckTargets:                             map[string]string{},
		MessageBrokerSubscriberMaxPollSilenceSec:     0,
		MessageBrokerSubscriberMaxProgressSilenceSec: 0,
		MessageBrokerSubscriberMaxLag:                0,
	}
}

func secondsFromEnv(key, defaultValue string) time.Duration {
	return time.Duration(env.WithDefaultInt(key, defaultValue)) * time.Second
}

func embeddingStrategyFromConfig(cfg embeddingConfig) model.EmbeddingStrategy {
	log.Trace("embeddingStrategyFromConfig")

	return model.NormalizeEmbeddingStrategy(model.EmbeddingStrategy{
		StrategyVersion:     cfg.StrategyVersion,
		ChunkerName:         cfg.ChunkerName,
		ChunkerVersion:      cfg.ChunkerVersion,
		ChunkSize:           cfg.ChunkSize,
		ChunkOverlap:        cfg.ChunkOverlap,
		EmbeddingProvider:   cfg.Provider,
		EmbeddingModel:      cfg.Model,
		EmbeddingDimensions: cfg.Dimensions,
	})
}

func newQueryEmbeddingProviderFactory(cfg embeddingConfig) usecase.QueryEmbeddingProviderFactory {
	log.Trace("newQueryEmbeddingProviderFactory")

	return func(strategy model.EmbeddingStrategy) (usecase.QueryEmbeddingProvider, error) {
		log.Trace("queryEmbeddingProviderFactory")

		strategy = model.NormalizeEmbeddingStrategy(strategy)
		queryCfg := cfg
		queryCfg.Provider = strategy.EmbeddingProvider
		queryCfg.Model = strategy.EmbeddingModel
		queryCfg.Dimensions = strategy.EmbeddingDimensions
		return newEmbeddingProvider(queryCfg)
	}
}

func newEmbeddingProvider(cfg embeddingConfig) (materialization.EmbeddingProvider, error) {
	log.Trace("newEmbeddingProvider")

	provider := strings.ToLower(strings.TrimSpace(cfg.Provider))
	switch provider {
	case "tei", "ollama":
		return materialization.NewHTTPEmbeddingProvider(provider, cfg.Endpoint, cfg.Model, cfg.Dimensions, cfg.RequestTimeout), nil
	case "deterministic":
		return materialization.NewDeterministicEmbeddingProvider(cfg.Dimensions), nil
	default:
		return nil, fmt.Errorf("unsupported embedding provider %q", cfg.Provider)
	}
}

func newPostgresOutbox(database *coreDB.Database, backend string) (messagingConn.OutboxWriter, error) {
	log.Trace("newPostgresOutbox")

	if backend != "postgres" {
		return nil, fmt.Errorf("unsupported outbox backend %q", backend)
	}
	return messagingConn.NewPostgresOutbox(database.Pool, database.Name, "")
}

func postgresConnectionString(user, password, host, port, dbName, sslMode string, maxConnections int) string {
	encodedUser := url.QueryEscape(user)
	encodedPassword := url.QueryEscape(password)
	q := url.Values{}
	q.Set("sslmode", sslMode)
	q.Set("pool_max_conns", strconv.Itoa(maxConnections))
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?%s", encodedUser, encodedPassword, host, port, dbName, q.Encode())
}
