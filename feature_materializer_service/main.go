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
	coreBucket "lib/shared_lib/bucket"
	coreDB "lib/shared_lib/db"
	env "lib/shared_lib/env"
	coreHealthCheck "lib/shared_lib/healthcheck"
	logs "lib/shared_lib/logs"
	messagingConn "lib/shared_lib/messaging"
	sharedTenant "lib/shared_lib/tenant"
	trace "lib/shared_lib/trace"
	shareduow "lib/shared_lib/uow"

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
	DataStream           dataStreamConfig
	Iceberg              icebergConfig
	Temporal             temporalConfig
	GRPCPort             int
	DatasetUploadedTopic string
	DataRegistryTopic    string
	ProfileTopic         string
	PublishTopics        featuremessaging.MaterializationTopics
	Health               healthConfig
}

type artifactBucketConfig struct {
	Name           string
	Region         string
	UploadPartSize int64
}

type embeddingConfig struct {
	Provider         string
	URL              string
	Model            string
	Dimensions       int
	MaxRows          int
	StrategyVersion  string
	ExtractorName    string
	ExtractorVersion string
	CleanerName      string
	CleanerVersion   string
	ChunkerName      string
	ChunkerVersion   string
	ChunkSize        int
	ChunkOverlap     int
	RequestTimeout   time.Duration
}

type dataStreamConfig struct {
	Address        string
	RequestTimeout time.Duration
	AuthToken      string
	Insecure       bool
	ServerName     string
	CACertPath     string
	ClientCertPath string
	ClientKeyPath  string
}

type icebergConfig struct {
	WriterBinaryPath  string
	WriterTimeout     time.Duration
	PolarisBaseURL    string
	PolarisCatalog    string
	PolarisWarehouse  string
	PolarisCredential string
	PolarisToken      string
	PolarisScope      string
	S3Endpoint        string
	S3AccessKeyID     string
	S3SecretAccessKey string
	S3Region          string
	S3PathStyle       bool
}

type temporalConfig struct {
	Address   string
	Namespace string
	TaskQueue string
}

type healthConfig struct {
	CpuThresholdPercentage                    int
	MemFreeThresholdPercentage                int
	HealthCheckPort                           int
	DBConnectionString                        string
	MessageBrokerConnectionString             string
	DbLatencyThreshold                        time.Duration
	MessageBrokerLatencyThreshold             time.Duration
	ServiceLatencyThreshold                   time.Duration
	MessageBrokerSubscriberMaxPollSilence     time.Duration
	MessageBrokerSubscriberMaxProgressSilence time.Duration
	MessageBrokerSubscriberMaxLag             int64
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

	subscriberFactories := []messagingConn.Messenger{}
	defer func() {
		for _, factory := range subscriberFactories {
			_ = factory.Close(cancelCtx)
		}
	}()

	outboxWriter, err := newPostgresOutbox(database, cfg.OutboxBackend)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create postgres outbox")
	}
	orderedOutbox, ok := outboxWriter.(messagingConn.OrderedOutbox)
	if !ok {
		log.Fatal("postgres outbox does not support ordered transactional enqueue")
	}
	outboxSignal := make(chan struct{}, 1)
	outboxWriter = messagingConn.NewSignaledOutbox(outboxWriter, outboxSignal)
	cfg.OutboxRelay.Signal = outboxSignal
	relayOutbox, ok := outboxWriter.(messagingConn.RelayOutbox)
	if !ok {
		log.Fatal("postgres outbox does not support relay operations")
	}
	outboxPublisher, err := messagingConn.NewPublisher(cfg.Messaging.Brokers)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create outbox relay publisher")
	}
	relayPublisher, ok := outboxPublisher.(messagingConn.RelayPublisher)
	if !ok {
		log.Fatal("publisher does not support outbox relay publishing")
	}
	outboxRelay := messagingConn.NewOutboxRelay(relayOutbox, relayPublisher, cfg.OutboxRelay)
	relayCtx, stopOutboxRelay := context.WithCancel(cancelCtx)
	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		if relayErr := outboxRelay.Run(relayCtx); relayErr != nil && !errors.Is(relayErr, context.Canceled) {
			log.WithContext(cancelCtx).WithError(relayErr).Error("outbox relay stopped unexpectedly")
		}
	}()
	defer func() {
		stopOutboxRelay()
		<-relayDone
		outboxPublisher.Close()
	}()

	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.Temporal.Address,
		Namespace: cfg.Temporal.Namespace,
	})
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to connect to Temporal")
	}
	defer temporalClient.Close()

	snapshotRepo := featuredb.NewSnapshotRepository(database)
	snapshotUnitOfWork := shareduow.New(database.Pool,
		shareduow.WithTransactionalOutbox(orderedOutbox),
		shareduow.WithOutboxSignal(func() { messagingConn.NotifyOutboxSignal(outboxSignal) }),
	)
	tenantDB := sharedTenant.NewPostgresProjectionStore(database)
	artifactStore, err := materialization.NewObjectArtifactStore(cancelCtx, cfg.ArtifactBucket.Name, cfg.ArtifactBucket.Region, cfg.ArtifactBucket.UploadPartSize)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create artifact store")
	}

	dataStreamReader := materialization.NewFlightDataStreamReaderWithConfig(materialization.FlightDataStreamReaderConfig{
		Address:        cfg.DataStream.Address,
		Timeout:        cfg.DataStream.RequestTimeout,
		AuthToken:      cfg.DataStream.AuthToken,
		Insecure:       cfg.DataStream.Insecure,
		ServerName:     cfg.DataStream.ServerName,
		CACertPath:     cfg.DataStream.CACertPath,
		ClientCertPath: cfg.DataStream.ClientCertPath,
		ClientKeyPath:  cfg.DataStream.ClientKeyPath,
	})
	icebergWriter := materialization.NewExternalIcebergTableWriter(materialization.ExternalIcebergTableWriterConfig{
		BinaryPath:        cfg.Iceberg.WriterBinaryPath,
		Timeout:           cfg.Iceberg.WriterTimeout,
		PolarisBaseURL:    cfg.Iceberg.PolarisBaseURL,
		PolarisCatalog:    cfg.Iceberg.PolarisCatalog,
		PolarisWarehouse:  cfg.Iceberg.PolarisWarehouse,
		PolarisCredential: cfg.Iceberg.PolarisCredential,
		PolarisToken:      cfg.Iceberg.PolarisToken,
		PolarisScope:      cfg.Iceberg.PolarisScope,
		S3Endpoint:        cfg.Iceberg.S3Endpoint,
		S3AccessKeyID:     cfg.Iceberg.S3AccessKeyID,
		S3SecretAccessKey: cfg.Iceberg.S3SecretAccessKey,
		S3Region:          cfg.Iceberg.S3Region,
		S3PathStyle:       cfg.Iceberg.S3PathStyle,
	})
	rawWriter := materialization.NewRawSnapshotWriter(artifactStore, materialization.WithRawIcebergTableWriter(icebergWriter))
	dataStreamRawWriter := materialization.NewDataStreamRawSnapshotWriter(
		artifactStore,
		dataStreamReader,
		materialization.WithDataStreamRawIcebergTableWriter(icebergWriter),
	)
	featureBuilder := materialization.NewFeatureSnapshotBuilder(artifactStore, materialization.WithFeatureIcebergTableWriter(icebergWriter))
	embeddingStrategy := embeddingStrategyFromConfig(cfg.Embedding)
	embeddingProvider, err := newEmbeddingProvider(cfg.Embedding)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create embedding provider")
	}
	embeddingChunker := materialization.NewTokenWindowChunker(embeddingStrategy)
	embeddingWriter := materialization.NewEmbeddingWriter(artifactStore, embeddingProvider, embeddingChunker, embeddingStrategy, "pgvector", cfg.Embedding.MaxRows)
	rawDispatcher := materialization.NewRawSnapshotWriterDispatcher(dataStreamRawWriter, rawWriter)
	featureDispatcher := materialization.NewFeatureSnapshotBuilderDispatcher(featureBuilder)
	embeddingDispatcher := materialization.NewEmbeddingWriterDispatcher(embeddingWriter)
	snapshotEventBuilder := featuremessaging.NewSnapshotEventBuilder(cfg.PublishTopics)

	rawSnapshotUsecase := usecase.NewRawSnapshotUsecase(snapshotRepo, snapshotUnitOfWork, snapshotEventBuilder, rawDispatcher)
	featureSnapshotUsecase := usecase.NewFeatureSnapshotUsecase(snapshotRepo, snapshotUnitOfWork, snapshotEventBuilder, snapshotRepo, featureDispatcher)
	embeddingUsecase := usecase.NewEmbeddingMaterializationUsecase(snapshotRepo, snapshotUnitOfWork, snapshotEventBuilder, snapshotRepo, embeddingDispatcher)
	embeddingSearchUsecase := usecase.NewEmbeddingSearchUsecase(snapshotRepo, newQueryEmbeddingProviderFactory(cfg.Embedding))
	activities := featuretemporal.NewMaterializationActivities(rawSnapshotUsecase, featureSnapshotUsecase, embeddingUsecase)
	materializationWorker := featuretemporal.NewMaterializationWorker(temporalClient, cfg.Temporal.TaskQueue, activities)
	if err := materializationWorker.Start(); err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to start Temporal worker")
	}
	defer materializationWorker.Stop()

	workflowStarter := featuretemporal.NewMaterializationWorkflowStarter(temporalClient, cfg.Temporal.TaskQueue, embeddingStrategy)

	grpcService := featuregrpc.NewFeatureMaterializerGrpcServer(embeddingSearchUsecase)
	defer grpcService.Close()

	healthCheck := coreHealthCheck.NewMonitor(newHealthCheckConfig(cfg.Health))
	healthCheck = healthCheck.WithCpuCheck().WithDatabaseCheck().WithMemoryCheck().WithMessageBrokerCheck()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	startSubscriber := func(name string, topics []string, configure func(messagingConn.Subscriber)) {
		factory, monitor, err := messagingConn.StartStreamSubscriber(cancelCtx, messagingConn.StreamSubscriberConfig{
			Brokers:          cfg.Messaging.Brokers,
			DLQURL:           cfg.Messaging.DlqURL,
			BaseGroupID:      cfg.Messaging.GroupID,
			AutoOffsetReset:  cfg.Messaging.AutoOffsetReset,
			Cancel:           cancelFtn,
			Monitor:          healthCheck,
			OnUnexpectedStop: func() { quit <- syscall.SIGTERM },
		}, name, topics, configure)
		if err != nil {
			log.WithContext(cancelCtx).WithError(err).Fatalf("unable to create %s subscriber", name)
		}
		healthCheck = monitor
		subscriberFactories = append(subscriberFactories, factory)
	}

	startSubscriber("dataset-file-uploaded", []string{cfg.DatasetUploadedTopic}, func(subscriber messagingConn.Subscriber) {
		featuremessaging.ConfigureSubscriberErrorPolicy(subscriber)
		messagingConn.AddListener(subscriber, featuremessaging.NewDatasetFileUploadedEventListener(workflowStarter))
	})
	startSubscriber("dataset-created", []string{cfg.DataRegistryTopic}, func(subscriber messagingConn.Subscriber) {
		featuremessaging.ConfigureSubscriberErrorPolicy(subscriber)
		messagingConn.AddListener(subscriber, featuremessaging.NewDatasetCreatedEventListener(workflowStarter))
	})
	startSubscriber("dataset-updated", []string{cfg.DataRegistryTopic}, func(subscriber messagingConn.Subscriber) {
		featuremessaging.ConfigureSubscriberErrorPolicy(subscriber)
		messagingConn.AddListener(subscriber, featuremessaging.NewDatasetUpdatedEventListener(workflowStarter))
	})
	startSubscriber("tenant-created", []string{cfg.ProfileTopic}, func(subscriber messagingConn.Subscriber) {
		sharedTenant.ConfigureProfileProjectionErrorPolicy(subscriber)
		messagingConn.AddListener(subscriber, sharedTenant.NewUserCreatedProjectionListener(tenantDB))
	})
	startSubscriber("tenant-updated", []string{cfg.ProfileTopic}, func(subscriber messagingConn.Subscriber) {
		sharedTenant.ConfigureProfileProjectionErrorPolicy(subscriber)
		messagingConn.AddListener(subscriber, sharedTenant.NewUserUpdatedProjectionListener(tenantDB))
	})
	startSubscriber("tenant-deleted", []string{cfg.ProfileTopic}, func(subscriber messagingConn.Subscriber) {
		sharedTenant.ConfigureProfileProjectionErrorPolicy(subscriber)
		messagingConn.AddListener(subscriber, sharedTenant.NewUserDeletedProjectionListener(tenantDB))
	})

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
	dbName := env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_DB_NAME", "bighill_feature_materializer_db")
	dbConnectionString := postgresConnectionString(
		env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_DB_USER", "bighill_feature_materializer_db_user"),
		env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_DB_PASSWORD", ""),
		env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_DB_HOST", env.WithDefaultString("PGHOST", "127.0.0.1")),
		env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_DB_PORT", env.WithDefaultString("PGPORT", "5432")),
		dbName,
		env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_DB_SSLMODE", env.WithDefaultString("PGSSLMODE", "disable")),
		env.WithDefaultInt("FEATURE_MATERIALIZER_SERVICE_DB_MAX_CONNECTIONS", "20"),
	)
	uploadPartSizeMB := env.WithDefaultInt64("FEATURE_MATERIALIZER_SERVICE_ARTIFACT_UPLOAD_PART_SIZE_MB", "10")
	defaultArtifactBucketRegion := "eu-west-1"
	if env.IsDevEnv() {
		defaultArtifactBucketRegion = coreBucket.LocalDevS3Region
	}
	return materializerConfig{
		ServiceName:        env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_NAME", "feature-materializer-service"),
		DBName:             dbName,
		DBConnectionString: dbConnectionString,
		Messaging: messagingConn.MessengerConfig{
			DlqURL:          env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_DLQ", "http://localhost:4566/feature-materializer-dev-env-queue/"),
			GroupID:         env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_KAFKA_BASE_GROUP_ID", "feature-materializer"),
			Brokers:         brokers,
			AutoOffsetReset: env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_KAFKA_AUTO_OFFSET_RESET", "earliest"),
		},
		OutboxBackend: env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_OUTBOX", "postgres"),
		OutboxRelay: messagingConn.OutboxRelayConfig{
			PollInterval:   time.Duration(env.WithDefaultInt("FEATURE_MATERIALIZER_SERVICE_OUTBOX_RELAY_POLL_MS", "250")) * time.Millisecond,
			FailureBackoff: time.Duration(env.WithDefaultInt("FEATURE_MATERIALIZER_SERVICE_OUTBOX_RELAY_FAILURE_BACKOFF_MS", "2000")) * time.Millisecond,
			BatchSize:      int32(env.WithDefaultInt("FEATURE_MATERIALIZER_SERVICE_OUTBOX_RELAY_BATCH_SIZE", "100")),
		},
		ArtifactBucket: artifactBucketConfig{
			Name:           env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_ARTIFACT_BUCKET_NAME", "local-dev-bucket"),
			Region:         env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_ARTIFACT_BUCKET_REGION", defaultArtifactBucketRegion),
			UploadPartSize: uploadPartSizeMB * 1024 * 1024,
		},
		Embedding: embeddingConfig{
			Provider:         env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_EMBEDDING_PROVIDER", model.DefaultEmbeddingProvider),
			URL:              env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_EMBEDDING_URL", ""),
			Model:            env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_EMBEDDING_MODEL", model.DefaultEmbeddingModel),
			Dimensions:       env.WithDefaultInt("FEATURE_MATERIALIZER_SERVICE_EMBEDDING_DIMENSIONS", strconv.Itoa(model.DefaultEmbeddingDimensions)),
			MaxRows:          env.WithDefaultInt("FEATURE_MATERIALIZER_SERVICE_EMBEDDING_MAX_ROWS", "1000"),
			StrategyVersion:  env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_EMBEDDING_STRATEGY_VERSION", model.DefaultEmbeddingStrategyVersion),
			ExtractorName:    env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_EXTRACTOR_NAME", model.DefaultExtractorName),
			ExtractorVersion: env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_EXTRACTOR_VERSION", model.DefaultExtractorVersion),
			CleanerName:      env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_CLEANER_NAME", model.DefaultCleanerName),
			CleanerVersion:   env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_CLEANER_VERSION", model.DefaultCleanerVersion),
			ChunkerName:      env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_EMBEDDING_CHUNKER_NAME", model.DefaultChunkerName),
			ChunkerVersion:   env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_EMBEDDING_CHUNKER_VERSION", model.DefaultChunkerVersion),
			ChunkSize:        env.WithDefaultInt("FEATURE_MATERIALIZER_SERVICE_EMBEDDING_CHUNK_SIZE", strconv.Itoa(model.DefaultChunkSize)),
			ChunkOverlap:     env.WithDefaultInt("FEATURE_MATERIALIZER_SERVICE_EMBEDDING_CHUNK_OVERLAP", strconv.Itoa(model.DefaultChunkOverlap)),
			RequestTimeout:   secondsFromEnv("FEATURE_MATERIALIZER_SERVICE_EMBEDDING_REQUEST_TIMEOUT_SECONDS", "30"),
		},
		DataStream: dataStreamConfig{
			Address:        env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_DATA_STREAM_GRPC_ADDRESS", "localhost:7070"),
			RequestTimeout: secondsFromEnv("FEATURE_MATERIALIZER_SERVICE_DATA_STREAM_REQUEST_TIMEOUT_SECONDS", "60"),
			AuthToken:      env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_DATA_STREAM_AUTH_TOKEN", ""),
			Insecure:       env.WithDefaultBool("FEATURE_MATERIALIZER_SERVICE_DATA_STREAM_INSECURE", false),
			ServerName:     env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_DATA_STREAM_TLS_SERVER_NAME", ""),
			CACertPath:     env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_DATA_STREAM_TLS_CA_CERT_PATH", ""),
			ClientCertPath: env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_DATA_STREAM_TLS_CLIENT_CERT_PATH", ""),
			ClientKeyPath:  env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_DATA_STREAM_TLS_CLIENT_KEY_PATH", ""),
		},
		Iceberg: icebergConfig{
			WriterBinaryPath:  env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_ICEBERG_WRITER_BINARY_PATH", "/usr/local/bin/datafusion_query_engine"),
			WriterTimeout:     secondsFromEnv("FEATURE_MATERIALIZER_SERVICE_ICEBERG_WRITER_TIMEOUT_SECONDS", "120"),
			PolarisBaseURL:    env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_POLARIS_BASE_URL", "http://localhost:8181"),
			PolarisCatalog:    env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_POLARIS_CATALOG", "bighill"),
			PolarisWarehouse:  env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_POLARIS_WAREHOUSE", "s3://bighill-mlops-lakehouse/"),
			PolarisCredential: env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_POLARIS_CREDENTIAL", ""),
			PolarisToken:      env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_POLARIS_TOKEN", ""),
			PolarisScope:      env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_POLARIS_SCOPE", "PRINCIPAL_ROLE:ALL"),
			S3Endpoint:        env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_POLARIS_STORAGE_ENDPOINT", "http://localhost:9100"),
			S3AccessKeyID:     env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_POLARIS_STORAGE_ACCESS_KEY_ID", "polaris_root"),
			S3SecretAccessKey: env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_POLARIS_STORAGE_SECRET_ACCESS_KEY", "polaris_pass"),
			S3Region:          env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_POLARIS_STORAGE_REGION", "eu-west-1"),
			S3PathStyle:       env.WithDefaultBool("FEATURE_MATERIALIZER_SERVICE_POLARIS_STORAGE_PATH_STYLE", true),
		},
		Temporal: temporalConfig{
			Address:   env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_TEMPORAL_ADDRESS", env.WithDefaultString("TEMPORAL_ADDRESS", "localhost:7233")),
			Namespace: env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_TEMPORAL_NAMESPACE", env.WithDefaultString("TEMPORAL_NAMESPACE", "default")),
			TaskQueue: env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_TEMPORAL_TASK_QUEUE", usecase.DefaultMaterializeWorkflowTaskQueue),
		},
		GRPCPort:             env.WithDefaultInt("FEATURE_MATERIALIZER_SERVICE_API_GRPC_PORT", "7072"),
		DatasetUploadedTopic: env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_INGESTION_SUBSCRIBER_TOPIC", "ingestion"),
		DataRegistryTopic:    env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_DATA_REGISTRY_SUBSCRIBER_TOPIC", "data_registry"),
		ProfileTopic:         env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_PROFILE_SUBSCRIBER_TOPIC", "profile"),
		PublishTopics: featuremessaging.MaterializationTopics{
			FeatureMaterializer: env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_TOPIC", "feature_materializer"),
		},
		Health: healthConfig{
			CpuThresholdPercentage:                    env.WithDefaultInt("FEATURE_MATERIALIZER_SERVICE_HEALTHCHECK_CPU_THRESHOLD_PERCENT", "80"),
			MemFreeThresholdPercentage:                env.WithDefaultInt("FEATURE_MATERIALIZER_SERVICE_HEALTHCHECK_FREE_MEM_THRESHOLD_PERCENT", "20"),
			HealthCheckPort:                           env.WithDefaultInt("FEATURE_MATERIALIZER_SERVICE_HEALTHCHECK_PORT", "5057"),
			DBConnectionString:                        dbConnectionString,
			MessageBrokerConnectionString:             brokers,
			DbLatencyThreshold:                        secondsFromEnv("FEATURE_MATERIALIZER_SERVICE_HEALTHCHECK_DB_LATENCY_THRESHOLD_SECONDS", "5"),
			MessageBrokerLatencyThreshold:             secondsFromEnv("FEATURE_MATERIALIZER_SERVICE_HEALTHCHECK_MSG_BROKER_LATENCY_THRESHOLD_SECONDS", "5"),
			ServiceLatencyThreshold:                   secondsFromEnv("FEATURE_MATERIALIZER_SERVICE_HEALTHCHECK_SERVICE_LATENCY_THRESHOLD_SECONDS", "5"),
			MessageBrokerSubscriberMaxPollSilence:     secondsFromEnv("FEATURE_MATERIALIZER_SERVICE_HEALTHCHECK_MESSAGE_BROKER_SUBSCRIBER_MAX_POLL_SILENCE_SECONDS", "30"),
			MessageBrokerSubscriberMaxProgressSilence: secondsFromEnv("FEATURE_MATERIALIZER_SERVICE_HEALTHCHECK_MESSAGE_BROKER_SUBSCRIBER_MAX_PROGRESS_SILENCE_SECONDS", "90"),
			MessageBrokerSubscriberMaxLag:             int64(env.WithDefaultInt("FEATURE_MATERIALIZER_SERVICE_HEALTHCHECK_MESSAGE_BROKER_SUBSCRIBER_MAX_LAG", "100000")),
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
		MessageBrokerSubscriberMaxPollSilenceSec:     cfg.MessageBrokerSubscriberMaxPollSilence,
		MessageBrokerSubscriberMaxProgressSilenceSec: cfg.MessageBrokerSubscriberMaxProgressSilence,
		MessageBrokerSubscriberMaxLag:                cfg.MessageBrokerSubscriberMaxLag,
	}
}

func secondsFromEnv(key, defaultValue string) time.Duration {
	return time.Duration(env.WithDefaultInt(key, defaultValue)) * time.Second
}

func embeddingStrategyFromConfig(cfg embeddingConfig) model.EmbeddingStrategy {
	log.Trace("embeddingStrategyFromConfig")

	return model.NormalizeEmbeddingStrategy(model.EmbeddingStrategy{
		StrategyVersion:     cfg.StrategyVersion,
		ExtractorName:       cfg.ExtractorName,
		ExtractorVersion:    cfg.ExtractorVersion,
		CleanerName:         cfg.CleanerName,
		CleanerVersion:      cfg.CleanerVersion,
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
		return materialization.NewHTTPEmbeddingProvider(provider, cfg.URL, cfg.Model, cfg.Dimensions, cfg.RequestTimeout), nil
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
