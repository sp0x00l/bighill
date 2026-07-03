package main

import (
	"context"
	usecase "data_ingestion_service/pkg/app"
	ingestionmessaging "data_ingestion_service/pkg/infra/network/messaging"
	"data_ingestion_service/pkg/infra/network/rest"
	coreRest "data_ingestion_service/pkg/infra/network/restsupport"
	"data_ingestion_service/pkg/infra/repo/bucket"
	"data_ingestion_service/pkg/infra/repo/db"
	"errors"
	"fmt"
	authProvider "lib/shared_lib/auth"
	coreBucket "lib/shared_lib/bucket"
	coreDB "lib/shared_lib/db"
	env "lib/shared_lib/env"
	coreHealthCheck "lib/shared_lib/healthcheck"
	kms "lib/shared_lib/key_management"
	logs "lib/shared_lib/logs"
	messagingConn "lib/shared_lib/messaging"
	trace "lib/shared_lib/trace"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/redis/rueidis"
	log "github.com/sirupsen/logrus"
)

var Version string

type ingestionConfig struct {
	ServiceName                   string
	HTTPPort                      int
	DirectUploadMaxFileSizeBytes  int64
	UploadSessionMaxFileSizeBytes int64
	UploadSessionTTL              time.Duration
	UploadValidationReadMaxBytes  int64
	BucketName                    string
	BucketRegion                  string
	BucketUploadPartSize          int64
	DBName                        string
	DBConnectionString            string
	Redis                         rueidis.ClientOption
	Messaging                     messagingConn.MessengerConfig
	OutboxBackend                 string
	OutboxRelay                   messagingConn.OutboxRelayConfig
	DatasetUploadedTopic          string
	DataRegistryTopic             string
	Health                        healthConfig
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
	cfg := readIngestionConfig()
	if err := validateIngestionConfig(cfg); err != nil {
		log.WithError(err).Fatal("invalid data ingestion configuration")
	}
	serviceName := cfg.ServiceName

	log.Trace(fmt.Sprintf("starting the %s service", serviceName))
	traceShutdown := trace.Init(cancelCtx, serviceName, Version)

	coreDb, err := coreDB.InitDatabase(ctx, cfg.DBName, cfg.DBConnectionString, log.StandardLogger())
	if err != nil {
		log.Errorf("database init failed: %v", err)
		os.Exit(1)
	}

	outboxWriter, err := newPostgresOutbox(coreDb, cfg.OutboxBackend)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create postgres outbox")
	}
	orderedOutbox, ok := outboxWriter.(messagingConn.OrderedOutbox)
	if !ok {
		log.Fatal("postgres outbox does not support transactional enqueue operations")
	}
	outboxSignal := make(chan struct{}, 1)
	outboxWriter = messagingConn.NewSignaledOutbox(outboxWriter, outboxSignal)
	cfg.OutboxRelay.Signal = outboxSignal
	publisher, err := messagingConn.NewPublisher(cfg.Messaging.Brokers, messagingConn.WithOutbox(outboxWriter))
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create the publisher")
	}
	relayOutbox, ok := outboxWriter.(messagingConn.RelayOutbox)
	if !ok {
		log.Fatal("postgres outbox does not support relay operations")
	}
	relayPublisher, ok := publisher.(messagingConn.RelayPublisher)
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
		publisher.Close()
	}()

	subscriberFactories := []messagingConn.Messenger{}
	defer func() {
		for _, factory := range subscriberFactories {
			_ = factory.Close(cancelCtx)
		}
	}()

	redisClient, err := rueidis.NewClient(cfg.Redis)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("failed to initialize redis client")
	}
	defer redisClient.Close()
	authStore := authProvider.NewRevocationStore(redisClient, authProvider.WithKeyPrefix("auth:"))

	kmsClient, err := kms.NewKMSClient(cancelCtx)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create KMS client")
	}
	authProv, err := authProvider.NewAuthProvider(cancelCtx, kmsClient)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create auth provider")
	}

	uploader := coreBucket.NewBucket(ctx, cfg.BucketRegion, cfg.BucketUploadPartSize)
	uploadBucket := bucket.NewDataBucket(cfg.BucketName, uploader)
	datasetDB := db.NewDatasetDB(coreDb)
	datasetUseCase := usecase.NewDatasetUseCase(datasetDB)
	formatDetector := rest.NewDetector(
		map[string]rest.FormatValidatorFunc{
			rest.FileTypeCSV:      rest.IsCSV,
			rest.FileTypeJSON:     rest.IsJSON,
			rest.FileTypeParquet:  rest.IsParquet,
			rest.FileTypePDF:      rest.IsPDF,
			rest.FileTypeHTML:     rest.IsHTML,
			rest.FileTypeMarkdown: rest.IsMarkdown,
			rest.FileTypeText:     rest.IsText,
		},
	)
	uploadSessionDB := db.NewUploadSessionDB(
		coreDb,
		db.WithUploadSessionOutbox(orderedOutbox, cfg.DatasetUploadedTopic),
		db.WithUploadSessionOutboxSignal(func() { messagingConn.NotifyOutboxSignal(outboxSignal) }),
	)
	uploadUseCase := usecase.NewDataUploadUseCase(
		uploadBucket,
		usecase.WithUploadSessionRepository(uploadSessionDB),
		usecase.WithUploadDatasetRepository(datasetDB),
		usecase.WithUploadFileDetector(formatDetector),
		usecase.WithUploadPolicy(cfg.UploadSessionMaxFileSizeBytes, cfg.UploadSessionTTL, cfg.UploadValidationReadMaxBytes),
	)

	authHandler := rest.NewAuthHandler(authProv, authStore)
	routes := rest.NewDataUploadHandlers(uploadUseCase, datasetUseCase, formatDetector, authHandler, cfg.DirectUploadMaxFileSizeBytes).GetRoutes()

	log.Infof("%s API HTTP port: %d", serviceName, cfg.HTTPPort)

	restService := coreRest.NewService(routes, cfg.HTTPPort, serviceName)
	healthCheck := coreHealthCheck.NewMonitor(newHealthCheckConfig(cfg.Health))
	healthCheck = healthCheck.WithCpuCheck().WithMemoryCheck().WithDatabaseCheck().WithMessageBrokerCheck()

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

	go func() {
		if err := restService.Connect(); err != nil {
			if err != http.ErrServerClosed {
				log.Fatalf("unable to start the %s rest service: %v", serviceName, err)
			}
			quit <- syscall.SIGTERM
		}
	}()

	startSubscriber("dataset-created", []string{cfg.DataRegistryTopic}, func(subscriber messagingConn.Subscriber) {
		messagingConn.AddListener(subscriber, ingestionmessaging.NewDatasetCreatedEventListener(datasetUseCase))
	})
	startSubscriber("dataset-updated", []string{cfg.DataRegistryTopic}, func(subscriber messagingConn.Subscriber) {
		messagingConn.AddListener(subscriber, ingestionmessaging.NewDatasetUpdatedEventListener(datasetUseCase))
	})
	startSubscriber("dataset-deleted", []string{cfg.DataRegistryTopic}, func(subscriber messagingConn.Subscriber) {
		messagingConn.AddListener(subscriber, ingestionmessaging.NewDatasetDeletedEventListener(datasetUseCase))
	})

	go func() {
		if err := healthCheck.Connect(ctx); err != nil {
			if err != http.ErrServerClosed {
				log.Fatalf("unable to start health check for the %s service: %v", serviceName, err)
			}
			quit <- syscall.SIGTERM
		}
	}()

	<-quit

	datasetDB.Close()
	restService.Close()
	healthCheck.Close()

	cancelFtn()
	traceShutdown()
	log.Trace(fmt.Sprintf("stopped the %s service", serviceName))
}

func readIngestionConfig() ingestionConfig {
	brokers := env.WithDefaultString("KAFKA_BROKER", "localhost:9092")
	dbName := env.WithDefaultString("DATA_INGESTION_SERVICE_DB_NAME", "bighill_data_ingestion_db")
	dbConnectionString := postgresConnectionString(
		env.WithDefaultString("DATA_INGESTION_SERVICE_DB_USER", "bighill_data_ingestion_db_user"),
		env.WithDefaultString("DATA_INGESTION_SERVICE_DB_PASSWORD", ""),
		env.WithDefaultString("PGHOST", "127.0.0.1"),
		env.WithDefaultString("PGPORT", "5432"),
		dbName,
		env.WithDefaultString("PGSSLMODE", "disable"),
		env.WithDefaultInt("DATA_INGESTION_SERVICE_DB_MAX_CONNECTIONS", "20"),
	)
	maxFileSizeMB := env.WithDefaultInt64("DATA_INGESTION_SERVICE_FILE_MAX_SIZE_MB", "2000")
	directMaxFileSizeMB := env.WithDefaultInt64("DATA_INGESTION_SERVICE_DIRECT_UPLOAD_MAX_SIZE_MB", "5")
	validationReadMaxMB := env.WithDefaultInt64("DATA_INGESTION_SERVICE_UPLOAD_VALIDATION_READ_MAX_SIZE_MB", "5")
	uploadPartSizeMB := env.WithDefaultInt64("DATA_INGESTION_SERVICE_FILES_UPLOAD_PART_SIZE_MB", "10")
	return ingestionConfig{
		ServiceName:                   env.WithDefaultString("DATA_INGESTION_SERVICE_NAME", "data-ingestion-service"),
		HTTPPort:                      env.WithDefaultInt("DATA_INGESTION_SERVICE_API_HTTP_PORT", "8086"),
		DirectUploadMaxFileSizeBytes:  directMaxFileSizeMB * 1000 * 1000,
		UploadSessionMaxFileSizeBytes: maxFileSizeMB * 1000 * 1000,
		UploadSessionTTL:              time.Duration(env.WithDefaultInt("DATA_INGESTION_SERVICE_UPLOAD_SESSION_TTL_SECONDS", "900")) * time.Second,
		UploadValidationReadMaxBytes:  validationReadMaxMB * 1000 * 1000,
		BucketName:                    env.WithDefaultString("DATA_INGESTION_SERVICE_FILES_BUCKET_NAME", "local-dev-bucket"),
		BucketRegion:                  env.WithDefaultString("DATA_INGESTION_SERVICE_FILES_BUCKET_REGION", "local-dev"),
		BucketUploadPartSize:          uploadPartSizeMB * 1024 * 1024,
		DBName:                        dbName,
		DBConnectionString:            dbConnectionString,
		Redis: rueidis.ClientOption{
			InitAddress: []string{env.WithDefaultString("DATA_INGESTION_SERVICE_REDIS_ADDRESS", "localhost:6379")},
			Username:    env.WithDefaultString("DATA_INGESTION_SERVICE_REDIS_USERNAME", ""),
			Password:    env.WithDefaultString("DATA_INGESTION_SERVICE_REDIS_PASSWORD", ""),
		},
		Messaging: messagingConn.MessengerConfig{
			DlqURL:          env.WithDefaultString("DATA_INGESTION_SERVICE_DLQ", "http://localhost:4566/data-ingestion-dev-env-queue/"),
			GroupID:         env.WithDefaultString("DATA_INGESTION_SERVICE_KAFKA_BASE_GROUP_ID", "data-ingestion"),
			Brokers:         brokers,
			AutoOffsetReset: env.WithDefaultString("DATA_INGESTION_SERVICE_KAFKA_AUTO_OFFSET_RESET", "earliest"),
		},
		OutboxBackend: env.WithDefaultString("DATA_INGESTION_SERVICE_OUTBOX", "postgres"),
		OutboxRelay: messagingConn.OutboxRelayConfig{
			PollInterval:   time.Duration(env.WithDefaultInt("DATA_INGESTION_SERVICE_OUTBOX_RELAY_POLL_MS", "250")) * time.Millisecond,
			FailureBackoff: time.Duration(env.WithDefaultInt("DATA_INGESTION_SERVICE_OUTBOX_RELAY_FAILURE_BACKOFF_MS", "2000")) * time.Millisecond,
			BatchSize:      int32(env.WithDefaultInt("DATA_INGESTION_SERVICE_OUTBOX_RELAY_BATCH_SIZE", "100")),
		},
		DatasetUploadedTopic: env.WithDefaultString("DATA_INGESTION_SERVICE_TOPIC", "data_ingestion"),
		DataRegistryTopic:    env.WithDefaultString("DATA_INGESTION_SERVICE_DATA_REGISTRY_SUBSCRIBER_TOPIC", "data_registry"),
		Health: healthConfig{
			CpuThresholdPercentage:                    env.WithDefaultInt("DATA_INGESTION_SERVICE_HEALTHCHECK_CPU_THRESHOLD_PERCENT", "80"),
			MemFreeThresholdPercentage:                env.WithDefaultInt("DATA_INGESTION_SERVICE_HEALTHCHECK_FREE_MEM_THRESHOLD_PERCENT", "20"),
			HealthCheckPort:                           env.WithDefaultInt("DATA_INGESTION_SERVICE_HEALTHCHECK_PORT", "5056"),
			DBConnectionString:                        dbConnectionString,
			MessageBrokerConnectionString:             brokers,
			DbLatencyThreshold:                        secondsFromEnv("DATA_INGESTION_SERVICE_HEALTHCHECK_DB_LATENCY_THRESHOLD_SECONDS", "5"),
			MessageBrokerLatencyThreshold:             secondsFromEnv("DATA_INGESTION_SERVICE_HEALTHCHECK_MSG_BROKER_LATENCY_THRESHOLD_SECONDS", "5"),
			ServiceLatencyThreshold:                   secondsFromEnv("DATA_INGESTION_SERVICE_HEALTHCHECK_SERVICE_LATENCY_THRESHOLD_SECONDS", "5"),
			MessageBrokerSubscriberMaxPollSilence:     secondsFromEnv("DATA_INGESTION_SERVICE_HEALTHCHECK_MESSAGE_BROKER_SUBSCRIBER_MAX_POLL_SILENCE_SECONDS", "30"),
			MessageBrokerSubscriberMaxProgressSilence: secondsFromEnv("DATA_INGESTION_SERVICE_HEALTHCHECK_MESSAGE_BROKER_SUBSCRIBER_MAX_PROGRESS_SILENCE_SECONDS", "90"),
			MessageBrokerSubscriberMaxLag:             int64(env.WithDefaultInt("DATA_INGESTION_SERVICE_HEALTHCHECK_MESSAGE_BROKER_SUBSCRIBER_MAX_LAG", "100000")),
		},
	}
}

func validateIngestionConfig(cfg ingestionConfig) error {
	log.Trace("validateIngestionConfig")

	if cfg.DirectUploadMaxFileSizeBytes <= 0 {
		return fmt.Errorf("DATA_INGESTION_SERVICE_DIRECT_UPLOAD_MAX_SIZE_MB must be greater than zero")
	}
	if cfg.UploadSessionMaxFileSizeBytes <= 0 {
		return fmt.Errorf("DATA_INGESTION_SERVICE_FILE_MAX_SIZE_MB must be greater than zero")
	}
	if cfg.UploadValidationReadMaxBytes <= 0 {
		return fmt.Errorf("DATA_INGESTION_SERVICE_UPLOAD_VALIDATION_READ_MAX_SIZE_MB must be greater than zero")
	}
	if cfg.BucketUploadPartSize <= 0 {
		return fmt.Errorf("DATA_INGESTION_SERVICE_FILES_UPLOAD_PART_SIZE_MB must be greater than zero")
	}
	if cfg.UploadSessionTTL <= 0 {
		return fmt.Errorf("DATA_INGESTION_SERVICE_UPLOAD_SESSION_TTL_SECONDS must be greater than zero")
	}
	if cfg.UploadSessionMaxFileSizeBytes < cfg.DirectUploadMaxFileSizeBytes {
		return fmt.Errorf("DATA_INGESTION_SERVICE_FILE_MAX_SIZE_MB must be greater than or equal to DATA_INGESTION_SERVICE_DIRECT_UPLOAD_MAX_SIZE_MB")
	}
	return nil
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

func postgresConnectionString(user, password, host, port, dbName, sslMode string, maxConnections int) string {
	u := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(user, password),
		Host:   net.JoinHostPort(host, port),
		Path:   dbName,
	}
	q := u.Query()
	q.Set("sslmode", sslMode)
	q.Set("pool_max_conns", strconv.Itoa(maxConnections))
	u.RawQuery = q.Encode()
	return u.String()
}

func secondsFromEnv(key, defaultValue string) time.Duration {
	return time.Duration(env.WithDefaultInt(key, defaultValue)) * time.Second
}

func newPostgresOutbox(database *coreDB.Database, backend string) (messagingConn.OutboxWriter, error) {
	log.Trace("newPostgresOutbox")

	if backend != "postgres" {
		return nil, fmt.Errorf("unsupported outbox backend %q", backend)
	}
	return messagingConn.NewPostgresOutbox(database.Pool, database.Name, "")
}
