package main

import (
	"context"
	"errors"
	"fmt"
	usecase "ingestion_service/pkg/app"
	"ingestion_service/pkg/infra/download"
	ingestionadapter "ingestion_service/pkg/infra/network/adapter"
	ingestionmessaging "ingestion_service/pkg/infra/network/messaging"
	"ingestion_service/pkg/infra/network/rest"
	"ingestion_service/pkg/infra/repo/bucket"
	"ingestion_service/pkg/infra/repo/db"
	authProvider "lib/shared_lib/auth"
	coreBucket "lib/shared_lib/bucket"
	coreDB "lib/shared_lib/db"
	env "lib/shared_lib/env"
	coreHealthCheck "lib/shared_lib/healthcheck"
	kms "lib/shared_lib/key_management"
	"lib/shared_lib/lifecycle"
	logs "lib/shared_lib/logs"
	messagingConn "lib/shared_lib/messaging"
	"lib/shared_lib/secret"
	serializers "lib/shared_lib/serializer"
	sharedTenant "lib/shared_lib/tenant"
	trace "lib/shared_lib/trace"
	shareduow "lib/shared_lib/uow"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/redis/rueidis"
	log "github.com/sirupsen/logrus"
)

var Version string

type ingestionConfig struct {
	ServiceName                   string
	HTTPPort                      int
	HTTPReadTimeout               time.Duration
	HTTPWriteTimeout              time.Duration
	HTTPIdleTimeout               time.Duration
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
	ProfileTopic                  string
	HuggingFaceTokenEncryptionKey string
	HuggingFaceDownloadMode       string
	HuggingFaceDownloadCommand    string
	HuggingFaceDownloadWorkingDir string
	HuggingFaceOutputURI          string
	HuggingFaceDownloadTimeout    time.Duration
	HuggingFaceJobEnvKeys         download.HuggingFaceJobEnvKeys
	HuggingFaceJobNamespace       string
	HuggingFaceJobImage           string
	HuggingFaceJobImagePullPolicy string
	HuggingFaceJobServiceAccount  string
	HuggingFaceJobTTLSeconds      int
	HuggingFaceJobBackoffLimit    int
	HuggingFaceJobCPU             string
	HuggingFaceJobMemory          string
	HuggingFaceJobPollInterval    time.Duration
	Health                        healthConfig
	Lifecycle                     lifecycle.Config
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
		log.WithError(err).Fatal("invalid ingestion configuration")
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
	uploadSessionDB := db.NewUploadSessionDB(coreDb)
	uploadSessionUOW := shareduow.New(coreDb.Pool,
		shareduow.WithTransactionalOutbox(orderedOutbox),
		shareduow.WithOutboxSignal(func() { messagingConn.NotifyOutboxSignal(outboxSignal) }),
	)
	tenantDB := sharedTenant.NewPostgresProjectionStore(coreDb)
	huggingFaceTokenCodec, err := secret.NewAESGCMCodec(cfg.HuggingFaceTokenEncryptionKey)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create Hugging Face token codec")
	}
	var modelDownloader usecase.ModelArtifactDownloader
	switch strings.ToLower(strings.TrimSpace(cfg.HuggingFaceDownloadMode)) {
	case "command":
		modelDownloader, err = download.NewHuggingFaceCommandDownloader(download.HuggingFaceCommandDownloaderConfig{
			Command:          cfg.HuggingFaceDownloadCommand,
			WorkingDirectory: cfg.HuggingFaceDownloadWorkingDir,
			OutputURI:        cfg.HuggingFaceOutputURI,
			Timeout:          cfg.HuggingFaceDownloadTimeout,
			EnvKeys:          cfg.HuggingFaceJobEnvKeys,
		})
		if err != nil {
			log.WithContext(cancelCtx).WithError(err).Fatal("invalid Hugging Face downloader configuration")
		}
	case "kubernetes":
		modelDownloader, err = download.NewHuggingFaceKubernetesJobDownloader(download.HuggingFaceKubernetesJobDownloaderConfig{
			Namespace:               cfg.HuggingFaceJobNamespace,
			Image:                   cfg.HuggingFaceJobImage,
			ImagePullPolicy:         cfg.HuggingFaceJobImagePullPolicy,
			ServiceAccountName:      cfg.HuggingFaceJobServiceAccount,
			Command:                 cfg.HuggingFaceDownloadCommand,
			OutputURI:               cfg.HuggingFaceOutputURI,
			TTLSecondsAfterFinished: cfg.HuggingFaceJobTTLSeconds,
			BackoffLimit:            cfg.HuggingFaceJobBackoffLimit,
			CPU:                     cfg.HuggingFaceJobCPU,
			Memory:                  cfg.HuggingFaceJobMemory,
			PollInterval:            cfg.HuggingFaceJobPollInterval,
			Timeout:                 cfg.HuggingFaceDownloadTimeout,
			EnvKeys:                 cfg.HuggingFaceJobEnvKeys,
		}, download.NewObjectModelManifestReader(uploader))
		if err != nil {
			log.WithContext(cancelCtx).WithError(err).Fatal("invalid Hugging Face Kubernetes downloader configuration")
		}
	default:
		log.WithContext(cancelCtx).Fatalf("invalid Hugging Face downloader mode %q", cfg.HuggingFaceDownloadMode)
	}
	uploadEventBuilder := ingestionmessaging.NewUploadEventBuilder(cfg.DatasetUploadedTopic)
	uploadUseCase := usecase.NewDataUploadUseCase(
		uploadBucket,
		usecase.WithUploadSessionRepository(uploadSessionDB),
		usecase.WithUploadSessionUnitOfWork(uploadSessionUOW, uploadEventBuilder),
		usecase.WithUploadDatasetRepository(datasetDB),
		usecase.WithUploadTenantsRepository(tenantDB),
		usecase.WithHuggingFaceTokenDecryptor(huggingFaceTokenCodec),
		usecase.WithUploadFileDetector(formatDetector),
		usecase.WithModelArtifactDownloader(modelDownloader),
		usecase.WithUploadPolicy(cfg.UploadSessionMaxFileSizeBytes, cfg.UploadSessionTTL, cfg.UploadValidationReadMaxBytes),
	)

	authHandler := rest.NewAuthHandler(authProv, authStore)
	uploadDTOAdapter := ingestionadapter.NewUploadDTOAdapter(serializers.NewJSONSerializer())
	routes := rest.NewDataUploadHandlers(uploadUseCase, datasetUseCase, uploadDTOAdapter, formatDetector, authHandler, cfg.DirectUploadMaxFileSizeBytes).GetRoutes()

	log.Infof("%s API HTTP port: %d", serviceName, cfg.HTTPPort)

	restService := rest.NewService(
		routes,
		cfg.HTTPPort,
		serviceName,
		rest.WithHTTPTimeouts(cfg.HTTPReadTimeout, cfg.HTTPWriteTimeout, cfg.HTTPIdleTimeout),
	)
	healthCheck := coreHealthCheck.NewMonitor(newHealthCheckConfig(cfg.Health))
	healthCheck = healthCheck.WithCpuCheck().WithMemoryCheck().WithDatabaseCheck().WithMessageBrokerCheck()

	components := []lifecycle.Component{
		lifecycle.CloserComponent("ingestion-observability", func() error {
			traceShutdown()
			return nil
		}),
		lifecycle.CloserComponent("ingestion-database", func() error {
			datasetDB.Close()
			return nil
		}),
		lifecycle.CloserComponent("ingestion-publisher", func() error {
			publisher.Close()
			return nil
		}),
		lifecycle.CloserComponent("ingestion-redis", func() error {
			redisClient.Close()
			return nil
		}),
		lifecycle.HealthCheckComponent("ingestion-healthcheck", healthCheck),
		lifecycle.ServerComponent("ingestion-http", restService),
		lifecycle.WorkerComponent("ingestion-outbox-relay", func(ctx context.Context) error {
			if err := outboxRelay.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		}),
	}

	startSubscriber := func(name string, topics []string, configure func(messagingConn.Subscriber)) {
		var factory messagingConn.Messenger
		components = append(components, lifecycle.NewFuncComponent(lifecycle.ComponentConfig{
			Name: "ingestion-subscriber-" + name,
			Start: func(ctx context.Context) error {
				startedFactory, monitor, err := messagingConn.StartStreamSubscriber(ctx, messagingConn.StreamSubscriberConfig{
					Brokers:          cfg.Messaging.Brokers,
					DLQURL:           cfg.Messaging.DlqURL,
					BaseGroupID:      cfg.Messaging.GroupID,
					AutoOffsetReset:  cfg.Messaging.AutoOffsetReset,
					Cancel:           cancelFtn,
					Monitor:          healthCheck,
					OnUnexpectedStop: cancelFtn,
				}, name, topics, configure)
				if err != nil {
					return err
				}
				factory = startedFactory
				healthCheck = monitor
				<-ctx.Done()
				return ctx.Err()
			},
			Close: func() error {
				if factory == nil {
					return nil
				}
				return factory.Close(cancelCtx)
			},
		}))
	}

	startSubscriber("dataset-created", []string{cfg.DataRegistryTopic}, func(subscriber messagingConn.Subscriber) {
		messagingConn.AddListener(subscriber, ingestionmessaging.NewDatasetCreatedEventListener(datasetUseCase))
	})
	startSubscriber("dataset-updated", []string{cfg.DataRegistryTopic}, func(subscriber messagingConn.Subscriber) {
		messagingConn.AddListener(subscriber, ingestionmessaging.NewDatasetUpdatedEventListener(datasetUseCase))
	})
	startSubscriber("dataset-deleted", []string{cfg.DataRegistryTopic}, func(subscriber messagingConn.Subscriber) {
		messagingConn.AddListener(subscriber, ingestionmessaging.NewDatasetDeletedEventListener(datasetUseCase))
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

	supervisor := lifecycle.NewSupervisorWithConfig(cfg.Lifecycle, components...)
	if err := supervisor.RunWithSignals(cancelCtx, syscall.SIGINT, syscall.SIGTERM); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.WithContext(cancelCtx).WithError(err).Errorf("%s service stopped with error", serviceName)
	}
	cancelFtn()
	log.Trace(fmt.Sprintf("stopped the %s service", serviceName))
}

func readIngestionConfig() ingestionConfig {
	env.RequireServiceEnvironment()

	brokers := env.WithDefaultString("KAFKA_BROKER", "localhost:9092")
	dbName := env.WithDefaultString("INGESTION_SERVICE_DB_NAME", "bighill_ingestion_db")
	dbConnectionString := postgresConnectionString(
		env.WithDefaultString("INGESTION_SERVICE_DB_USER", "bighill_ingestion_db_user"),
		env.WithDefaultString("INGESTION_SERVICE_DB_PASSWORD", ""),
		env.WithDefaultString("PGHOST", "127.0.0.1"),
		env.WithDefaultString("PGPORT", "5432"),
		dbName,
		env.WithDefaultString("PGSSLMODE", "disable"),
		env.WithDefaultInt("INGESTION_SERVICE_DB_MAX_CONNECTIONS", "20"),
	)
	maxFileSizeMB := env.WithDefaultInt64("INGESTION_SERVICE_FILE_MAX_SIZE_MB", "2000")
	directMaxFileSizeMB := env.WithDefaultInt64("INGESTION_SERVICE_DIRECT_UPLOAD_MAX_SIZE_MB", "5")
	validationReadMaxMB := env.WithDefaultInt64("INGESTION_SERVICE_UPLOAD_VALIDATION_READ_MAX_SIZE_MB", "5")
	uploadPartSizeMB := env.WithDefaultInt64("INGESTION_SERVICE_FILES_UPLOAD_PART_SIZE_MB", "10")
	defaultBucketName := ""
	defaultHuggingFaceOutputURI := ""
	defaultBucketRegion := "eu-west-1"
	if env.IsDevEnv() {
		defaultBucketName = "local-dev-bucket"
		defaultHuggingFaceOutputURI = "s3://local-dev-bucket/models/huggingface"
		defaultBucketRegion = coreBucket.LocalDevS3Region
	}
	return ingestionConfig{
		ServiceName:                   env.WithDefaultString("INGESTION_SERVICE_NAME", "ingestion-service"),
		HTTPPort:                      env.WithDefaultInt("INGESTION_SERVICE_API_HTTP_PORT", "8086"),
		HTTPReadTimeout:               secondsFromEnv("INGESTION_SERVICE_HTTP_READ_TIMEOUT_SECONDS", "30"),
		HTTPWriteTimeout:              secondsFromEnv("INGESTION_SERVICE_HTTP_WRITE_TIMEOUT_SECONDS", "1200"),
		HTTPIdleTimeout:               secondsFromEnv("INGESTION_SERVICE_HTTP_IDLE_TIMEOUT_SECONDS", "120"),
		DirectUploadMaxFileSizeBytes:  directMaxFileSizeMB * 1000 * 1000,
		UploadSessionMaxFileSizeBytes: maxFileSizeMB * 1000 * 1000,
		UploadSessionTTL:              time.Duration(env.WithDefaultInt("INGESTION_SERVICE_UPLOAD_SESSION_TTL_SECONDS", "900")) * time.Second,
		UploadValidationReadMaxBytes:  validationReadMaxMB * 1000 * 1000,
		BucketName:                    env.WithDefaultString("INGESTION_SERVICE_FILES_BUCKET_NAME", defaultBucketName),
		BucketRegion:                  env.WithDefaultString("INGESTION_SERVICE_FILES_BUCKET_REGION", defaultBucketRegion),
		BucketUploadPartSize:          uploadPartSizeMB * 1024 * 1024,
		DBName:                        dbName,
		DBConnectionString:            dbConnectionString,
		Redis: rueidis.ClientOption{
			InitAddress: []string{env.WithDefaultString("INGESTION_SERVICE_REDIS_ADDRESS", "localhost:6379")},
			Username:    env.WithDefaultString("INGESTION_SERVICE_REDIS_USERNAME", ""),
			Password:    env.WithDefaultString("INGESTION_SERVICE_REDIS_PASSWORD", ""),
		},
		Messaging: messagingConn.MessengerConfig{
			DlqURL:          env.WithDefaultString("INGESTION_SERVICE_DLQ", "http://localhost:4566/ingestion-dev-env-queue/"),
			GroupID:         env.WithDefaultString("INGESTION_SERVICE_KAFKA_BASE_GROUP_ID", "ingestion"),
			Brokers:         brokers,
			AutoOffsetReset: env.WithDefaultString("INGESTION_SERVICE_KAFKA_AUTO_OFFSET_RESET", "earliest"),
		},
		OutboxBackend: env.WithDefaultString("INGESTION_SERVICE_OUTBOX", "postgres"),
		OutboxRelay: messagingConn.OutboxRelayConfig{
			PollInterval:   time.Duration(env.WithDefaultInt("INGESTION_SERVICE_OUTBOX_RELAY_POLL_MS", "250")) * time.Millisecond,
			FailureBackoff: time.Duration(env.WithDefaultInt("INGESTION_SERVICE_OUTBOX_RELAY_FAILURE_BACKOFF_MS", "2000")) * time.Millisecond,
			BatchSize:      int32(env.WithDefaultInt("INGESTION_SERVICE_OUTBOX_RELAY_BATCH_SIZE", "100")),
		},
		DatasetUploadedTopic:          env.WithDefaultString("INGESTION_SERVICE_TOPIC", "ingestion"),
		DataRegistryTopic:             env.WithDefaultString("INGESTION_SERVICE_DATA_REGISTRY_SUBSCRIBER_TOPIC", "data_registry"),
		ProfileTopic:                  env.WithDefaultString("INGESTION_SERVICE_PROFILE_SUBSCRIBER_TOPIC", "profile"),
		HuggingFaceTokenEncryptionKey: env.MustString("INGESTION_SERVICE_HUGGINGFACE_TOKEN_ENCRYPTION_KEY"),
		HuggingFaceDownloadMode: env.WithDefaultString(
			"INGESTION_SERVICE_HUGGINGFACE_DOWNLOAD_MODE",
			"command",
		),
		HuggingFaceDownloadCommand: env.WithDefaultString(
			"INGESTION_SERVICE_HUGGINGFACE_DOWNLOAD_COMMAND",
			"python -m training_jobs.model_onboard",
		),
		HuggingFaceDownloadWorkingDir: env.WithDefaultString("INGESTION_SERVICE_HUGGINGFACE_DOWNLOAD_WORKING_DIRECTORY", ""),
		HuggingFaceOutputURI:          env.WithDefaultString("INGESTION_SERVICE_HUGGINGFACE_OUTPUT_URI", defaultHuggingFaceOutputURI),
		HuggingFaceDownloadTimeout:    time.Duration(env.WithDefaultInt("INGESTION_SERVICE_HUGGINGFACE_DOWNLOAD_TIMEOUT_SECONDS", "1200")) * time.Second,
		HuggingFaceJobEnvKeys:         huggingFaceJobEnvKeysFromEnv(),
		HuggingFaceJobNamespace:       env.WithDefaultString("INGESTION_SERVICE_HUGGINGFACE_JOB_NAMESPACE", "default"),
		HuggingFaceJobImage:           env.WithDefaultString("INGESTION_SERVICE_HUGGINGFACE_JOB_IMAGE", "training-jobs:0.0.1"),
		HuggingFaceJobImagePullPolicy: env.WithDefaultString("INGESTION_SERVICE_HUGGINGFACE_JOB_IMAGE_PULL_POLICY", "IfNotPresent"),
		HuggingFaceJobServiceAccount:  env.WithDefaultString("INGESTION_SERVICE_HUGGINGFACE_JOB_SERVICE_ACCOUNT", "ingestion-service"),
		HuggingFaceJobTTLSeconds:      env.WithDefaultInt("INGESTION_SERVICE_HUGGINGFACE_JOB_TTL_SECONDS_AFTER_FINISHED", "3600"),
		HuggingFaceJobBackoffLimit:    env.WithDefaultInt("INGESTION_SERVICE_HUGGINGFACE_JOB_BACKOFF_LIMIT", "0"),
		HuggingFaceJobCPU:             env.WithDefaultString("INGESTION_SERVICE_HUGGINGFACE_JOB_CPU", "1"),
		HuggingFaceJobMemory:          env.WithDefaultString("INGESTION_SERVICE_HUGGINGFACE_JOB_MEMORY", "4Gi"),
		HuggingFaceJobPollInterval:    time.Duration(env.WithDefaultInt("INGESTION_SERVICE_HUGGINGFACE_JOB_POLL_INTERVAL_SECONDS", "10")) * time.Second,
		Health: healthConfig{
			CpuThresholdPercentage:                    env.WithDefaultInt("INGESTION_SERVICE_HEALTHCHECK_CPU_THRESHOLD_PERCENT", "80"),
			MemFreeThresholdPercentage:                env.WithDefaultInt("INGESTION_SERVICE_HEALTHCHECK_FREE_MEM_THRESHOLD_PERCENT", "20"),
			HealthCheckPort:                           env.WithDefaultInt("INGESTION_SERVICE_HEALTHCHECK_PORT", "5056"),
			DBConnectionString:                        dbConnectionString,
			MessageBrokerConnectionString:             brokers,
			DbLatencyThreshold:                        secondsFromEnv("INGESTION_SERVICE_HEALTHCHECK_DB_LATENCY_THRESHOLD_SECONDS", "5"),
			MessageBrokerLatencyThreshold:             secondsFromEnv("INGESTION_SERVICE_HEALTHCHECK_MSG_BROKER_LATENCY_THRESHOLD_SECONDS", "5"),
			ServiceLatencyThreshold:                   secondsFromEnv("INGESTION_SERVICE_HEALTHCHECK_SERVICE_LATENCY_THRESHOLD_SECONDS", "5"),
			MessageBrokerSubscriberMaxPollSilence:     secondsFromEnv("INGESTION_SERVICE_HEALTHCHECK_MESSAGE_BROKER_SUBSCRIBER_MAX_POLL_SILENCE_SECONDS", "30"),
			MessageBrokerSubscriberMaxProgressSilence: secondsFromEnv("INGESTION_SERVICE_HEALTHCHECK_MESSAGE_BROKER_SUBSCRIBER_MAX_PROGRESS_SILENCE_SECONDS", "90"),
			MessageBrokerSubscriberMaxLag:             int64(env.WithDefaultInt("INGESTION_SERVICE_HEALTHCHECK_MESSAGE_BROKER_SUBSCRIBER_MAX_LAG", "100000")),
		},
		Lifecycle: lifecycle.Config{
			ReadinessTimeout: secondsFromEnv("INGESTION_SERVICE_LIFECYCLE_READINESS_TIMEOUT_SECONDS", "30"),
			DrainTimeout:     secondsFromEnv("INGESTION_SERVICE_LIFECYCLE_DRAIN_TIMEOUT_SECONDS", "30"),
			CloseTimeout:     secondsFromEnv("INGESTION_SERVICE_LIFECYCLE_CLOSE_TIMEOUT_SECONDS", "10"),
		},
	}
}

func validateIngestionConfig(cfg ingestionConfig) error {
	log.Trace("validateIngestionConfig")

	if cfg.DirectUploadMaxFileSizeBytes <= 0 {
		return fmt.Errorf("INGESTION_SERVICE_DIRECT_UPLOAD_MAX_SIZE_MB must be greater than zero")
	}
	if cfg.HTTPReadTimeout <= 0 {
		return fmt.Errorf("INGESTION_SERVICE_HTTP_READ_TIMEOUT_SECONDS must be greater than zero")
	}
	if cfg.HTTPWriteTimeout <= 0 {
		return fmt.Errorf("INGESTION_SERVICE_HTTP_WRITE_TIMEOUT_SECONDS must be greater than zero")
	}
	if cfg.HTTPIdleTimeout <= 0 {
		return fmt.Errorf("INGESTION_SERVICE_HTTP_IDLE_TIMEOUT_SECONDS must be greater than zero")
	}
	if cfg.UploadSessionMaxFileSizeBytes <= 0 {
		return fmt.Errorf("INGESTION_SERVICE_FILE_MAX_SIZE_MB must be greater than zero")
	}
	if cfg.UploadValidationReadMaxBytes <= 0 {
		return fmt.Errorf("INGESTION_SERVICE_UPLOAD_VALIDATION_READ_MAX_SIZE_MB must be greater than zero")
	}
	if cfg.BucketUploadPartSize <= 0 {
		return fmt.Errorf("INGESTION_SERVICE_FILES_UPLOAD_PART_SIZE_MB must be greater than zero")
	}
	if strings.TrimSpace(cfg.BucketName) == "" {
		return fmt.Errorf("INGESTION_SERVICE_FILES_BUCKET_NAME must be set")
	}
	if !env.IsDevEnv() && strings.TrimSpace(cfg.BucketName) == "local-dev-bucket" {
		return fmt.Errorf("INGESTION_SERVICE_FILES_BUCKET_NAME must not be local-dev-bucket outside dev environments")
	}
	if cfg.UploadSessionTTL <= 0 {
		return fmt.Errorf("INGESTION_SERVICE_UPLOAD_SESSION_TTL_SECONDS must be greater than zero")
	}
	if cfg.UploadSessionMaxFileSizeBytes < cfg.DirectUploadMaxFileSizeBytes {
		return fmt.Errorf("INGESTION_SERVICE_FILE_MAX_SIZE_MB must be greater than or equal to INGESTION_SERVICE_DIRECT_UPLOAD_MAX_SIZE_MB")
	}
	if strings.TrimSpace(cfg.HuggingFaceDownloadCommand) == "" {
		return fmt.Errorf("INGESTION_SERVICE_HUGGINGFACE_DOWNLOAD_COMMAND must be set")
	}
	if strings.TrimSpace(cfg.HuggingFaceOutputURI) == "" {
		return fmt.Errorf("INGESTION_SERVICE_HUGGINGFACE_OUTPUT_URI must be set")
	}
	if !env.IsDevEnv() && strings.Contains(cfg.HuggingFaceOutputURI, "local-dev-bucket") {
		return fmt.Errorf("INGESTION_SERVICE_HUGGINGFACE_OUTPUT_URI must not use local-dev-bucket outside dev environments")
	}
	if cfg.HuggingFaceDownloadTimeout <= 0 {
		return fmt.Errorf("INGESTION_SERVICE_HUGGINGFACE_DOWNLOAD_TIMEOUT_SECONDS must be greater than zero")
	}
	if err := validateHuggingFaceJobEnvKeys(cfg.HuggingFaceJobEnvKeys); err != nil {
		return err
	}
	switch strings.ToLower(strings.TrimSpace(cfg.HuggingFaceDownloadMode)) {
	case "command":
	case "kubernetes":
		if strings.TrimSpace(cfg.HuggingFaceJobNamespace) == "" {
			return fmt.Errorf("INGESTION_SERVICE_HUGGINGFACE_JOB_NAMESPACE must be set when INGESTION_SERVICE_HUGGINGFACE_DOWNLOAD_MODE=kubernetes")
		}
		if strings.TrimSpace(cfg.HuggingFaceJobImage) == "" {
			return fmt.Errorf("INGESTION_SERVICE_HUGGINGFACE_JOB_IMAGE must be set when INGESTION_SERVICE_HUGGINGFACE_DOWNLOAD_MODE=kubernetes")
		}
		if strings.TrimSpace(cfg.HuggingFaceJobImagePullPolicy) == "" {
			return fmt.Errorf("INGESTION_SERVICE_HUGGINGFACE_JOB_IMAGE_PULL_POLICY must be set when INGESTION_SERVICE_HUGGINGFACE_DOWNLOAD_MODE=kubernetes")
		}
		if cfg.HuggingFaceJobPollInterval <= 0 {
			return fmt.Errorf("INGESTION_SERVICE_HUGGINGFACE_JOB_POLL_INTERVAL_SECONDS must be greater than zero")
		}
		if cfg.HuggingFaceJobBackoffLimit < 0 {
			return fmt.Errorf("INGESTION_SERVICE_HUGGINGFACE_JOB_BACKOFF_LIMIT must be greater than or equal to zero")
		}
	default:
		return fmt.Errorf("INGESTION_SERVICE_HUGGINGFACE_DOWNLOAD_MODE must be command or kubernetes")
	}
	return nil
}

func validateHuggingFaceJobEnvKeys(keys download.HuggingFaceJobEnvKeys) error {
	log.Trace("validateHuggingFaceJobEnvKeys")

	required := map[string]string{
		"INGESTION_SERVICE_HUGGINGFACE_JOB_ENV_RESOURCE_ID":     keys.ResourceID,
		"INGESTION_SERVICE_HUGGINGFACE_JOB_ENV_MODEL_NAME":      keys.ModelName,
		"INGESTION_SERVICE_HUGGINGFACE_JOB_ENV_MODEL_VERSION":   keys.ModelVersion,
		"INGESTION_SERVICE_HUGGINGFACE_JOB_ENV_BASE_MODEL":      keys.BaseModel,
		"INGESTION_SERVICE_HUGGINGFACE_JOB_ENV_ARTIFACT_TYPE":   keys.ArtifactType,
		"INGESTION_SERVICE_HUGGINGFACE_JOB_ENV_ARTIFACT_FORMAT": keys.ArtifactFormat,
		"INGESTION_SERVICE_HUGGINGFACE_JOB_ENV_FILE_NAME":       keys.FileName,
		"INGESTION_SERVICE_HUGGINGFACE_JOB_ENV_REPO_ID":         keys.RepoID,
		"INGESTION_SERVICE_HUGGINGFACE_JOB_ENV_REVISION":        keys.Revision,
		"INGESTION_SERVICE_HUGGINGFACE_JOB_ENV_TOKEN":           keys.Token,
		"INGESTION_SERVICE_HUGGINGFACE_JOB_ENV_OUTPUT_URI":      keys.OutputURI,
	}
	for name, value := range required {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s must be set", name)
		}
	}
	return nil
}

func huggingFaceJobEnvKeysFromEnv() download.HuggingFaceJobEnvKeys {
	log.Trace("huggingFaceJobEnvKeysFromEnv")

	return download.HuggingFaceJobEnvKeys{
		ResourceID:     env.WithDefaultString("INGESTION_SERVICE_HUGGINGFACE_JOB_ENV_RESOURCE_ID", "INGESTION_SERVICE_MODEL_RESOURCE_ID"),
		ModelName:      env.WithDefaultString("INGESTION_SERVICE_HUGGINGFACE_JOB_ENV_MODEL_NAME", "INGESTION_SERVICE_MODEL_NAME"),
		ModelVersion:   env.WithDefaultString("INGESTION_SERVICE_HUGGINGFACE_JOB_ENV_MODEL_VERSION", "INGESTION_SERVICE_MODEL_VERSION"),
		BaseModel:      env.WithDefaultString("INGESTION_SERVICE_HUGGINGFACE_JOB_ENV_BASE_MODEL", "INGESTION_SERVICE_MODEL_BASE_MODEL"),
		ArtifactType:   env.WithDefaultString("INGESTION_SERVICE_HUGGINGFACE_JOB_ENV_ARTIFACT_TYPE", "INGESTION_SERVICE_MODEL_ARTIFACT_TYPE"),
		ArtifactFormat: env.WithDefaultString("INGESTION_SERVICE_HUGGINGFACE_JOB_ENV_ARTIFACT_FORMAT", "INGESTION_SERVICE_MODEL_ARTIFACT_FORMAT"),
		FileName:       env.WithDefaultString("INGESTION_SERVICE_HUGGINGFACE_JOB_ENV_FILE_NAME", "INGESTION_SERVICE_HUGGINGFACE_FILE"),
		RepoID:         env.WithDefaultString("INGESTION_SERVICE_HUGGINGFACE_JOB_ENV_REPO_ID", "INGESTION_SERVICE_HUGGINGFACE_REPO_ID"),
		Revision:       env.WithDefaultString("INGESTION_SERVICE_HUGGINGFACE_JOB_ENV_REVISION", "INGESTION_SERVICE_HUGGINGFACE_REVISION"),
		Token:          env.WithDefaultString("INGESTION_SERVICE_HUGGINGFACE_JOB_ENV_TOKEN", "INGESTION_SERVICE_HUGGINGFACE_TOKEN"),
		OutputURI:      env.WithDefaultString("INGESTION_SERVICE_HUGGINGFACE_JOB_ENV_OUTPUT_URI", "INGESTION_SERVICE_HUGGINGFACE_OUTPUT_URI"),
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
