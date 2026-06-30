package main

import (
	"context"
	usecase "data_ingestion_service/pkg/app"
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
	ServiceName          string
	HTTPPort             int
	MaxFileSizeBytes     int64
	BucketName           string
	BucketRegion         string
	BucketUploadPartSize int64
	DBName               string
	DBConnectionString   string
	Redis                rueidis.ClientOption
	Messaging            messagingConn.MessengerConfig
	OutboxRelay          messagingConn.OutboxRelayConfig
	DatasetUploadedTopic string
	Health               healthConfig
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
	cfg := readIngestionConfig()
	serviceName := cfg.ServiceName

	log.Trace(fmt.Sprintf("starting the %s service", serviceName))
	traceShutdown := trace.Init(cancelCtx, serviceName, Version)

	coreDb, err := coreDB.InitDatabase(ctx, cfg.DBName, cfg.DBConnectionString, log.StandardLogger())
	if err != nil {
		log.Errorf("database init failed: %v", err)
		os.Exit(1)
	}

	messagingFactory := messagingConn.NewMessenger(cfg.Messaging, cancelFtn)
	defer func() {
		_ = messagingFactory.Close(cancelCtx)
	}()
	publisher, err := messagingFactory.Publisher(cancelCtx)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create the publisher")
	}

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

	outboxRelay, err := messagingFactory.OutboxRelay(cancelCtx, cfg.OutboxRelay)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Warn("unable to create outbox relay")
	} else {
		go func() {
			if relayErr := outboxRelay.Run(cancelCtx); relayErr != nil && !errors.Is(relayErr, context.Canceled) {
				log.WithContext(cancelCtx).WithError(relayErr).Error("outbox relay stopped unexpectedly")
			}
		}()
	}

	uploader := coreBucket.NewBucket(ctx, cfg.BucketRegion, cfg.BucketUploadPartSize)
	uploadBucket := bucket.NewDataBucket(cfg.BucketName, uploader)
	datasetDB := db.NewDatasetDB(coreDb)
	datasetUseCase := usecase.NewDatasetUseCase(datasetDB)
	uploadUseCase := usecase.NewDataUploadUseCase(
		uploadBucket,
		usecase.WithUploadEventPublisher(publisher, cfg.DatasetUploadedTopic),
	)

	formatDetector := rest.NewDetector(
		map[string]rest.FormatValidatorFunc{
			rest.FileTypeCSV:     rest.IsCSV,
			rest.FileTypeJSON:    rest.IsJSON,
			rest.FileTypeParquet: rest.IsParquet,
		},
	)

	authHandler := rest.NewAuthHandler(authProv, authStore)
	routes := rest.NewDataUploadHandlers(uploadUseCase, datasetUseCase, formatDetector, authHandler, cfg.MaxFileSizeBytes).GetRoutes()

	log.Infof("%s API HTTP port: %d", serviceName, cfg.HTTPPort)

	restService := coreRest.NewService(routes, cfg.HTTPPort, serviceName)
	healthCheck := coreHealthCheck.NewMonitor(newHealthCheckConfig(cfg.Health))
	healthCheck = healthCheck.WithCpuCheck().WithMemoryCheck().WithDatabaseCheck().WithMessageBrokerCheck()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := restService.Connect(); err != nil {
			if err != http.ErrServerClosed {
				log.Fatalf("unable to start the %s rest service: %v", serviceName, err)
			}
			quit <- syscall.SIGTERM
		}
	}()

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
	dbName := env.WithDefaultString("DATA_INGESTION_DB_NAME", "bighill_data_ingestion_db")
	dbConnectionString := postgresConnectionString(
		env.WithDefaultString("DATA_INGESTION_DB_USER", "bighill_data_ingestion_db_user"),
		env.WithDefaultString("DATA_INGESTION_DB_PASSWORD", ""),
		env.WithDefaultString("PGHOST", "127.0.0.1"),
		env.WithDefaultString("PGPORT", "5432"),
		dbName,
		env.WithDefaultString("PGSSLMODE", "disable"),
		env.WithDefaultInt("DATA_INGESTION_DB_MAX_CONNECTIONS", "20"),
	)
	maxFileSizeMB := env.WithDefaultInt64("DATA_INGESTION_FILE_MAX_SIZE_MB", "2000")
	uploadPartSizeMB := env.WithDefaultInt64("DATA_INGESTION_FILES_UPLOAD_PART_SIZE_MS", "10")
	return ingestionConfig{
		ServiceName:          env.WithDefaultString("DATA_INGESTION_SERVICE_NAME", "data-ingestion-service"),
		HTTPPort:             env.WithDefaultInt("DATA_INGESTION_API_HTTP_PORT", "8086"),
		MaxFileSizeBytes:     maxFileSizeMB * 1000 * 1000,
		BucketName:           env.WithDefaultString("DATA_INGESTION_FILES_BUCKET_NAME", "local-dev-bucket"),
		BucketRegion:         env.WithDefaultString("DATA_INGESTION_FILES_BUCKET_REGION", "local-dev"),
		BucketUploadPartSize: uploadPartSizeMB * 1024 * 1024,
		DBName:               dbName,
		DBConnectionString:   dbConnectionString,
		Redis: rueidis.ClientOption{
			InitAddress: []string{env.WithDefaultString("DATA_INGESTION_SERVICE_REDIS_ADDRESS", "localhost:6379")},
			Username:    env.WithDefaultString("DATA_INGESTION_SERVICE_REDIS_USERNAME", ""),
			Password:    env.WithDefaultString("DATA_INGESTION_SERVICE_REDIS_PASSWORD", ""),
		},
		Messaging: messagingConn.MessengerConfig{
			DlqURL:    env.WithDefaultString("DATA_INGESTION_SERVICE_DLQ", "http://localhost:4566/data-ingestion-dev-env-queue/"),
			OutboxURL: env.WithDefaultString("DATA_INGESTION_SERVICE_OUTBOX", ""),
			GroupID:   env.WithDefaultString("DATA_INGESTION_SERVICE_KAFKA_GROUP_ID", "data-ingestion-group"),
			Brokers:   brokers,
		},
		OutboxRelay: messagingConn.OutboxRelayConfig{
			PollInterval:   time.Duration(env.WithDefaultInt("DATA_INGESTION_SERVICE_OUTBOX_RELAY_POLL_MS", "250")) * time.Millisecond,
			FailureBackoff: time.Duration(env.WithDefaultInt("DATA_INGESTION_SERVICE_OUTBOX_RELAY_FAILURE_BACKOFF_MS", "2000")) * time.Millisecond,
			BatchSize:      int32(env.WithDefaultInt("DATA_INGESTION_SERVICE_OUTBOX_RELAY_BATCH_SIZE", "100")),
		},
		DatasetUploadedTopic: env.WithDefaultString("DATA_INGESTION_SERVICE_TOPIC", "data_ingestion"),
		Health: healthConfig{
			CpuThresholdPercentage:        env.WithDefaultInt("DATA_INGESTION_HEALTHCHECK_CPU_THRESHOLD_PERCENT", "80"),
			MemFreeThresholdPercentage:    env.WithDefaultInt("DATA_INGESTION_HEALTHCHECK_FREE_MEM_THRESHOLD_PERCENT", "20"),
			HealthCheckPort:               env.WithDefaultInt("DATA_INGESTION_HEALTHCHECK_PORT", "5056"),
			DBConnectionString:            dbConnectionString,
			MessageBrokerConnectionString: brokers,
			DbLatencyThreshold:            secondsFromEnv("DATA_INGESTION_HEALTHCHECK_DB_LATENCY_THRESHOLD_SECONDS", "5"),
			MessageBrokerLatencyThreshold: secondsFromEnv("DATA_INGESTION_HEALTHCHECK_MSG_BROKER_LATENCY_THRESHOLD_SECONDS", "5"),
			ServiceLatencyThreshold:       secondsFromEnv("DATA_INGESTION_HEALTHCHECK_SERVICE_LATENCY_THRESHOLD_SECONDS", "5"),
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
