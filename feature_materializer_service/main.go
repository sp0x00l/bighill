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
	"syscall"
	"time"

	usecase "feature_materializer_service/pkg/app"
	featuremessaging "feature_materializer_service/pkg/infra/network/messaging"
	featuredb "feature_materializer_service/pkg/infra/repo/db"
	coreDB "lib/shared_lib/db"
	env "lib/shared_lib/env"
	coreHealthCheck "lib/shared_lib/healthcheck"
	logs "lib/shared_lib/logs"
	messagingConn "lib/shared_lib/messaging"
	trace "lib/shared_lib/trace"

	log "github.com/sirupsen/logrus"
)

var Version string

type materializerConfig struct {
	ServiceName          string
	DBName               string
	DBConnectionString   string
	Messaging            messagingConn.MessengerConfig
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

	subscriber, err := messagingFactory.Subscriber(cancelCtx)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create the subscriber")
	}

	snapshotRepo := featuredb.NewSnapshotRepository(database)
	rawSnapshotUsecase := usecase.NewRawSnapshotUsecase(snapshotRepo, nil)
	datasetFileSubscriber := featuremessaging.NewDatasetFileUploadedSubscriber(subscriber, rawSnapshotUsecase, []string{cfg.DatasetUploadedTopic})

	healthCheck := coreHealthCheck.NewMonitor(newHealthCheckConfig(cfg.Health))
	healthCheck = healthCheck.WithCpuCheck().WithDatabaseCheck().WithMemoryCheck().WithMessageBrokerCheck()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := datasetFileSubscriber.Start(cancelCtx); err != nil && !errors.Is(err, context.Canceled) {
			log.WithContext(cancelCtx).WithError(err).Fatal("dataset file subscriber stopped unexpectedly")
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
	return materializerConfig{
		ServiceName:        env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_NAME", "feature-materializer-service"),
		DBName:             dbName,
		DBConnectionString: dbConnectionString,
		Messaging: messagingConn.MessengerConfig{
			DlqURL:    env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_DLQ", "http://localhost:4566/feature-materializer-dev-env-queue/"),
			GroupID:   env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_KAFKA_GROUP_ID", "feature-materializer-group"),
			Brokers:   brokers,
			OutboxURL: env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_OUTBOX", ""),
		},
		DatasetUploadedTopic: env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_DATASET_FILE_UPLOADED_SUBSCRIBER_TOPIC", "dataset_file_uploaded"),
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

func postgresConnectionString(user, password, host, port, dbName, sslMode string, maxConnections int) string {
	encodedUser := url.QueryEscape(user)
	encodedPassword := url.QueryEscape(password)
	q := url.Values{}
	q.Set("sslmode", sslMode)
	q.Set("pool_max_conns", strconv.Itoa(maxConnections))
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?%s", encodedUser, encodedPassword, host, port, dbName, q.Encode())
}
