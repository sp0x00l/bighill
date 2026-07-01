package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"inference_service/pkg/app"
	"inference_service/pkg/infra/generation"
	inferencegrpc "inference_service/pkg/infra/network/grpc"
	inferencemessaging "inference_service/pkg/infra/network/messaging"
	inferencedb "inference_service/pkg/infra/repo/db"

	coreDB "lib/shared_lib/db"
	env "lib/shared_lib/env"
	coreHealthCheck "lib/shared_lib/healthcheck"
	logs "lib/shared_lib/logs"
	messagingConn "lib/shared_lib/messaging"
	trace "lib/shared_lib/trace"

	log "github.com/sirupsen/logrus"
)

var Version string

type inferenceConfig struct {
	ServiceName         string
	DBName              string
	DBConnectionString  string
	Messaging           messagingConn.MessengerConfig
	Topics              inferencemessaging.InferenceTopics
	FeatureMaterializer inferencegrpc.FeatureMaterializerClientConfig
	GRPCPort            int
	Health              healthConfig
}

type healthConfig struct {
	CpuThresholdPercentage        int
	MemFreeThresholdPercent       int
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

	cfg := readInferenceConfig()
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

	modelRepository := inferencedb.NewInferenceModelRepository(database)
	datasetRepository := inferencedb.NewInferenceDatasetRepository(database)
	retrievalClient, err := inferencegrpc.NewFeatureMaterializerClient(cancelCtx, cfg.FeatureMaterializer)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create feature materializer client")
	}
	defer func() {
		_ = retrievalClient.Close()
	}()
	generator := generation.NewDeterministicGenerator()
	inferenceUsecase := app.NewInferenceUsecase(
		modelRepository,
		app.WithInferenceDatasetRepository(datasetRepository),
		app.WithRetrievalClient(retrievalClient),
		app.WithGenerationAdapter(generator),
	)
	modelUpdatedSubscriber := inferencemessaging.NewModelUpdatedSubscriber(subscriber, inferenceUsecase, cfg.Topics)
	grpcService := inferencegrpc.NewInferenceGrpcServer(inferenceUsecase)
	defer grpcService.Close()

	healthCheck := coreHealthCheck.NewMonitor(newHealthCheckConfig(cfg.Health))
	healthCheck = healthCheck.WithCpuCheck().WithDatabaseCheck().WithMemoryCheck().WithMessageBrokerCheck()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := healthCheck.Connect(cancelCtx); err != nil {
			if err != http.ErrServerClosed {
				log.Fatalf("unable to start health check for the %s service: %v", serviceName, err)
			}
			quit <- syscall.SIGTERM
		}
	}()

	go func() {
		if err := modelUpdatedSubscriber.Start(cancelCtx); err != nil && !errors.Is(err, context.Canceled) {
			log.WithContext(cancelCtx).WithError(err).Error("model updated subscriber stopped unexpectedly")
			quit <- syscall.SIGTERM
		}
	}()

	go func() {
		if err := grpcService.Connect(cfg.GRPCPort); err != nil {
			log.Errorf("unable to start the %s grpc service: %v", serviceName, err)
			quit <- syscall.SIGTERM
		}
	}()

	<-quit

	cancelFtn()
	healthCheck.Close()
	log.Trace(fmt.Sprintf("stopped the %s service", serviceName))
}

func readInferenceConfig() inferenceConfig {
	brokers := env.WithDefaultString("KAFKA_BROKER", "localhost:9092")
	dbName := env.WithDefaultString("INFERENCE_DB_NAME", "bighill_inference_db")
	dbConnectionString := postgresConnectionString(
		env.WithDefaultString("INFERENCE_DB_USER", "bighill_inference_db_user"),
		env.WithDefaultString("INFERENCE_DB_PASSWORD", ""),
		env.WithDefaultString("PGHOST", "127.0.0.1"),
		env.WithDefaultString("PGPORT", "5432"),
		dbName,
		env.WithDefaultString("PGSSLMODE", "disable"),
		env.WithDefaultInt("INFERENCE_DB_MAX_CONNECTIONS", "20"),
	)
	return inferenceConfig{
		ServiceName:        env.WithDefaultString("INFERENCE_SERVICE_NAME", "inference-service"),
		DBName:             dbName,
		DBConnectionString: dbConnectionString,
		Messaging: messagingConn.MessengerConfig{
			DlqURL:  env.WithDefaultString("INFERENCE_SERVICE_DLQ", "http://localhost:4566/inference-dev-env-queue/"),
			GroupID: env.WithDefaultString("INFERENCE_SERVICE_KAFKA_GROUP_ID", "inference-group"),
			Brokers: brokers,
		},
		Topics: inferencemessaging.InferenceTopics{
			ModelRegistry: env.WithDefaultString("INFERENCE_SERVICE_MODEL_REGISTRY_SUBSCRIBER_TOPIC", "model_registry"),
			DataRegistry:  env.WithDefaultString("INFERENCE_SERVICE_DATA_REGISTRY_SUBSCRIBER_TOPIC", "data_registry"),
		},
		FeatureMaterializer: inferencegrpc.FeatureMaterializerClientConfig{
			Address:       env.WithDefaultString("INFERENCE_FEATURE_MATERIALIZER_GRPC_ADDRESS", "localhost:7072"),
			DialTimeoutMs: env.WithDefaultInt("INFERENCE_FEATURE_MATERIALIZER_GRPC_DIAL_TIMEOUT_MS", "500"),
			CallTimeoutMs: env.WithDefaultInt("INFERENCE_FEATURE_MATERIALIZER_GRPC_CALL_TIMEOUT_MS", "15000"),
			RetryCount:    env.WithDefaultInt("INFERENCE_FEATURE_MATERIALIZER_GRPC_RETRY_COUNT", "3"),
		},
		GRPCPort: env.WithDefaultInt("INFERENCE_API_GRPC_PORT", "7073"),
		Health: healthConfig{
			CpuThresholdPercentage:        env.WithDefaultInt("INFERENCE_HEALTHCHECK_CPU_THRESHOLD_PERCENT", "80"),
			MemFreeThresholdPercent:       env.WithDefaultInt("INFERENCE_HEALTHCHECK_FREE_MEM_THRESHOLD_PERCENT", "20"),
			HealthCheckPort:               env.WithDefaultInt("INFERENCE_HEALTHCHECK_PORT", "5059"),
			DBConnectionString:            dbConnectionString,
			MessageBrokerConnectionString: brokers,
			DbLatencyThreshold:            secondsFromEnv("INFERENCE_HEALTHCHECK_DB_LATENCY_THRESHOLD_SECONDS", "5"),
			MessageBrokerLatencyThreshold: secondsFromEnv("INFERENCE_HEALTHCHECK_MSG_BROKER_LATENCY_THRESHOLD_SECONDS", "5"),
			ServiceLatencyThreshold:       secondsFromEnv("INFERENCE_HEALTHCHECK_SERVICE_LATENCY_THRESHOLD_SECONDS", "5"),
		},
	}
}

func newHealthCheckConfig(cfg healthConfig) coreHealthCheck.HealthCheckConfig {
	return coreHealthCheck.HealthCheckConfig{
		CpuThresholdPercentage:                       cfg.CpuThresholdPercentage,
		MemFreeThresholdPercentage:                   cfg.MemFreeThresholdPercent,
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
	log.Trace("postgresConnectionString")

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
