package main

import (
	"context"
	"data_stream_service/pkg/infra"
	"data_stream_service/pkg/infra/network/data"
	"errors"
	"fmt"
	env "lib/shared_lib/env"
	coreHealthCheck "lib/shared_lib/healthcheck"
	logs "lib/shared_lib/logs"
	messagingConn "lib/shared_lib/messaging"
	trace "lib/shared_lib/trace"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
)

var Version string

type streamConfig struct {
	ServiceName string
	GRPCHost    string
	GRPCPort    int
	Messaging   messagingConn.MessengerConfig
	OutboxRelay messagingConn.OutboxRelayConfig
	Health      healthConfig
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
	cfg := readStreamConfig()
	serviceName := cfg.ServiceName

	log.Trace(fmt.Sprintf("starting the %s service", serviceName))
	traceShutdown := trace.Init(cancelCtx, serviceName, Version)

	messagingFactory := messagingConn.NewMessenger(cfg.Messaging, cancelFtn)
	defer func() {
		_ = messagingFactory.Close(cancelCtx)
	}()
	if _, err := messagingFactory.Publisher(cancelCtx); err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create the publisher")
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

	healthCheck := coreHealthCheck.NewMonitor(newHealthCheckConfig(cfg.Health))
	healthCheck = healthCheck.WithCpuCheck().WithMemoryCheck().WithMessageBrokerCheck()

	streamCfg := infra.DataConfig{
		Server: infra.ServerConnectionConfig{
			Hostname: cfg.GRPCHost,
			Port:     cfg.GRPCPort,
		},
		QueryEngine: infra.QueryEngineConfig{
			Mode:               env.WithDefaultString("DATA_STREAM_QUERY_ENGINE_MODE", "registry"),
			DataRoot:           env.WithDefaultString("DATA_STREAM_QUERY_ENGINE_DATA_ROOT", "tmp/local_s3_storage"),
			BinaryPath:         env.WithDefaultString("DATA_STREAM_QUERY_ENGINE_BINARY_PATH", "../query_engine/datafusion_query_engine/target/release/datafusion_query_engine"),
			TimeoutSec:         env.WithDefaultInt("DATA_STREAM_QUERY_ENGINE_TIMEOUT_SECONDS", "30"),
			RegistryAddress:    env.WithDefaultString("DATA_STREAM_DATA_REGISTRY_GRPC_ADDRESS", "localhost:7071"),
			RegistryDialMs:     env.WithDefaultInt("DATA_STREAM_DATA_REGISTRY_GRPC_DIAL_TIMEOUT_MS", "500"),
			RegistryCallMs:     env.WithDefaultInt("DATA_STREAM_DATA_REGISTRY_GRPC_CALL_TIMEOUT_MS", "15000"),
			RegistryRetryCount: env.WithDefaultInt("DATA_STREAM_DATA_REGISTRY_GRPC_RETRY_COUNT", "3"),
		},
	}
	queryEngine, err := data.NewQueryEngine(streamCfg.QueryEngine)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to initialize query engine")
	}
	dataServer := data.NewFlightServer(&data.FlightServerAuth{}, streamCfg, queryEngine)
	dataShutdown := dataServer.Connect()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := healthCheck.Connect(ctx); err != nil {
			if err != http.ErrServerClosed {
				log.Fatalf("unable to start health check for the %s service: %v", serviceName, err)
			}
			quit <- syscall.SIGTERM
		}
	}()

	<-quit

	dataShutdown()
	healthCheck.Close()
	cancelFtn()
	traceShutdown()
	log.Trace(fmt.Sprintf("stopped the %s service", serviceName))
}

func readStreamConfig() streamConfig {
	brokers := env.WithDefaultString("KAFKA_BROKER", "localhost:9092")
	return streamConfig{
		ServiceName: env.WithDefaultString("DATA_STREAM_SERVICE_NAME", "data-stream-service"),
		GRPCHost:    env.WithDefaultString("DATA_STREAM_API_GRPC_HOST", "localhost"),
		GRPCPort:    env.WithDefaultInt("DATA_STREAM_API_GRPC_PORT", "7070"),
		Messaging: messagingConn.MessengerConfig{
			DlqURL:    env.WithDefaultString("DATA_STREAM_SERVICE_DLQ", "http://localhost:4566/data-stream-dev-env-queue/"),
			OutboxURL: env.WithDefaultString("DATA_STREAM_SERVICE_OUTBOX", ""),
			GroupID:   env.WithDefaultString("DATA_STREAM_SERVICE_KAFKA_GROUP_ID", "data-stream-group"),
			Brokers:   brokers,
		},
		OutboxRelay: messagingConn.OutboxRelayConfig{
			PollInterval:   time.Duration(env.WithDefaultInt("DATA_STREAM_SERVICE_OUTBOX_RELAY_POLL_MS", "250")) * time.Millisecond,
			FailureBackoff: time.Duration(env.WithDefaultInt("DATA_STREAM_SERVICE_OUTBOX_RELAY_FAILURE_BACKOFF_MS", "2000")) * time.Millisecond,
			BatchSize:      int32(env.WithDefaultInt("DATA_STREAM_SERVICE_OUTBOX_RELAY_BATCH_SIZE", "100")),
		},
		Health: healthConfig{
			CpuThresholdPercentage:        env.WithDefaultInt("DATA_STREAM_HEALTHCHECK_CPU_THRESHOLD_PERCENT", "80"),
			MemFreeThresholdPercentage:    env.WithDefaultInt("DATA_STREAM_HEALTHCHECK_FREE_MEM_THRESHOLD_PERCENT", "20"),
			HealthCheckPort:               env.WithDefaultInt("DATA_STREAM_HEALTHCHECK_PORT", "5050"),
			MessageBrokerConnectionString: brokers,
			MessageBrokerLatencyThreshold: secondsFromEnv("DATA_STREAM_HEALTHCHECK_MSG_BROKER_LATENCY_THRESHOLD_SECONDS", "5"),
			ServiceLatencyThreshold:       secondsFromEnv("DATA_STREAM_HEALTHCHECK_SERVICE_LATENCY_THRESHOLD_SECONDS", "5"),
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
