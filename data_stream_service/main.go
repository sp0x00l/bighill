package main

import (
	"context"
	"data_stream_service/pkg/infra"
	"data_stream_service/pkg/infra/network/data"
	"fmt"
	env "lib/shared_lib/env"
	coreHealthCheck "lib/shared_lib/healthcheck"
	logs "lib/shared_lib/logs"
	trace "lib/shared_lib/trace"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
)

var Version string

type streamConfig struct {
	ServiceName          string
	GRPCHost             string
	GRPCPort             int
	FlightAuthToken      string
	FlightAllowAnonymous bool
	TLSCertPath          string
	TLSKeyPath           string
	TLSClientCAPath      string
	RequireClientCert    bool
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
	cfg := readStreamConfig()
	serviceName := cfg.ServiceName

	log.Trace(fmt.Sprintf("starting the %s service", serviceName))
	traceShutdown := trace.Init(cancelCtx, serviceName, Version)

	healthCheck := coreHealthCheck.NewMonitor(newHealthCheckConfig(cfg.Health))
	healthCheck = healthCheck.WithCpuCheck().WithMemoryCheck().WithMessageBrokerCheck()

	streamCfg := infra.DataConfig{
		Server: infra.ServerConnectionConfig{
			Hostname:          cfg.GRPCHost,
			Port:              cfg.GRPCPort,
			TLSCertPath:       cfg.TLSCertPath,
			TLSKeyPath:        cfg.TLSKeyPath,
			TLSClientCAPath:   cfg.TLSClientCAPath,
			RequireClientCert: cfg.RequireClientCert,
		},
		QueryEngine: infra.QueryEngineConfig{
			Mode:               env.WithDefaultString("DATA_STREAM_SERVICE_QUERY_ENGINE_MODE", "registry"),
			DataRoot:           env.WithDefaultString("DATA_STREAM_SERVICE_QUERY_ENGINE_DATA_ROOT", "tmp/local_s3_storage"),
			BinaryPath:         env.WithDefaultString("DATA_STREAM_SERVICE_QUERY_ENGINE_BINARY_PATH", "internal/infra/queryengine/datafusion_query_engine/target/release/datafusion_query_engine"),
			TimeoutSec:         env.WithDefaultInt("DATA_STREAM_SERVICE_QUERY_ENGINE_TIMEOUT_SECONDS", "30"),
			RegistryAddress:    env.WithDefaultString("DATA_STREAM_SERVICE_DATA_REGISTRY_GRPC_ADDRESS", "localhost:7071"),
			RegistryDialMs:     env.WithDefaultInt("DATA_STREAM_SERVICE_DATA_REGISTRY_GRPC_DIAL_TIMEOUT_MS", "500"),
			RegistryCallMs:     env.WithDefaultInt("DATA_STREAM_SERVICE_DATA_REGISTRY_GRPC_CALL_TIMEOUT_MS", "15000"),
			RegistryRetryCount: env.WithDefaultInt("DATA_STREAM_SERVICE_DATA_REGISTRY_GRPC_RETRY_COUNT", "3"),
			PolarisBaseURL:     env.WithDefaultString("DATA_STREAM_SERVICE_POLARIS_BASE_URL", "http://localhost:8181"),
			PolarisCatalog:     env.WithDefaultString("DATA_STREAM_SERVICE_POLARIS_CATALOG", "bighill"),
			PolarisWarehouse:   env.WithDefaultString("DATA_STREAM_SERVICE_POLARIS_WAREHOUSE", "s3://bighill-mlops-lakehouse/"),
			PolarisCredential:  env.WithDefaultString("DATA_STREAM_SERVICE_POLARIS_CREDENTIAL", ""),
			PolarisToken:       env.WithDefaultString("DATA_STREAM_SERVICE_POLARIS_TOKEN", ""),
			PolarisScope:       env.WithDefaultString("DATA_STREAM_SERVICE_POLARIS_SCOPE", "PRINCIPAL_ROLE:ALL"),
			PolarisS3Endpoint:  env.WithDefaultString("DATA_STREAM_SERVICE_POLARIS_STORAGE_ENDPOINT", "http://localhost:9100"),
			PolarisS3AccessKey: env.WithDefaultString("DATA_STREAM_SERVICE_POLARIS_STORAGE_ACCESS_KEY_ID", "polaris_root"),
			PolarisS3SecretKey: env.WithDefaultString("DATA_STREAM_SERVICE_POLARIS_STORAGE_SECRET_ACCESS_KEY", "polaris_pass"),
			PolarisS3Region:    env.WithDefaultString("DATA_STREAM_SERVICE_POLARIS_STORAGE_REGION", "eu-west-1"),
			PolarisS3PathStyle: env.WithDefaultBool("DATA_STREAM_SERVICE_POLARIS_STORAGE_PATH_STYLE", true),
		},
	}
	queryEngine, err := data.NewQueryEngine(streamCfg.QueryEngine)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to initialize query engine")
	}
	if !cfg.FlightAllowAnonymous && strings.TrimSpace(cfg.FlightAuthToken) == "" {
		log.WithContext(cancelCtx).Fatal("DATA_STREAM_SERVICE_FLIGHT_AUTH_TOKEN is required when anonymous Flight access is disabled")
	}
	if !cfg.FlightAllowAnonymous && (strings.TrimSpace(cfg.TLSCertPath) == "" || strings.TrimSpace(cfg.TLSKeyPath) == "" || strings.TrimSpace(cfg.TLSClientCAPath) == "" || !cfg.RequireClientCert) {
		log.WithContext(cancelCtx).Fatal("data stream Flight requires mTLS when anonymous access is disabled")
	}
	dataServer := data.NewFlightServer(data.NewFlightServerAuth(cfg.FlightAuthToken, cfg.FlightAllowAnonymous), streamCfg, queryEngine)
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
		ServiceName:          env.WithDefaultString("DATA_STREAM_SERVICE_NAME", "data-stream-service"),
		GRPCHost:             env.WithDefaultString("DATA_STREAM_SERVICE_API_GRPC_HOST", "localhost"),
		GRPCPort:             env.WithDefaultInt("DATA_STREAM_SERVICE_API_GRPC_PORT", "7070"),
		FlightAuthToken:      env.WithDefaultString("DATA_STREAM_SERVICE_FLIGHT_AUTH_TOKEN", ""),
		FlightAllowAnonymous: env.WithDefaultBool("DATA_STREAM_SERVICE_FLIGHT_ALLOW_ANONYMOUS", false),
		TLSCertPath:          env.WithDefaultString("DATA_STREAM_SERVICE_FLIGHT_TLS_CERT_PATH", ""),
		TLSKeyPath:           env.WithDefaultString("DATA_STREAM_SERVICE_FLIGHT_TLS_KEY_PATH", ""),
		TLSClientCAPath:      env.WithDefaultString("DATA_STREAM_SERVICE_FLIGHT_TLS_CLIENT_CA_CERT_PATH", ""),
		RequireClientCert:    env.WithDefaultBool("DATA_STREAM_SERVICE_FLIGHT_TLS_REQUIRE_CLIENT_CERT", false),
		Health: healthConfig{
			CpuThresholdPercentage:        env.WithDefaultInt("DATA_STREAM_SERVICE_HEALTHCHECK_CPU_THRESHOLD_PERCENT", "80"),
			MemFreeThresholdPercentage:    env.WithDefaultInt("DATA_STREAM_SERVICE_HEALTHCHECK_FREE_MEM_THRESHOLD_PERCENT", "20"),
			HealthCheckPort:               env.WithDefaultInt("DATA_STREAM_SERVICE_HEALTHCHECK_PORT", "5050"),
			MessageBrokerConnectionString: brokers,
			MessageBrokerLatencyThreshold: secondsFromEnv("DATA_STREAM_SERVICE_HEALTHCHECK_MSG_BROKER_LATENCY_THRESHOLD_SECONDS", "5"),
			ServiceLatencyThreshold:       secondsFromEnv("DATA_STREAM_SERVICE_HEALTHCHECK_SERVICE_LATENCY_THRESHOLD_SECONDS", "5"),
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
