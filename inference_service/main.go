package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"inference_service/pkg/app"

	env "lib/shared_lib/env"
	coreHealthCheck "lib/shared_lib/healthcheck"
	logs "lib/shared_lib/logs"
	trace "lib/shared_lib/trace"

	log "github.com/sirupsen/logrus"
)

var Version string

type inferenceConfig struct {
	ServiceName string
	Health      healthConfig
}

type healthConfig struct {
	CpuThresholdPercentage  int
	MemFreeThresholdPercent int
	HealthCheckPort         int
	ServiceLatencyThreshold time.Duration
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

	_ = app.NewInferenceUsecase()

	healthCheck := coreHealthCheck.NewMonitor(newHealthCheckConfig(cfg.Health))
	healthCheck = healthCheck.WithCpuCheck().WithMemoryCheck()

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

	<-quit

	cancelFtn()
	healthCheck.Close()
	log.Trace(fmt.Sprintf("stopped the %s service", serviceName))
}

func readInferenceConfig() inferenceConfig {
	return inferenceConfig{
		ServiceName: env.WithDefaultString("INFERENCE_SERVICE_NAME", "inference-service"),
		Health: healthConfig{
			CpuThresholdPercentage:  env.WithDefaultInt("INFERENCE_HEALTHCHECK_CPU_THRESHOLD_PERCENT", "80"),
			MemFreeThresholdPercent: env.WithDefaultInt("INFERENCE_HEALTHCHECK_FREE_MEM_THRESHOLD_PERCENT", "20"),
			HealthCheckPort:         env.WithDefaultInt("INFERENCE_HEALTHCHECK_PORT", "5059"),
			ServiceLatencyThreshold: secondsFromEnv("INFERENCE_HEALTHCHECK_SERVICE_LATENCY_THRESHOLD_SECONDS", "5"),
		},
	}
}

func newHealthCheckConfig(cfg healthConfig) coreHealthCheck.HealthCheckConfig {
	return coreHealthCheck.HealthCheckConfig{
		CpuThresholdPercentage:                       cfg.CpuThresholdPercentage,
		MemFreeThresholdPercentage:                   cfg.MemFreeThresholdPercent,
		HealthCheckPort:                              cfg.HealthCheckPort,
		DBConnectionString:                           "",
		MessageBrokerConnectionString:                "",
		DbLatencyThresholdSec:                        0,
		MessageBrokerLatencyThresholdSec:             0,
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
