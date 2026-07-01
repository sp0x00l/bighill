package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"training_service/pkg/app"
	"training_service/pkg/infra/temporalworker"

	env "lib/shared_lib/env"
	coreHealthCheck "lib/shared_lib/healthcheck"
	logs "lib/shared_lib/logs"
	trace "lib/shared_lib/trace"

	log "github.com/sirupsen/logrus"
	"go.temporal.io/sdk/client"
)

var Version string

type trainingConfig struct {
	ServiceName string
	Temporal    temporalConfig
	Health      healthConfig
}

type temporalConfig struct {
	Address   string
	Namespace string
	TaskQueue string
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

	cfg := readTrainingConfig()
	serviceName := cfg.ServiceName

	log.Trace(fmt.Sprintf("starting the %s service", serviceName))
	traceShutdown := trace.Init(cancelCtx, serviceName, Version)
	defer traceShutdown()

	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.Temporal.Address,
		Namespace: cfg.Temporal.Namespace,
	})
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to connect to Temporal")
	}
	defer temporalClient.Close()

	activities := temporalworker.NewTrainingActivities()
	trainingWorker := temporalworker.NewTrainingWorker(temporalClient, cfg.Temporal.TaskQueue, activities)
	if err := trainingWorker.Start(); err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to start Temporal worker")
	}
	defer trainingWorker.Stop()

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

	log.WithContext(cancelCtx).WithFields(log.Fields{
		"temporal_address":    cfg.Temporal.Address,
		"temporal_namespace":  cfg.Temporal.Namespace,
		"temporal_task_queue": cfg.Temporal.TaskQueue,
		"workflow":            app.TrainModelWorkflowName,
	}).Info("training Temporal worker started")

	<-quit

	cancelFtn()
	healthCheck.Close()
	log.Trace(fmt.Sprintf("stopped the %s service", serviceName))
}

func readTrainingConfig() trainingConfig {
	return trainingConfig{
		ServiceName: env.WithDefaultString("TRAINING_SERVICE_NAME", "training-service"),
		Temporal: temporalConfig{
			Address:   env.WithDefaultString("TRAINING_SERVICE_TEMPORAL_ADDRESS", env.WithDefaultString("TEMPORAL_ADDRESS", "localhost:7233")),
			Namespace: env.WithDefaultString("TRAINING_SERVICE_TEMPORAL_NAMESPACE", env.WithDefaultString("TEMPORAL_NAMESPACE", "default")),
			TaskQueue: env.WithDefaultString("TRAINING_SERVICE_TEMPORAL_TASK_QUEUE", app.DefaultTrainingWorkflowTaskQueue),
		},
		Health: healthConfig{
			CpuThresholdPercentage:  env.WithDefaultInt("TRAINING_HEALTHCHECK_CPU_THRESHOLD_PERCENT", "80"),
			MemFreeThresholdPercent: env.WithDefaultInt("TRAINING_HEALTHCHECK_FREE_MEM_THRESHOLD_PERCENT", "20"),
			HealthCheckPort:         env.WithDefaultInt("TRAINING_HEALTHCHECK_PORT", "5058"),
			ServiceLatencyThreshold: secondsFromEnv("TRAINING_HEALTHCHECK_SERVICE_LATENCY_THRESHOLD_SECONDS", "5"),
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
