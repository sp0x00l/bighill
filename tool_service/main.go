package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"tool_service/pkg/app"
	"tool_service/pkg/domain/model"
	"tool_service/pkg/infra/audit"
	"tool_service/pkg/infra/executor"
	toolgrpc "tool_service/pkg/infra/network/grpc"
	staticrepo "tool_service/pkg/infra/repo/static"

	env "lib/shared_lib/env"
	"lib/shared_lib/healthcheck"
	logs "lib/shared_lib/logs"
	"lib/shared_lib/observability"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

var (
	Version string
)

type runtimeConfig struct {
	serviceName       string
	grpcPort          int
	healthCheckConfig healthcheck.HealthCheckConfig
	httpToolConfig    httpToolConfig
}

type httpToolConfig struct {
	allowedHosts     []string
	allowedOrgIDs    []uuid.UUID
	timeout          time.Duration
	maxResponseBytes int64
}

func init() {
	logs.Init()
}

func main() {
	log.Trace("tool main")

	cfg := loadConfig()

	ctx := context.Background()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	obsShutdown := observability.Init(runCtx, cfg.serviceName, Version)
	defer obsShutdown()

	healthCheck := healthcheck.NewMonitor(cfg.healthCheckConfig).WithCpuCheck().WithMemoryCheck()
	defer healthCheck.Close()

	validate := validator.New()
	httpArgsAdapter := executor.NewHTTPGetArgumentsDTOAdapter(validate)
	httpExecutor := executor.NewHTTPGetExecutor(nil, httpArgsAdapter, cfg.httpToolConfig.timeout, cfg.httpToolConfig.maxResponseBytes)
	registry := staticrepo.NewToolRegistry(localTools(cfg.httpToolConfig.allowedHosts, cfg.httpToolConfig.allowedOrgIDs))
	auditRepository := audit.NewLogInvocationAuditRepository()
	usecase := app.NewToolUsecase(registry, map[model.ToolExecutorKind]app.ToolExecutor{
		model.ToolExecutorKindHTTPGet: httpExecutor,
	}, app.WithInvocationAuditRepository(auditRepository))
	dtoAdapter := toolgrpc.NewToolDTOAdapter(validate)
	server := toolgrpc.NewToolServer(usecase, dtoAdapter)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.WithContext(runCtx).Infof("tool service health listener starting on %d", cfg.healthCheckConfig.HealthCheckPort)
		if err := healthCheck.Connect(runCtx); err != nil && err != http.ErrServerClosed {
			log.WithContext(runCtx).WithError(err).Fatal("unable to start tool healthcheck")
		}
	}()

	go func() {
		log.WithContext(runCtx).Infof("tool service gRPC listener starting on %d", cfg.grpcPort)
		if err := server.Connect(cfg.grpcPort); err != nil {
			log.WithContext(runCtx).WithError(err).Fatal("unable to start tool gRPC service")
		}
	}()

	<-quit
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.WithContext(shutdownCtx).WithError(err).Error("tool gRPC shutdown failed")
	}
	cancel()
	log.Tracef("stopped %s version %s", cfg.serviceName, Version)
}

func loadConfig() runtimeConfig {
	log.Trace("loadConfig")

	serviceLatencyThreshold := time.Duration(env.MustInt("TOOL_SERVICE_HEALTHCHECK_SERVICE_LATENCY_THRESHOLD_SECONDS")) * time.Second
	cfg := runtimeConfig{
		serviceName: env.MustString("TOOL_SERVICE_NAME"),
		grpcPort:    env.MustInt("TOOL_SERVICE_GRPC_PORT"),
		healthCheckConfig: healthcheck.HealthCheckConfig{
			CpuThresholdPercentage:     env.MustInt("TOOL_SERVICE_HEALTHCHECK_CPU_THRESHOLD_PERCENT"),
			MemFreeThresholdPercentage: env.MustInt("TOOL_SERVICE_HEALTHCHECK_FREE_MEM_THRESHOLD_PERCENT"),
			HealthCheckPort:            env.MustInt("TOOL_SERVICE_HEALTHCHECK_PORT"),
			ServiceLatencyThresholdSec: serviceLatencyThreshold,
		},
		httpToolConfig: httpToolConfig{
			allowedHosts:     optionalStringSlice(env.MustString("TOOL_SERVICE_HTTP_TOOL_ALLOWED_HOSTS")),
			allowedOrgIDs:    optionalUUIDSlice(env.MustString("TOOL_SERVICE_ALLOWED_ORG_IDS")),
			timeout:          time.Duration(env.MustInt("TOOL_SERVICE_HTTP_TOOL_TIMEOUT_MS")) * time.Millisecond,
			maxResponseBytes: env.MustInt64("TOOL_SERVICE_HTTP_TOOL_MAX_RESPONSE_BYTES"),
		},
	}
	validateConfig(cfg)
	return cfg
}

func optionalStringSlice(value string) []string {
	log.Trace("optionalStringSlice")

	value = strings.TrimSpace(value)
	if value == "" {
		return []string{}
	}
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item == "" {
			log.Fatal("TOOL_SERVICE_HTTP_TOOL_ALLOWED_HOSTS contains an empty item")
		}
		result = append(result, item)
	}
	return result
}

func optionalUUIDSlice(value string) []uuid.UUID {
	log.Trace("optionalUUIDSlice")

	value = strings.TrimSpace(value)
	if value == "" {
		return []uuid.UUID{}
	}
	parts := strings.Split(value, ",")
	result := make([]uuid.UUID, 0, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item == "" {
			log.Fatal("TOOL_SERVICE_ALLOWED_ORG_IDS contains an empty item")
		}
		id, err := uuid.Parse(item)
		if err != nil || id == uuid.Nil {
			log.Fatal("TOOL_SERVICE_ALLOWED_ORG_IDS contains an invalid UUID")
		}
		result = append(result, id)
	}
	return result
}

func validateConfig(cfg runtimeConfig) {
	log.Trace("validateConfig")

	if cfg.grpcPort <= 0 {
		log.Fatal("TOOL_SERVICE_GRPC_PORT must be positive")
	}
	if cfg.httpToolConfig.timeout <= 0 {
		log.Fatal("TOOL_SERVICE_HTTP_TOOL_TIMEOUT_MS must be positive")
	}
	if cfg.httpToolConfig.maxResponseBytes <= 0 {
		log.Fatal("TOOL_SERVICE_HTTP_TOOL_MAX_RESPONSE_BYTES must be positive")
	}
}

func localTools(allowedHosts []string, allowedOrgIDs []uuid.UUID) []*model.ToolDefinition {
	log.Trace("localTools")

	return []*model.ToolDefinition{
		{
			Name:                  "http_get",
			Description:           "Fetches content from an allowlisted HTTP endpoint.",
			ParametersJSON:        []byte(`{"type":"object","additionalProperties":false,"required":["url"],"properties":{"url":{"type":"string","format":"uri"}}}`),
			ImplementationVersion: fmt.Sprintf("http_get:%s", Version),
			ExecutorKind:          model.ToolExecutorKindHTTPGet,
			EgressHosts:           allowedHosts,
			AllowedOrgIDs:         allowedOrgIDs,
			Enabled:               true,
		},
	}
}
