package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"tool_service/pkg/app"
	"tool_service/pkg/domain/model"
	toolcredential "tool_service/pkg/infra/credential"
	"tool_service/pkg/infra/executor"
	toolgrpc "tool_service/pkg/infra/network/grpc"
	toolpolicy "tool_service/pkg/infra/policy"
	tooldb "tool_service/pkg/infra/repo/db"
	staticrepo "tool_service/pkg/infra/repo/static"

	coreDB "lib/shared_lib/db"
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
	dbName            string
	dbConnection      string
	grpcPort          int
	healthCheckConfig healthcheck.HealthCheckConfig
	httpToolConfig    httpToolConfig
	pinnedMCPConfig   pinnedMCPConfig
}

type httpToolConfig struct {
	allowedHosts     []string
	allowedOrgIDs    []uuid.UUID
	timeout          time.Duration
	maxResponseBytes int64
}

type pinnedMCPConfig struct {
	endpoint            string
	transport           string
	toolNames           []string
	credentialSecretRef string
	allowedOrgIDs       []uuid.UUID
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

	database, err := coreDB.InitDatabase(runCtx, cfg.dbName, cfg.dbConnection, log.StandardLogger())
	if err != nil {
		log.WithContext(runCtx).WithError(err).Fatal("unable to connect to tool service database")
	}
	defer database.Close()

	healthCheck := healthcheck.NewMonitor(cfg.healthCheckConfig).WithCpuCheck().WithDatabaseCheck().WithMemoryCheck()
	defer healthCheck.Close()

	validate := validator.New()
	httpArgsAdapter := executor.NewHTTPGetArgumentsDTOAdapter(validate)
	httpExecutor := executor.NewHTTPGetExecutor(nil, httpArgsAdapter)
	credentialResolver := toolcredential.NewEnvResolver(nil)
	mcpExecutor := executor.NewMCPExecutor(cfg.pinnedMCPConfig.endpoint, credentialResolver)
	tools := configuredTools(runCtx, cfg, credentialResolver)
	registry, err := staticrepo.NewToolRegistry(tools)
	if err != nil {
		log.WithContext(runCtx).WithError(err).Fatal("unable to build tool registry")
	}
	policyResolver := toolpolicy.NewBoundaryPolicyResolver(toolpolicy.BoundaryPolicyConfig{
		HTTPTimeout:            cfg.httpToolConfig.timeout,
		HTTPMaxResponseBytes:   cfg.httpToolConfig.maxResponseBytes,
		PinnedMCPCredentialRef: cfg.pinnedMCPConfig.credentialSecretRef,
	})
	auditRepository := tooldb.NewInvocationAuditRepository(database)
	usecase := app.NewToolUsecase(registry, map[model.ToolExecutorKind]app.ToolExecutor{
		model.ToolExecutorKindHTTPGet: httpExecutor,
		model.ToolExecutorKindMCP:     mcpExecutor,
	}, app.WithBoundaryPolicyResolver(policyResolver), app.WithInvocationAuditRepository(auditRepository))
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
	dbLatencyThreshold := time.Duration(env.MustInt("TOOL_SERVICE_HEALTHCHECK_DB_LATENCY_THRESHOLD_SECONDS")) * time.Second
	dbConfig := coreDB.DatabaseConfig{}
	dbConfig.RequireDbName("TOOL_SERVICE_DB_NAME")
	dbConfig.RequireDbUser("TOOL_SERVICE_DB_USER")
	dbConfig.RequireDbPassword("TOOL_SERVICE_DB_PASSWORD")
	dbConfig.RequireDbMaxConnections("TOOL_SERVICE_DB_MAX_CONNECTIONS")
	dbConfig.RequireDbHost("PGHOST")
	dbConfig.RequireDbPort("PGPORT")
	dbConfig.RequireDbSSLMode("PGSSLMODE")
	dbName := dbConfig.GetName()
	dbConnectionString := dbConfig.GetConnectionString()
	cfg := runtimeConfig{
		serviceName:  env.MustString("TOOL_SERVICE_NAME"),
		dbName:       dbName,
		dbConnection: dbConnectionString,
		grpcPort:     env.MustInt("TOOL_SERVICE_GRPC_PORT"),
		healthCheckConfig: healthcheck.HealthCheckConfig{
			CpuThresholdPercentage:     env.MustInt("TOOL_SERVICE_HEALTHCHECK_CPU_THRESHOLD_PERCENT"),
			MemFreeThresholdPercentage: env.MustInt("TOOL_SERVICE_HEALTHCHECK_FREE_MEM_THRESHOLD_PERCENT"),
			HealthCheckPort:            env.MustInt("TOOL_SERVICE_HEALTHCHECK_PORT"),
			DBConnectionString:         dbConnectionString,
			DbLatencyThresholdSec:      dbLatencyThreshold,
			ServiceLatencyThresholdSec: serviceLatencyThreshold,
		},
		httpToolConfig: httpToolConfig{
			allowedHosts:     optionalStringSlice("TOOL_SERVICE_HTTP_TOOL_ALLOWED_HOSTS", env.MustString("TOOL_SERVICE_HTTP_TOOL_ALLOWED_HOSTS")),
			allowedOrgIDs:    optionalUUIDSlice("TOOL_SERVICE_ALLOWED_ORG_IDS", env.MustString("TOOL_SERVICE_ALLOWED_ORG_IDS")),
			timeout:          time.Duration(env.MustInt("TOOL_SERVICE_HTTP_TOOL_TIMEOUT_MS")) * time.Millisecond,
			maxResponseBytes: env.MustInt64("TOOL_SERVICE_HTTP_TOOL_MAX_RESPONSE_BYTES"),
		},
		pinnedMCPConfig: pinnedMCPConfig{
			endpoint:            strings.TrimSpace(env.MustString("TOOL_SERVICE_PINNED_MCP_SERVER_ENDPOINT")),
			transport:           strings.ToLower(strings.TrimSpace(env.MustString("TOOL_SERVICE_PINNED_MCP_SERVER_TRANSPORT"))),
			toolNames:           optionalStringSlice("TOOL_SERVICE_PINNED_MCP_TOOL_NAMES", env.MustString("TOOL_SERVICE_PINNED_MCP_TOOL_NAMES")),
			credentialSecretRef: strings.TrimSpace(env.MustString("TOOL_SERVICE_PINNED_MCP_CREDENTIAL_REF")),
			allowedOrgIDs:       optionalUUIDSlice("TOOL_SERVICE_PINNED_MCP_ALLOWED_ORG_IDS", env.MustString("TOOL_SERVICE_PINNED_MCP_ALLOWED_ORG_IDS")),
		},
	}
	validateConfig(cfg)
	return cfg
}

func optionalStringSlice(name string, value string) []string {
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
			log.Fatalf("%s contains an empty item", name)
		}
		result = append(result, item)
	}
	return result
}

func optionalUUIDSlice(name string, value string) []uuid.UUID {
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
			log.Fatalf("%s contains an empty item", name)
		}
		id, err := uuid.Parse(item)
		if err != nil || id == uuid.Nil {
			log.Fatalf("%s contains an invalid UUID", name)
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
	if cfg.pinnedMCPConfig.endpoint == "" && len(cfg.pinnedMCPConfig.toolNames) == 0 {
		return
	}
	if cfg.pinnedMCPConfig.endpoint == "" {
		log.Fatal("TOOL_SERVICE_PINNED_MCP_SERVER_ENDPOINT is required when pinned MCP tools are configured")
	}
	if cfg.pinnedMCPConfig.transport != "http" {
		log.Fatal("TOOL_SERVICE_PINNED_MCP_SERVER_TRANSPORT must be http")
	}
	if len(cfg.pinnedMCPConfig.toolNames) == 0 {
		log.Fatal("TOOL_SERVICE_PINNED_MCP_TOOL_NAMES is required when pinned MCP endpoint is configured")
	}
	if cfg.pinnedMCPConfig.credentialSecretRef == "" {
		log.Fatal("TOOL_SERVICE_PINNED_MCP_CREDENTIAL_REF is required when pinned MCP endpoint is configured")
	}
	if len(cfg.pinnedMCPConfig.allowedOrgIDs) == 0 {
		log.Fatal("TOOL_SERVICE_PINNED_MCP_ALLOWED_ORG_IDS is required when pinned MCP endpoint is configured")
	}
}

func configuredTools(ctx context.Context, cfg runtimeConfig, credentialResolver executor.CredentialResolver) []*model.ToolDefinition {
	log.Trace("configuredTools")

	tools := []*model.ToolDefinition{
		{
			Name:                  "http_get",
			Description:           "Fetches content from an allowlisted HTTP endpoint.",
			ParametersJSON:        []byte(`{"type":"object","additionalProperties":false,"required":["url"],"properties":{"url":{"type":"string","format":"uri"}}}`),
			ImplementationVersion: fmt.Sprintf("http_get:%s", Version),
			ExecutorKind:          model.ToolExecutorKindHTTPGet,
			EgressHosts:           cfg.httpToolConfig.allowedHosts,
			AllowedOrgIDs:         cfg.httpToolConfig.allowedOrgIDs,
			Enabled:               true,
		},
	}
	if cfg.pinnedMCPConfig.endpoint == "" {
		return tools
	}
	mcpTools, err := discoverMCPTools(ctx, cfg, credentialResolver)
	if err != nil {
		log.WithContext(ctx).WithError(err).Warn("mcp tools unavailable")
		return tools
	}
	return append(tools, mcpTools...)
}

func discoverMCPTools(ctx context.Context, cfg runtimeConfig, credentialResolver executor.CredentialResolver) ([]*model.ToolDefinition, error) {
	log.Trace("discoverMCPTools")

	discoveryCtx, cancel := context.WithTimeout(ctx, cfg.httpToolConfig.timeout)
	defer cancel()
	return executor.DiscoverMCPTools(discoveryCtx, executor.MCPDiscoveryConfig{
		Endpoint:      cfg.pinnedMCPConfig.endpoint,
		DeclaredTools: cfg.pinnedMCPConfig.toolNames,
		AllowedOrgIDs: cfg.pinnedMCPConfig.allowedOrgIDs,
	}, model.PolicySet{
		Egress: model.EgressPolicy{
			AllowedSchemes: []string{"http", "https"},
			AllowedHosts:   []string{mcpEndpointHost(cfg.pinnedMCPConfig.endpoint)},
		},
		Timeout: model.TimeoutPolicy{
			CallTimeout: cfg.httpToolConfig.timeout,
		},
		ResponseCap: model.ResponseCapPolicy{
			MaxBytes: cfg.httpToolConfig.maxResponseBytes,
		},
		Credential: model.CredentialPolicy{
			SecretRef:  cfg.pinnedMCPConfig.credentialSecretRef,
			HeaderName: "Authorization",
			Prefix:     "Bearer ",
		},
		Schema: model.SchemaPolicy{
			InputSchemaJSON: []byte(`{"type":"object"}`),
		},
	}, credentialResolver)
}

func mcpEndpointHost(endpoint string) string {
	log.Trace("mcpEndpointHost")

	parsed, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil {
		return ""
	}
	return strings.ToLower(parsed.Hostname())
}
