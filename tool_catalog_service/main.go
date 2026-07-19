package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"syscall"
	"time"

	coreDB "lib/shared_lib/db"
	env "lib/shared_lib/env"
	coreHealthCheck "lib/shared_lib/healthcheck"
	"lib/shared_lib/lifecycle"
	logs "lib/shared_lib/logs"
	messagingConn "lib/shared_lib/messaging"
	serializers "lib/shared_lib/serializer"
	trace "lib/shared_lib/trace"
	shareduow "lib/shared_lib/uow"
	"tool_catalog_service/pkg/app"
	toolcatalogadapter "tool_catalog_service/pkg/infra/network/adapter"
	toolcatalogmcp "tool_catalog_service/pkg/infra/network/mcp"
	toolcatalogmessaging "tool_catalog_service/pkg/infra/network/messaging"
	toolcatalogrest "tool_catalog_service/pkg/infra/network/rest"
	toolcatalogdb "tool_catalog_service/pkg/infra/repo/db"

	log "github.com/sirupsen/logrus"
)

var Version string

type toolCatalogConfig struct {
	ServiceName        string
	HTTPPort           int
	DBName             string
	DBConnectionString string
	Messaging          messagingConn.MessengerConfig
	OutboxBackend      string
	OutboxRelay        messagingConn.OutboxRelayConfig
	ToolCatalogTopic   string
	MCPVerifyTimeout   time.Duration
	Health             healthConfig
	Lifecycle          lifecycle.Config
}

type healthConfig struct {
	CpuThresholdPercentage                    int
	MemFreeThresholdPercent                   int
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
	defer cancelFtn()

	cfg := readToolCatalogConfig()
	serviceName := cfg.ServiceName
	log.Trace(fmt.Sprintf("starting the %s service", serviceName))
	traceShutdown := trace.Init(cancelCtx, serviceName, Version)

	database, err := coreDB.InitDatabase(cancelCtx, cfg.DBName, cfg.DBConnectionString, log.StandardLogger())
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("database init failed")
	}
	outboxWriter, err := newPostgresOutbox(database, cfg.OutboxBackend)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create postgres outbox")
	}
	orderedOutbox, ok := outboxWriter.(messagingConn.OrderedOutbox)
	if !ok {
		log.Fatal("postgres outbox does not support ordered transactional enqueue")
	}
	outboxSignal := make(chan struct{}, 1)
	outboxWriter = messagingConn.NewSignaledOutbox(outboxWriter, outboxSignal)
	cfg.OutboxRelay.Signal = outboxSignal
	relayOutbox, ok := outboxWriter.(messagingConn.RelayOutbox)
	if !ok {
		log.Fatal("postgres outbox does not support relay operations")
	}
	outboxPublisher, err := messagingConn.NewPublisher(cfg.Messaging.Brokers)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create outbox relay publisher")
	}
	relayPublisher, ok := outboxPublisher.(messagingConn.RelayPublisher)
	if !ok {
		log.Fatal("publisher does not support outbox relay publishing")
	}
	outboxRelay := messagingConn.NewOutboxRelay(relayOutbox, relayPublisher, cfg.OutboxRelay)

	repository := toolcatalogdb.NewToolCatalogRepository(database)
	unitOfWork := shareduow.New(database.Pool,
		shareduow.WithTransactionalOutbox(orderedOutbox),
		shareduow.WithOutboxSignal(func() { messagingConn.NotifyOutboxSignal(outboxSignal) }),
	)
	encoder := serializers.NewJSONSerializer()
	eventBuilder := toolcatalogmessaging.NewToolCatalogEventBuilder(cfg.ToolCatalogTopic)
	manifestVerifier := toolcatalogmcp.NewManifestVerifier(&http.Client{Timeout: cfg.MCPVerifyTimeout}, cfg.MCPVerifyTimeout)
	usecase := app.NewToolCatalogUsecase(repository, unitOfWork, eventBuilder, encoder, app.WithCapabilityManifestVerifier(manifestVerifier))
	dtoAdapter := toolcatalogadapter.NewToolCatalogDTOAdapter(encoder)
	routes := toolcatalogrest.NewToolCatalogHandlers(usecase, dtoAdapter).GetRoutes()
	restService := toolcatalogrest.NewService(routes, cfg.HTTPPort, serviceName)

	healthCheck := coreHealthCheck.NewMonitor(newHealthCheckConfig(cfg.Health))
	healthCheck = healthCheck.WithCpuCheck().WithDatabaseCheck().WithMemoryCheck().WithMessageBrokerCheck()

	components := []lifecycle.Component{
		lifecycle.CloserComponent("tool-catalog-observability", func() error {
			traceShutdown()
			return nil
		}),
		lifecycle.CloserComponent("tool-catalog-database", func() error {
			database.Close()
			return nil
		}),
		lifecycle.CloserComponent("tool-catalog-publisher", func() error {
			outboxPublisher.Close()
			return nil
		}),
		lifecycle.HealthCheckComponent("tool-catalog-healthcheck", healthCheck),
		lifecycle.ServerComponent("tool-catalog-http", restService),
		lifecycle.WorkerComponent("tool-catalog-outbox-relay", func(ctx context.Context) error {
			if err := outboxRelay.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		}),
	}

	supervisor := lifecycle.NewSupervisorWithConfig(cfg.Lifecycle, components...)
	if err := supervisor.RunWithSignals(cancelCtx, syscall.SIGINT, syscall.SIGTERM); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.WithContext(cancelCtx).WithError(err).Errorf("%s service stopped with error", serviceName)
	}
	cancelFtn()
	log.Trace(fmt.Sprintf("stopped the %s service", serviceName))
}

func readToolCatalogConfig() toolCatalogConfig {
	env.RequireServiceEnvironment()

	brokers := env.MustString("KAFKA_BROKER")
	dbConfig := coreDB.DatabaseConfig{}
	dbConfig.RequireDbName("TOOL_CATALOG_SERVICE_DB_NAME")
	dbConfig.RequireDbUser("TOOL_CATALOG_SERVICE_DB_USER")
	dbConfig.RequireDbPassword("TOOL_CATALOG_SERVICE_DB_PASSWORD")
	dbConfig.RequireDbMaxConnections("TOOL_CATALOG_SERVICE_DB_MAX_CONNECTIONS")
	dbConfig.RequireDbHost("PGHOST")
	dbConfig.RequireDbPort("PGPORT")
	dbConfig.RequireDbSSLMode("PGSSLMODE")
	dbName := dbConfig.GetName()
	dbConnectionString := dbConfig.GetConnectionString()
	return toolCatalogConfig{
		ServiceName:        env.MustString("TOOL_CATALOG_SERVICE_NAME"),
		HTTPPort:           env.MustInt("TOOL_CATALOG_SERVICE_API_HTTP_PORT"),
		DBName:             dbName,
		DBConnectionString: dbConnectionString,
		Messaging: messagingConn.MessengerConfig{
			DlqURL:          env.MustString("TOOL_CATALOG_SERVICE_DLQ"),
			GroupID:         env.MustString("TOOL_CATALOG_SERVICE_KAFKA_BASE_GROUP_ID"),
			Brokers:         brokers,
			AutoOffsetReset: env.MustString("TOOL_CATALOG_SERVICE_KAFKA_AUTO_OFFSET_RESET"),
		},
		OutboxBackend: env.MustString("TOOL_CATALOG_SERVICE_OUTBOX"),
		OutboxRelay: messagingConn.OutboxRelayConfig{
			PollInterval:   time.Duration(env.MustInt("TOOL_CATALOG_SERVICE_OUTBOX_RELAY_POLL_MS")) * time.Millisecond,
			FailureBackoff: time.Duration(env.MustInt("TOOL_CATALOG_SERVICE_OUTBOX_RELAY_FAILURE_BACKOFF_MS")) * time.Millisecond,
			BatchSize:      int32(env.MustInt("TOOL_CATALOG_SERVICE_OUTBOX_RELAY_BATCH_SIZE")),
		},
		ToolCatalogTopic: env.MustString("TOOL_CATALOG_SERVICE_TOPIC"),
		MCPVerifyTimeout: secondsFromEnv("TOOL_CATALOG_SERVICE_MCP_VERIFY_TIMEOUT_SECONDS"),
		Health: healthConfig{
			CpuThresholdPercentage:                    env.MustInt("TOOL_CATALOG_SERVICE_HEALTHCHECK_CPU_THRESHOLD_PERCENT"),
			MemFreeThresholdPercent:                   env.MustInt("TOOL_CATALOG_SERVICE_HEALTHCHECK_FREE_MEM_THRESHOLD_PERCENT"),
			HealthCheckPort:                           env.MustInt("TOOL_CATALOG_SERVICE_HEALTHCHECK_PORT"),
			DBConnectionString:                        dbConnectionString,
			MessageBrokerConnectionString:             brokers,
			DbLatencyThreshold:                        secondsFromEnv("TOOL_CATALOG_SERVICE_HEALTHCHECK_DB_LATENCY_THRESHOLD_SECONDS"),
			MessageBrokerLatencyThreshold:             secondsFromEnv("TOOL_CATALOG_SERVICE_HEALTHCHECK_MSG_BROKER_LATENCY_THRESHOLD_SECONDS"),
			ServiceLatencyThreshold:                   secondsFromEnv("TOOL_CATALOG_SERVICE_HEALTHCHECK_SERVICE_LATENCY_THRESHOLD_SECONDS"),
			MessageBrokerSubscriberMaxPollSilence:     secondsFromEnv("TOOL_CATALOG_SERVICE_HEALTHCHECK_MESSAGE_BROKER_SUBSCRIBER_MAX_POLL_SILENCE_SECONDS"),
			MessageBrokerSubscriberMaxProgressSilence: secondsFromEnv("TOOL_CATALOG_SERVICE_HEALTHCHECK_MESSAGE_BROKER_SUBSCRIBER_MAX_PROGRESS_SILENCE_SECONDS"),
			MessageBrokerSubscriberMaxLag:             int64(env.MustInt("TOOL_CATALOG_SERVICE_HEALTHCHECK_MESSAGE_BROKER_SUBSCRIBER_MAX_LAG")),
		},
		Lifecycle: lifecycle.Config{
			ReadinessTimeout: secondsFromEnv("TOOL_CATALOG_SERVICE_LIFECYCLE_READINESS_TIMEOUT_SECONDS"),
			DrainTimeout:     secondsFromEnv("TOOL_CATALOG_SERVICE_LIFECYCLE_DRAIN_TIMEOUT_SECONDS"),
			CloseTimeout:     secondsFromEnv("TOOL_CATALOG_SERVICE_LIFECYCLE_CLOSE_TIMEOUT_SECONDS"),
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
		MessageBrokerSubscriberMaxPollSilenceSec:     cfg.MessageBrokerSubscriberMaxPollSilence,
		MessageBrokerSubscriberMaxProgressSilenceSec: cfg.MessageBrokerSubscriberMaxProgressSilence,
		MessageBrokerSubscriberMaxLag:                cfg.MessageBrokerSubscriberMaxLag,
	}
}

func secondsFromEnv(key string) time.Duration {
	return time.Duration(env.MustInt(key)) * time.Second
}

func newPostgresOutbox(database *coreDB.Database, backend string) (messagingConn.OutboxWriter, error) {
	log.Trace("newPostgresOutbox")

	if backend != "postgres" {
		return nil, fmt.Errorf("unsupported outbox backend %q", backend)
	}
	return messagingConn.NewPostgresOutbox(database.Pool, database.Name, "")
}
