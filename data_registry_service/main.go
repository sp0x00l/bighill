package main

import (
	"context"
	usecase "data_registry_service/pkg/app"
	serializers "data_registry_service/pkg/common/serializer"
	"data_registry_service/pkg/infra/network/adapter"
	catalogclient "data_registry_service/pkg/infra/network/client"
	registrygrpc "data_registry_service/pkg/infra/network/grpc"
	registrymessaging "data_registry_service/pkg/infra/network/messaging"
	"data_registry_service/pkg/infra/network/rest"
	coreRest "data_registry_service/pkg/infra/network/restsupport"
	"data_registry_service/pkg/infra/repo/db"
	"errors"
	"fmt"
	coreDB "lib/shared_lib/db"
	env "lib/shared_lib/env"
	coreHealthCheck "lib/shared_lib/healthcheck"
	logs "lib/shared_lib/logs"
	messagingConn "lib/shared_lib/messaging"
	"lib/shared_lib/trace"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
)

var Version string

type registryConfig struct {
	ServiceName           string
	HTTPPort              int
	GRPCPort              int
	DBName                string
	DBConnectionString    string
	Messaging             messagingConn.MessengerConfig
	OutboxBackend         string
	OutboxRelay           messagingConn.OutboxRelayConfig
	Topic                 string
	MaterializationTopics registrymessaging.MaterializationTopics
	Health                healthConfig
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
	cfg := readRegistryConfig()
	serviceName := cfg.ServiceName

	log.Info(fmt.Sprintf("starting %s service", serviceName))
	traceShutdown := trace.Init(cancelCtx, serviceName, Version)

	database, err := coreDB.InitDatabase(ctx, cfg.DBName, cfg.DBConnectionString, log.StandardLogger())
	if err != nil {
		log.Errorf("database init failed: %v", err)
		os.Exit(1)
	}

	outboxWriter, err := newPostgresOutbox(database, cfg.OutboxBackend)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create postgres outbox")
	}
	orderedOutbox, ok := outboxWriter.(messagingConn.OrderedOutbox)
	if !ok {
		log.Fatal("postgres outbox does not support transactional enqueue operations")
	}
	outboxSignal := make(chan struct{}, 1)
	outboxWriter = messagingConn.NewSignaledOutbox(outboxWriter, outboxSignal)
	cfg.OutboxRelay.Signal = outboxSignal
	publisher, err := messagingConn.NewPublisher(cfg.Messaging.Brokers, messagingConn.WithOutbox(outboxWriter))
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create the publisher")
	}
	relayOutbox, ok := outboxWriter.(messagingConn.RelayOutbox)
	if !ok {
		log.Fatal("postgres outbox does not support relay operations")
	}
	relayPublisher, ok := publisher.(messagingConn.RelayPublisher)
	if !ok {
		log.Fatal("publisher does not support outbox relay publishing")
	}
	outboxRelay := messagingConn.NewOutboxRelay(relayOutbox, relayPublisher, cfg.OutboxRelay)
	relayCtx, stopOutboxRelay := context.WithCancel(cancelCtx)
	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		if relayErr := outboxRelay.Run(relayCtx); relayErr != nil && !errors.Is(relayErr, context.Canceled) {
			log.WithContext(cancelCtx).WithError(relayErr).Error("outbox relay stopped unexpectedly")
		}
	}()
	defer func() {
		stopOutboxRelay()
		<-relayDone
		publisher.Close()
	}()

	messagingFactory := messagingConn.NewMessenger(cfg.Messaging, cancelFtn)
	defer func() {
		_ = messagingFactory.Close(cancelCtx)
	}()
	subscriber, err := messagingFactory.Subscriber(cancelCtx)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create the subscriber")
	}

	sourceConnectorDB := db.NewSourceConnectorDB(database)
	datasetDB := db.NewDatasetDB(database,
		db.WithTransactionalOutbox(orderedOutbox, cfg.Topic),
		db.WithOutboxSignal(func() { messagingConn.NotifyOutboxSignal(outboxSignal) }),
	)

	datasetUseCase := usecase.NewDatasetUseCase(datasetDB)
	encoder := serializers.NewJSONSerializer()

	catalogClient := catalogclient.NewLocalCatalogClient()
	sourceConnectorUseCase := usecase.NewSourceUsecase(sourceConnectorDB, catalogClient)
	connectorRestDTOAdapter := adapter.NewRestSourceConnDTOAdapter(adapter.GetConnCfgToDTOFunc, adapter.GetConnCfgFromDTOFunc, encoder)
	filtersAdapter := adapter.NewFilterDTOAdapter()

	datasetDTOAdapter := adapter.NewDatasetDTOAdapter(encoder)
	routes := rest.NewDataRegistryHandlers(datasetUseCase, sourceConnectorUseCase, datasetDTOAdapter, connectorRestDTOAdapter, filtersAdapter).GetRoutes()

	log.Infof("%s API HTTP port: %d", serviceName, cfg.HTTPPort)
	log.Infof("%s API gRPC port: %d", serviceName, cfg.GRPCPort)

	restService := coreRest.NewService(routes, cfg.HTTPPort, serviceName)
	grpcService := registrygrpc.NewDataRegistryGrpcServer(sourceConnectorUseCase)
	materializationSubscriber := registrymessaging.NewMaterializationSubscriber(subscriber, datasetUseCase, cfg.MaterializationTopics)

	healthCheck := coreHealthCheck.NewMonitor(newHealthCheckConfig(cfg.Health))
	healthCheck = healthCheck.WithCpuCheck().WithDatabaseCheck().WithMemoryCheck().WithMessageBrokerCheck()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := restService.Connect(); err != nil {
			if err != http.ErrServerClosed {
				log.Errorf("unable to start the %s rest service: %v", serviceName, err)
			}
			quit <- syscall.SIGTERM
		}
	}()

	go func() {
		if err := grpcService.Connect(cfg.GRPCPort); err != nil {
			log.Errorf("unable to start the %s grpc service: %v", serviceName, err)
			quit <- syscall.SIGTERM
		}
	}()

	go func() {
		if err := materializationSubscriber.Start(cancelCtx); err != nil && !errors.Is(err, context.Canceled) {
			log.WithContext(cancelCtx).WithError(err).Error("materialization subscriber stopped unexpectedly")
			quit <- syscall.SIGTERM
		}
	}()

	go func() {
		if err := healthCheck.Connect(ctx); err != nil {
			if err != http.ErrServerClosed {
				log.Errorf("unable to start health check for the %s service: %v", serviceName, err)
			}
			quit <- syscall.SIGTERM
		}
	}()

	<-quit

	restService.Close()
	grpcService.Close()
	datasetDB.Close()
	healthCheck.Close()

	cancelFtn()
	traceShutdown()
	log.Trace(fmt.Sprintf("stopped the %s service", serviceName))
}

func readRegistryConfig() registryConfig {
	brokers := env.WithDefaultString("KAFKA_BROKER", "localhost:9092")
	dbName := env.WithDefaultString("DATA_REGISTRY_SERVICE_DB_NAME", "bighill_data_registry_db")
	dbConnectionString := postgresConnectionString(
		env.WithDefaultString("DATA_REGISTRY_SERVICE_DB_USER", "bighill_data_registry_db_user"),
		env.WithDefaultString("DATA_REGISTRY_SERVICE_DB_PASSWORD", ""),
		env.WithDefaultString("PGHOST", "127.0.0.1"),
		env.WithDefaultString("PGPORT", "5432"),
		dbName,
		env.WithDefaultString("PGSSLMODE", "disable"),
		env.WithDefaultInt("DATA_REGISTRY_SERVICE_DB_MAX_CONNECTIONS", "20"),
	)
	return registryConfig{
		ServiceName:        env.WithDefaultString("DATA_REGISTRY_SERVICE_NAME", "data-registry-service"),
		HTTPPort:           env.WithDefaultInt("DATA_REGISTRY_SERVICE_API_HTTP_PORT", "8081"),
		GRPCPort:           env.WithDefaultInt("DATA_REGISTRY_SERVICE_API_GRPC_PORT", "7071"),
		DBName:             dbName,
		DBConnectionString: dbConnectionString,
		Messaging: messagingConn.MessengerConfig{
			DlqURL:  env.WithDefaultString("DATA_REGISTRY_SERVICE_DLQ", "http://localhost:4566/data-registry-dev-env-queue/"),
			GroupID: env.WithDefaultString("DATA_REGISTRY_SERVICE_KAFKA_GROUP_ID", "data-registry-group"),
			Brokers: brokers,
		},
		OutboxBackend: env.WithDefaultString("DATA_REGISTRY_SERVICE_OUTBOX", "postgres"),
		OutboxRelay: messagingConn.OutboxRelayConfig{
			PollInterval:   time.Duration(env.WithDefaultInt("DATA_REGISTRY_SERVICE_OUTBOX_RELAY_POLL_MS", "250")) * time.Millisecond,
			FailureBackoff: time.Duration(env.WithDefaultInt("DATA_REGISTRY_SERVICE_OUTBOX_RELAY_FAILURE_BACKOFF_MS", "2000")) * time.Millisecond,
			BatchSize:      int32(env.WithDefaultInt("DATA_REGISTRY_SERVICE_OUTBOX_RELAY_BATCH_SIZE", "100")),
		},
		Topic: env.WithDefaultString("DATA_REGISTRY_SERVICE_TOPIC", "data_registry"),
		MaterializationTopics: registrymessaging.MaterializationTopics{
			FeatureMaterializer: env.WithDefaultString("DATA_REGISTRY_SERVICE_FEATURE_MATERIALIZER_SUBSCRIBER_TOPIC", "feature_materializer"),
		},
		Health: healthConfig{
			CpuThresholdPercentage:        env.WithDefaultInt("DATA_REGISTRY_SERVICE_HEALTHCHECK_CPU_THRESHOLD_PERCENT", "80"),
			MemFreeThresholdPercentage:    env.WithDefaultInt("DATA_REGISTRY_SERVICE_HEALTHCHECK_FREE_MEM_THRESHOLD_PERCENT", "20"),
			HealthCheckPort:               env.WithDefaultInt("DATA_REGISTRY_SERVICE_HEALTHCHECK_PORT", "5051"),
			DBConnectionString:            dbConnectionString,
			MessageBrokerConnectionString: brokers,
			DbLatencyThreshold:            secondsFromEnv("DATA_REGISTRY_SERVICE_HEALTHCHECK_DB_LATENCY_THRESHOLD_SECONDS", "5"),
			MessageBrokerLatencyThreshold: secondsFromEnv("DATA_REGISTRY_SERVICE_HEALTHCHECK_MSG_BROKER_LATENCY_THRESHOLD_SECONDS", "5"),
			ServiceLatencyThreshold:       secondsFromEnv("DATA_REGISTRY_SERVICE_HEALTHCHECK_SERVICE_LATENCY_THRESHOLD_SECONDS", "5"),
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

func newPostgresOutbox(database *coreDB.Database, backend string) (messagingConn.OutboxWriter, error) {
	log.Trace("newPostgresOutbox")

	if backend != "postgres" {
		return nil, fmt.Errorf("unsupported outbox backend %q", backend)
	}
	return messagingConn.NewPostgresOutbox(database.Pool, database.Name, "")
}
