package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"syscall"
	"time"

	"agent_registry_service/pkg/app"
	agentadapter "agent_registry_service/pkg/infra/network/adapter"
	agentclient "agent_registry_service/pkg/infra/network/client"
	agentmessaging "agent_registry_service/pkg/infra/network/messaging"
	agentrest "agent_registry_service/pkg/infra/network/rest"
	agentdb "agent_registry_service/pkg/infra/repo/db"
	agenttraining "agent_registry_service/pkg/infra/training"
	coreDB "lib/shared_lib/db"
	env "lib/shared_lib/env"
	coreHealthCheck "lib/shared_lib/healthcheck"
	"lib/shared_lib/lifecycle"
	logs "lib/shared_lib/logs"
	messagingConn "lib/shared_lib/messaging"
	serializers "lib/shared_lib/serializer"
	trace "lib/shared_lib/trace"
	shareduow "lib/shared_lib/uow"

	log "github.com/sirupsen/logrus"
)

var Version string

type agentRegistryConfig struct {
	ServiceName                    string
	HTTPPort                       int
	DBName                         string
	DBConnectionString             string
	Messaging                      messagingConn.MessengerConfig
	OutboxBackend                  string
	OutboxRelay                    messagingConn.OutboxRelayConfig
	AgentRegistryTopic             string
	TrainingTopic                  string
	AgentAdapterTrainingDispatcher string
	TrainingBaseURL                string
	TrainingTimeout                time.Duration
	InferenceBaseURL               string
	InferenceTimeout               time.Duration
	InferencePollInterval          time.Duration
	InferencePollAttempts          int
	Health                         healthConfig
	Lifecycle                      lifecycle.Config
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

	cfg := readAgentRegistryConfig()
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

	repository := agentdb.NewAgentRegistryRepository(database)
	unitOfWork := shareduow.New(database.Pool,
		shareduow.WithTransactionalOutbox(orderedOutbox),
		shareduow.WithOutboxSignal(func() { messagingConn.NotifyOutboxSignal(outboxSignal) }),
	)
	inferenceVerifier := agentclient.NewInferenceVerifier(agentclient.InferenceVerifierConfig{
		BaseURL:        cfg.InferenceBaseURL,
		RequestTimeout: cfg.InferenceTimeout,
		PollInterval:   cfg.InferencePollInterval,
		PollAttempts:   cfg.InferencePollAttempts,
	})
	eventBuilder := agentmessaging.NewAgentRegistryEventBuilder(cfg.AgentRegistryTopic)
	agentTrainingDispatcher, err := newAgentAdapterTrainingDispatcher(cfg)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to configure agent training dispatcher")
	}
	usecase := app.NewAgentRegistryUsecase(repository, unitOfWork, inferenceVerifier, eventBuilder, inferenceVerifier, agentTrainingDispatcher)
	dtoAdapter := agentadapter.NewAgentRegistryDTOAdapter(serializers.NewJSONSerializer())
	routes := agentrest.NewAgentRegistryHandlers(usecase, dtoAdapter).GetRoutes()
	restService := agentrest.NewService(routes, cfg.HTTPPort, serviceName)

	healthCheck := coreHealthCheck.NewMonitor(newHealthCheckConfig(cfg.Health))
	healthCheck = healthCheck.WithCpuCheck().WithDatabaseCheck().WithMemoryCheck().WithMessageBrokerCheck()

	components := []lifecycle.Component{
		lifecycle.CloserComponent("agent-registry-observability", func() error {
			traceShutdown()
			return nil
		}),
		lifecycle.CloserComponent("agent-registry-database", func() error {
			database.Close()
			return nil
		}),
		lifecycle.CloserComponent("agent-registry-publisher", func() error {
			outboxPublisher.Close()
			return nil
		}),
		lifecycle.HealthCheckComponent("agent-registry-healthcheck", healthCheck),
		lifecycle.ServerComponent("agent-registry-http", restService),
		lifecycle.WorkerComponent("agent-registry-outbox-relay", func(ctx context.Context) error {
			if err := outboxRelay.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		}),
	}

	startSubscriber := func(name string, topics []string, configure func(messagingConn.Subscriber)) {
		var factory messagingConn.Messenger
		components = append(components, lifecycle.NewFuncComponent(lifecycle.ComponentConfig{
			Name: "agent-registry-subscriber-" + name,
			Start: func(ctx context.Context) error {
				startedFactory, monitor, err := messagingConn.StartStreamSubscriber(ctx, messagingConn.StreamSubscriberConfig{
					Brokers:          cfg.Messaging.Brokers,
					DLQURL:           cfg.Messaging.DlqURL,
					BaseGroupID:      cfg.Messaging.GroupID,
					AutoOffsetReset:  cfg.Messaging.AutoOffsetReset,
					Cancel:           cancelFtn,
					Monitor:          healthCheck,
					OnUnexpectedStop: cancelFtn,
				}, name, topics, configure)
				if err != nil {
					return err
				}
				factory = startedFactory
				healthCheck = monitor
				<-ctx.Done()
				return ctx.Err()
			},
			Close: func() error {
				if factory == nil {
					return nil
				}
				return factory.Close(cancelCtx)
			},
		}))
	}

	startSubscriber("agent-training-completed", []string{cfg.TrainingTopic}, func(subscriber messagingConn.Subscriber) {
		messagingConn.AddListener(subscriber, agentmessaging.NewAgentTrainingCompletedEventListener(usecase))
	})
	startSubscriber("agent-training-failed", []string{cfg.TrainingTopic}, func(subscriber messagingConn.Subscriber) {
		messagingConn.AddListener(subscriber, agentmessaging.NewAgentTrainingFailedEventListener(usecase))
	})

	supervisor := lifecycle.NewSupervisorWithConfig(cfg.Lifecycle, components...)
	if err := supervisor.RunWithSignals(cancelCtx, syscall.SIGINT, syscall.SIGTERM); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.WithContext(cancelCtx).WithError(err).Errorf("%s service stopped with error", serviceName)
	}
	cancelFtn()
	log.Trace(fmt.Sprintf("stopped the %s service", serviceName))
}

func readAgentRegistryConfig() agentRegistryConfig {
	env.RequireServiceEnvironment()

	brokers := env.MustString("KAFKA_BROKER")
	dbConfig := coreDB.DatabaseConfig{}
	dbConfig.RequireDbName("AGENT_REGISTRY_SERVICE_DB_NAME")
	dbConfig.RequireDbUser("AGENT_REGISTRY_SERVICE_DB_USER")
	dbConfig.RequireDbPassword("AGENT_REGISTRY_SERVICE_DB_PASSWORD")
	dbConfig.RequireDbMaxConnections("AGENT_REGISTRY_SERVICE_DB_MAX_CONNECTIONS")
	dbConfig.RequireDbHost("PGHOST")
	dbConfig.RequireDbPort("PGPORT")
	dbConfig.RequireDbSSLMode("PGSSLMODE")
	dbName := dbConfig.GetName()
	dbConnectionString := dbConfig.GetConnectionString()
	return agentRegistryConfig{
		ServiceName:        env.MustString("AGENT_REGISTRY_SERVICE_NAME"),
		HTTPPort:           env.MustInt("AGENT_REGISTRY_SERVICE_API_HTTP_PORT"),
		DBName:             dbName,
		DBConnectionString: dbConnectionString,
		Messaging: messagingConn.MessengerConfig{
			DlqURL:          env.MustString("AGENT_REGISTRY_SERVICE_DLQ"),
			GroupID:         env.MustString("AGENT_REGISTRY_SERVICE_KAFKA_BASE_GROUP_ID"),
			Brokers:         brokers,
			AutoOffsetReset: env.MustString("AGENT_REGISTRY_SERVICE_KAFKA_AUTO_OFFSET_RESET"),
		},
		OutboxBackend: env.MustString("AGENT_REGISTRY_SERVICE_OUTBOX"),
		OutboxRelay: messagingConn.OutboxRelayConfig{
			PollInterval:   time.Duration(env.MustInt("AGENT_REGISTRY_SERVICE_OUTBOX_RELAY_POLL_MS")) * time.Millisecond,
			FailureBackoff: time.Duration(env.MustInt("AGENT_REGISTRY_SERVICE_OUTBOX_RELAY_FAILURE_BACKOFF_MS")) * time.Millisecond,
			BatchSize:      int32(env.MustInt("AGENT_REGISTRY_SERVICE_OUTBOX_RELAY_BATCH_SIZE")),
		},
		AgentRegistryTopic:             env.MustString("AGENT_REGISTRY_SERVICE_TOPIC"),
		TrainingTopic:                  env.MustString("AGENT_REGISTRY_SERVICE_TRAINING_TOPIC"),
		AgentAdapterTrainingDispatcher: env.MustString("AGENT_REGISTRY_SERVICE_AGENT_TRAINING_DISPATCHER"),
		TrainingBaseURL:                env.MustString("AGENT_REGISTRY_SERVICE_TRAINING_BASE_URL"),
		TrainingTimeout:                secondsFromEnv("AGENT_REGISTRY_SERVICE_TRAINING_REQUEST_TIMEOUT_SECONDS"),
		InferenceBaseURL:               env.MustString("AGENT_REGISTRY_SERVICE_INFERENCE_BASE_URL"),
		InferenceTimeout:               secondsFromEnv("AGENT_REGISTRY_SERVICE_INFERENCE_REQUEST_TIMEOUT_SECONDS"),
		InferencePollInterval:          time.Duration(env.MustInt("AGENT_REGISTRY_SERVICE_INFERENCE_EVAL_POLL_INTERVAL_MS")) * time.Millisecond,
		InferencePollAttempts:          env.MustInt("AGENT_REGISTRY_SERVICE_INFERENCE_EVAL_POLL_ATTEMPTS"),
		Health: healthConfig{
			CpuThresholdPercentage:                    env.MustInt("AGENT_REGISTRY_SERVICE_HEALTHCHECK_CPU_THRESHOLD_PERCENT"),
			MemFreeThresholdPercent:                   env.MustInt("AGENT_REGISTRY_SERVICE_HEALTHCHECK_FREE_MEM_THRESHOLD_PERCENT"),
			HealthCheckPort:                           env.MustInt("AGENT_REGISTRY_SERVICE_HEALTHCHECK_PORT"),
			DBConnectionString:                        dbConnectionString,
			MessageBrokerConnectionString:             brokers,
			DbLatencyThreshold:                        secondsFromEnv("AGENT_REGISTRY_SERVICE_HEALTHCHECK_DB_LATENCY_THRESHOLD_SECONDS"),
			MessageBrokerLatencyThreshold:             secondsFromEnv("AGENT_REGISTRY_SERVICE_HEALTHCHECK_MSG_BROKER_LATENCY_THRESHOLD_SECONDS"),
			ServiceLatencyThreshold:                   secondsFromEnv("AGENT_REGISTRY_SERVICE_HEALTHCHECK_SERVICE_LATENCY_THRESHOLD_SECONDS"),
			MessageBrokerSubscriberMaxPollSilence:     secondsFromEnv("AGENT_REGISTRY_SERVICE_HEALTHCHECK_MESSAGE_BROKER_SUBSCRIBER_MAX_POLL_SILENCE_SECONDS"),
			MessageBrokerSubscriberMaxProgressSilence: secondsFromEnv("AGENT_REGISTRY_SERVICE_HEALTHCHECK_MESSAGE_BROKER_SUBSCRIBER_MAX_PROGRESS_SILENCE_SECONDS"),
			MessageBrokerSubscriberMaxLag:             int64(env.MustInt("AGENT_REGISTRY_SERVICE_HEALTHCHECK_MESSAGE_BROKER_SUBSCRIBER_MAX_LAG")),
		},
		Lifecycle: lifecycle.Config{
			ReadinessTimeout: secondsFromEnv("AGENT_REGISTRY_SERVICE_LIFECYCLE_READINESS_TIMEOUT_SECONDS"),
			DrainTimeout:     secondsFromEnv("AGENT_REGISTRY_SERVICE_LIFECYCLE_DRAIN_TIMEOUT_SECONDS"),
			CloseTimeout:     secondsFromEnv("AGENT_REGISTRY_SERVICE_LIFECYCLE_CLOSE_TIMEOUT_SECONDS"),
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

func newAgentAdapterTrainingDispatcher(cfg agentRegistryConfig) (app.AgentAdapterTrainingDispatcher, error) {
	log.Trace("newAgentAdapterTrainingDispatcher")

	switch cfg.AgentAdapterTrainingDispatcher {
	case "training-service":
		return agenttraining.NewTrainingServiceAgentAdapterDispatcher(agenttraining.TrainingServiceAgentAdapterDispatcherConfig{
			BaseURL:        cfg.TrainingBaseURL,
			RequestTimeout: cfg.TrainingTimeout,
		}), nil
	default:
		return nil, fmt.Errorf("unsupported agent training dispatcher %q", cfg.AgentAdapterTrainingDispatcher)
	}
}

func newPostgresOutbox(database *coreDB.Database, backend string) (messagingConn.OutboxWriter, error) {
	log.Trace("newPostgresOutbox")

	if backend != "postgres" {
		return nil, fmt.Errorf("unsupported outbox backend %q", backend)
	}
	return messagingConn.NewPostgresOutbox(database.Pool, database.Name, "")
}
