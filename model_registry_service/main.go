package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"model_registry_service/pkg/app"
	registryadapter "model_registry_service/pkg/infra/network/adapter"
	registrykubernetes "model_registry_service/pkg/infra/network/kubernetes"
	localserving "model_registry_service/pkg/infra/network/kubernetes/localserving"
	registrymessaging "model_registry_service/pkg/infra/network/messaging"
	registryrest "model_registry_service/pkg/infra/network/rest"
	modeldb "model_registry_service/pkg/infra/repo/db"

	coreDB "lib/shared_lib/db"
	env "lib/shared_lib/env"
	coreHealthCheck "lib/shared_lib/healthcheck"
	"lib/shared_lib/lifecycle"
	logs "lib/shared_lib/logs"
	messagingConn "lib/shared_lib/messaging"
	serializers "lib/shared_lib/serializer"
	sharedTenant "lib/shared_lib/tenant"
	trace "lib/shared_lib/trace"
	shareduow "lib/shared_lib/uow"

	log "github.com/sirupsen/logrus"
)

var Version string

type registryConfig struct {
	ServiceName        string
	HTTPPort           int
	DBName             string
	DBConnectionString string
	Messaging          messagingConn.MessengerConfig
	OutboxBackend      string
	OutboxRelay        messagingConn.OutboxRelayConfig
	Topics             registrymessaging.ModelRegistryTopics
	ProfileTopic       string
	Serving            servingConfig
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

type servingConfig struct {
	Enabled          bool
	Backend          string
	LocalStore       string
	LocalResyncEvery time.Duration
	Namespace        string
	CRDGroup         string
	CRDVersion       string
	CRDResource      string
	CRDKind          string
	StatusPollEvery  time.Duration
}

func init() {
	logs.Init()
}

func main() {
	ctx := context.Background()
	cancelCtx, cancelFtn := context.WithCancel(ctx)
	defer cancelFtn()

	cfg := readModelRegistryConfig()
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

	modelRepository := modeldb.NewModelRepository(database)
	modelUnitOfWork := shareduow.New(database.Pool,
		shareduow.WithTransactionalOutbox(orderedOutbox),
		shareduow.WithOutboxSignal(func() { messagingConn.NotifyOutboxSignal(outboxSignal) }),
	)
	tenantDB := sharedTenant.NewPostgresProjectionStore(database)
	var servingObserver interface {
		Start(context.Context) error
	}
	modelUsecaseOptions := []app.ModelRegistryOption{}
	var servingDeployer app.ModelServingDeployer
	if cfg.Serving.Enabled {
		servingDeployer, err = newServingBackend(cfg.Serving)
		if err != nil {
			log.WithContext(cancelCtx).WithError(err).Fatal("unable to create served model serving backend")
		}
		modelUsecaseOptions = append(modelUsecaseOptions, app.WithModelServingDeployer(servingDeployer))
	} else {
		log.WithContext(cancelCtx).Info("served model reconciliation disabled; model serving status will only change through explicit registry events")
	}
	modelEventBuilder := registrymessaging.NewModelEventBuilder(cfg.Topics.ModelRegistry)
	modelUsecase := app.NewModelRegistryUsecase(modelRepository, modelUnitOfWork, modelEventBuilder, modelUsecaseOptions...)
	modelDTOAdapter := registryadapter.NewModelDTOAdapter(serializers.NewJSONSerializer())
	modelRoutes := registryrest.NewModelHandlers(modelUsecase, modelDTOAdapter).GetRoutes()
	restService := registryrest.NewService(modelRoutes, cfg.HTTPPort, serviceName)
	if cfg.Serving.Enabled {
		servingObserver, err = newServingObserver(cfg.Serving, servingDeployer, modelUsecase)
		if err != nil {
			log.WithContext(cancelCtx).WithError(err).Fatal("unable to create served model observer")
		}
	}
	healthCheck := coreHealthCheck.NewMonitor(newHealthCheckConfig(cfg.Health))
	healthCheck = healthCheck.WithCpuCheck().WithDatabaseCheck().WithMemoryCheck().WithMessageBrokerCheck()

	components := []lifecycle.Component{
		lifecycle.CloserComponent("model-registry-observability", func() error {
			traceShutdown()
			return nil
		}),
		lifecycle.CloserComponent("model-registry-database", func() error {
			database.Close()
			return nil
		}),
		lifecycle.CloserComponent("model-registry-publisher", func() error {
			outboxPublisher.Close()
			return nil
		}),
		lifecycle.HealthCheckComponent("model-registry-healthcheck", healthCheck),
		lifecycle.ServerComponent("model-registry-http", restService),
		lifecycle.WorkerComponent("model-registry-outbox-relay", func(ctx context.Context) error {
			if err := outboxRelay.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		}),
	}

	startSubscriber := func(name string, topics []string, configure func(messagingConn.Subscriber)) {
		var factory messagingConn.Messenger
		components = append(components, lifecycle.NewFuncComponent(lifecycle.ComponentConfig{
			Name: "model-registry-subscriber-" + name,
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

	startSubscriber("training-completed", []string{cfg.Topics.Training}, func(subscriber messagingConn.Subscriber) {
		messagingConn.AddListener(subscriber, registrymessaging.NewModelTrainingCompletedEventListener(modelUsecase))
	})
	startSubscriber("training-failed", []string{cfg.Topics.Training}, func(subscriber messagingConn.Subscriber) {
		messagingConn.AddListener(subscriber, registrymessaging.NewModelTrainingFailedEventListener(modelUsecase))
	})
	startSubscriber("promotion-report-ready", []string{cfg.Topics.Training}, func(subscriber messagingConn.Subscriber) {
		messagingConn.AddListener(subscriber, registrymessaging.NewPromotionReportReadyEventListener(modelUsecase))
	})
	startSubscriber("model-artifact-ingested", []string{cfg.Topics.Ingestion}, func(subscriber messagingConn.Subscriber) {
		messagingConn.AddListener(subscriber, registrymessaging.NewModelArtifactIngestedEventListener(modelUsecase))
	})
	startSubscriber("tenant-created", []string{cfg.ProfileTopic}, func(subscriber messagingConn.Subscriber) {
		sharedTenant.ConfigureProfileProjectionErrorPolicy(subscriber)
		messagingConn.AddListener(subscriber, sharedTenant.NewUserCreatedProjectionListener(tenantDB))
	})
	startSubscriber("tenant-updated", []string{cfg.ProfileTopic}, func(subscriber messagingConn.Subscriber) {
		sharedTenant.ConfigureProfileProjectionErrorPolicy(subscriber)
		messagingConn.AddListener(subscriber, sharedTenant.NewUserUpdatedProjectionListener(tenantDB))
	})
	startSubscriber("tenant-deleted", []string{cfg.ProfileTopic}, func(subscriber messagingConn.Subscriber) {
		sharedTenant.ConfigureProfileProjectionErrorPolicy(subscriber)
		messagingConn.AddListener(subscriber, sharedTenant.NewUserDeletedProjectionListener(tenantDB))
	})

	if servingObserver != nil {
		components = append(components, lifecycle.WorkerComponent("model-registry-serving-observer", func(ctx context.Context) error {
			if err := servingObserver.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		}))
	}

	supervisor := lifecycle.NewSupervisorWithConfig(cfg.Lifecycle, components...)
	if err := supervisor.RunWithSignals(cancelCtx, syscall.SIGINT, syscall.SIGTERM); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.WithContext(cancelCtx).WithError(err).Errorf("%s service stopped with error", serviceName)
	}
	cancelFtn()
	log.Trace(fmt.Sprintf("stopped the %s service", serviceName))
}

func readModelRegistryConfig() registryConfig {
	brokers := env.WithDefaultString("KAFKA_BROKER", "localhost:9092")
	dbName := env.WithDefaultString("MODEL_REGISTRY_SERVICE_DB_NAME", "bighill_model_registry_db")
	dbConnectionString := postgresConnectionString(
		env.WithDefaultString("MODEL_REGISTRY_SERVICE_DB_USER", "bighill_model_registry_db_user"),
		env.WithDefaultString("MODEL_REGISTRY_SERVICE_DB_PASSWORD", ""),
		env.WithDefaultString("PGHOST", "127.0.0.1"),
		env.WithDefaultString("PGPORT", "5432"),
		dbName,
		env.WithDefaultString("PGSSLMODE", "disable"),
		env.WithDefaultInt("MODEL_REGISTRY_SERVICE_DB_MAX_CONNECTIONS", "20"),
	)
	return registryConfig{
		ServiceName:        env.WithDefaultString("MODEL_REGISTRY_SERVICE_NAME", "model-registry-service"),
		HTTPPort:           env.WithDefaultInt("MODEL_REGISTRY_SERVICE_API_HTTP_PORT", "8084"),
		DBName:             dbName,
		DBConnectionString: dbConnectionString,
		Messaging: messagingConn.MessengerConfig{
			DlqURL:          env.WithDefaultString("MODEL_REGISTRY_SERVICE_DLQ", "http://localhost:4566/model-registry-dev-env-queue/"),
			GroupID:         env.WithDefaultString("MODEL_REGISTRY_SERVICE_KAFKA_BASE_GROUP_ID", "model-registry"),
			Brokers:         brokers,
			AutoOffsetReset: env.WithDefaultString("MODEL_REGISTRY_SERVICE_KAFKA_AUTO_OFFSET_RESET", "earliest"),
		},
		OutboxBackend: env.WithDefaultString("MODEL_REGISTRY_SERVICE_OUTBOX", "postgres"),
		OutboxRelay: messagingConn.OutboxRelayConfig{
			PollInterval:   time.Duration(env.WithDefaultInt("MODEL_REGISTRY_SERVICE_OUTBOX_RELAY_POLL_MS", "250")) * time.Millisecond,
			FailureBackoff: time.Duration(env.WithDefaultInt("MODEL_REGISTRY_SERVICE_OUTBOX_RELAY_FAILURE_BACKOFF_MS", "2000")) * time.Millisecond,
			BatchSize:      int32(env.WithDefaultInt("MODEL_REGISTRY_SERVICE_OUTBOX_RELAY_BATCH_SIZE", "100")),
		},
		Topics: registrymessaging.ModelRegistryTopics{
			ModelRegistry: env.WithDefaultString("MODEL_REGISTRY_SERVICE_TOPIC", "model_registry"),
			Training:      env.WithDefaultString("MODEL_REGISTRY_SERVICE_TRAINING_SUBSCRIBER_TOPIC", "training"),
			Ingestion:     env.WithDefaultString("MODEL_REGISTRY_SERVICE_INGESTION_SUBSCRIBER_TOPIC", "ingestion"),
		},
		ProfileTopic: env.WithDefaultString("MODEL_REGISTRY_SERVICE_PROFILE_SUBSCRIBER_TOPIC", "profile"),
		Serving: servingConfig{
			Enabled:          env.WithDefaultBool("MODEL_REGISTRY_SERVICE_SERVING_RECONCILIATION_ENABLED", true),
			Backend:          env.WithDefaultString("MODEL_REGISTRY_SERVICE_SERVING_BACKEND", defaultServingBackend()),
			LocalStore:       env.WithDefaultString("MODEL_REGISTRY_SERVICE_SERVING_LOCAL_STORE_PATH", defaultLocalStorePath()),
			LocalResyncEvery: time.Duration(env.WithDefaultInt("MODEL_REGISTRY_SERVICE_SERVING_LOCAL_RESYNC_SECONDS", "30")) * time.Second,
			Namespace:        env.WithDefaultString("MODEL_REGISTRY_SERVICE_SERVING_NAMESPACE", "default"),
			CRDGroup:         env.WithDefaultString("MODEL_REGISTRY_SERVICE_SERVING_CRD_GROUP", "serving.bighill.io"),
			CRDVersion:       env.WithDefaultString("MODEL_REGISTRY_SERVICE_SERVING_CRD_VERSION", "v1alpha1"),
			CRDResource:      env.WithDefaultString("MODEL_REGISTRY_SERVICE_SERVING_CRD_RESOURCE", "servedmodels"),
			CRDKind:          env.WithDefaultString("MODEL_REGISTRY_SERVICE_SERVING_CRD_KIND", "ServedModel"),
			StatusPollEvery:  time.Duration(env.WithDefaultInt("MODEL_REGISTRY_SERVICE_SERVING_STATUS_POLL_MS", "1000")) * time.Millisecond,
		},
		Health: healthConfig{
			CpuThresholdPercentage:                    env.WithDefaultInt("MODEL_REGISTRY_SERVICE_HEALTHCHECK_CPU_THRESHOLD_PERCENT", "80"),
			MemFreeThresholdPercent:                   env.WithDefaultInt("MODEL_REGISTRY_SERVICE_HEALTHCHECK_FREE_MEM_THRESHOLD_PERCENT", "20"),
			HealthCheckPort:                           env.WithDefaultInt("MODEL_REGISTRY_SERVICE_HEALTHCHECK_PORT", "5060"),
			DBConnectionString:                        dbConnectionString,
			MessageBrokerConnectionString:             brokers,
			DbLatencyThreshold:                        secondsFromEnv("MODEL_REGISTRY_SERVICE_HEALTHCHECK_DB_LATENCY_THRESHOLD_SECONDS", "5"),
			MessageBrokerLatencyThreshold:             secondsFromEnv("MODEL_REGISTRY_SERVICE_HEALTHCHECK_MSG_BROKER_LATENCY_THRESHOLD_SECONDS", "5"),
			ServiceLatencyThreshold:                   secondsFromEnv("MODEL_REGISTRY_SERVICE_HEALTHCHECK_SERVICE_LATENCY_THRESHOLD_SECONDS", "5"),
			MessageBrokerSubscriberMaxPollSilence:     secondsFromEnv("MODEL_REGISTRY_SERVICE_HEALTHCHECK_MESSAGE_BROKER_SUBSCRIBER_MAX_POLL_SILENCE_SECONDS", "30"),
			MessageBrokerSubscriberMaxProgressSilence: secondsFromEnv("MODEL_REGISTRY_SERVICE_HEALTHCHECK_MESSAGE_BROKER_SUBSCRIBER_MAX_PROGRESS_SILENCE_SECONDS", "90"),
			MessageBrokerSubscriberMaxLag:             int64(env.WithDefaultInt("MODEL_REGISTRY_SERVICE_HEALTHCHECK_MESSAGE_BROKER_SUBSCRIBER_MAX_LAG", "100000")),
		},
		Lifecycle: lifecycle.Config{
			ReadinessTimeout: secondsFromEnv("MODEL_REGISTRY_SERVICE_LIFECYCLE_READINESS_TIMEOUT_SECONDS", "30"),
			DrainTimeout:     secondsFromEnv("MODEL_REGISTRY_SERVICE_LIFECYCLE_DRAIN_TIMEOUT_SECONDS", "30"),
			CloseTimeout:     secondsFromEnv("MODEL_REGISTRY_SERVICE_LIFECYCLE_CLOSE_TIMEOUT_SECONDS", "10"),
		},
	}
}

func defaultLocalStorePath() string {
	log.Trace("defaultLocalStorePath")

	return filepath.Join(os.TempDir(), "bighill", "local_served_models", "served_models.json")
}

func newServingBackend(cfg servingConfig) (app.ModelServingDeployer, error) {
	log.Trace("newServingBackend")

	switch cfg.Backend {
	case "local":
		return localserving.NewAdapter(cfg.Namespace, cfg.LocalStore)
	case "kubernetes":
		return registrykubernetes.NewServedModelAdapter(registrykubernetes.ServedModelConfig{
			Namespace:    cfg.Namespace,
			Group:        cfg.CRDGroup,
			Version:      cfg.CRDVersion,
			Resource:     cfg.CRDResource,
			Kind:         cfg.CRDKind,
			PollInterval: cfg.StatusPollEvery,
		})
	default:
		return nil, fmt.Errorf("unsupported model registry serving backend %q", cfg.Backend)
	}
}

func newServingObserver(cfg servingConfig, deployer app.ModelServingDeployer, recorder localserving.ServingStatusRecorder) (interface {
	Start(context.Context) error
}, error) {
	log.Trace("newServingObserver")

	switch cfg.Backend {
	case "local":
		adapter, ok := deployer.(*localserving.Adapter)
		if !ok {
			return nil, fmt.Errorf("local serving deployer has unexpected type")
		}
		return localserving.NewStatusObserver(adapter, recorder, cfg.LocalResyncEvery)
	case "kubernetes":
		adapter, ok := deployer.(*registrykubernetes.ServedModelAdapter)
		if !ok {
			return nil, fmt.Errorf("kubernetes serving deployer has unexpected type")
		}
		return registrykubernetes.NewServedModelStatusObserver(adapter, recorder, cfg.StatusPollEvery)
	default:
		return nil, fmt.Errorf("unsupported model registry serving backend %q", cfg.Backend)
	}
}

func defaultServingBackend() string {
	log.Trace("defaultServingBackend")

	return "kubernetes"
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
