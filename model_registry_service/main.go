package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"model_registry_service/pkg/app"
	registryk8s "model_registry_service/pkg/infra/network/k8s"
	localserving "model_registry_service/pkg/infra/network/localserving"
	registrymessaging "model_registry_service/pkg/infra/network/messaging"
	modeldb "model_registry_service/pkg/infra/repo/db"

	coreDB "lib/shared_lib/db"
	env "lib/shared_lib/env"
	coreHealthCheck "lib/shared_lib/healthcheck"
	logs "lib/shared_lib/logs"
	messagingConn "lib/shared_lib/messaging"
	sharedTenant "lib/shared_lib/tenant"
	trace "lib/shared_lib/trace"

	log "github.com/sirupsen/logrus"
)

var Version string

type registryConfig struct {
	ServiceName        string
	DBName             string
	DBConnectionString string
	Messaging          messagingConn.MessengerConfig
	OutboxBackend      string
	OutboxRelay        messagingConn.OutboxRelayConfig
	Topics             registrymessaging.ModelRegistryTopics
	ProfileTopic       string
	Serving            servingConfig
	Health             healthConfig
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
	defer traceShutdown()

	database, err := coreDB.InitDatabase(cancelCtx, cfg.DBName, cfg.DBConnectionString, log.StandardLogger())
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("database init failed")
	}
	defer database.Close()

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
		outboxPublisher.Close()
	}()

	subscriberFactories := []messagingConn.Messenger{}
	defer func() {
		for _, factory := range subscriberFactories {
			_ = factory.Close(cancelCtx)
		}
	}()

	modelRepository := modeldb.NewModelRepository(database,
		modeldb.WithTransactionalOutbox(orderedOutbox, cfg.Topics.ModelRegistry),
		modeldb.WithOutboxSignal(func() { messagingConn.NotifyOutboxSignal(outboxSignal) }),
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
	modelUsecase := app.NewModelRegistryUsecase(modelRepository, modelUsecaseOptions...)
	if cfg.Serving.Enabled {
		servingObserver, err = newServingObserver(cfg.Serving, servingDeployer, modelUsecase)
		if err != nil {
			log.WithContext(cancelCtx).WithError(err).Fatal("unable to create served model observer")
		}
	}
	healthCheck := coreHealthCheck.NewMonitor(newHealthCheckConfig(cfg.Health))
	healthCheck = healthCheck.WithCpuCheck().WithDatabaseCheck().WithMemoryCheck().WithMessageBrokerCheck()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	startSubscriber := func(name string, topics []string, configure func(messagingConn.Subscriber)) {
		factory, monitor, err := messagingConn.StartStreamSubscriber(cancelCtx, messagingConn.StreamSubscriberConfig{
			Brokers:          cfg.Messaging.Brokers,
			DLQURL:           cfg.Messaging.DlqURL,
			BaseGroupID:      cfg.Messaging.GroupID,
			AutoOffsetReset:  cfg.Messaging.AutoOffsetReset,
			Cancel:           cancelFtn,
			Monitor:          healthCheck,
			OnUnexpectedStop: func() { quit <- syscall.SIGTERM },
		}, name, topics, configure)
		if err != nil {
			log.WithContext(cancelCtx).WithError(err).Fatalf("unable to create %s subscriber", name)
		}
		healthCheck = monitor
		subscriberFactories = append(subscriberFactories, factory)
	}

	startSubscriber("training-completed", []string{cfg.Topics.Training}, func(subscriber messagingConn.Subscriber) {
		messagingConn.AddListener(subscriber, registrymessaging.NewModelTrainingCompletedEventListener(modelUsecase))
	})
	startSubscriber("training-failed", []string{cfg.Topics.Training}, func(subscriber messagingConn.Subscriber) {
		messagingConn.AddListener(subscriber, registrymessaging.NewModelTrainingFailedEventListener(modelUsecase))
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

	go func() {
		if err := healthCheck.Connect(cancelCtx); err != nil {
			if err != http.ErrServerClosed {
				log.Fatalf("unable to start health check for the %s service: %v", serviceName, err)
			}
			quit <- syscall.SIGTERM
		}
	}()
	if servingObserver != nil {
		go func() {
			if err := servingObserver.Start(cancelCtx); err != nil && !errors.Is(err, context.Canceled) {
				log.WithContext(cancelCtx).WithError(err).Error("served model status observer stopped unexpectedly")
				quit <- syscall.SIGTERM
			}
		}()
	}

	<-quit

	cancelFtn()
	healthCheck.Close()
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
		return registryk8s.NewServedModelAdapter(registryk8s.ServedModelConfig{
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
		adapter, ok := deployer.(*registryk8s.ServedModelAdapter)
		if !ok {
			return nil, fmt.Errorf("kubernetes serving deployer has unexpected type")
		}
		return registryk8s.NewServedModelStatusObserver(adapter, recorder, cfg.StatusPollEvery)
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
