package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"time"

	"inference_service/pkg/app"
	"inference_service/pkg/domain/model"
	"inference_service/pkg/infra/generation"
	inferencemodelserving "inference_service/pkg/infra/modelserving"
	inferenceadapter "inference_service/pkg/infra/network/adapter"
	inferencegrpc "inference_service/pkg/infra/network/grpc"
	inferencemessaging "inference_service/pkg/infra/network/messaging"
	inferencerest "inference_service/pkg/infra/network/rest"
	inferencepreference "inference_service/pkg/infra/preference"
	inferencedb "inference_service/pkg/infra/repo/db"
	"inference_service/pkg/infra/retrieval"
	inferencetemporal "inference_service/pkg/infra/temporalworker"
	inferencetools "inference_service/pkg/infra/tools"

	coreBucket "lib/shared_lib/bucket"
	coreDB "lib/shared_lib/db"
	env "lib/shared_lib/env"
	coreHealthCheck "lib/shared_lib/healthcheck"
	"lib/shared_lib/lifecycle"
	logs "lib/shared_lib/logs"
	messagingConn "lib/shared_lib/messaging"
	serializers "lib/shared_lib/serializer"
	sharedTenant "lib/shared_lib/tenant"
	trace "lib/shared_lib/trace"
	"lib/shared_lib/transport"
	shareduow "lib/shared_lib/uow"
	"lib/shared_lib/userevents"

	log "github.com/sirupsen/logrus"
	"go.temporal.io/sdk/client"
)

var Version string

type inferenceConfig struct {
	ServiceName         string
	DBName              string
	DBConnectionString  string
	Messaging           messagingConn.MessengerConfig
	OutboxBackend       string
	OutboxRelay         messagingConn.OutboxRelayConfig
	Topics              inferencemessaging.InferenceTopics
	TenantTopic         string
	FeatureMaterializer inferencegrpc.FeatureMaterializerClientConfig
	ToolService         inferencetools.ToolServiceClientConfig
	Generation          generationConfig
	Reranker            rerankerConfig
	QueryTransformer    queryTransformerConfig
	ModelServing        modelServingConfig
	Agent               agentConfig
	Temporal            temporalConfig
	UserEvents          userevents.Config
	PreferenceDataset   preferenceDatasetConfig
	GRPCPort            int
	HTTPPort            int
	HTTPServer          httpServerConfig
	Health              healthConfig
	Lifecycle           lifecycle.Config
}

type generationConfig struct {
	RequestTimeout   time.Duration
	MaxOutputTokens  int
	PromptStrategy   string
	MaxContextTokens int
	MaxContextChunks int
	RAGMergeStrategy string
}

type rerankerConfig struct {
	Provider            string
	URL                 string
	Model               string
	RequestTimeout      time.Duration
	CandidateMultiplier int
}

type queryTransformerConfig struct {
	Provider       string
	RequestTimeout time.Duration
}

type modelServingConfig struct {
	Endpoint       string
	RequestTimeout time.Duration
	LoadTimeout    time.Duration
	PollInterval   time.Duration
}

type agentConfig struct {
	MaxStepsCap       int
	TokenBudgetCap    int
	WallMsCap         int
	RunReaperInterval time.Duration
	RunReaperGrace    time.Duration
}

type temporalConfig struct {
	Address              string
	Namespace            string
	TaskQueue            string
	ConnectTimeout       time.Duration
	ConnectRetryInterval time.Duration
}

type preferenceDatasetConfig struct {
	ExportEnabled    bool
	URITemplate      string
	MinExamples      int
	Limit            int
	BucketRegion     string
	UploadPartSizeMB int64
}

type httpServerConfig struct {
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
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

type temporalDialFunc func(client.Options) (client.Client, error)

func init() {
	logs.Init()
}

func dialTemporalClient(ctx context.Context, cfg temporalConfig) (client.Client, error) {
	log.Trace("dialTemporalClient")

	return dialTemporalClientWith(ctx, cfg, client.Dial)
}

func dialTemporalClientWith(ctx context.Context, cfg temporalConfig, dial temporalDialFunc) (client.Client, error) {
	log.Trace("dialTemporalClientWith")

	dialCtx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer cancel()
	options := client.Options{
		HostPort:  cfg.Address,
		Namespace: cfg.Namespace,
	}
	var lastErr error
	for {
		temporalClient, err := dial(options)
		if err == nil {
			return temporalClient, nil
		}
		lastErr = err
		log.WithContext(ctx).WithError(err).WithFields(log.Fields{
			"temporal_address":   cfg.Address,
			"temporal_namespace": cfg.Namespace,
		}).Warn("Temporal not ready; retrying")
		select {
		case <-dialCtx.Done():
			return nil, fmt.Errorf("connect to Temporal at %s namespace %s: %w: %v", cfg.Address, cfg.Namespace, dialCtx.Err(), lastErr)
		case <-time.After(cfg.ConnectRetryInterval):
		}
	}
}

func main() {
	ctx := context.Background()
	cancelCtx, cancelFtn := context.WithCancel(ctx)
	defer cancelFtn()

	cfg := readInferenceConfig()
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

	modelRepository := inferencedb.NewInferenceModelRepository(database)
	datasetRepository := inferencedb.NewInferenceDatasetRepository(database)
	endpointRepository := inferencedb.NewPublishedEndpointRepository(database)
	agentSpecRepository := inferencedb.NewAgentSpecRepository(database)
	capabilityReportRepository := inferencedb.NewCapabilityReportRepository(database)
	requestRepository := inferencedb.NewInferenceRequestRepository(database)
	trajectoryRepository := inferencedb.NewAgentTrajectoryRepository(database)
	feedbackRepository := inferencedb.NewInferenceFeedbackRepository(database)
	lineageEvalRepository := inferencedb.NewLineageEvalRepository(database)
	inferenceUnitOfWork := shareduow.New(database.Pool,
		shareduow.WithTransactionalOutbox(orderedOutbox),
		shareduow.WithOutboxSignal(func() { messagingConn.NotifyOutboxSignal(outboxSignal) }),
	)
	tenantDB := sharedTenant.NewPostgresProjectionStore(database)
	if err := inferencegrpc.ValidateFeatureMaterializerClientConfig(cfg.FeatureMaterializer); err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("invalid feature materializer client configuration")
	}
	retrievalClient, err := inferencegrpc.NewFeatureMaterializerClient(cancelCtx, cfg.FeatureMaterializer)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create feature materializer client")
	}
	toolInvoker, closeToolInvoker, err := newToolInvoker(cancelCtx, cfg.ToolService, retrievalClient)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create tool invoker")
	}
	userEventPublisher := userevents.Publisher(userevents.NewNoopPublisher())
	if cfg.UserEvents.Enabled {
		userEventPublisher, err = userevents.NewRedisPublisher(cfg.UserEvents)
		if err != nil {
			log.WithContext(cancelCtx).WithError(err).Fatal("failed to initialize user event publisher")
		}
	}
	generationAdapters := newGenerationAdapters(cfg.Generation)
	temporalClient, err := dialTemporalClient(cancelCtx, cfg.Temporal)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to connect to Temporal")
	}
	agentRunWorkflowStarter := inferencetemporal.NewAgentRunWorkflowStarter(temporalClient, cfg.Temporal.TaskQueue)
	reranker, err := newRerankerAdapter(cfg.Reranker)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create reranker adapter")
	}
	queryTransformer, err := newQueryTransformer(cfg.QueryTransformer, generationAdapters)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create query transformer")
	}
	promptStrategy, err := promptStrategyFromConfig(cfg.Generation)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("invalid prompt strategy configuration")
	}
	defaultRAGMergeStrategy, err := model.ToRAGMergeStrategy(cfg.Generation.RAGMergeStrategy)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("invalid rag merge strategy configuration")
	}
	inferenceOptions := []app.InferenceOption{
		app.WithInferenceDatasetRepository(datasetRepository),
		app.WithPublishedEndpointRepository(endpointRepository),
		app.WithAgentSpecRepository(agentSpecRepository),
		app.WithCapabilityReportRepository(capabilityReportRepository),
		app.WithInferenceRequestRepository(requestRepository),
		app.WithAgentTrajectoryRepository(trajectoryRepository),
		app.WithInferenceFeedbackRepository(feedbackRepository),
		app.WithLineageEvalSetRepository(lineageEvalRepository),
		app.WithInferenceUnitOfWork(inferenceUnitOfWork),
		app.WithRetrievalClient(retrievalClient),
		app.WithToolInvoker(toolInvoker),
		app.WithAgentRunWorkflowStarter(agentRunWorkflowStarter),
		app.WithUserEventPublisher(userEventPublisher),
		app.WithGenerationAdapters(generationAdapters),
		app.WithPromptStrategy(promptStrategy),
		app.WithDefaultRAGMergeStrategy(defaultRAGMergeStrategy),
		app.WithContextPacker(app.NewContextWindowPacker(promptStrategy)),
		app.WithPromptBuilder(app.NewDefaultPromptBuilder(promptStrategy)),
	}
	if reranker != nil {
		inferenceOptions = append(inferenceOptions,
			app.WithReranker(reranker),
			app.WithRerankCandidateMultiplier(cfg.Reranker.CandidateMultiplier),
		)
	}
	if queryTransformer != nil {
		inferenceOptions = append(inferenceOptions,
			app.WithQueryTransformer(queryTransformer),
			app.WithQueryTransformerTimeout(cfg.QueryTransformer.RequestTimeout),
		)
	}
	if strings.TrimSpace(cfg.ModelServing.Endpoint) != "" {
		loadTrigger, err := inferencemodelserving.NewHTTPLoadTrigger(inferencemodelserving.LoadTriggerConfig{
			Endpoint:       cfg.ModelServing.Endpoint,
			RequestTimeout: cfg.ModelServing.RequestTimeout,
		})
		if err != nil {
			log.WithContext(cancelCtx).WithError(err).Fatal("unable to create model serving load trigger")
		}
		inferenceOptions = append(inferenceOptions, app.WithModelServingLoadTrigger(loadTrigger, cfg.ModelServing.LoadTimeout, cfg.ModelServing.PollInterval))
	}
	preferenceDatasetWriter := inferencepreference.NewS3ObjectDatasetWriter(cancelCtx, cfg.PreferenceDataset.BucketRegion, cfg.PreferenceDataset.UploadPartSizeMB*1024*1024)
	if preferenceDatasetWriter == nil {
		log.WithContext(cancelCtx).Fatal("unable to create preference dataset writer")
	}
	inferenceOptions = append(inferenceOptions, app.WithPreferenceDatasetWriter(preferenceDatasetWriter))
	inferenceUsecase := app.NewInferenceUsecase(
		modelRepository,
		inferenceOptions...,
	)
	agentRunActivities := inferencetemporal.NewAgentRunActivities(inferenceUsecase)
	agentRunWorker := inferencetemporal.NewAgentRunWorker(temporalClient, cfg.Temporal.TaskQueue, agentRunActivities)
	grpcService := inferencegrpc.NewInferenceGrpcServer(inferenceUsecase)
	endpointDTOAdapter := inferenceadapter.NewEndpointDTOAdapter(serializers.NewJSONSerializer())
	generationDTOAdapter := inferenceadapter.NewGenerationDTOAdapter(serializers.NewJSONSerializer())
	feedbackDTOAdapter := inferenceadapter.NewFeedbackDTOAdapter(serializers.NewJSONSerializer())
	agentSpecDTOAdapter := inferenceadapter.NewAgentSpecDTOAdapter(
		serializers.NewJSONSerializer(),
		inferenceadapter.WithAgentSpecBudgetCaps(cfg.Agent.MaxStepsCap, cfg.Agent.TokenBudgetCap, cfg.Agent.WallMsCap),
	)
	preferenceDatasetDTOAdapter := inferenceadapter.NewPreferenceDatasetDTOAdapter(serializers.NewJSONSerializer())
	agentTrajectoryDTOAdapter := inferenceadapter.NewAgentTrajectoryDTOAdapter(serializers.NewJSONSerializer())
	restService := inferencerest.NewService(
		inferencerest.NewInferenceHandlers(
			inferenceUsecase,
			endpointDTOAdapter,
			generationDTOAdapter,
			feedbackDTOAdapter,
			agentSpecDTOAdapter,
			preferenceDatasetDTOAdapter,
			agentTrajectoryDTOAdapter,
		).GetRoutes(),
		cfg.HTTPPort,
		serviceName,
		transport.WithServerTimeouts(cfg.HTTPServer.ReadTimeout, cfg.HTTPServer.WriteTimeout, cfg.HTTPServer.IdleTimeout),
	)

	healthCheck := coreHealthCheck.NewMonitor(newHealthCheckConfig(cfg.Health))
	healthCheck = healthCheck.WithCpuCheck().WithDatabaseCheck().WithMemoryCheck().WithMessageBrokerCheck()

	components := []lifecycle.Component{
		lifecycle.CloserComponent("inference-observability", func() error {
			traceShutdown()
			return nil
		}),
		lifecycle.CloserComponent("inference-database", func() error {
			database.Close()
			return nil
		}),
		lifecycle.CloserComponent("inference-temporal-client", func() error {
			temporalClient.Close()
			return nil
		}),
		lifecycle.CloserComponent("inference-feature-materializer-client", retrievalClient.Close),
		lifecycle.CloserComponent("inference-tool-invoker", closeToolInvoker),
		lifecycle.CloserComponent("inference-publisher", func() error {
			outboxPublisher.Close()
			return nil
		}),
		lifecycle.CloserComponent("inference-user-event-publisher", func() error {
			userEventPublisher.Close()
			return nil
		}),
		lifecycle.HealthCheckComponent("inference-healthcheck", healthCheck),
		lifecycle.ServerComponent("inference-http", restService),
		lifecycle.NewFuncComponent(lifecycle.ComponentConfig{
			Name: "inference-grpc",
			Start: func(context.Context) error {
				return grpcService.Connect(cfg.GRPCPort)
			},
			Drain: grpcService.Shutdown,
			Health: func() lifecycle.Health {
				return lifecycle.Health{Name: "inference-grpc", State: lifecycle.StateReady, Ready: grpcService.Ready()}
			},
		}),
		lifecycle.WorkerComponent("inference-outbox-relay", func(ctx context.Context) error {
			if err := outboxRelay.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		}),
		lifecycle.WorkerComponent("inference-agent-run-reaper", func(ctx context.Context) error {
			return runAgentRunReaper(ctx, inferenceUsecase, cfg.Agent.RunReaperInterval, cfg.Agent.RunReaperGrace)
		}),
		lifecycle.TemporalWorkerComponent("inference-agent-run-worker", agentRunWorker),
	}

	startSubscriber := func(name string, topics []string, configure func(messagingConn.Subscriber)) {
		var factory messagingConn.Messenger
		components = append(components, lifecycle.NewFuncComponent(lifecycle.ComponentConfig{
			Name: "inference-subscriber-" + name,
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

	startSubscriber("model-updated", []string{cfg.Topics.ModelRegistry}, func(subscriber messagingConn.Subscriber) {
		messagingConn.AddListener(subscriber, inferencemessaging.NewModelUpdatedEventListener(inferenceUsecase))
	})
	startSubscriber("dataset-updated", []string{cfg.Topics.DataRegistry}, func(subscriber messagingConn.Subscriber) {
		messagingConn.AddListener(subscriber, inferencemessaging.NewDatasetUpdatedEventListener(inferenceUsecase))
	})
	startSubscriber("tenant-created", []string{cfg.TenantTopic}, func(subscriber messagingConn.Subscriber) {
		sharedTenant.ConfigureProfileProjectionErrorPolicy(subscriber)
		messagingConn.AddListener(subscriber, sharedTenant.NewUserCreatedProjectionListener(tenantDB))
	})
	startSubscriber("tenant-updated", []string{cfg.TenantTopic}, func(subscriber messagingConn.Subscriber) {
		sharedTenant.ConfigureProfileProjectionErrorPolicy(subscriber)
		messagingConn.AddListener(subscriber, sharedTenant.NewUserUpdatedProjectionListener(tenantDB))
	})
	startSubscriber("tenant-deleted", []string{cfg.TenantTopic}, func(subscriber messagingConn.Subscriber) {
		sharedTenant.ConfigureProfileProjectionErrorPolicy(subscriber)
		messagingConn.AddListener(subscriber, sharedTenant.NewUserDeletedProjectionListener(tenantDB))
	})

	supervisor := lifecycle.NewSupervisorWithConfig(cfg.Lifecycle, components...)
	if err := supervisor.RunWithSignals(cancelCtx, syscall.SIGINT, syscall.SIGTERM); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.WithContext(cancelCtx).WithError(err).Errorf("%s service stopped with error", serviceName)
	}
	cancelFtn()
	log.Trace(fmt.Sprintf("stopped the %s service", serviceName))
}

func readInferenceConfig() inferenceConfig {
	env.RequireServiceEnvironment()

	brokers := env.WithDefaultString("KAFKA_BROKER", "localhost:9092")
	dbName := env.WithDefaultString("INFERENCE_SERVICE_DB_NAME", "bighill_inference_db")
	dbConnectionString := postgresConnectionString(
		env.WithDefaultString("INFERENCE_SERVICE_DB_USER", "bighill_inference_db_user"),
		env.WithDefaultString("INFERENCE_SERVICE_DB_PASSWORD", ""),
		env.WithDefaultString("PGHOST", "127.0.0.1"),
		env.WithDefaultString("PGPORT", "5432"),
		dbName,
		env.WithDefaultString("PGSSLMODE", "disable"),
		env.WithDefaultInt("INFERENCE_SERVICE_DB_MAX_CONNECTIONS", "20"),
	)
	defaultPreferenceBucketRegion := "eu-west-1"
	if env.IsDevEnv() {
		defaultPreferenceBucketRegion = coreBucket.LocalDevS3Region
	}
	preferenceDatasetExportEnabled := env.WithDefaultBool("INFERENCE_SERVICE_PREFERENCE_DATASET_EXPORT_ENABLED", false)
	preferenceDatasetURITemplate := env.WithDefaultString("INFERENCE_SERVICE_PREFERENCE_DATASET_URI_TEMPLATE", "")
	if preferenceDatasetExportEnabled && strings.TrimSpace(preferenceDatasetURITemplate) == "" {
		log.Fatal("INFERENCE_SERVICE_PREFERENCE_DATASET_URI_TEMPLATE is required when preference dataset builds are enabled")
	}
	cfg := inferenceConfig{
		ServiceName:        env.WithDefaultString("INFERENCE_SERVICE_NAME", "inference-service"),
		DBName:             dbName,
		DBConnectionString: dbConnectionString,
		Messaging: messagingConn.MessengerConfig{
			DlqURL:          env.WithDefaultString("INFERENCE_SERVICE_DLQ", "http://localhost:4566/inference-dev-env-queue/"),
			GroupID:         env.WithDefaultString("INFERENCE_SERVICE_KAFKA_BASE_GROUP_ID", "inference"),
			Brokers:         brokers,
			AutoOffsetReset: env.WithDefaultString("INFERENCE_SERVICE_KAFKA_AUTO_OFFSET_RESET", "earliest"),
		},
		OutboxBackend: env.WithDefaultString("INFERENCE_SERVICE_OUTBOX", "postgres"),
		OutboxRelay: messagingConn.OutboxRelayConfig{
			PollInterval:   time.Duration(env.WithDefaultInt("INFERENCE_SERVICE_OUTBOX_RELAY_POLL_MS", "250")) * time.Millisecond,
			FailureBackoff: time.Duration(env.WithDefaultInt("INFERENCE_SERVICE_OUTBOX_RELAY_FAILURE_BACKOFF_MS", "2000")) * time.Millisecond,
			BatchSize:      int32(env.WithDefaultInt("INFERENCE_SERVICE_OUTBOX_RELAY_BATCH_SIZE", "100")),
		},
		Topics: inferencemessaging.InferenceTopics{
			ModelRegistry:     env.WithDefaultString("INFERENCE_SERVICE_MODEL_REGISTRY_SUBSCRIBER_TOPIC", "model_registry"),
			DataRegistry:      env.WithDefaultString("INFERENCE_SERVICE_DATA_REGISTRY_SUBSCRIBER_TOPIC", "data_registry"),
			PreferenceDataset: env.WithDefaultString("INFERENCE_SERVICE_PREFERENCE_DATASET_TOPIC", "inference"),
		},
		TenantTopic: env.WithDefaultString("INFERENCE_SERVICE_TENANT_SUBSCRIBER_TOPIC", "tenant"),
		FeatureMaterializer: inferencegrpc.FeatureMaterializerClientConfig{
			Address:       env.WithDefaultString("INFERENCE_SERVICE_FEATURE_MATERIALIZER_GRPC_ADDRESS", "localhost:7072"),
			DialTimeoutMs: env.WithDefaultInt("INFERENCE_SERVICE_FEATURE_MATERIALIZER_GRPC_DIAL_TIMEOUT_MS", "500"),
			CallTimeoutMs: env.WithDefaultInt("INFERENCE_SERVICE_FEATURE_MATERIALIZER_GRPC_CALL_TIMEOUT_MS", "15000"),
			RetryCount:    env.WithDefaultInt("INFERENCE_SERVICE_FEATURE_MATERIALIZER_GRPC_RETRY_COUNT", "3"),
		},
		ToolService: inferencetools.ToolServiceClientConfig{
			Address:       env.WithDefaultString("INFERENCE_SERVICE_TOOL_SERVICE_GRPC_ADDRESS", ""),
			DialTimeoutMs: env.WithDefaultInt("INFERENCE_SERVICE_TOOL_SERVICE_GRPC_DIAL_TIMEOUT_MS", "500"),
			CallTimeoutMs: env.WithDefaultInt("INFERENCE_SERVICE_TOOL_SERVICE_GRPC_CALL_TIMEOUT_MS", "5000"),
			RetryCount:    env.WithDefaultInt("INFERENCE_SERVICE_TOOL_SERVICE_GRPC_RETRY_COUNT", "3"),
		},
		Generation: generationConfig{
			RequestTimeout:   secondsFromEnv("INFERENCE_SERVICE_GENERATION_REQUEST_TIMEOUT_SECONDS", "60"),
			MaxOutputTokens:  env.WithDefaultInt("INFERENCE_SERVICE_GENERATION_MAX_OUTPUT_TOKENS", "256"),
			PromptStrategy:   env.WithDefaultString("INFERENCE_SERVICE_PROMPT_STRATEGY_VERSION", "rag-prompt-v1"),
			MaxContextTokens: env.WithDefaultInt("INFERENCE_SERVICE_PROMPT_MAX_CONTEXT_TOKENS", "3000"),
			MaxContextChunks: env.WithDefaultInt("INFERENCE_SERVICE_PROMPT_MAX_CONTEXT_CHUNKS", "8"),
			RAGMergeStrategy: env.MustString("INFERENCE_SERVICE_RAG_MERGE_STRATEGY"),
		},
		Reranker: rerankerConfig{
			Provider:            env.WithDefaultString("INFERENCE_SERVICE_RERANKER_PROVIDER", ""),
			URL:                 env.WithDefaultString("INFERENCE_SERVICE_RERANKER_URL", ""),
			Model:               env.WithDefaultString("INFERENCE_SERVICE_RERANKER_MODEL", ""),
			RequestTimeout:      secondsFromEnv("INFERENCE_SERVICE_RERANKER_REQUEST_TIMEOUT_SECONDS", "30"),
			CandidateMultiplier: env.WithDefaultInt("INFERENCE_SERVICE_RERANKER_CANDIDATE_MULTIPLIER", "5"),
		},
		QueryTransformer: queryTransformerConfig{
			Provider:       env.WithDefaultString("INFERENCE_SERVICE_QUERY_TRANSFORMER_PROVIDER", ""),
			RequestTimeout: secondsFromEnv("INFERENCE_SERVICE_QUERY_TRANSFORMER_REQUEST_TIMEOUT_SECONDS", "30"),
		},
		ModelServing: modelServingConfig{
			Endpoint:       env.WithDefaultString("INFERENCE_SERVICE_MODEL_SERVING_ENDPOINT", ""),
			RequestTimeout: secondsFromEnv("INFERENCE_SERVICE_MODEL_SERVING_REQUEST_TIMEOUT_SECONDS", "5"),
			LoadTimeout:    secondsFromEnv("INFERENCE_SERVICE_MODEL_SERVING_LOAD_TIMEOUT_SECONDS", "60"),
			PollInterval:   time.Duration(env.WithDefaultInt("INFERENCE_SERVICE_MODEL_SERVING_LOAD_POLL_MS", "1000")) * time.Millisecond,
		},
		Agent: agentConfig{
			MaxStepsCap:       env.WithDefaultInt("INFERENCE_SERVICE_AGENT_MAX_STEPS_CAP", "3"),
			TokenBudgetCap:    env.WithDefaultInt("INFERENCE_SERVICE_AGENT_TOKEN_BUDGET_CAP", "8192"),
			WallMsCap:         env.WithDefaultInt("INFERENCE_SERVICE_AGENT_WALL_MS_CAP", "120000"),
			RunReaperInterval: time.Duration(env.WithDefaultInt("INFERENCE_SERVICE_AGENT_RUN_REAPER_INTERVAL_SECONDS", "30")) * time.Second,
			RunReaperGrace:    time.Duration(env.WithDefaultInt("INFERENCE_SERVICE_AGENT_RUN_REAPER_GRACE_SECONDS", "30")) * time.Second,
		},
		Temporal: temporalConfig{
			Address:              env.MustString("INFERENCE_SERVICE_TEMPORAL_ADDRESS"),
			Namespace:            env.MustString("INFERENCE_SERVICE_TEMPORAL_NAMESPACE"),
			TaskQueue:            env.MustString("INFERENCE_SERVICE_TEMPORAL_TASK_QUEUE"),
			ConnectTimeout:       time.Duration(env.MustInt("INFERENCE_SERVICE_TEMPORAL_CONNECT_TIMEOUT_SECONDS")) * time.Second,
			ConnectRetryInterval: time.Duration(env.MustInt("INFERENCE_SERVICE_TEMPORAL_CONNECT_RETRY_INTERVAL_SECONDS")) * time.Second,
		},
		UserEvents: userevents.Config{
			Enabled:        env.WithDefaultBool("USER_EVENTS_ENABLED", false),
			RedisAddress:   env.WithDefaultString("USER_EVENTS_REDIS_ADDRESS", ""),
			RedisUsername:  env.WithDefaultString("USER_EVENTS_REDIS_USERNAME", ""),
			RedisPassword:  env.WithDefaultString("USER_EVENTS_REDIS_PASSWORD", ""),
			RedisTLS:       env.WithDefaultBool("USER_EVENTS_REDIS_TLS", false),
			ChannelPrefix:  env.WithDefaultString("USER_EVENTS_CHANNEL_PREFIX", userevents.DefaultChannelPrefix),
			PublishTimeout: time.Duration(env.WithDefaultInt("USER_EVENTS_PUBLISH_TIMEOUT_MS", "500")) * time.Millisecond,
			StreamMaxLen:   int64(env.WithDefaultInt("USER_EVENTS_STREAM_MAX_LEN", "1000")),
		},
		PreferenceDataset: preferenceDatasetConfig{
			ExportEnabled:    preferenceDatasetExportEnabled,
			URITemplate:      preferenceDatasetURITemplate,
			MinExamples:      env.WithDefaultInt("INFERENCE_SERVICE_PREFERENCE_DATASET_MIN_EXAMPLES", "1"),
			Limit:            env.WithDefaultInt("INFERENCE_SERVICE_PREFERENCE_DATASET_LIMIT", "1000"),
			BucketRegion:     env.WithDefaultString("INFERENCE_SERVICE_PREFERENCE_DATASET_BUCKET_REGION", defaultPreferenceBucketRegion),
			UploadPartSizeMB: env.WithDefaultInt64("INFERENCE_SERVICE_PREFERENCE_DATASET_UPLOAD_PART_SIZE_MB", "10"),
		},
		GRPCPort: env.WithDefaultInt("INFERENCE_SERVICE_API_GRPC_PORT", "7073"),
		HTTPPort: env.WithDefaultInt("INFERENCE_SERVICE_API_HTTP_PORT", "8087"),
		HTTPServer: httpServerConfig{
			ReadTimeout:  secondsFromEnv("INFERENCE_SERVICE_HTTP_READ_TIMEOUT_SECONDS", "30"),
			WriteTimeout: secondsFromEnv("INFERENCE_SERVICE_HTTP_WRITE_TIMEOUT_SECONDS", "120"),
			IdleTimeout:  secondsFromEnv("INFERENCE_SERVICE_HTTP_IDLE_TIMEOUT_SECONDS", "120"),
		},
		Health: healthConfig{
			CpuThresholdPercentage:                    env.WithDefaultInt("INFERENCE_SERVICE_HEALTHCHECK_CPU_THRESHOLD_PERCENT", "80"),
			MemFreeThresholdPercent:                   env.WithDefaultInt("INFERENCE_SERVICE_HEALTHCHECK_FREE_MEM_THRESHOLD_PERCENT", "20"),
			HealthCheckPort:                           env.WithDefaultInt("INFERENCE_SERVICE_HEALTHCHECK_PORT", "5059"),
			DBConnectionString:                        dbConnectionString,
			MessageBrokerConnectionString:             brokers,
			DbLatencyThreshold:                        secondsFromEnv("INFERENCE_SERVICE_HEALTHCHECK_DB_LATENCY_THRESHOLD_SECONDS", "5"),
			MessageBrokerLatencyThreshold:             secondsFromEnv("INFERENCE_SERVICE_HEALTHCHECK_MSG_BROKER_LATENCY_THRESHOLD_SECONDS", "5"),
			ServiceLatencyThreshold:                   secondsFromEnv("INFERENCE_SERVICE_HEALTHCHECK_SERVICE_LATENCY_THRESHOLD_SECONDS", "5"),
			MessageBrokerSubscriberMaxPollSilence:     secondsFromEnv("INFERENCE_SERVICE_HEALTHCHECK_MESSAGE_BROKER_SUBSCRIBER_MAX_POLL_SILENCE_SECONDS", "30"),
			MessageBrokerSubscriberMaxProgressSilence: secondsFromEnv("INFERENCE_SERVICE_HEALTHCHECK_MESSAGE_BROKER_SUBSCRIBER_MAX_PROGRESS_SILENCE_SECONDS", "90"),
			MessageBrokerSubscriberMaxLag:             int64(env.WithDefaultInt("INFERENCE_SERVICE_HEALTHCHECK_MESSAGE_BROKER_SUBSCRIBER_MAX_LAG", "100000")),
		},
		Lifecycle: lifecycle.Config{
			ReadinessTimeout: secondsFromEnv("INFERENCE_SERVICE_LIFECYCLE_READINESS_TIMEOUT_SECONDS", "30"),
			DrainTimeout:     secondsFromEnv("INFERENCE_SERVICE_LIFECYCLE_DRAIN_TIMEOUT_SECONDS", "30"),
			CloseTimeout:     secondsFromEnv("INFERENCE_SERVICE_LIFECYCLE_CLOSE_TIMEOUT_SECONDS", "10"),
		},
	}
	if err := validateInferenceConfig(cfg); err != nil {
		log.Fatalf("invalid inference service configuration: %v", err)
	}
	return cfg
}

func validateInferenceConfig(cfg inferenceConfig) error {
	log.Trace("validateInferenceConfig")

	if err := validateGenerationConfig(cfg.Generation); err != nil {
		return err
	}
	if err := validateRerankerConfig(cfg.Reranker); err != nil {
		return err
	}
	mergeStrategy, err := model.ToRAGMergeStrategy(cfg.Generation.RAGMergeStrategy)
	if err != nil {
		return fmt.Errorf("INFERENCE_SERVICE_RAG_MERGE_STRATEGY is invalid: %w", err)
	}
	if mergeStrategy == model.RAGMergeStrategyReranker && strings.TrimSpace(cfg.Reranker.Provider) == "" {
		return fmt.Errorf("INFERENCE_SERVICE_RAG_MERGE_STRATEGY=reranker requires INFERENCE_SERVICE_RERANKER_PROVIDER")
	}
	if err := validateQueryTransformerConfig(cfg.QueryTransformer); err != nil {
		return err
	}
	if err := validateModelServingConfig(cfg.ModelServing); err != nil {
		return err
	}
	if err := validateToolServiceConfig(cfg.ToolService); err != nil {
		return err
	}
	if err := validateAgentConfig(cfg.Agent); err != nil {
		return err
	}
	if err := validateTemporalConfig(cfg.Temporal); err != nil {
		return err
	}
	if err := validateHTTPServerConfig(cfg.HTTPServer, cfg.Generation); err != nil {
		return err
	}
	if !env.IsDevEnv() && strings.Contains(cfg.PreferenceDataset.URITemplate, "local-dev-bucket") {
		return fmt.Errorf("INFERENCE_SERVICE_PREFERENCE_DATASET_URI_TEMPLATE must not use local-dev-bucket outside dev environments")
	}
	return nil
}

func validateGenerationConfig(cfg generationConfig) error {
	log.Trace("validateGenerationConfig")

	if cfg.RequestTimeout <= 0 {
		return fmt.Errorf("INFERENCE_SERVICE_GENERATION_REQUEST_TIMEOUT_SECONDS must be greater than zero")
	}
	if cfg.MaxOutputTokens <= 0 {
		return fmt.Errorf("INFERENCE_SERVICE_GENERATION_MAX_OUTPUT_TOKENS must be greater than zero")
	}
	if _, err := model.ToRAGMergeStrategy(cfg.RAGMergeStrategy); err != nil {
		return fmt.Errorf("INFERENCE_SERVICE_RAG_MERGE_STRATEGY is invalid: %w", err)
	}
	return nil
}

func validateRerankerConfig(cfg rerankerConfig) error {
	log.Trace("validateRerankerConfig")

	provider := strings.ToLower(strings.TrimSpace(cfg.Provider))
	switch provider {
	case "":
		return nil
	case "tei":
		if strings.TrimSpace(cfg.URL) == "" {
			return fmt.Errorf("INFERENCE_SERVICE_RERANKER_URL is required for tei reranking")
		}
		if strings.TrimSpace(cfg.Model) == "" {
			return fmt.Errorf("INFERENCE_SERVICE_RERANKER_MODEL is required for tei reranking")
		}
		if cfg.CandidateMultiplier < 2 {
			return fmt.Errorf("reranker candidate multiplier must be at least 2")
		}
	default:
		return fmt.Errorf("unsupported reranker provider %q", cfg.Provider)
	}
	return nil
}

func validateQueryTransformerConfig(cfg queryTransformerConfig) error {
	log.Trace("validateQueryTransformerConfig")

	provider := strings.ToLower(strings.TrimSpace(cfg.Provider))
	switch provider {
	case "":
		return nil
	case "self_query":
		if cfg.RequestTimeout <= 0 {
			return fmt.Errorf("INFERENCE_SERVICE_QUERY_TRANSFORMER_REQUEST_TIMEOUT_SECONDS must be greater than zero")
		}
	default:
		return fmt.Errorf("unsupported query transformer provider %q", cfg.Provider)
	}
	return nil
}

func validateModelServingConfig(cfg modelServingConfig) error {
	log.Trace("validateModelServingConfig")

	if strings.TrimSpace(cfg.Endpoint) == "" {
		return nil
	}
	if cfg.RequestTimeout <= 0 {
		return fmt.Errorf("INFERENCE_SERVICE_MODEL_SERVING_REQUEST_TIMEOUT_SECONDS must be greater than zero")
	}
	if cfg.LoadTimeout <= 0 {
		return fmt.Errorf("INFERENCE_SERVICE_MODEL_SERVING_LOAD_TIMEOUT_SECONDS must be greater than zero")
	}
	if cfg.PollInterval <= 0 {
		return fmt.Errorf("INFERENCE_SERVICE_MODEL_SERVING_LOAD_POLL_MS must be greater than zero")
	}
	return nil
}

func validateToolServiceConfig(cfg inferencetools.ToolServiceClientConfig) error {
	log.Trace("validateToolServiceConfig")

	if strings.TrimSpace(cfg.Address) == "" {
		return nil
	}
	return inferencetools.ValidateToolServiceClientConfig(cfg)
}

func validateAgentConfig(cfg agentConfig) error {
	log.Trace("validateAgentConfig")

	if cfg.MaxStepsCap <= 0 {
		return fmt.Errorf("INFERENCE_SERVICE_AGENT_MAX_STEPS_CAP must be greater than zero")
	}
	if cfg.TokenBudgetCap <= 0 {
		return fmt.Errorf("INFERENCE_SERVICE_AGENT_TOKEN_BUDGET_CAP must be greater than zero")
	}
	if cfg.WallMsCap <= 0 {
		return fmt.Errorf("INFERENCE_SERVICE_AGENT_WALL_MS_CAP must be greater than zero")
	}
	if cfg.RunReaperInterval <= 0 {
		return fmt.Errorf("INFERENCE_SERVICE_AGENT_RUN_REAPER_INTERVAL_SECONDS must be greater than zero")
	}
	if cfg.RunReaperGrace <= 0 {
		return fmt.Errorf("INFERENCE_SERVICE_AGENT_RUN_REAPER_GRACE_SECONDS must be greater than zero")
	}
	return nil
}

func validateTemporalConfig(cfg temporalConfig) error {
	log.Trace("validateTemporalConfig")

	if strings.TrimSpace(cfg.Address) == "" {
		return fmt.Errorf("INFERENCE_SERVICE_TEMPORAL_ADDRESS is required")
	}
	if strings.TrimSpace(cfg.Namespace) == "" {
		return fmt.Errorf("INFERENCE_SERVICE_TEMPORAL_NAMESPACE is required")
	}
	if strings.TrimSpace(cfg.TaskQueue) == "" {
		return fmt.Errorf("INFERENCE_SERVICE_TEMPORAL_TASK_QUEUE is required")
	}
	if cfg.ConnectTimeout <= 0 {
		return fmt.Errorf("INFERENCE_SERVICE_TEMPORAL_CONNECT_TIMEOUT_SECONDS must be greater than zero")
	}
	if cfg.ConnectRetryInterval <= 0 {
		return fmt.Errorf("INFERENCE_SERVICE_TEMPORAL_CONNECT_RETRY_INTERVAL_SECONDS must be greater than zero")
	}
	return nil
}

func runAgentRunReaper(ctx context.Context, inferenceUsecase app.InferenceUsecase, interval time.Duration, grace time.Duration) error {
	log.Trace("runAgentRunReaper")

	reap := func() {
		count, err := inferenceUsecase.ReapExpiredAgentRuns(ctx, grace)
		if err != nil {
			log.WithContext(ctx).WithError(err).Error("agent run reaper failed")
			return
		}
		if count > 0 {
			log.WithContext(ctx).WithField("count", count).Warn("marked expired agent runs failed")
		}
	}
	reap()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			reap()
		}
	}
}

func validateHTTPServerConfig(cfg httpServerConfig, generation generationConfig) error {
	log.Trace("validateHTTPServerConfig")

	if cfg.ReadTimeout <= 0 {
		return fmt.Errorf("INFERENCE_SERVICE_HTTP_READ_TIMEOUT_SECONDS must be greater than zero")
	}
	if cfg.WriteTimeout <= 0 {
		return fmt.Errorf("INFERENCE_SERVICE_HTTP_WRITE_TIMEOUT_SECONDS must be greater than zero")
	}
	if cfg.IdleTimeout <= 0 {
		return fmt.Errorf("INFERENCE_SERVICE_HTTP_IDLE_TIMEOUT_SECONDS must be greater than zero")
	}
	if cfg.WriteTimeout <= generation.RequestTimeout {
		return fmt.Errorf("INFERENCE_SERVICE_HTTP_WRITE_TIMEOUT_SECONDS must be greater than INFERENCE_SERVICE_GENERATION_REQUEST_TIMEOUT_SECONDS")
	}
	return nil
}

func newGenerationAdapters(cfg generationConfig) map[string]app.GenerationAdapter {
	log.Trace("newGenerationAdapters")

	return map[string]app.GenerationAdapter{
		model.ServingProtocolOpenAIChatCompletions.String(): generation.NewOpenAIChatCompletionsGenerator(cfg.RequestTimeout, cfg.MaxOutputTokens),
		model.ServingProtocolOllamaGenerate.String():        generation.NewOllamaGenerateGenerator(cfg.RequestTimeout, cfg.MaxOutputTokens),
	}
}

func newToolInvoker(ctx context.Context, cfg inferencetools.ToolServiceClientConfig, retrievalClient app.RetrievalClient) (app.ToolInvoker, func() error, error) {
	log.Trace("newToolInvoker")

	searchInvoker, err := inferencetools.NewSearchKnowledgeToolInvoker(retrievalClient)
	if err != nil {
		return nil, nil, err
	}
	graphInvoker, err := inferencetools.NewGraphSearchToolInvoker(retrievalClient)
	if err != nil {
		return nil, nil, err
	}
	localInvoker, err := inferencetools.NewCompositeLocalToolInvoker(map[string]app.ToolInvoker{
		inferencetools.SearchKnowledgeToolName: searchInvoker,
		inferencetools.GraphSearchToolName:     graphInvoker,
	})
	if err != nil {
		return nil, nil, err
	}
	var remoteInvoker app.ToolInvoker
	var remoteCloser func() error
	if strings.TrimSpace(cfg.Address) != "" {
		remote, err := inferencetools.NewRemoteToolInvoker(ctx, cfg)
		if err != nil {
			return nil, nil, err
		}
		remoteInvoker = remote
		remoteCloser = remote.Close
	}
	routedInvoker, err := inferencetools.NewRoutedToolInvoker(localInvoker, remoteInvoker, localInvoker.ToolNames())
	if err != nil {
		if remoteCloser != nil {
			_ = remoteCloser()
		}
		return nil, nil, err
	}
	closeFn := func() error {
		if remoteCloser == nil {
			return nil
		}
		return remoteCloser()
	}
	return routedInvoker, closeFn, nil
}

func newQueryTransformer(cfg queryTransformerConfig, adapters map[string]app.GenerationAdapter) (app.QueryTransformer, error) {
	log.Trace("newQueryTransformer")

	switch strings.ToLower(strings.TrimSpace(cfg.Provider)) {
	case "":
		return nil, nil
	case "self_query":
		generators := make(map[string]retrieval.QueryGenerator, len(adapters))
		for protocol, adapter := range adapters {
			if adapter != nil {
				generators[protocol] = adapter
			}
		}
		return retrieval.NewSelfQueryTransformer(generators), nil
	default:
		return nil, fmt.Errorf("unsupported query transformer provider %q", cfg.Provider)
	}
}

func newRerankerAdapter(cfg rerankerConfig) (app.Reranker, error) {
	log.Trace("newRerankerAdapter")

	switch strings.ToLower(strings.TrimSpace(cfg.Provider)) {
	case "":
		return nil, nil
	case "tei":
		if cfg.CandidateMultiplier < 2 {
			return nil, fmt.Errorf("reranker candidate multiplier must be at least 2")
		}
		return retrieval.NewTEIReranker(cfg.URL, cfg.Model, cfg.RequestTimeout)
	default:
		return nil, fmt.Errorf("unsupported reranker provider %q", cfg.Provider)
	}
}

func promptStrategyFromConfig(cfg generationConfig) (model.PromptStrategy, error) {
	log.Trace("promptStrategyFromConfig")

	strategy := model.PromptStrategy{
		Version:          strings.TrimSpace(cfg.PromptStrategy),
		SystemPrompt:     "You answer using only the retrieved context. If the context does not contain the answer, say that the answer is not available in the retrieved context.",
		MaxContextTokens: cfg.MaxContextTokens,
		MaxContextChunks: cfg.MaxContextChunks,
	}
	if strategy.Version == "" {
		return model.PromptStrategy{}, fmt.Errorf("prompt strategy version is required")
	}
	if strategy.SystemPrompt == "" {
		return model.PromptStrategy{}, fmt.Errorf("prompt system prompt is required")
	}
	if strategy.MaxContextTokens <= 0 {
		return model.PromptStrategy{}, fmt.Errorf("prompt max context tokens must be greater than zero")
	}
	if strategy.MaxContextChunks <= 0 {
		return model.PromptStrategy{}, fmt.Errorf("prompt max context chunks must be greater than zero")
	}
	return strategy, nil
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

func postgresConnectionString(user, password, host, port, dbName, sslMode string, maxConnections int) string {
	log.Trace("postgresConnectionString")

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
