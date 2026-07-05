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
	"strconv"
	"strings"
	"syscall"
	"time"

	"inference_service/pkg/app"
	"inference_service/pkg/domain/model"
	"inference_service/pkg/infra/generation"
	inferencegrpc "inference_service/pkg/infra/network/grpc"
	inferencemessaging "inference_service/pkg/infra/network/messaging"
	inferencepreference "inference_service/pkg/infra/preference"
	inferencedb "inference_service/pkg/infra/repo/db"
	"inference_service/pkg/infra/retrieval"

	coreDB "lib/shared_lib/db"
	env "lib/shared_lib/env"
	coreHealthCheck "lib/shared_lib/healthcheck"
	logs "lib/shared_lib/logs"
	messagingConn "lib/shared_lib/messaging"
	sharedTenant "lib/shared_lib/tenant"
	trace "lib/shared_lib/trace"
	shareduow "lib/shared_lib/uow"

	log "github.com/sirupsen/logrus"
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
	ProfileTopic        string
	FeatureMaterializer inferencegrpc.FeatureMaterializerClientConfig
	Generation          generationConfig
	Reranker            rerankerConfig
	QueryTransformer    queryTransformerConfig
	PreferenceDataset   preferenceDatasetConfig
	GRPCPort            int
	Health              healthConfig
}

type generationConfig struct {
	Provider         string
	Endpoint         string
	Model            string
	RequestTimeout   time.Duration
	PromptStrategy   string
	MaxContextTokens int
	MaxContextChunks int
}

type rerankerConfig struct {
	Provider            string
	URL                 string
	Model               string
	RequestTimeout      time.Duration
	CandidateMultiplier int
}

type queryTransformerConfig struct {
	Provider string
}

type preferenceDatasetConfig struct {
	ExportEnabled    bool
	URITemplate      string
	MinExamples      int
	Limit            int
	BucketRegion     string
	UploadPartSizeMB int64
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

	cfg := readInferenceConfig()
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

	modelRepository := inferencedb.NewInferenceModelRepository(database)
	datasetRepository := inferencedb.NewInferenceDatasetRepository(database)
	requestRepository := inferencedb.NewInferenceRequestRepository(database)
	feedbackRepository := inferencedb.NewInferenceFeedbackRepository(database)
	inferenceUnitOfWork := shareduow.New(database.Pool,
		shareduow.WithTransactionalOutbox(orderedOutbox),
		shareduow.WithOutboxSignal(func() { messagingConn.NotifyOutboxSignal(outboxSignal) }),
	)
	tenantDB := sharedTenant.NewPostgresProjectionStore(database)
	retrievalClient, err := inferencegrpc.NewFeatureMaterializerClient(cancelCtx, cfg.FeatureMaterializer)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create feature materializer client")
	}
	defer func() {
		_ = retrievalClient.Close()
	}()
	generator, err := newGenerationAdapter(cfg.Generation)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create generation adapter")
	}
	reranker, err := newRerankerAdapter(cfg.Reranker)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create reranker adapter")
	}
	queryTransformer, err := newQueryTransformer(cfg.QueryTransformer, generator)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create query transformer")
	}
	promptStrategy, err := promptStrategyFromConfig(cfg.Generation)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("invalid prompt strategy configuration")
	}
	preferenceEventBuilder := inferencemessaging.NewPreferenceDatasetEventBuilder(cfg.Topics.PreferenceDataset)
	inferenceOptions := []app.InferenceOption{
		app.WithInferenceDatasetRepository(datasetRepository),
		app.WithInferenceRequestRepository(requestRepository),
		app.WithInferenceFeedbackRepository(feedbackRepository),
		app.WithInferenceUnitOfWork(inferenceUnitOfWork, preferenceEventBuilder),
		app.WithRetrievalClient(retrievalClient),
		app.WithGenerationAdapter(generator),
		app.WithPromptStrategy(promptStrategy),
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
		inferenceOptions = append(inferenceOptions, app.WithQueryTransformer(queryTransformer))
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
	grpcService := inferencegrpc.NewInferenceGrpcServer(inferenceUsecase)
	defer grpcService.Close()

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

	startSubscriber("model-updated", []string{cfg.Topics.ModelRegistry}, func(subscriber messagingConn.Subscriber) {
		messagingConn.AddListener(subscriber, inferencemessaging.NewModelUpdatedEventListener(inferenceUsecase))
	})
	startSubscriber("dataset-updated", []string{cfg.Topics.DataRegistry}, func(subscriber messagingConn.Subscriber) {
		messagingConn.AddListener(subscriber, inferencemessaging.NewDatasetUpdatedEventListener(inferenceUsecase))
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

	go func() {
		if err := grpcService.Connect(cfg.GRPCPort); err != nil {
			log.Errorf("unable to start the %s grpc service: %v", serviceName, err)
			quit <- syscall.SIGTERM
		}
	}()

	<-quit

	cancelFtn()
	healthCheck.Close()
	log.Trace(fmt.Sprintf("stopped the %s service", serviceName))
}

func readInferenceConfig() inferenceConfig {
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
	return inferenceConfig{
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
		ProfileTopic: env.WithDefaultString("INFERENCE_SERVICE_PROFILE_SUBSCRIBER_TOPIC", "profile"),
		FeatureMaterializer: inferencegrpc.FeatureMaterializerClientConfig{
			Address:       env.WithDefaultString("INFERENCE_SERVICE_FEATURE_MATERIALIZER_GRPC_ADDRESS", "localhost:7072"),
			DialTimeoutMs: env.WithDefaultInt("INFERENCE_SERVICE_FEATURE_MATERIALIZER_GRPC_DIAL_TIMEOUT_MS", "500"),
			CallTimeoutMs: env.WithDefaultInt("INFERENCE_SERVICE_FEATURE_MATERIALIZER_GRPC_CALL_TIMEOUT_MS", "15000"),
			RetryCount:    env.WithDefaultInt("INFERENCE_SERVICE_FEATURE_MATERIALIZER_GRPC_RETRY_COUNT", "3"),
		},
		Generation: generationConfig{
			Provider:         env.WithDefaultString("INFERENCE_SERVICE_GENERATION_PROVIDER", "deterministic"),
			Endpoint:         env.WithDefaultString("INFERENCE_SERVICE_GENERATION_ENDPOINT", "http://localhost:11434"),
			Model:            env.WithDefaultString("INFERENCE_SERVICE_GENERATION_MODEL", "llama3.1:8b"),
			RequestTimeout:   secondsFromEnv("INFERENCE_SERVICE_GENERATION_REQUEST_TIMEOUT_SECONDS", "60"),
			PromptStrategy:   env.WithDefaultString("INFERENCE_SERVICE_PROMPT_STRATEGY_VERSION", "rag-prompt-v1"),
			MaxContextTokens: env.WithDefaultInt("INFERENCE_SERVICE_PROMPT_MAX_CONTEXT_TOKENS", "3000"),
			MaxContextChunks: env.WithDefaultInt("INFERENCE_SERVICE_PROMPT_MAX_CONTEXT_CHUNKS", "8"),
		},
		Reranker: rerankerConfig{
			Provider:            env.WithDefaultString("INFERENCE_SERVICE_RERANKER_PROVIDER", "disabled"),
			URL:                 env.WithDefaultString("INFERENCE_SERVICE_RERANKER_URL", ""),
			Model:               env.WithDefaultString("INFERENCE_SERVICE_RERANKER_MODEL", ""),
			RequestTimeout:      secondsFromEnv("INFERENCE_SERVICE_RERANKER_REQUEST_TIMEOUT_SECONDS", "30"),
			CandidateMultiplier: env.WithDefaultInt("INFERENCE_SERVICE_RERANKER_CANDIDATE_MULTIPLIER", "5"),
		},
		QueryTransformer: queryTransformerConfig{
			Provider: env.WithDefaultString("INFERENCE_SERVICE_QUERY_TRANSFORMER_PROVIDER", "disabled"),
		},
		PreferenceDataset: preferenceDatasetConfig{
			ExportEnabled:    env.WithDefaultBool("INFERENCE_SERVICE_PREFERENCE_DATASET_EXPORT_ENABLED", false),
			URITemplate:      env.WithDefaultString("INFERENCE_SERVICE_PREFERENCE_DATASET_URI_TEMPLATE", "s3://local-dev-bucket/preferences/{dataset_id}/{preference_dataset_id}.jsonl"),
			MinExamples:      env.WithDefaultInt("INFERENCE_SERVICE_PREFERENCE_DATASET_MIN_EXAMPLES", "1"),
			Limit:            env.WithDefaultInt("INFERENCE_SERVICE_PREFERENCE_DATASET_LIMIT", "1000"),
			BucketRegion:     env.WithDefaultString("INFERENCE_SERVICE_PREFERENCE_DATASET_BUCKET_REGION", "eu-west-1"),
			UploadPartSizeMB: env.WithDefaultInt64("INFERENCE_SERVICE_PREFERENCE_DATASET_UPLOAD_PART_SIZE_MB", "10"),
		},
		GRPCPort: env.WithDefaultInt("INFERENCE_SERVICE_API_GRPC_PORT", "7073"),
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
	}
}

func newGenerationAdapter(cfg generationConfig) (app.GenerationAdapter, error) {
	log.Trace("newGenerationAdapter")

	switch strings.ToLower(strings.TrimSpace(cfg.Provider)) {
	case "", "deterministic":
		return generation.NewDeterministicGenerator(), nil
	case "ollama", "vllm":
		return generation.NewHTTPGenerator(strings.ToLower(strings.TrimSpace(cfg.Provider)), cfg.Endpoint, cfg.Model, cfg.RequestTimeout), nil
	default:
		return nil, fmt.Errorf("unsupported generation provider %q", cfg.Provider)
	}
}

func newQueryTransformer(cfg queryTransformerConfig, generator app.GenerationAdapter) (app.QueryTransformer, error) {
	log.Trace("newQueryTransformer")

	switch strings.ToLower(strings.TrimSpace(cfg.Provider)) {
	case "", "disabled", "none":
		return nil, nil
	case "self_query":
		return retrieval.NewSelfQueryTransformer(generator), nil
	default:
		return nil, fmt.Errorf("unsupported query transformer provider %q", cfg.Provider)
	}
}

func newRerankerAdapter(cfg rerankerConfig) (app.Reranker, error) {
	log.Trace("newRerankerAdapter")

	switch strings.ToLower(strings.TrimSpace(cfg.Provider)) {
	case "", "disabled", "none":
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
