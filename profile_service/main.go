package main

import (
	dbConn "lib/shared_lib/db"
	env "lib/shared_lib/env"
	"lib/shared_lib/healthcheck"
	kms "lib/shared_lib/key_management"
	"lib/shared_lib/lifecycle"
	logs "lib/shared_lib/logs"
	messagingConn "lib/shared_lib/messaging"
	"lib/shared_lib/observability"
	"lib/shared_lib/secret"
	"lib/shared_lib/transport"
	shareduow "lib/shared_lib/uow"
	"time"

	authProvider "lib/shared_lib/auth"

	"context"
	"errors"
	"fmt"
	sharedclock "lib/shared_lib/clock"
	"net/http"

	usecase "profile_service/pkg/app"
	"profile_service/pkg/infra/network/messaging"
	"profile_service/pkg/infra/network/rest"
	"profile_service/pkg/infra/repo/db"
	"syscall"

	"github.com/redis/rueidis"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
)

// Version is set at compile time
var Version string

type runtimeConfig struct {
	serviceName             string
	servicePort             int
	authExpirationInMinutes int
	emailValidationTTL      time.Duration
	oauthStateTTL           time.Duration
	googleOAuth             rest.OAuthProviderConfig
	discordOAuth            rest.OAuthProviderConfig
	redisOption             rueidis.ClientOption
	messagingConfig         messagingConn.MessengerConfig
	outboxBackend           string
	outboxRelayConfig       messagingConn.OutboxRelayConfig
	kafkaPublisherTopic     string
	healthCheckConfig       healthcheck.HealthCheckConfig
	useStagingTestToken     bool
	huggingFaceTokenKey     string
	lifecycleConfig         lifecycle.Config
}

func init() {
	logs.Init()
}

func main() {
	ctx := context.Background()
	clock := sharedclock.System{}
	cfg := loadConfig()

	cancelCtx, cancelFtn := context.WithCancel(ctx)
	obsShutdown := observability.Init(cancelCtx, cfg.serviceName, Version)
	tracer := otel.Tracer(cfg.serviceName)
	log.Infof("Connected %s version %s", cfg.serviceName, Version)

	dbConfig := dbConn.DatabaseConfig{}
	dbConfig.RequireDbName("PROFILE_SERVICE_DB_NAME")
	dbConfig.RequireDbUser("PROFILE_SERVICE_DB_USER")
	dbConfig.RequireDbPassword("PROFILE_SERVICE_DB_PASSWORD")
	dbConfig.RequireDbMaxConnections("PROFILE_SERVICE_DB_MAX_CONNECTIONS")
	dbConnectionStr := dbConfig.GetConnectionString()

	database, err := dbConn.InitDatabase(ctx, dbConfig.GetName(), dbConnectionStr, log.StandardLogger())
	if err != nil {
		log.Fatalf("database init failed: %v", err)
	}
	profileDB := db.NewProfileDB(database)

	redisClient, err := rueidis.NewClient(cfg.redisOption)
	if err != nil {
		log.WithContext(ctx).WithError(err).Fatal("failed to initialize redis client")
	}
	authStore := authProvider.NewRevocationStore(redisClient, authProvider.WithKeyPrefix("auth:"))
	oauthStateStore := db.NewOAuthStateStore(redisClient, "auth:oauth:")

	outboxWriter, err := newPostgresOutbox(database, cfg.outboxBackend)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create postgres outbox")
	}
	orderedOutbox, ok := outboxWriter.(messagingConn.OrderedOutbox)
	if !ok {
		log.Fatal("postgres outbox does not support transactional enqueue operations")
	}
	outboxSignal := make(chan struct{}, 1)
	outboxWriter = messagingConn.NewSignaledOutbox(outboxWriter, outboxSignal)
	cfg.outboxRelayConfig.Signal = outboxSignal
	msgPublisher, err := messagingConn.NewPublisher(cfg.messagingConfig.Brokers, messagingConn.WithOutbox(outboxWriter))
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create the publisher")
	}
	relayOutbox, ok := outboxWriter.(messagingConn.RelayOutbox)
	if !ok {
		log.Fatal("postgres outbox does not support relay operations")
	}
	relayPublisher, ok := msgPublisher.(messagingConn.RelayPublisher)
	if !ok {
		log.Fatal("publisher does not support outbox relay publishing")
	}
	outboxRelay := messagingConn.NewOutboxRelay(relayOutbox, relayPublisher, cfg.outboxRelayConfig)

	profileUnitOfWork := shareduow.New(database.Pool,
		shareduow.WithTransactionalOutbox(orderedOutbox),
		shareduow.WithOutboxSignal(func() { messagingConn.NotifyOutboxSignal(outboxSignal) }),
	)
	profileEventBuilder := messaging.NewUserEventBuilder(cfg.kafkaPublisherTopic)
	secretCodec, err := secret.NewAESGCMCodec(cfg.huggingFaceTokenKey)
	if err != nil {
		log.WithContext(ctx).WithError(err).Fatal("unable to create Hugging Face token codec")
	}

	// Initialize KMS and auth provider for JWT token creation
	kmsClient, err := kms.NewKMSClient(ctx)
	if err != nil {
		log.WithContext(ctx).WithError(err).Fatal("unable to create KMS client")
	}
	authProv, err := authProvider.NewAuthProvider(ctx, kmsClient)
	if err != nil {
		log.WithContext(ctx).WithError(err).Fatal("unable to create auth provider")
	}

	oauthHTTPClient := &http.Client{Timeout: 10 * time.Second}
	oauthProviders := rest.NewOAuthProviderClients(oauthHTTPClient,
		cfg.googleOAuth,
		cfg.discordOAuth,
	)

	profilesUseCase := usecase.NewProfilesUseCase(
		usecase.ProfilesUseCaseDeps{
			ProfilesRepository: profileDB,
			UnitOfWork:         profileUnitOfWork,
			EventBuilder:       profileEventBuilder,
			AuthStore:          authStore,
			AuthProvider:       authProv,
			SecretEncryptor:    secretCodec,
		},
		usecase.ProfilesUseCaseConfig{
			AuthExpirationInMinutes: cfg.authExpirationInMinutes,
			EmailValidationTTL:      cfg.emailValidationTTL,
			OAuthProviders:          oauthProviders,
			OAuthStateStore:         oauthStateStore,
			OAuthStateTTL:           cfg.oauthStateTTL,
			UseStagingTestToken:     cfg.useStagingTestToken,
		},
		usecase.WithProfileClock(clock),
	)

	dtoProfileAdapter := rest.NewProfilesDTOAdapter()
	routeHandlers := rest.NewHttpHandler(profilesUseCase, dtoProfileAdapter).GetRoutes()
	restService := transport.NewHttpServer(tracer, routeHandlers, cfg.servicePort, cfg.serviceName)

	healthCheckConfig := cfg.healthCheckConfig
	healthCheckConfig.DBConnectionString = dbConnectionStr
	healthCheck := healthcheck.NewMonitor(healthCheckConfig)
	healthCheck = healthCheck.WithCpuCheck().WithMemoryCheck().WithDatabaseCheck().WithMessageBrokerCheck()

	supervisor := lifecycle.NewSupervisorWithConfig(cfg.lifecycleConfig,
		lifecycle.CloserComponent("profile-observability", func() error {
			obsShutdown()
			return nil
		}),
		lifecycle.CloserComponent("profile-database", func() error {
			database.Close()
			return nil
		}),
		lifecycle.CloserComponent("profile-publisher", func() error {
			msgPublisher.Close()
			return nil
		}),
		lifecycle.CloserComponent("profile-redis", func() error {
			redisClient.Close()
			return nil
		}),
		lifecycle.HealthCheckComponent("profile-healthcheck", healthCheck),
		lifecycle.ServerComponent("profile-http", restService),
		lifecycle.WorkerComponent("profile-outbox-relay", func(ctx context.Context) error {
			if err := outboxRelay.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		}),
	)

	if err := supervisor.RunWithSignals(cancelCtx, syscall.SIGINT, syscall.SIGTERM); err != nil {
		log.WithContext(cancelCtx).WithError(err).Errorf("%s service stopped with error", cfg.serviceName)
	}
	cancelFtn()
	log.Tracef("stopped %s version %s", cfg.serviceName, Version)
}

func loadConfig() runtimeConfig {
	env.RequireServiceEnvironment()

	brokers := env.MustString("KAFKA_BROKER")
	return runtimeConfig{
		serviceName:             env.MustString("PROFILE_SERVICE_NAME"),
		servicePort:             env.MustInt("PROFILE_SERVICE_HTTP_PORT"),
		authExpirationInMinutes: env.MustInt("PROFILE_SERVICE_AUTH_EXPIRATION_MINUTES"),
		emailValidationTTL:      time.Duration(env.MustInt("PROFILE_SERVICE_EMAIL_VALIDATION_TTL_MINUTES")) * time.Minute,
		oauthStateTTL:           time.Duration(env.MustInt("PROFILE_SERVICE_OAUTH_STATE_TTL_MINUTES")) * time.Minute,
		googleOAuth: rest.OAuthProviderConfig{
			ClientID:     env.MustString("PROFILE_SERVICE_OAUTH_GOOGLE_CLIENT_ID"),
			ClientSecret: env.MustString("PROFILE_SERVICE_OAUTH_GOOGLE_CLIENT_SECRET"),
		},
		discordOAuth: rest.OAuthProviderConfig{
			ClientID:     env.MustString("PROFILE_SERVICE_OAUTH_DISCORD_CLIENT_ID"),
			ClientSecret: env.MustString("PROFILE_SERVICE_OAUTH_DISCORD_CLIENT_SECRET"),
		},
		redisOption: rueidis.ClientOption{
			InitAddress: []string{env.MustString("PROFILE_SERVICE_REDIS_ADDRESS")},
			Username:    env.MustString("PROFILE_SERVICE_REDIS_USERNAME"),
			Password:    env.MustString("PROFILE_SERVICE_REDIS_PASSWORD"),
		},
		messagingConfig: messagingConn.MessengerConfig{
			DlqURL:  env.MustString("PROFILE_SERVICE_DLQ"),
			GroupID: env.MustString("PROFILE_SERVICE_KAFKA_GROUP_ID"),
			Brokers: brokers,
		},
		outboxBackend: env.MustString("PROFILE_SERVICE_OUTBOX"),
		outboxRelayConfig: messagingConn.OutboxRelayConfig{
			PollInterval:   time.Duration(env.MustInt("PROFILE_SERVICE_OUTBOX_RELAY_POLL_MS")) * time.Millisecond,
			FailureBackoff: time.Duration(env.MustInt("PROFILE_SERVICE_OUTBOX_RELAY_FAILURE_BACKOFF_MS")) * time.Millisecond,
			BatchSize:      int32(env.MustInt("PROFILE_SERVICE_OUTBOX_RELAY_BATCH_SIZE")),
		},
		kafkaPublisherTopic: env.MustString("PROFILE_SERVICE_KAFKA_PUBLISHER_TOPIC"),
		useStagingTestToken: env.WithDefaultBool("PROFILE_SERVICE_USE_STAGING_TEST_EMAIL_TOKEN", env.IsStaging()),
		huggingFaceTokenKey: env.MustString("PROFILE_SERVICE_HUGGINGFACE_TOKEN_ENCRYPTION_KEY"),
		lifecycleConfig: lifecycle.Config{
			ReadinessTimeout: secondsFromEnv("PROFILE_SERVICE_LIFECYCLE_READINESS_TIMEOUT_SECONDS", "30"),
			DrainTimeout:     secondsFromEnv("PROFILE_SERVICE_LIFECYCLE_DRAIN_TIMEOUT_SECONDS", "30"),
			CloseTimeout:     secondsFromEnv("PROFILE_SERVICE_LIFECYCLE_CLOSE_TIMEOUT_SECONDS", "10"),
		},
		healthCheckConfig: healthcheck.HealthCheckConfig{
			CpuThresholdPercentage:           env.MustInt("PROFILE_SERVICE_HEALTHCHECK_CPU_THRESHOLD_PERCENT"),
			MemFreeThresholdPercentage:       env.MustInt("PROFILE_SERVICE_HEALTHCHECK_FREE_MEM_THRESHOLD_PERCENT"),
			HealthCheckPort:                  env.MustInt("PROFILE_SERVICE_HEALTHCHECK_PORT"),
			ServiceLatencyThresholdSec:       time.Duration(env.MustInt("PROFILE_SERVICE_HEALTHCHECK_SERVICE_LATENCY_THRESHOLD_SECONDS")) * time.Second,
			DbLatencyThresholdSec:            time.Duration(env.MustInt("PROFILE_SERVICE_HEALTHCHECK_DB_LATENCY_THRESHOLD_SECONDS")) * time.Second,
			MessageBrokerLatencyThresholdSec: time.Duration(env.MustInt("PROFILE_SERVICE_HEALTHCHECK_KAFKA_LATENCY_THRESHOLD_SECONDS")) * time.Second,
			MessageBrokerConnectionString:    brokers,
		},
	}
}

func secondsFromEnv(key, defaultValue string) time.Duration {
	return time.Duration(env.WithDefaultInt(key, defaultValue)) * time.Second
}

func newPostgresOutbox(database *dbConn.Database, backend string) (messagingConn.OutboxWriter, error) {
	log.Trace("newPostgresOutbox")

	if backend != "postgres" {
		return nil, fmt.Errorf("unsupported outbox backend %q", backend)
	}
	return messagingConn.NewPostgresOutbox(database.Pool, database.Name, "")
}
