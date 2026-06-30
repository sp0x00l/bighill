package main

import (
	dbConn "lib/shared_lib/db"
	env "lib/shared_lib/env"
	"lib/shared_lib/healthcheck"
	kms "lib/shared_lib/key_management"
	logs "lib/shared_lib/logs"
	messagingConn "lib/shared_lib/messaging"
	"lib/shared_lib/observability"
	"lib/shared_lib/transport"
	"time"

	authProvider "lib/shared_lib/auth"

	"context"
	"errors"
	sharedclock "lib/shared_lib/clock"
	"net/http"

	"os"
	"os/signal"
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
	outboxRelayConfig       messagingConn.OutboxRelayConfig
	kafkaPublisherTopic     string
	healthCheckConfig       healthcheck.HealthCheckConfig
	useStagingTestToken     bool
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
	defer redisClient.Close()
	authStore := authProvider.NewRevocationStore(redisClient, authProvider.WithKeyPrefix("auth:"))
	oauthStateStore := db.NewOAuthStateStore(redisClient, "auth:oauth:")

	messagingFactory := messagingConn.NewMessenger(cfg.messagingConfig, cancelFtn)

	msgPublisher, err := messagingFactory.Publisher(cancelCtx)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create the publisher")
	}
	outboxRelay, err := messagingFactory.OutboxRelay(cancelCtx, cfg.outboxRelayConfig)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Warn("unable to create outbox relay")
	} else {
		go func() {
			if relayErr := outboxRelay.Run(cancelCtx); relayErr != nil && !errors.Is(relayErr, context.Canceled) {
				log.WithContext(cancelCtx).WithError(relayErr).Error("outbox relay stopped unexpectedly")
			}
		}()
	}

	profilePublisher := messaging.NewUserEventPublisher(msgPublisher, cfg.kafkaPublisherTopic)

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
			MsgPublisher:       profilePublisher,
			AuthStore:          authStore,
			AuthProvider:       authProv,
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
	defer healthCheck.Close()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := healthCheck.Connect(cancelCtx); err != nil {
			if err != http.ErrServerClosed {
				log.Fatalf("unable to start the %s healthcheck: %v", cfg.serviceName, err)
			}
		}
	}()

	go func() {
		if err := restService.Connect(); err != nil {
			if err != http.ErrServerClosed {
				log.Fatalf("unable to start the %s service: %v", cfg.serviceName, err)
			}
			quit <- syscall.SIGTERM
		}
	}()

	<-quit

	restService.Close()
	obsShutdown()
	cancelFtn()
	log.Tracef("stopped %s version %s", cfg.serviceName, Version)
}

func loadConfig() runtimeConfig {
	brokers := env.MustString("KAFKA_BROKER")
	return runtimeConfig{
		serviceName:             env.MustString("PROFILE_SERVICE_NAME"),
		servicePort:             env.MustInt("PROFILE_SERVICE_HTTP_PORT"),
		authExpirationInMinutes: env.MustInt("PROFILE_AUTH_EXPIRATION_MINUTES"),
		emailValidationTTL:      time.Duration(env.MustInt("PROFILE_EMAIL_VALIDATION_TTL_MINUTES")) * time.Minute,
		oauthStateTTL:           time.Duration(env.MustInt("PROFILE_OAUTH_STATE_TTL_MINUTES")) * time.Minute,
		googleOAuth: rest.OAuthProviderConfig{
			ClientID:     env.MustString("PROFILE_OAUTH_GOOGLE_CLIENT_ID"),
			ClientSecret: env.MustString("PROFILE_OAUTH_GOOGLE_CLIENT_SECRET"),
		},
		discordOAuth: rest.OAuthProviderConfig{
			ClientID:     env.MustString("PROFILE_OAUTH_DISCORD_CLIENT_ID"),
			ClientSecret: env.MustString("PROFILE_OAUTH_DISCORD_CLIENT_SECRET"),
		},
		redisOption: rueidis.ClientOption{
			InitAddress: []string{env.MustString("PROFILE_SERVICE_REDIS_ADDRESS")},
			Username:    env.MustString("PROFILE_SERVICE_REDIS_USERNAME"),
			Password:    env.MustString("PROFILE_SERVICE_REDIS_PASSWORD"),
		},
		messagingConfig: messagingConn.MessengerConfig{
			DlqURL:    env.MustString("PROFILE_DLQ"),
			OutboxURL: env.MustString("PROFILE_OUTBOX"),
			GroupID:   env.MustString("PROFILE_KAFKA_GROUP_ID"),
			Brokers:   brokers,
		},
		outboxRelayConfig: messagingConn.OutboxRelayConfig{
			PollInterval:   time.Duration(env.MustInt("PROFILE_SERVICE_OUTBOX_RELAY_POLL_MS")) * time.Millisecond,
			FailureBackoff: time.Duration(env.MustInt("PROFILE_SERVICE_OUTBOX_RELAY_FAILURE_BACKOFF_MS")) * time.Millisecond,
			BatchSize:      int32(env.MustInt("PROFILE_SERVICE_OUTBOX_RELAY_BATCH_SIZE")),
		},
		kafkaPublisherTopic: env.MustString("PROFILE_SERVICE_KAFKA_PUBLISHER_TOPIC"),
		useStagingTestToken: env.WithDefaultBool("PROFILE_USE_STAGING_TEST_EMAIL_TOKEN", env.IsStaging()),
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
