package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"socket_service/pkg/app"
	"socket_service/pkg/infra/network"
	socketredis "socket_service/pkg/infra/redis"

	env "lib/shared_lib/env"
	"lib/shared_lib/healthcheck"
	logs "lib/shared_lib/logs"
	"lib/shared_lib/observability"
	"lib/shared_lib/userevents"

	"github.com/redis/rueidis"
	log "github.com/sirupsen/logrus"
)

var (
	Version string
)

type runtimeConfig struct {
	serviceName       string
	httpPort          int
	healthCheckConfig healthcheck.HealthCheckConfig
	redisAddress      string
	redisUsername     string
	redisPassword     string
	redisTLS          bool
	channelPrefix     string
	redisLiveBlockMS  int64
	socketTicketTTL   time.Duration
	websocketConfig   network.WebSocketConfig
}

func init() {
	logs.Init()
}

func main() {
	log.Trace("socket main")

	cfg := loadConfig()

	ctx := context.Background()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	obsShutdown := observability.Init(runCtx, cfg.serviceName, Version)
	defer obsShutdown()

	healthCheck := healthcheck.NewMonitor(cfg.healthCheckConfig).WithCpuCheck().WithMemoryCheck()
	defer healthCheck.Close()

	redisClient, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{cfg.redisAddress},
		Username:    cfg.redisUsername,
		Password:    cfg.redisPassword,
		TLSConfig:   userEventTLSConfig(cfg.redisTLS),
	})
	if err != nil {
		log.WithContext(ctx).WithError(err).Fatal("failed to initialize redis client")
	}
	defer redisClient.Close()

	streamReader := socketredis.NewStreamReader(redisClient, cfg.redisLiveBlockMS)
	ticketStore := socketredis.NewTicketStore(redisClient)
	roomResolver := app.NewRoomResolver(cfg.channelPrefix)
	subscriptionUsecase := app.NewSubscriptionUsecase(ticketStore, roomResolver, streamReader)
	protocolAdapter := network.NewProtocolDTOAdapter()
	socketHandler := network.NewWebSocketHandler(subscriptionUsecase, protocolAdapter, cfg.websocketConfig)

	httpMux := http.NewServeMux()
	httpMux.Handle("/v1/socket", socketHandler)
	httpMux.HandleFunc("/v1/socket-token", socketHandler.ServeTokenHTTP)
	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.httpPort),
		Handler: httpMux,
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.WithContext(runCtx).Infof("socket service health listener starting on %d", cfg.healthCheckConfig.HealthCheckPort)
		if err := healthCheck.Connect(runCtx); err != nil && err != http.ErrServerClosed {
			log.WithContext(runCtx).WithError(err).Fatal("unable to start socket healthcheck")
		}
	}()

	go func() {
		log.WithContext(runCtx).Infof("socket service http listener starting on %s", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.WithContext(runCtx).WithError(err).Fatal("unable to start socket http service")
		}
	}()

	<-quit
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.WithContext(shutdownCtx).WithError(err).Error("socket http shutdown failed")
	}
	cancel()
	log.Tracef("stopped %s version %s", cfg.serviceName, Version)
}

func loadConfig() runtimeConfig {
	log.Trace("loadConfig")

	serviceLatencyThreshold := time.Duration(env.MustInt("SOCKET_SERVICE_HEALTHCHECK_SERVICE_LATENCY_THRESHOLD_SECONDS")) * time.Second
	cfg := runtimeConfig{
		serviceName:      env.MustString("SOCKET_SERVICE_NAME"),
		httpPort:         env.MustInt("SOCKET_SERVICE_HTTP_PORT"),
		redisAddress:     env.MustString("SOCKET_SERVICE_REDIS_ADDRESS"),
		redisUsername:    env.MustString("SOCKET_SERVICE_REDIS_USERNAME"),
		redisPassword:    env.MustString("SOCKET_SERVICE_REDIS_PASSWORD"),
		redisTLS:         env.MustBool("SOCKET_SERVICE_REDIS_TLS"),
		channelPrefix:    env.MustString("SOCKET_SERVICE_CHANNEL_PREFIX"),
		redisLiveBlockMS: int64(env.MustInt("SOCKET_SERVICE_REDIS_LIVE_BLOCK_MS")),
		socketTicketTTL:  time.Duration(env.MustInt("SOCKET_SERVICE_TICKET_TTL_SECONDS")) * time.Second,
		healthCheckConfig: healthcheck.HealthCheckConfig{
			CpuThresholdPercentage:     env.MustInt("SOCKET_SERVICE_HEALTHCHECK_CPU_THRESHOLD_PERCENT"),
			MemFreeThresholdPercentage: env.MustInt("SOCKET_SERVICE_HEALTHCHECK_FREE_MEM_THRESHOLD_PERCENT"),
			HealthCheckPort:            env.MustInt("SOCKET_SERVICE_HEALTHCHECK_PORT"),
			ServiceLatencyThresholdSec: serviceLatencyThreshold,
		},
		websocketConfig: network.WebSocketConfig{
			ReplayLimit:     env.MustInt("SOCKET_SERVICE_REPLAY_LIMIT"),
			SendQueueSize:   env.MustInt("SOCKET_SERVICE_SEND_QUEUE_SIZE"),
			TicketTTL:       time.Duration(env.MustInt("SOCKET_SERVICE_TICKET_TTL_SECONDS")) * time.Second,
			AllowedOrigins:  env.MustStringSlice("SOCKET_SERVICE_ALLOWED_ORIGINS"),
			ReadTimeout:     time.Duration(env.MustInt("SOCKET_SERVICE_READ_TIMEOUT_SECONDS")) * time.Second,
			WriteTimeout:    time.Duration(env.MustInt("SOCKET_SERVICE_WRITE_TIMEOUT_SECONDS")) * time.Second,
			PingInterval:    time.Duration(env.MustInt("SOCKET_SERVICE_PING_INTERVAL_SECONDS")) * time.Second,
			PongTimeout:     time.Duration(env.MustInt("SOCKET_SERVICE_PONG_TIMEOUT_SECONDS")) * time.Second,
			MaxMessageBytes: env.MustInt64("SOCKET_SERVICE_MAX_MESSAGE_BYTES"),
		},
	}
	validateConfig(cfg)
	return cfg
}

func validateConfig(cfg runtimeConfig) {
	log.Trace("validateConfig")

	if cfg.websocketConfig.ReplayLimit <= 0 {
		log.Fatal("SOCKET_SERVICE_REPLAY_LIMIT must be positive")
	}
	if cfg.websocketConfig.SendQueueSize <= 0 {
		log.Fatal("SOCKET_SERVICE_SEND_QUEUE_SIZE must be positive")
	}
	if cfg.websocketConfig.ReadTimeout <= 0 {
		log.Fatal("SOCKET_SERVICE_READ_TIMEOUT_SECONDS must be positive")
	}
	if cfg.websocketConfig.WriteTimeout <= 0 {
		log.Fatal("SOCKET_SERVICE_WRITE_TIMEOUT_SECONDS must be positive")
	}
	if cfg.websocketConfig.PingInterval <= 0 {
		log.Fatal("SOCKET_SERVICE_PING_INTERVAL_SECONDS must be positive")
	}
	if cfg.websocketConfig.PongTimeout <= 0 {
		log.Fatal("SOCKET_SERVICE_PONG_TIMEOUT_SECONDS must be positive")
	}
	if cfg.websocketConfig.MaxMessageBytes <= 0 {
		log.Fatal("SOCKET_SERVICE_MAX_MESSAGE_BYTES must be positive")
	}
	if cfg.redisLiveBlockMS <= 0 {
		log.Fatal("SOCKET_SERVICE_REDIS_LIVE_BLOCK_MS must be positive")
	}
	if cfg.socketTicketTTL <= 0 {
		log.Fatal("SOCKET_SERVICE_TICKET_TTL_SECONDS must be positive")
	}
	if len(cfg.websocketConfig.AllowedOrigins) == 0 {
		log.Fatal("SOCKET_SERVICE_ALLOWED_ORIGINS must be configured")
	}
}

func userEventTLSConfig(redisTLS bool) *tls.Config {
	log.Trace("userEventTLSConfig")

	return userevents.Config{RedisTLS: redisTLS}.TLSConfig()
}
