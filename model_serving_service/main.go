package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"model_serving_service/pkg/app"
	servingk8s "model_serving_service/pkg/infra/network/k8s"
	localserving "model_serving_service/pkg/infra/network/localserving"

	env "lib/shared_lib/env"
	logs "lib/shared_lib/logs"
	trace "lib/shared_lib/trace"

	log "github.com/sirupsen/logrus"
)

var Version string

type modelServingConfig struct {
	ServiceName string
	Namespace   string
	HealthPort  int
	PollEvery   time.Duration
	Backend     string
	LocalStore  string
	ServedModel servedModelConfig
	Runtime     runtimeConfig
}

type servedModelConfig struct {
	Group    string
	Version  string
	Resource string
}

type runtimeConfig struct {
	Image           string
	ImagePullPolicy string
	ServiceAccount  string
	MultiTenant     bool
	Replicas        int32
	Port            int32
	CPU             string
	Memory          string
	GPUResource     string
	GPU             string
	RequestTimeout  time.Duration
}

func init() {
	logs.Init()
}

func main() {
	ctx := context.Background()
	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	cfg := readModelServingConfig()
	serviceName := cfg.ServiceName
	log.Trace(fmt.Sprintf("starting the %s service", serviceName))
	traceShutdown := trace.Init(cancelCtx, serviceName, Version)
	defer traceShutdown()

	store, runtimeAdapter, err := newServingBackend(cfg)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create serving backend")
	}
	reconciler := app.NewServedModelReconciler(runtimeAdapter, store)
	controller := servingk8s.NewServedModelController(store, reconciler, cfg.PollEvery, servingk8s.WithSharedRuntimeSerialization(cfg.Runtime.MultiTenant))

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	healthServer := newHealthServer(cfg.HealthPort)
	go func() {
		if err := healthServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.WithContext(cancelCtx).WithError(err).Error("health server stopped unexpectedly")
			quit <- syscall.SIGTERM
		}
	}()
	go func() {
		if err := controller.Start(cancelCtx); err != nil && !errors.Is(err, context.Canceled) {
			log.WithContext(cancelCtx).WithError(err).Error("served model controller stopped unexpectedly")
			quit <- syscall.SIGTERM
		}
	}()

	<-quit

	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = healthServer.Shutdown(shutdownCtx)
	log.Trace(fmt.Sprintf("stopped the %s service", serviceName))
}

func readModelServingConfig() modelServingConfig {
	return modelServingConfig{
		ServiceName: env.WithDefaultString("MODEL_SERVING_SERVICE_NAME", "model-serving-service"),
		Namespace:   env.WithDefaultString("MODEL_SERVING_SERVICE_NAMESPACE", "default"),
		HealthPort:  env.WithDefaultInt("MODEL_SERVING_SERVICE_HEALTHCHECK_PORT", "5061"),
		PollEvery:   time.Duration(env.WithDefaultInt("MODEL_SERVING_SERVICE_POLL_MS", "1000")) * time.Millisecond,
		Backend:     env.WithDefaultString("MODEL_SERVING_SERVICE_BACKEND", defaultServingBackend()),
		LocalStore:  env.WithDefaultString("MODEL_SERVING_SERVICE_LOCAL_STORE_PATH", ""),
		ServedModel: servedModelConfig{
			Group:    env.WithDefaultString("MODEL_SERVING_SERVICE_SERVED_MODEL_CRD_GROUP", "serving.bighill.io"),
			Version:  env.WithDefaultString("MODEL_SERVING_SERVICE_SERVED_MODEL_CRD_VERSION", "v1alpha1"),
			Resource: env.WithDefaultString("MODEL_SERVING_SERVICE_SERVED_MODEL_CRD_RESOURCE", "servedmodels"),
		},
		Runtime: runtimeConfig{
			Image:           env.WithDefaultString("MODEL_SERVING_SERVICE_VLLM_IMAGE", "vllm/vllm-openai:latest"),
			ImagePullPolicy: env.WithDefaultString("MODEL_SERVING_SERVICE_VLLM_IMAGE_PULL_POLICY", "IfNotPresent"),
			ServiceAccount:  env.WithDefaultString("MODEL_SERVING_SERVICE_VLLM_SERVICE_ACCOUNT", ""),
			MultiTenant:     env.WithDefaultBool("MODEL_SERVING_SERVICE_VLLM_MULTI_TENANT_ENABLED", false),
			Replicas:        int32(env.WithDefaultInt("MODEL_SERVING_SERVICE_VLLM_REPLICAS", "1")),
			Port:            int32(env.WithDefaultInt("MODEL_SERVING_SERVICE_VLLM_PORT", "8000")),
			CPU:             env.WithDefaultString("MODEL_SERVING_SERVICE_VLLM_CPU", "1"),
			Memory:          env.WithDefaultString("MODEL_SERVING_SERVICE_VLLM_MEMORY", "4Gi"),
			GPUResource:     env.WithDefaultString("MODEL_SERVING_SERVICE_VLLM_GPU_RESOURCE", "nvidia.com/gpu"),
			GPU:             env.WithDefaultString("MODEL_SERVING_SERVICE_VLLM_GPU", "1"),
			RequestTimeout:  time.Duration(env.WithDefaultInt("MODEL_SERVING_SERVICE_VLLM_REQUEST_TIMEOUT_MS", "5000")) * time.Millisecond,
		},
	}
}

func newServingBackend(cfg modelServingConfig) (servingk8s.ServedModelRepository, app.ServingRuntime, error) {
	log.Trace("newServingBackend")

	switch cfg.Backend {
	case "local":
		store, err := localserving.NewStore(cfg.Namespace, cfg.LocalStore)
		if err != nil {
			return nil, nil, err
		}
		return store, localserving.NewRuntime(cfg.Namespace, cfg.Runtime.Port), nil
	case "kubernetes":
		client, err := servingk8s.NewDynamicClient()
		if err != nil {
			return nil, nil, err
		}
		store, err := servingk8s.NewServedModelStore(servingk8s.ServedModelStoreConfig{
			Namespace: cfg.Namespace,
			Group:     cfg.ServedModel.Group,
			Version:   cfg.ServedModel.Version,
			Resource:  cfg.ServedModel.Resource,
		}, client)
		if err != nil {
			return nil, nil, err
		}
		runtimeAdapter, err := servingk8s.NewVLLMRuntime(servingk8s.VLLMRuntimeConfig{
			Namespace:       cfg.Namespace,
			Image:           cfg.Runtime.Image,
			ImagePullPolicy: cfg.Runtime.ImagePullPolicy,
			ServiceAccount:  cfg.Runtime.ServiceAccount,
			MultiTenant:     cfg.Runtime.MultiTenant,
			Replicas:        cfg.Runtime.Replicas,
			Port:            cfg.Runtime.Port,
			CPU:             cfg.Runtime.CPU,
			Memory:          cfg.Runtime.Memory,
			GPUResource:     cfg.Runtime.GPUResource,
			GPU:             cfg.Runtime.GPU,
			RequestTimeout:  cfg.Runtime.RequestTimeout,
		}, client)
		if err != nil {
			return nil, nil, err
		}
		return store, runtimeAdapter, nil
	default:
		return nil, nil, fmt.Errorf("unsupported model serving backend %q", cfg.Backend)
	}
}

func defaultServingBackend() string {
	log.Trace("defaultServingBackend")

	return "kubernetes"
}

func newHealthServer(port int) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return &http.Server{
		Addr:              ":" + strconv.Itoa(port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}
