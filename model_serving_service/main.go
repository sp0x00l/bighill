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
	Replicas        int32
	Port            int32
	CPU             string
	Memory          string
	GPUResource     string
	GPU             string
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

	client, err := servingk8s.NewDynamicClient()
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create kubernetes client")
	}
	store, err := servingk8s.NewServedModelStore(servingk8s.ServedModelStoreConfig{
		Namespace: cfg.Namespace,
		Group:     cfg.ServedModel.Group,
		Version:   cfg.ServedModel.Version,
		Resource:  cfg.ServedModel.Resource,
	}, client)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create served model store")
	}
	runtimeAdapter, err := servingk8s.NewVLLMRuntime(servingk8s.VLLMRuntimeConfig{
		Namespace:       cfg.Namespace,
		Image:           cfg.Runtime.Image,
		ImagePullPolicy: cfg.Runtime.ImagePullPolicy,
		ServiceAccount:  cfg.Runtime.ServiceAccount,
		Replicas:        cfg.Runtime.Replicas,
		Port:            cfg.Runtime.Port,
		CPU:             cfg.Runtime.CPU,
		Memory:          cfg.Runtime.Memory,
		GPUResource:     cfg.Runtime.GPUResource,
		GPU:             cfg.Runtime.GPU,
	}, client)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create vllm runtime")
	}
	reconciler := app.NewServedModelReconciler(runtimeAdapter, store)
	controller := servingk8s.NewServedModelController(store, reconciler, cfg.PollEvery)

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
		Namespace:   env.WithDefaultString("MODEL_SERVING_NAMESPACE", "default"),
		HealthPort:  env.WithDefaultInt("MODEL_SERVING_HEALTHCHECK_PORT", "5061"),
		PollEvery:   time.Duration(env.WithDefaultInt("MODEL_SERVING_POLL_MS", "1000")) * time.Millisecond,
		ServedModel: servedModelConfig{
			Group:    env.WithDefaultString("MODEL_SERVING_SERVED_MODEL_CRD_GROUP", "serving.bighill.io"),
			Version:  env.WithDefaultString("MODEL_SERVING_SERVED_MODEL_CRD_VERSION", "v1alpha1"),
			Resource: env.WithDefaultString("MODEL_SERVING_SERVED_MODEL_CRD_RESOURCE", "servedmodels"),
		},
		Runtime: runtimeConfig{
			Image:           env.WithDefaultString("MODEL_SERVING_VLLM_IMAGE", "vllm/vllm-openai:latest"),
			ImagePullPolicy: env.WithDefaultString("MODEL_SERVING_VLLM_IMAGE_PULL_POLICY", "IfNotPresent"),
			ServiceAccount:  env.WithDefaultString("MODEL_SERVING_VLLM_SERVICE_ACCOUNT", ""),
			Replicas:        int32(env.WithDefaultInt("MODEL_SERVING_VLLM_REPLICAS", "1")),
			Port:            int32(env.WithDefaultInt("MODEL_SERVING_VLLM_PORT", "8000")),
			CPU:             env.WithDefaultString("MODEL_SERVING_VLLM_CPU", "1"),
			Memory:          env.WithDefaultString("MODEL_SERVING_VLLM_MEMORY", "4Gi"),
			GPUResource:     env.WithDefaultString("MODEL_SERVING_VLLM_GPU_RESOURCE", "nvidia.com/gpu"),
			GPU:             env.WithDefaultString("MODEL_SERVING_VLLM_GPU", "1"),
		},
	}
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
