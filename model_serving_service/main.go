package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"model_serving_service/pkg/app"
	"model_serving_service/pkg/domain/model"
	servingkubernetes "model_serving_service/pkg/infra/network/k8s"
	localserving "model_serving_service/pkg/infra/network/localserving"

	env "lib/shared_lib/env"
	coreHealthCheck "lib/shared_lib/healthcheck"
	"lib/shared_lib/lifecycle"
	logs "lib/shared_lib/logs"
	trace "lib/shared_lib/trace"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

var Version string

type modelServingConfig struct {
	ServiceName string
	Namespace   string
	PollEvery   time.Duration
	Backend     string
	LocalStore  string
	ServedModel servedModelConfig
	BaseRuntime baseRuntimeConfig
	Runtime     runtimeConfig
	Health      healthConfig
	Lifecycle   lifecycle.Config
}

type servedModelConfig struct {
	Group    string
	Version  string
	Resource string
}

type baseRuntimeConfig struct {
	Group    string
	Version  string
	Resource string
}

type runtimeConfig struct {
	Image               string
	ImagePullPolicy     string
	ServiceAccount      string
	ForceDedicated      bool
	MaxLoras            int
	MaxLoraRank         int
	Replicas            int32
	Port                int32
	CPU                 string
	Memory              string
	GPUResource         string
	GPU                 string
	RequestTimeout      time.Duration
	LocalOllamaEndpoint string
	LocalArtifactCache  string
	LocalS3StorageDir   string
	GGUFInspector       string
	OllamaCreateTimeout time.Duration
}

type healthConfig struct {
	CpuThresholdPercentage     int
	MemFreeThresholdPercentage int
	HealthCheckPort            int
	ServiceLatencyThreshold    time.Duration
	ControllerMaxSilence       time.Duration
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

	store, runtimeAdapter, baseRuntimeStore, err := newServingBackend(cfg)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create serving backend")
	}
	reconciler := app.NewServedModelReconciler(runtimeAdapter, store)
	controllerOptions := []servingkubernetes.ServedModelControllerOption{}
	if baseRuntimeStore != nil {
		controllerOptions = append(controllerOptions, servingkubernetes.WithBaseRuntimeStore(baseRuntimeStore))
	}
	controller := servingkubernetes.NewServedModelController(store, reconciler, cfg.PollEvery, controllerOptions...)

	healthCheck := coreHealthCheck.NewMonitor(newHealthCheckConfig(cfg.Health))
	healthCheck = healthCheck.WithCpuCheck().WithMemoryCheck()
	healthCheck = healthCheck.Register("served_model_controller", servedModelControllerReadinessCheck(controller, cfg.Health.ControllerMaxSilence))
	healthServer := newModelServingHealthServer(cfg.Health.HealthCheckPort, healthCheck, controller, store, cfg.Health.ControllerMaxSilence)
	supervisor := lifecycle.NewSupervisorWithConfig(cfg.Lifecycle,
		lifecycle.CloserComponent("model-serving-observability", func() error {
			traceShutdown()
			return nil
		}),
		lifecycle.ServerComponent("model-serving-health", healthServer),
		lifecycle.WorkerComponent("model-serving-controller", func(ctx context.Context) error {
			if err := controller.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		}),
	)
	if err := supervisor.RunWithSignals(cancelCtx, syscall.SIGINT, syscall.SIGTERM); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.WithContext(cancelCtx).WithError(err).Errorf("%s service stopped with error", serviceName)
	}
	cancel()
	log.Trace(fmt.Sprintf("stopped the %s service", serviceName))
}

func readModelServingConfig() modelServingConfig {
	env.RequireServiceEnvironment()

	return modelServingConfig{
		ServiceName: env.WithDefaultString("MODEL_SERVING_SERVICE_NAME", "model-serving-service"),
		Namespace:   env.WithDefaultString("MODEL_SERVING_SERVICE_NAMESPACE", "default"),
		PollEvery:   time.Duration(env.WithDefaultInt("MODEL_SERVING_SERVICE_POLL_MS", "1000")) * time.Millisecond,
		Backend:     env.WithDefaultString("MODEL_SERVING_SERVICE_BACKEND", defaultServingBackend()),
		LocalStore:  env.WithDefaultString("MODEL_SERVING_SERVICE_LOCAL_STORE_PATH", defaultLocalStorePath()),
		ServedModel: servedModelConfig{
			Group:    env.WithDefaultString("MODEL_SERVING_SERVICE_SERVED_MODEL_CRD_GROUP", "serving.bighill.io"),
			Version:  env.WithDefaultString("MODEL_SERVING_SERVICE_SERVED_MODEL_CRD_VERSION", "v1alpha1"),
			Resource: env.WithDefaultString("MODEL_SERVING_SERVICE_SERVED_MODEL_CRD_RESOURCE", "servedmodels"),
		},
		BaseRuntime: baseRuntimeConfig{
			Group:    env.WithDefaultString("MODEL_SERVING_SERVICE_BASE_RUNTIME_CRD_GROUP", "serving.bighill.io"),
			Version:  env.WithDefaultString("MODEL_SERVING_SERVICE_BASE_RUNTIME_CRD_VERSION", "v1alpha1"),
			Resource: env.WithDefaultString("MODEL_SERVING_SERVICE_BASE_RUNTIME_CRD_RESOURCE", "baseruntimes"),
		},
		Runtime: runtimeConfig{
			Image:               env.WithDefaultString("MODEL_SERVING_SERVICE_VLLM_IMAGE", "vllm/vllm-openai:latest"),
			ImagePullPolicy:     env.WithDefaultString("MODEL_SERVING_SERVICE_VLLM_IMAGE_PULL_POLICY", "IfNotPresent"),
			ServiceAccount:      env.WithDefaultString("MODEL_SERVING_SERVICE_VLLM_SERVICE_ACCOUNT", ""),
			ForceDedicated:      env.WithDefaultBool("MODEL_SERVING_SERVICE_VLLM_FORCE_DEDICATED_ADAPTERS", false),
			MaxLoras:            env.WithDefaultInt("MODEL_SERVING_SERVICE_VLLM_MAX_LORAS", "8"),
			MaxLoraRank:         env.WithDefaultInt("MODEL_SERVING_SERVICE_VLLM_MAX_LORA_RANK", "16"),
			Replicas:            int32(env.WithDefaultInt("MODEL_SERVING_SERVICE_VLLM_REPLICAS", "1")),
			Port:                int32(env.WithDefaultInt("MODEL_SERVING_SERVICE_VLLM_PORT", "8000")),
			CPU:                 env.WithDefaultString("MODEL_SERVING_SERVICE_VLLM_CPU", "1"),
			Memory:              env.WithDefaultString("MODEL_SERVING_SERVICE_VLLM_MEMORY", "4Gi"),
			GPUResource:         env.WithDefaultString("MODEL_SERVING_SERVICE_VLLM_GPU_RESOURCE", "nvidia.com/gpu"),
			GPU:                 env.WithDefaultString("MODEL_SERVING_SERVICE_VLLM_GPU", "1"),
			RequestTimeout:      time.Duration(env.WithDefaultInt("MODEL_SERVING_SERVICE_VLLM_REQUEST_TIMEOUT_MS", "5000")) * time.Millisecond,
			LocalOllamaEndpoint: env.WithDefaultString("MODEL_SERVING_SERVICE_LOCAL_OLLAMA_ENDPOINT", "http://localhost:11434"),
			LocalArtifactCache:  env.WithDefaultString("MODEL_SERVING_SERVICE_LOCAL_ARTIFACT_CACHE_DIR", filepath.Join(os.TempDir(), "bighill", "model_serving_artifacts")),
			LocalS3StorageDir:   env.WithDefaultString("BIGHILL_LOCAL_S3_STORAGE_DIR", ""),
			GGUFInspector:       env.WithDefaultString("MODEL_SERVING_SERVICE_GGUF_INSPECTOR_COMMAND", "python3 -m bighill_model_artifacts.gguf"),
			OllamaCreateTimeout: secondsFromEnv("MODEL_SERVING_SERVICE_LOCAL_OLLAMA_CREATE_TIMEOUT_SECONDS", "1200"),
		},
		Health: healthConfig{
			CpuThresholdPercentage:     env.WithDefaultInt("MODEL_SERVING_SERVICE_HEALTHCHECK_CPU_THRESHOLD_PERCENT", "80"),
			MemFreeThresholdPercentage: env.WithDefaultInt("MODEL_SERVING_SERVICE_HEALTHCHECK_FREE_MEM_THRESHOLD_PERCENT", "20"),
			HealthCheckPort:            env.WithDefaultInt("MODEL_SERVING_SERVICE_HEALTHCHECK_PORT", "5061"),
			ServiceLatencyThreshold:    secondsFromEnv("MODEL_SERVING_SERVICE_HEALTHCHECK_SERVICE_LATENCY_THRESHOLD_SECONDS", "5"),
			ControllerMaxSilence:       secondsFromEnv("MODEL_SERVING_SERVICE_HEALTHCHECK_CONTROLLER_MAX_SILENCE_SECONDS", "30"),
		},
		Lifecycle: lifecycle.Config{
			ReadinessTimeout: secondsFromEnv("MODEL_SERVING_SERVICE_LIFECYCLE_READINESS_TIMEOUT_SECONDS", "30"),
			DrainTimeout:     secondsFromEnv("MODEL_SERVING_SERVICE_LIFECYCLE_DRAIN_TIMEOUT_SECONDS", "30"),
			CloseTimeout:     secondsFromEnv("MODEL_SERVING_SERVICE_LIFECYCLE_CLOSE_TIMEOUT_SECONDS", "10"),
		},
	}
}

func defaultLocalStorePath() string {
	log.Trace("defaultLocalStorePath")

	return filepath.Join(os.TempDir(), "bighill", "local_served_models", "served_models.json")
}

func newServingBackend(cfg modelServingConfig) (servingkubernetes.ServedModelRepository, app.ServingRuntime, servingkubernetes.BaseRuntimeRepository, error) {
	log.Trace("newServingBackend")

	switch cfg.Backend {
	case "local":
		store, err := localserving.NewStore(cfg.Namespace, cfg.LocalStore)
		if err != nil {
			return nil, nil, nil, err
		}
		runtimeAdapter, err := localserving.NewRuntime(cfg.Namespace, cfg.Runtime.Port, cfg.Runtime.LocalOllamaEndpoint,
			localserving.WithArtifactCache(cfg.Runtime.LocalArtifactCache),
			localserving.WithLocalS3Dir(cfg.Runtime.LocalS3StorageDir),
			localserving.WithGGUFInspectorCommand(cfg.Runtime.GGUFInspector),
			localserving.WithCreateTimeout(cfg.Runtime.OllamaCreateTimeout),
		)
		if err != nil {
			return nil, nil, nil, err
		}
		return store, runtimeAdapter, nil, nil
	case "kubernetes":
		client, err := servingkubernetes.NewDynamicClient()
		if err != nil {
			return nil, nil, nil, err
		}
		store, err := servingkubernetes.NewServedModelStore(servingkubernetes.ServedModelStoreConfig{
			Namespace: cfg.Namespace,
			Group:     cfg.ServedModel.Group,
			Version:   cfg.ServedModel.Version,
			Resource:  cfg.ServedModel.Resource,
		}, client)
		if err != nil {
			return nil, nil, nil, err
		}
		baseRuntimeStore, err := servingkubernetes.NewBaseRuntimeStore(servingkubernetes.BaseRuntimeStoreConfig{
			Namespace: cfg.Namespace,
			Group:     cfg.BaseRuntime.Group,
			Version:   cfg.BaseRuntime.Version,
			Resource:  cfg.BaseRuntime.Resource,
		}, client)
		if err != nil {
			return nil, nil, nil, err
		}
		runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(servingkubernetes.VLLMRuntimeConfig{
			Namespace:               cfg.Namespace,
			Image:                   cfg.Runtime.Image,
			ImagePullPolicy:         cfg.Runtime.ImagePullPolicy,
			ServiceAccount:          cfg.Runtime.ServiceAccount,
			ForceDedicated:          cfg.Runtime.ForceDedicated,
			MaxLoras:                cfg.Runtime.MaxLoras,
			MaxLoraRank:             cfg.Runtime.MaxLoraRank,
			Replicas:                cfg.Runtime.Replicas,
			Port:                    cfg.Runtime.Port,
			CPU:                     cfg.Runtime.CPU,
			Memory:                  cfg.Runtime.Memory,
			GPUResource:             cfg.Runtime.GPUResource,
			GPU:                     cfg.Runtime.GPU,
			RequestTimeout:          cfg.Runtime.RequestTimeout,
			BaseRuntimeStore:        baseRuntimeStore,
			ServedModelStatusWriter: store,
		}, client)
		if err != nil {
			return nil, nil, nil, err
		}
		return store, runtimeAdapter, baseRuntimeStore, nil
	default:
		return nil, nil, nil, fmt.Errorf("unsupported model serving backend %q", cfg.Backend)
	}
}

func defaultServingBackend() string {
	log.Trace("defaultServingBackend")

	return "kubernetes"
}

func newHealthCheckConfig(cfg healthConfig) coreHealthCheck.HealthCheckConfig {
	return coreHealthCheck.HealthCheckConfig{
		CpuThresholdPercentage:     cfg.CpuThresholdPercentage,
		MemFreeThresholdPercentage: cfg.MemFreeThresholdPercentage,
		HealthCheckPort:            cfg.HealthCheckPort,
		ServiceLatencyThresholdSec: cfg.ServiceLatencyThreshold,
		HttpCheckTargets:           map[string]string{},
	}
}

type modelServingHealthServer struct {
	server *http.Server
	ready  atomic.Bool
}

func newModelServingHealthServer(port int, readiness *coreHealthCheck.Monitor, controller *servingkubernetes.ServedModelController, store servingkubernetes.ServedModelRepository, maxSilence time.Duration) *modelServingHealthServer {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if err := readiness.Check(r.Context()); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, err.Error())
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/live", func(w http.ResponseWriter, r *http.Request) {
		if err := checkServedModelController(r.Context(), controller, maxSilence); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, err.Error())
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/v1/private/served-models/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/load") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		modelIDRaw := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/private/served-models/"), "/load")
		modelID, err := uuid.Parse(strings.Trim(modelIDRaw, "/"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "invalid model id")
			return
		}
		if err := triggerServedModelLoad(r.Context(), store, modelID); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, err.Error())
			return
		}
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte("accepted"))
	})
	return &modelServingHealthServer{server: &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}}
}

func triggerServedModelLoad(ctx context.Context, store servingkubernetes.ServedModelRepository, modelID uuid.UUID) error {
	log.Trace("triggerServedModelLoad")

	servedModels, _, err := store.ListWithResourceVersion(ctx)
	if err != nil {
		return err
	}
	for _, servedModel := range servedModels {
		if servedModel.ModelID != modelID {
			continue
		}
		status := &model.ServedModelStatus{
			ServingLoadStatus:  model.ModelLoadStatusNotLoaded,
			ServingTarget:      servingField(servedModel.Status, servedModel.ServingTarget, func(s *model.ServedModelStatus) string { return s.ServingTarget }),
			ServingModel:       servingField(servedModel.Status, servedModel.ServingModel, func(s *model.ServedModelStatus) string { return s.ServingModel }),
			ServingProtocol:    servedModel.ServingProtocol,
			FailureReason:      "",
			ObservedGeneration: servedModel.Generation,
		}
		if servedModel.Status != nil && servedModel.Status.ServingProtocol.String() != "" {
			status.ServingProtocol = servedModel.Status.ServingProtocol
		}
		return store.UpdateStatus(ctx, servedModel.ResourceName, status)
	}
	return fmt.Errorf("served model %s not found", modelID)
}

func servingField(status *model.ServedModelStatus, fallback string, read func(*model.ServedModelStatus) string) string {
	log.Trace("servingField")

	if status == nil {
		return fallback
	}
	if value := strings.TrimSpace(read(status)); value != "" {
		return value
	}
	return fallback
}

func (s *modelServingHealthServer) Connect() error {
	log.Trace("modelServingHealthServer Connect")

	listener, err := net.Listen("tcp", s.server.Addr)
	if err != nil {
		return err
	}
	s.ready.Store(true)
	defer s.ready.Store(false)
	log.Infof("health check monitor starting http listener on %s", s.server.Addr)
	return s.server.Serve(listener)
}

func (s *modelServingHealthServer) Shutdown(ctx context.Context) error {
	log.Trace("modelServingHealthServer Shutdown")

	s.ready.Store(false)
	if err := s.server.Shutdown(ctx); err != nil {
		log.WithContext(ctx).Errorf("health http server shutdown error: %v", err)
		return err
	}
	return nil
}

func (s *modelServingHealthServer) Ready() bool {
	log.Trace("modelServingHealthServer Ready")

	return s.ready.Load()
}

func servedModelControllerReadinessCheck(controller *servingkubernetes.ServedModelController, maxSilence time.Duration) func(context.Context, coreHealthCheck.HealthCheckConfig) error {
	return func(ctx context.Context, _ coreHealthCheck.HealthCheckConfig) error {
		return checkServedModelController(ctx, controller, maxSilence)
	}
}

func checkServedModelController(ctx context.Context, controller *servingkubernetes.ServedModelController, maxSilence time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if maxSilence <= 0 {
		maxSilence = 30 * time.Second
	}
	health := controller.Health()
	if !health.Started {
		return fmt.Errorf("served model controller has not started")
	}
	if health.LastActivityAt.IsZero() {
		return fmt.Errorf("served model controller has not reported activity")
	}
	if !health.WatchActive {
		if silence := time.Since(health.LastActivityAt); silence > maxSilence {
			if health.LastError != "" {
				return fmt.Errorf("served model controller inactive for %s after error: %s", silence.Truncate(time.Second), health.LastError)
			}
			return fmt.Errorf("served model controller inactive for %s", silence.Truncate(time.Second))
		}
	}
	if health.OutstandingServedModels > 0 {
		if health.LastSuccessfulReconcileAt.IsZero() {
			firstKnownAt := health.FirstKnownServedModelAt
			if firstKnownAt.IsZero() {
				firstKnownAt = health.LastActivityAt
			}
			if silence := time.Since(firstKnownAt); silence > maxSilence {
				return fmt.Errorf("served model controller has %d outstanding served models and no successful reconcile for %s", health.OutstandingServedModels, silence.Truncate(time.Second))
			}
			return nil
		}
		if silence := time.Since(health.LastSuccessfulReconcileAt); silence > maxSilence {
			if health.LastError != "" {
				return fmt.Errorf("served model controller has %d outstanding served models and no successful reconcile for %s after error: %s", health.OutstandingServedModels, silence.Truncate(time.Second), health.LastError)
			}
			return fmt.Errorf("served model controller has %d outstanding served models and no successful reconcile for %s", health.OutstandingServedModels, silence.Truncate(time.Second))
		}
	}
	return nil
}

func secondsFromEnv(key, defaultValue string) time.Duration {
	return time.Duration(env.WithDefaultInt(key, defaultValue)) * time.Second
}
