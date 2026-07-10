package main

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	env "lib/shared_lib/env"
	"model_serving_service/pkg/domain/model"
	servingkubernetes "model_serving_service/pkg/infra/network/k8s"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/watch"
)

func TestMainConfig(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Model serving main unit test suite")
}

var _ = Describe("readModelServingConfig", func() {
	BeforeEach(func() {
		env.ResetEnvironmentCache()
		Expect(os.Setenv("ENVIRONMENT", "local-dev")).To(Succeed())
		Expect(os.Unsetenv("MODEL_SERVING_SERVICE_NAME")).To(Succeed())
		Expect(os.Unsetenv("MODEL_SERVING_SERVICE_NAMESPACE")).To(Succeed())
		Expect(os.Unsetenv("MODEL_SERVING_SERVICE_HEALTHCHECK_CPU_THRESHOLD_PERCENT")).To(Succeed())
		Expect(os.Unsetenv("MODEL_SERVING_SERVICE_HEALTHCHECK_FREE_MEM_THRESHOLD_PERCENT")).To(Succeed())
		Expect(os.Unsetenv("MODEL_SERVING_SERVICE_HEALTHCHECK_PORT")).To(Succeed())
		Expect(os.Unsetenv("MODEL_SERVING_SERVICE_HEALTHCHECK_SERVICE_LATENCY_THRESHOLD_SECONDS")).To(Succeed())
		Expect(os.Unsetenv("MODEL_SERVING_SERVICE_HEALTHCHECK_CONTROLLER_MAX_SILENCE_SECONDS")).To(Succeed())
		Expect(os.Unsetenv("MODEL_SERVING_SERVICE_POLL_MS")).To(Succeed())
		Expect(os.Unsetenv("MODEL_SERVING_SERVICE_BACKEND")).To(Succeed())
		Expect(os.Unsetenv("MODEL_SERVING_SERVICE_LOCAL_STORE_PATH")).To(Succeed())
		Expect(os.Unsetenv("MODEL_SERVING_SERVICE_VLLM_IMAGE")).To(Succeed())
		Expect(os.Unsetenv("MODEL_SERVING_SERVICE_VLLM_MULTI_TENANT_ENABLED")).To(Succeed())
		Expect(os.Unsetenv("MODEL_SERVING_SERVICE_VLLM_REQUEST_TIMEOUT_MS")).To(Succeed())
		Expect(os.Unsetenv("MODEL_SERVING_SERVICE_LOCAL_OLLAMA_ENDPOINT")).To(Succeed())
		Expect(os.Unsetenv("MODEL_SERVING_SERVICE_LOCAL_ARTIFACT_CACHE_DIR")).To(Succeed())
		Expect(os.Unsetenv("BIGHILL_LOCAL_S3_STORAGE_DIR")).To(Succeed())
		Expect(os.Unsetenv("MODEL_SERVING_SERVICE_GGUF_INSPECTOR_COMMAND")).To(Succeed())
		Expect(os.Unsetenv("MODEL_SERVING_SERVICE_LOCAL_OLLAMA_CREATE_TIMEOUT_SECONDS")).To(Succeed())
	})

	It("uses operator defaults", func() {
		cfg := readModelServingConfig()

		Expect(cfg.ServiceName).To(Equal("model-serving-service"))
		Expect(cfg.Namespace).To(Equal("default"))
		Expect(cfg.Health.HealthCheckPort).To(Equal(5061))
		Expect(cfg.Health.CpuThresholdPercentage).To(Equal(80))
		Expect(cfg.Health.MemFreeThresholdPercentage).To(Equal(20))
		Expect(cfg.Health.ControllerMaxSilence.String()).To(Equal("30s"))
		Expect(cfg.PollEvery.String()).To(Equal("1s"))
		Expect(cfg.Backend).To(Equal("kubernetes"))
		Expect(cfg.LocalStore).To(ContainSubstring("local_served_models"))
		Expect(cfg.ServedModel.Group).To(Equal("serving.bighill.io"))
		Expect(cfg.ServedModel.Version).To(Equal("v1alpha1"))
		Expect(cfg.ServedModel.Resource).To(Equal("servedmodels"))
		Expect(cfg.Runtime.Image).To(Equal("vllm/vllm-openai:latest"))
		Expect(cfg.Runtime.MultiTenant).To(BeFalse())
		Expect(cfg.Runtime.RequestTimeout.String()).To(Equal("5s"))
		Expect(cfg.Runtime.Port).To(Equal(int32(8000)))
		Expect(cfg.Runtime.LocalOllamaEndpoint).To(Equal("http://localhost:11434"))
		Expect(cfg.Runtime.LocalArtifactCache).To(ContainSubstring("model_serving_artifacts"))
		Expect(cfg.Runtime.LocalS3StorageDir).To(BeEmpty())
		Expect(cfg.Runtime.GGUFInspector).To(Equal("python3 -m bighill_model_artifacts.gguf"))
		Expect(cfg.Runtime.OllamaCreateTimeout.String()).To(Equal("20m0s"))
	})

	It("reads explicit runtime config", func() {
		Expect(os.Setenv("MODEL_SERVING_SERVICE_NAME", "model-serving-service")).To(Succeed())
		Expect(os.Setenv("MODEL_SERVING_SERVICE_NAMESPACE", "ml-serving")).To(Succeed())
		Expect(os.Setenv("MODEL_SERVING_SERVICE_HEALTHCHECK_CPU_THRESHOLD_PERCENT", "70")).To(Succeed())
		Expect(os.Setenv("MODEL_SERVING_SERVICE_HEALTHCHECK_FREE_MEM_THRESHOLD_PERCENT", "25")).To(Succeed())
		Expect(os.Setenv("MODEL_SERVING_SERVICE_HEALTHCHECK_PORT", "6061")).To(Succeed())
		Expect(os.Setenv("MODEL_SERVING_SERVICE_HEALTHCHECK_SERVICE_LATENCY_THRESHOLD_SECONDS", "4")).To(Succeed())
		Expect(os.Setenv("MODEL_SERVING_SERVICE_HEALTHCHECK_CONTROLLER_MAX_SILENCE_SECONDS", "45")).To(Succeed())
		Expect(os.Setenv("MODEL_SERVING_SERVICE_POLL_MS", "2500")).To(Succeed())
		Expect(os.Setenv("MODEL_SERVING_SERVICE_BACKEND", "kubernetes")).To(Succeed())
		Expect(os.Setenv("MODEL_SERVING_SERVICE_LOCAL_STORE_PATH", "/tmp/served-models.json")).To(Succeed())
		Expect(os.Setenv("MODEL_SERVING_SERVICE_VLLM_IMAGE", "vllm/vllm-openai:v1")).To(Succeed())
		Expect(os.Setenv("MODEL_SERVING_SERVICE_VLLM_MULTI_TENANT_ENABLED", "true")).To(Succeed())
		Expect(os.Setenv("MODEL_SERVING_SERVICE_VLLM_REQUEST_TIMEOUT_MS", "2500")).To(Succeed())
		Expect(os.Setenv("MODEL_SERVING_SERVICE_LOCAL_OLLAMA_ENDPOINT", "http://ollama.local")).To(Succeed())
		Expect(os.Setenv("MODEL_SERVING_SERVICE_LOCAL_ARTIFACT_CACHE_DIR", "/tmp/model-artifacts")).To(Succeed())
		Expect(os.Setenv("BIGHILL_LOCAL_S3_STORAGE_DIR", "/tmp/local-s3")).To(Succeed())
		Expect(os.Setenv("MODEL_SERVING_SERVICE_GGUF_INSPECTOR_COMMAND", "python3 -m bighill_model_artifacts.gguf")).To(Succeed())
		Expect(os.Setenv("MODEL_SERVING_SERVICE_LOCAL_OLLAMA_CREATE_TIMEOUT_SECONDS", "42")).To(Succeed())

		cfg := readModelServingConfig()

		Expect(cfg.Namespace).To(Equal("ml-serving"))
		Expect(cfg.Health.HealthCheckPort).To(Equal(6061))
		Expect(cfg.Health.CpuThresholdPercentage).To(Equal(70))
		Expect(cfg.Health.MemFreeThresholdPercentage).To(Equal(25))
		Expect(cfg.Health.ServiceLatencyThreshold.String()).To(Equal("4s"))
		Expect(cfg.Health.ControllerMaxSilence.String()).To(Equal("45s"))
		Expect(cfg.PollEvery.String()).To(Equal("2.5s"))
		Expect(cfg.Backend).To(Equal("kubernetes"))
		Expect(cfg.LocalStore).To(Equal("/tmp/served-models.json"))
		Expect(cfg.Runtime.Image).To(Equal("vllm/vllm-openai:v1"))
		Expect(cfg.Runtime.MultiTenant).To(BeTrue())
		Expect(cfg.Runtime.RequestTimeout.String()).To(Equal("2.5s"))
		Expect(cfg.Runtime.LocalOllamaEndpoint).To(Equal("http://ollama.local"))
		Expect(cfg.Runtime.LocalArtifactCache).To(Equal("/tmp/model-artifacts"))
		Expect(cfg.Runtime.LocalS3StorageDir).To(Equal("/tmp/local-s3"))
		Expect(cfg.Runtime.GGUFInspector).To(Equal("python3 -m bighill_model_artifacts.gguf"))
		Expect(cfg.Runtime.OllamaCreateTimeout.String()).To(Equal("42s"))
	})

	It("uses the local backend without reading kubeconfig", func() {
		Expect(os.Setenv("MODEL_SERVING_SERVICE_BACKEND", "local")).To(Succeed())
		cfg := readModelServingConfig()
		cfg.LocalStore = GinkgoT().TempDir() + "/served_models.json"
		Expect(os.Setenv("KUBECONFIG", "/does/not/exist")).To(Succeed())

		store, runtimeAdapter, err := newServingBackend(cfg)

		Expect(err).NotTo(HaveOccurred())
		Expect(store).NotTo(BeNil())
		Expect(runtimeAdapter).NotTo(BeNil())
	})
})

var _ = Describe("model serving health", func() {
	It("fails liveness when known served models have no successful reconcile", func() {
		servedModel := modelServingHealthServedModel("served-model-unhealthy")
		store := &modelServingHealthStore{
			namespace: "default",
			listed:    []*model.ServedModel{servedModel},
			latest:    map[string]*model.ServedModel{servedModel.ResourceName: servedModel},
		}
		controller := servingkubernetes.NewServedModelController(store, modelServingFailingReconciler{}, time.Millisecond)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			done <- controller.Start(ctx)
		}()

		Eventually(func() int {
			return controller.Health().KnownServedModels
		}).Should(Equal(1))
		Eventually(func() int {
			return controller.Health().OutstandingServedModels
		}).Should(Equal(1))
		Eventually(func() error {
			return checkServedModelController(context.Background(), controller, time.Millisecond)
		}).Should(MatchError(ContainSubstring("outstanding served models")))

		cancel()
		Eventually(done).Should(Receive())
	})

	It("keeps liveness healthy for idle loaded served models", func() {
		servedModel := modelServingHealthServedModel("served-model-loaded")
		servedModel.Status = &model.ServedModelStatus{
			ServingLoadStatus:  model.ModelLoadStatusLoaded,
			ObservedGeneration: servedModel.Generation,
		}
		watcher := watch.NewFake()
		store := &modelServingHealthStore{
			namespace: "default",
			listed:    []*model.ServedModel{servedModel},
			latest:    map[string]*model.ServedModel{servedModel.ResourceName: servedModel},
			watcher:   watcher,
		}
		controller := servingkubernetes.NewServedModelController(store, modelServingLoadedReconciler{}, time.Hour)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			done <- controller.Start(ctx)
		}()

		Eventually(func() bool {
			return controller.Health().WatchActive
		}).Should(BeTrue())
		Eventually(func() int {
			return controller.Health().KnownServedModels
		}).Should(Equal(1))
		Eventually(func() int {
			return controller.Health().OutstandingServedModels
		}).Should(Equal(0))
		time.Sleep(5 * time.Millisecond)

		Expect(checkServedModelController(context.Background(), controller, time.Millisecond)).To(Succeed())

		cancel()
		watcher.Stop()
		Eventually(done).Should(Receive())
	})
})

type modelServingHealthStore struct {
	namespace string
	listed    []*model.ServedModel
	latest    map[string]*model.ServedModel
	watcher   watch.Interface
}

func (s *modelServingHealthStore) Namespace() string {
	return s.namespace
}

func (s *modelServingHealthStore) ListWithResourceVersion(context.Context) ([]*model.ServedModel, string, error) {
	return s.listed, "1", nil
}

func (s *modelServingHealthStore) Read(_ context.Context, resourceName string) (*model.ServedModel, error) {
	return s.latest[resourceName], nil
}

func (s *modelServingHealthStore) Watch(context.Context, string) (watch.Interface, error) {
	if s.watcher != nil {
		return s.watcher, nil
	}
	return watch.NewEmptyWatch(), nil
}

func (s *modelServingHealthStore) UpdateStatus(context.Context, string, *model.ServedModelStatus) error {
	return nil
}

type modelServingFailingReconciler struct{}

func (modelServingFailingReconciler) Reconcile(context.Context, *model.ServedModel) (*model.ServedModelStatus, error) {
	return nil, errors.New("forced reconcile failure")
}

type modelServingLoadedReconciler struct{}

func (modelServingLoadedReconciler) Reconcile(_ context.Context, servedModel *model.ServedModel) (*model.ServedModelStatus, error) {
	return &model.ServedModelStatus{
		ServingLoadStatus:  model.ModelLoadStatusLoaded,
		ObservedGeneration: servedModel.Generation,
	}, nil
}

func modelServingHealthServedModel(resourceName string) *model.ServedModel {
	return &model.ServedModel{
		ResourceName: resourceName,
		Namespace:    "default",
		Generation:   1,
		ModelID:      uuid.MustParse("4f4b8258-f9af-49f8-b5a8-f84d75891f3b"),
		Name:         "ranker",
		ModelVersion: 1,
		BaseModel:    "mistral",
		AdapterURI:   "s3://models/ranker",
	}
}
