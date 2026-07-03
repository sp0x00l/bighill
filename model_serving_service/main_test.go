package main

import (
	"os"
	"testing"

	env "lib/shared_lib/env"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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
		Expect(os.Unsetenv("MODEL_SERVING_SERVICE_HEALTHCHECK_PORT")).To(Succeed())
		Expect(os.Unsetenv("MODEL_SERVING_SERVICE_POLL_MS")).To(Succeed())
		Expect(os.Unsetenv("MODEL_SERVING_SERVICE_BACKEND")).To(Succeed())
		Expect(os.Unsetenv("MODEL_SERVING_SERVICE_LOCAL_STORE_PATH")).To(Succeed())
		Expect(os.Unsetenv("MODEL_SERVING_SERVICE_VLLM_IMAGE")).To(Succeed())
		Expect(os.Unsetenv("MODEL_SERVING_SERVICE_VLLM_MULTI_TENANT_ENABLED")).To(Succeed())
		Expect(os.Unsetenv("MODEL_SERVING_SERVICE_VLLM_REQUEST_TIMEOUT_MS")).To(Succeed())
	})

	It("uses operator defaults", func() {
		cfg := readModelServingConfig()

		Expect(cfg.ServiceName).To(Equal("model-serving-service"))
		Expect(cfg.Namespace).To(Equal("default"))
		Expect(cfg.HealthPort).To(Equal(5061))
		Expect(cfg.PollEvery.String()).To(Equal("1s"))
		Expect(cfg.Backend).To(Equal("kubernetes"))
		Expect(cfg.LocalStore).To(Equal(""))
		Expect(cfg.ServedModel.Group).To(Equal("serving.bighill.io"))
		Expect(cfg.ServedModel.Version).To(Equal("v1alpha1"))
		Expect(cfg.ServedModel.Resource).To(Equal("servedmodels"))
		Expect(cfg.Runtime.Image).To(Equal("vllm/vllm-openai:latest"))
		Expect(cfg.Runtime.MultiTenant).To(BeFalse())
		Expect(cfg.Runtime.RequestTimeout.String()).To(Equal("5s"))
		Expect(cfg.Runtime.Port).To(Equal(int32(8000)))
	})

	It("reads explicit runtime config", func() {
		Expect(os.Setenv("MODEL_SERVING_SERVICE_NAME", "model-serving-service")).To(Succeed())
		Expect(os.Setenv("MODEL_SERVING_SERVICE_NAMESPACE", "ml-serving")).To(Succeed())
		Expect(os.Setenv("MODEL_SERVING_SERVICE_HEALTHCHECK_PORT", "6061")).To(Succeed())
		Expect(os.Setenv("MODEL_SERVING_SERVICE_POLL_MS", "2500")).To(Succeed())
		Expect(os.Setenv("MODEL_SERVING_SERVICE_BACKEND", "kubernetes")).To(Succeed())
		Expect(os.Setenv("MODEL_SERVING_SERVICE_LOCAL_STORE_PATH", "/tmp/served-models.json")).To(Succeed())
		Expect(os.Setenv("MODEL_SERVING_SERVICE_VLLM_IMAGE", "vllm/vllm-openai:v1")).To(Succeed())
		Expect(os.Setenv("MODEL_SERVING_SERVICE_VLLM_MULTI_TENANT_ENABLED", "true")).To(Succeed())
		Expect(os.Setenv("MODEL_SERVING_SERVICE_VLLM_REQUEST_TIMEOUT_MS", "2500")).To(Succeed())

		cfg := readModelServingConfig()

		Expect(cfg.Namespace).To(Equal("ml-serving"))
		Expect(cfg.HealthPort).To(Equal(6061))
		Expect(cfg.PollEvery.String()).To(Equal("2.5s"))
		Expect(cfg.Backend).To(Equal("kubernetes"))
		Expect(cfg.LocalStore).To(Equal("/tmp/served-models.json"))
		Expect(cfg.Runtime.Image).To(Equal("vllm/vllm-openai:v1"))
		Expect(cfg.Runtime.MultiTenant).To(BeTrue())
		Expect(cfg.Runtime.RequestTimeout.String()).To(Equal("2.5s"))
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
