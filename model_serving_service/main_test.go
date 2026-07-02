package main

import (
	"os"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMainConfig(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Model serving main unit test suite")
}

var _ = Describe("readModelServingConfig", func() {
	BeforeEach(func() {
		Expect(os.Unsetenv("MODEL_SERVING_SERVICE_NAME")).To(Succeed())
		Expect(os.Unsetenv("MODEL_SERVING_NAMESPACE")).To(Succeed())
		Expect(os.Unsetenv("MODEL_SERVING_HEALTHCHECK_PORT")).To(Succeed())
		Expect(os.Unsetenv("MODEL_SERVING_POLL_MS")).To(Succeed())
		Expect(os.Unsetenv("MODEL_SERVING_VLLM_IMAGE")).To(Succeed())
	})

	It("uses operator defaults", func() {
		cfg := readModelServingConfig()

		Expect(cfg.ServiceName).To(Equal("model-serving-service"))
		Expect(cfg.Namespace).To(Equal("default"))
		Expect(cfg.HealthPort).To(Equal(5061))
		Expect(cfg.PollEvery.String()).To(Equal("1s"))
		Expect(cfg.ServedModel.Group).To(Equal("serving.bighill.io"))
		Expect(cfg.ServedModel.Version).To(Equal("v1alpha1"))
		Expect(cfg.ServedModel.Resource).To(Equal("servedmodels"))
		Expect(cfg.Runtime.Image).To(Equal("vllm/vllm-openai:latest"))
		Expect(cfg.Runtime.Port).To(Equal(int32(8000)))
	})

	It("reads explicit runtime config", func() {
		Expect(os.Setenv("MODEL_SERVING_SERVICE_NAME", "model-serving-service")).To(Succeed())
		Expect(os.Setenv("MODEL_SERVING_NAMESPACE", "ml-serving")).To(Succeed())
		Expect(os.Setenv("MODEL_SERVING_HEALTHCHECK_PORT", "6061")).To(Succeed())
		Expect(os.Setenv("MODEL_SERVING_POLL_MS", "2500")).To(Succeed())
		Expect(os.Setenv("MODEL_SERVING_VLLM_IMAGE", "vllm/vllm-openai:v1")).To(Succeed())

		cfg := readModelServingConfig()

		Expect(cfg.Namespace).To(Equal("ml-serving"))
		Expect(cfg.HealthPort).To(Equal(6061))
		Expect(cfg.PollEvery.String()).To(Equal("2.5s"))
		Expect(cfg.Runtime.Image).To(Equal("vllm/vllm-openai:v1"))
	})
})
