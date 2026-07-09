package main

import (
	"os"
	"os/exec"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestAPI(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "API Gateway API Suite")
}

var _ = Describe("routerConfigFromEnv", func() {
	It("uses explicit staging service routes", func() {
		setRouteEnv("staging", map[string]string{
			"DATA_REGISTRY_SERVICE_HTTP_HOST":  "data-registry.internal",
			"DATA_REGISTRY_SERVICE_HTTP_PORT":  "80",
			"INGESTION_SERVICE_HTTP_HOST":      "ingestion.internal",
			"INGESTION_SERVICE_HTTP_PORT":      "80",
			"MODEL_REGISTRY_SERVICE_HTTP_HOST": "model-registry.internal",
			"MODEL_REGISTRY_SERVICE_HTTP_PORT": "80",
			"PROFILE_SERVICE_HTTP_HOST":        "profile.internal",
			"PROFILE_SERVICE_HTTP_PORT":        "80",
			"TRAINING_SERVICE_HTTP_HOST":       "training.internal",
			"TRAINING_SERVICE_HTTP_PORT":       "80",
			"INFERENCE_SERVICE_HTTP_HOST":      "inference.internal",
			"INFERENCE_SERVICE_HTTP_PORT":      "80",
		})

		cfg := routerConfigFromEnv()

		Expect(cfg.DataRegistryServiceRoute).To(Equal("http://data-registry.internal:80"))
		Expect(cfg.ModelRegistryServiceRoute).To(Equal("http://model-registry.internal:80"))
		Expect(cfg.InferenceServiceRoute).To(Equal("http://inference.internal:80"))
	})

	It("rejects localhost service routes in staging", func() {
		if os.Getenv("BIGHILL_TEST_STAGING_LOCALHOST_FATAL") == "1" {
			setRouteEnv("staging", map[string]string{
				"DATA_REGISTRY_SERVICE_HTTP_HOST":  "127.0.0.1",
				"DATA_REGISTRY_SERVICE_HTTP_PORT":  "8081",
				"INGESTION_SERVICE_HTTP_HOST":      "ingestion.internal",
				"INGESTION_SERVICE_HTTP_PORT":      "80",
				"MODEL_REGISTRY_SERVICE_HTTP_HOST": "model-registry.internal",
				"MODEL_REGISTRY_SERVICE_HTTP_PORT": "80",
				"PROFILE_SERVICE_HTTP_HOST":        "profile.internal",
				"PROFILE_SERVICE_HTTP_PORT":        "80",
				"TRAINING_SERVICE_HTTP_HOST":       "training.internal",
				"TRAINING_SERVICE_HTTP_PORT":       "80",
				"INFERENCE_SERVICE_HTTP_HOST":      "inference.internal",
				"INFERENCE_SERVICE_HTTP_PORT":      "80",
			})
			_ = routerConfigFromEnv()
			return
		}

		cmd := exec.Command(os.Args[0], "-test.run=TestAPI", "-ginkgo.focus=rejects localhost service routes in staging")
		cmd.Env = append(os.Environ(), "BIGHILL_TEST_STAGING_LOCALHOST_FATAL=1")
		Expect(cmd.Run()).To(HaveOccurred())
	})
})

func setRouteEnv(environment string, values map[string]string) {
	setEnv("ENVIRONMENT", environment)
	for key, value := range values {
		setEnv(key, value)
	}
}

func setEnv(key string, value string) {
	previous, hadPrevious := os.LookupEnv(key)
	Expect(os.Setenv(key, value)).To(Succeed())
	DeferCleanup(func() {
		if hadPrevious {
			Expect(os.Setenv(key, previous)).To(Succeed())
			return
		}
		Expect(os.Unsetenv(key)).To(Succeed())
	})
}
