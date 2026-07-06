package main

import (
	"os"
	"strings"
	"testing"

	env "lib/shared_lib/env"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestModelRegistryMain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Model registry main unit test suite")
}

var _ = Describe("staging Helm values", func() {
	It("does not point the DLQ at LocalStack", func() {
		values := readTextFile("helm/staging-values.yaml")

		Expect(values).NotTo(ContainSubstring("localhost:4566"))
	})
})

func readTextFile(path string) string {
	content, err := os.ReadFile(path)
	Expect(err).NotTo(HaveOccurred())
	return strings.TrimSpace(string(content))
}

var _ = Describe("postgresConnectionString", func() {
	It("escapes credentials and includes connection options", func() {
		connection := postgresConnectionString("model user", "pa:ss/word", "localhost", "5432", "bighill_model_registry_db", "disable", 7)

		Expect(connection).To(ContainSubstring("postgres://model%20user:pa%3Ass%2Fword@localhost:5432/bighill_model_registry_db?"))
		Expect(connection).To(ContainSubstring("pool_max_conns=7"))
		Expect(connection).To(ContainSubstring("sslmode=disable"))
	})
})

var _ = Describe("readModelRegistryConfig", func() {
	BeforeEach(func() {
		env.ResetEnvironmentCache()
		Expect(os.Setenv("ENVIRONMENT", "local-dev")).To(Succeed())
		Expect(os.Unsetenv("MODEL_REGISTRY_SERVICE_API_HTTP_PORT")).To(Succeed())
		Expect(os.Unsetenv("MODEL_REGISTRY_SERVICE_TOPIC")).To(Succeed())
		Expect(os.Unsetenv("MODEL_REGISTRY_SERVICE_TRAINING_SUBSCRIBER_TOPIC")).To(Succeed())
		Expect(os.Unsetenv("MODEL_REGISTRY_SERVICE_INGESTION_SUBSCRIBER_TOPIC")).To(Succeed())
		Expect(os.Unsetenv("MODEL_REGISTRY_SERVICE_PROFILE_SUBSCRIBER_TOPIC")).To(Succeed())
		Expect(os.Unsetenv("MODEL_REGISTRY_SERVICE_OUTBOX")).To(Succeed())
		Expect(os.Unsetenv("MODEL_REGISTRY_SERVICE_KAFKA_BASE_GROUP_ID")).To(Succeed())
		Expect(os.Unsetenv("MODEL_REGISTRY_SERVICE_SERVING_RECONCILIATION_ENABLED")).To(Succeed())
		Expect(os.Unsetenv("MODEL_REGISTRY_SERVICE_SERVING_BACKEND")).To(Succeed())
		Expect(os.Unsetenv("MODEL_REGISTRY_SERVICE_SERVING_LOCAL_STORE_PATH")).To(Succeed())
		Expect(os.Unsetenv("MODEL_REGISTRY_SERVICE_SERVING_NAMESPACE")).To(Succeed())
		Expect(os.Unsetenv("MODEL_REGISTRY_SERVICE_SERVING_CRD_GROUP")).To(Succeed())
		Expect(os.Unsetenv("MODEL_REGISTRY_SERVICE_SERVING_CRD_VERSION")).To(Succeed())
		Expect(os.Unsetenv("MODEL_REGISTRY_SERVICE_SERVING_CRD_RESOURCE")).To(Succeed())
		Expect(os.Unsetenv("MODEL_REGISTRY_SERVICE_SERVING_CRD_KIND")).To(Succeed())
		Expect(os.Unsetenv("MODEL_REGISTRY_SERVICE_SERVING_STATUS_POLL_MS")).To(Succeed())
		Expect(os.Unsetenv("KAFKA_BROKER")).To(Succeed())
	})

	It("uses explicit local messaging and outbox defaults", func() {
		cfg := readModelRegistryConfig()

		Expect(cfg.HTTPPort).To(Equal(8084))
		Expect(cfg.Topics.ModelRegistry).To(Equal("model_registry"))
		Expect(cfg.Topics.Training).To(Equal("training"))
		Expect(cfg.Topics.Ingestion).To(Equal("ingestion"))
		Expect(cfg.ProfileTopic).To(Equal("profile"))
		Expect(cfg.OutboxBackend).To(Equal("postgres"))
		Expect(cfg.Messaging.GroupID).To(Equal("model-registry"))
		Expect(cfg.Messaging.Brokers).To(Equal("localhost:9092"))
		Expect(cfg.Health.MessageBrokerConnectionString).To(Equal("localhost:9092"))
		Expect(cfg.Serving.Enabled).To(BeTrue())
		Expect(cfg.Serving.Backend).To(Equal("kubernetes"))
		Expect(cfg.Serving.LocalStore).To(ContainSubstring("local_served_models"))
		Expect(cfg.Serving.LocalResyncEvery.String()).To(Equal("30s"))
		Expect(cfg.Serving.Namespace).To(Equal("default"))
		Expect(cfg.Serving.CRDGroup).To(Equal("serving.bighill.io"))
		Expect(cfg.Serving.CRDVersion).To(Equal("v1alpha1"))
		Expect(cfg.Serving.CRDResource).To(Equal("servedmodels"))
		Expect(cfg.Serving.CRDKind).To(Equal("ServedModel"))
		Expect(cfg.Serving.StatusPollEvery.String()).To(Equal("1s"))
	})

	It("uses the local serving backend without reading kubeconfig", func() {
		Expect(os.Setenv("MODEL_REGISTRY_SERVICE_SERVING_BACKEND", "local")).To(Succeed())
		cfg := readModelRegistryConfig()
		cfg.Serving.LocalStore = GinkgoT().TempDir() + "/served_models.json"
		Expect(os.Setenv("KUBECONFIG", "/does/not/exist")).To(Succeed())

		deployer, err := newServingBackend(cfg.Serving)

		Expect(err).NotTo(HaveOccurred())
		Expect(deployer).NotTo(BeNil())
	})
})
