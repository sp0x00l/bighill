package main

import (
	"os"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestModelRegistryMain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Model registry main unit test suite")
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
		Expect(os.Unsetenv("MODEL_REGISTRY_SERVICE_TOPIC")).To(Succeed())
		Expect(os.Unsetenv("MODEL_REGISTRY_SERVICE_TRAINING_SUBSCRIBER_TOPIC")).To(Succeed())
		Expect(os.Unsetenv("MODEL_REGISTRY_SERVICE_OUTBOX")).To(Succeed())
		Expect(os.Unsetenv("MODEL_REGISTRY_SERVICE_KAFKA_GROUP_ID")).To(Succeed())
		Expect(os.Unsetenv("KAFKA_BROKER")).To(Succeed())
	})

	It("uses explicit local messaging and outbox defaults", func() {
		cfg := readModelRegistryConfig()

		Expect(cfg.Topics.ModelRegistry).To(Equal("model_registry"))
		Expect(cfg.Topics.Training).To(Equal("training"))
		Expect(cfg.OutboxBackend).To(Equal("postgres"))
		Expect(cfg.Messaging.GroupID).To(Equal("model-registry-group"))
		Expect(cfg.Messaging.Brokers).To(Equal("localhost:9092"))
		Expect(cfg.Health.MessageBrokerConnectionString).To(Equal("localhost:9092"))
	})
})
