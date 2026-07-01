package main

import (
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestInferenceMain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Inference service main unit test suite")
}

var _ = Describe("readInferenceConfig", func() {
	BeforeEach(func() {
		Expect(os.Unsetenv("INFERENCE_DB_NAME")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_DB_USER")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_DB_PASSWORD")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_MODEL_REGISTRY_SUBSCRIBER_TOPIC")).To(Succeed())
		Expect(os.Unsetenv("INFERENCE_SERVICE_KAFKA_GROUP_ID")).To(Succeed())
	})

	It("uses local defaults", func() {
		cfg := readInferenceConfig()

		Expect(cfg.ServiceName).To(Equal("inference-service"))
		Expect(cfg.DBName).To(Equal("bighill_inference_db"))
		Expect(cfg.Messaging.GroupID).To(Equal("inference-group"))
		Expect(cfg.Topics.ModelRegistry).To(Equal("model_registry"))
		Expect(cfg.Health.HealthCheckPort).To(Equal(5059))
	})

	It("builds a Postgres connection string", func() {
		connection := postgresConnectionString("inference user", "pa:ss/word", "localhost", "5432", "bighill_inference_db", "disable", 7)

		Expect(connection).To(ContainSubstring("postgres://inference%20user:pa%3Ass%2Fword@localhost:5432/bighill_inference_db?"))
		Expect(connection).To(ContainSubstring("pool_max_conns=7"))
		Expect(connection).To(ContainSubstring("sslmode=disable"))
	})
})

var _ = Describe("newHealthCheckConfig", func() {
	It("maps health settings", func() {
		cfg := newHealthCheckConfig(healthConfig{
			CpuThresholdPercentage:  70,
			MemFreeThresholdPercent: 30,
			HealthCheckPort:         5059,
			DBConnectionString:      "postgres://localhost/db",
			DbLatencyThreshold:      4 * time.Second,
			ServiceLatencyThreshold: 3 * time.Second,
		})

		Expect(cfg.CpuThresholdPercentage).To(Equal(70))
		Expect(cfg.MemFreeThresholdPercentage).To(Equal(30))
		Expect(cfg.HealthCheckPort).To(Equal(5059))
		Expect(cfg.DBConnectionString).To(Equal("postgres://localhost/db"))
		Expect(cfg.DbLatencyThresholdSec).To(Equal(4 * time.Second))
		Expect(cfg.ServiceLatencyThresholdSec).To(Equal(3 * time.Second))
	})
})
