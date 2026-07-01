package main

import (
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestTrainingMain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Training service main unit test suite")
}

var _ = Describe("readTrainingConfig", func() {
	BeforeEach(func() {
		Expect(os.Unsetenv("TRAINING_SERVICE_TEMPORAL_ADDRESS")).To(Succeed())
		Expect(os.Unsetenv("TEMPORAL_ADDRESS")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_TEMPORAL_NAMESPACE")).To(Succeed())
		Expect(os.Unsetenv("TEMPORAL_NAMESPACE")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_TEMPORAL_TASK_QUEUE")).To(Succeed())
	})

	It("uses local Temporal defaults", func() {
		cfg := readTrainingConfig()

		Expect(cfg.ServiceName).To(Equal("training-service"))
		Expect(cfg.Temporal.Address).To(Equal("localhost:7233"))
		Expect(cfg.Temporal.Namespace).To(Equal("default"))
		Expect(cfg.Temporal.TaskQueue).To(Equal("training-service"))
		Expect(cfg.Health.HealthCheckPort).To(Equal(5058))
	})

	It("allows service-specific Temporal overrides", func() {
		Expect(os.Setenv("TRAINING_SERVICE_TEMPORAL_ADDRESS", "temporal:7233")).To(Succeed())
		Expect(os.Setenv("TRAINING_SERVICE_TEMPORAL_NAMESPACE", "mlops")).To(Succeed())
		Expect(os.Setenv("TRAINING_SERVICE_TEMPORAL_TASK_QUEUE", "gpu-training")).To(Succeed())

		cfg := readTrainingConfig()

		Expect(cfg.Temporal.Address).To(Equal("temporal:7233"))
		Expect(cfg.Temporal.Namespace).To(Equal("mlops"))
		Expect(cfg.Temporal.TaskQueue).To(Equal("gpu-training"))
	})
})

var _ = Describe("newHealthCheckConfig", func() {
	It("maps health settings", func() {
		cfg := newHealthCheckConfig(healthConfig{
			CpuThresholdPercentage:  70,
			MemFreeThresholdPercent: 30,
			HealthCheckPort:         5058,
			ServiceLatencyThreshold: 3 * time.Second,
		})

		Expect(cfg.CpuThresholdPercentage).To(Equal(70))
		Expect(cfg.MemFreeThresholdPercentage).To(Equal(30))
		Expect(cfg.HealthCheckPort).To(Equal(5058))
		Expect(cfg.ServiceLatencyThresholdSec).To(Equal(3 * time.Second))
	})
})
