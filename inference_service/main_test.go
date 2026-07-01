package main

import (
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
	It("uses local defaults", func() {
		cfg := readInferenceConfig()

		Expect(cfg.ServiceName).To(Equal("inference-service"))
		Expect(cfg.Health.HealthCheckPort).To(Equal(5059))
	})
})

var _ = Describe("newHealthCheckConfig", func() {
	It("maps health settings", func() {
		cfg := newHealthCheckConfig(healthConfig{
			CpuThresholdPercentage:  70,
			MemFreeThresholdPercent: 30,
			HealthCheckPort:         5059,
			ServiceLatencyThreshold: 3 * time.Second,
		})

		Expect(cfg.CpuThresholdPercentage).To(Equal(70))
		Expect(cfg.MemFreeThresholdPercentage).To(Equal(30))
		Expect(cfg.HealthCheckPort).To(Equal(5059))
		Expect(cfg.ServiceLatencyThresholdSec).To(Equal(3 * time.Second))
	})
})
