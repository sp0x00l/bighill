package main

import (
	"context"
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type manifestReaderStub struct{}

func (manifestReaderStub) Read(context.Context, string) ([]byte, error) {
	return nil, nil
}

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
		Expect(os.Unsetenv("TRAINING_SERVICE_EXECUTOR_PROVIDER")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_RAY_JOBS_URL")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_RAY_TRAINING_ENTRYPOINT")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_RAY_EVALUATION_ENTRYPOINT")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_RAY_REQUEST_TIMEOUT_SECONDS")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_RAY_POLL_INTERVAL_SECONDS")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_ARTIFACT_BUCKET_REGION")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_MODEL_URI_PREFIX")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_EVALUATION_URI_PREFIX")).To(Succeed())
	})

	It("uses local Temporal defaults", func() {
		cfg := readTrainingConfig()

		Expect(cfg.ServiceName).To(Equal("training-service"))
		Expect(cfg.Temporal.Address).To(Equal("localhost:7233"))
		Expect(cfg.Temporal.Namespace).To(Equal("default"))
		Expect(cfg.Temporal.TaskQueue).To(Equal("training-service"))
		Expect(cfg.Executor.Provider).To(Equal("ray"))
		Expect(cfg.Executor.RayJobsURL).To(Equal("http://localhost:8265"))
		Expect(cfg.Executor.RayTrainingEntrypoint).To(Equal("python -m training_jobs.train"))
		Expect(cfg.Executor.RayEvaluationEntrypoint).To(Equal("python -m training_jobs.evaluate"))
		Expect(cfg.Executor.RayRequestTimeout).To(Equal(30 * time.Second))
		Expect(cfg.Executor.RayPollInterval).To(Equal(30 * time.Second))
		Expect(cfg.Executor.ArtifactBucketRegion).To(Equal("local-dev"))
		Expect(cfg.Executor.ModelURIPrefix).To(Equal("s3://local-dev-bucket/models"))
		Expect(cfg.Executor.EvaluationURIPrefix).To(Equal("s3://local-dev-bucket/evaluations"))
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

var _ = Describe("newTrainingExecutor", func() {
	It("creates a Ray executor", func() {
		exec, err := newTrainingExecutor(trainingExecutorConfig{
			Provider:                "ray",
			RayJobsURL:              "http://ray.local",
			RayTrainingEntrypoint:   "python -m train",
			RayEvaluationEntrypoint: "python -m eval",
			RayRequestTimeout:       time.Second,
			RayPollInterval:         time.Second,
		}, manifestReaderStub{})

		Expect(err).NotTo(HaveOccurred())
		Expect(exec).NotTo(BeNil())
	})

	It("rejects unsupported providers", func() {
		exec, err := newTrainingExecutor(trainingExecutorConfig{Provider: "unknown"}, manifestReaderStub{})

		Expect(exec).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring("unsupported training executor provider")))
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
