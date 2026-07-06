package main

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"training_service/pkg/infra/executor"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type manifestReaderStub struct{}

func (manifestReaderStub) Read(context.Context, string) ([]byte, error) {
	return nil, nil
}

func (manifestReaderStub) Stat(context.Context, string) (executor.ObjectInfo, error) {
	return executor.ObjectInfo{SizeBytes: 1}, nil
}

func TestTrainingMain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Training service main unit test suite")
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

var _ = Describe("readTrainingConfig", func() {
	BeforeEach(func() {
		Expect(os.Unsetenv("TRAINING_SERVICE_TEMPORAL_ADDRESS")).To(Succeed())
		Expect(os.Unsetenv("TEMPORAL_ADDRESS")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_TEMPORAL_NAMESPACE")).To(Succeed())
		Expect(os.Unsetenv("TEMPORAL_NAMESPACE")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_TEMPORAL_TASK_QUEUE")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_API_HTTP_PORT")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_HTTP_CLIENT_TIMEOUT_SECONDS")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_TRAINING_TRIGGER_ENABLED")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_EVALUATION_PROFILE_NAME")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_EVALUATION_PROFILE")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_DPO_TRAINING_PROFILE_NAME")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_DPO_EVALUATION_PROFILE_NAME")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_DPO_EVALUATION_PROFILE")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_TRAINING_PROFILE_NAME")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_TRAINING_PROFILE_TRAINER")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_TRAINING_PROFILE_ADAPTER")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_TRAINING_PROFILE_QUANTIZATION")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_TRAINING_PROFILE_PREFERENCE_DATASET_URI")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_TRAINING_PROFILE_DPO_BETA")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_TRAINING_PROFILE_SEQUENCE_LENGTH")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_TRAINING_PROFILE_SAMPLE_PACKING")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_TRAINING_PROFILE_LEARNING_RATE")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_TRAINING_PROFILE_EPOCHS")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_TRAINING_PROFILE_MICRO_BATCH_SIZE")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_TRAINING_PROFILE_GRADIENT_ACCUMULATION_STEPS")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_TRAINING_PROFILE_LORA_R")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_TRAINING_PROFILE_LORA_ALPHA")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_EXECUTOR_PROVIDER")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_RAY_JOBS_URL")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_RAY_TRAINING_ENTRYPOINT")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_RAY_EVALUATION_ENTRYPOINT")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_AXOLOTL_COMMAND")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_RAY_REQUEST_TIMEOUT_SECONDS")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_RAY_POLL_INTERVAL_SECONDS")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_KUBERAY_NAMESPACE")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_KUBERAY_RAY_VERSION")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_KUBERAY_IMAGE")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_KUBERAY_IMAGE_PULL_POLICY")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_KUBERAY_SERVICE_ACCOUNT")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_KUBERAY_TTL_SECONDS_AFTER_FINISHED")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_KUBERAY_WORKER_REPLICAS")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_KUBERAY_CPU")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_KUBERAY_MEMORY")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_KUBERAY_GPU_RESOURCE")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_KUBERAY_GPU")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_ARTIFACT_BUCKET_REGION")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_MODEL_URI_PREFIX")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_EVALUATION_URI_PREFIX")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_SERVING_TARGET")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_SERVING_MODEL")).To(Succeed())
		Expect(os.Unsetenv("TRAINING_SERVICE_SERVING_LOAD_STATUS")).To(Succeed())
	})

	It("uses local Temporal defaults", func() {
		cfg := readTrainingConfig()

		Expect(cfg.ServiceName).To(Equal("training-service"))
		Expect(cfg.Temporal.Address).To(Equal("localhost:7233"))
		Expect(cfg.Temporal.Namespace).To(Equal("default"))
		Expect(cfg.Temporal.TaskQueue).To(Equal("training-service"))
		Expect(cfg.HTTPPort).To(Equal(8085))
		Expect(cfg.HTTPClientTimeout).To(Equal(10 * time.Second))
		Expect(cfg.TrainingTriggerEnabled).To(BeFalse())
		Expect(cfg.Executor.Provider).To(Equal("kuberay"))
		Expect(cfg.Executor.RayJobsURL).To(Equal("http://localhost:8265"))
		Expect(cfg.Executor.RayTrainingEntrypoint).To(Equal("python -m training_jobs.train"))
		Expect(cfg.Executor.RayEvaluationEntrypoint).To(Equal("python -m training_jobs.evaluate"))
		Expect(cfg.Executor.AxolotlCommand).To(Equal("axolotl train"))
		Expect(cfg.Executor.RayRequestTimeout).To(Equal(30 * time.Second))
		Expect(cfg.Executor.RayPollInterval).To(Equal(30 * time.Second))
		Expect(cfg.Executor.KubeRayNamespace).To(Equal("default"))
		Expect(cfg.Executor.KubeRayRayVersion).To(Equal("2.46.0"))
		Expect(cfg.Executor.KubeRayImage).To(Equal("training-jobs:0.0.1"))
		Expect(cfg.Executor.KubeRayImagePullPolicy).To(Equal("IfNotPresent"))
		Expect(cfg.Executor.KubeRayServiceAccount).To(Equal("training-jobs"))
		Expect(cfg.Executor.KubeRayTTLSeconds).To(Equal(3600))
		Expect(cfg.Executor.KubeRayWorkerReplicas).To(Equal(1))
		Expect(cfg.Executor.KubeRayCPU).To(Equal("1"))
		Expect(cfg.Executor.KubeRayMemory).To(Equal("4Gi"))
		Expect(cfg.Executor.KubeRayGPUResource).To(Equal("nvidia.com/gpu"))
		Expect(cfg.Executor.KubeRayGPU).To(Equal("1"))
		Expect(cfg.Executor.ArtifactBucketRegion).To(Equal("eu-west-1"))
		Expect(cfg.Executor.ModelURIPrefix).To(Equal("s3://local-dev-bucket/models"))
		Expect(cfg.Executor.EvaluationURIPrefix).To(Equal("s3://local-dev-bucket/evaluations"))
		Expect(cfg.Executor.ServingTarget).To(Equal(""))
		Expect(cfg.Executor.ServingModel).To(Equal(""))
		Expect(cfg.Executor.ServingLoadStatus).To(Equal("NOT_LOADED"))
		Expect(cfg.EvaluationProfileName).To(Equal("ragas-default@v1"))
		Expect(cfg.EvaluationProfile).To(Equal("smoke"))
		Expect(cfg.DPOTrainingProfileName).To(Equal("dpo-default@v1"))
		Expect(cfg.DPOEvaluationProfileName).To(Equal("dpo-default@v1"))
		Expect(cfg.DPOEvaluationProfile).To(MatchJSON(`{"metric_suite":"preference","evaluator_name":"pairwise-judge","evaluator_version":"v1"}`))
		Expect(cfg.Profile.Name).To(Equal("sft-default@v1"))
		Expect(cfg.Profile.Trainer).To(Equal("sft"))
		Expect(cfg.Profile.Adapter).To(Equal("qlora"))
		Expect(cfg.Profile.Quantization).To(Equal("4bit"))
		Expect(cfg.Profile.PreferenceDatasetURI).To(Equal(""))
		Expect(cfg.Profile.DPOBeta).To(Equal(0.1))
		Expect(cfg.Profile.SequenceLength).To(Equal(2048))
		Expect(cfg.Profile.SamplePacking).To(BeTrue())
		Expect(cfg.Profile.LearningRate).To(Equal(0.0002))
		Expect(cfg.Profile.Epochs).To(Equal(3.0))
		Expect(cfg.Profile.MicroBatchSize).To(Equal(1))
		Expect(cfg.Profile.GradientAccumulationSteps).To(Equal(4))
		Expect(cfg.Profile.LoRAR).To(Equal(16))
		Expect(cfg.Profile.LoRAAlpha).To(Equal(32))
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

	It("allows the training trigger to be enabled explicitly", func() {
		Expect(os.Setenv("TRAINING_SERVICE_TRAINING_TRIGGER_ENABLED", "true")).To(Succeed())

		cfg := readTrainingConfig()

		Expect(cfg.TrainingTriggerEnabled).To(BeTrue())
	})
})

var _ = Describe("newTrainingExecutor", func() {
	It("creates a Ray executor", func() {
		exec, err := newTrainingExecutor(trainingExecutorConfig{
			Provider:                "ray",
			RayJobsURL:              "http://ray.local",
			RayTrainingEntrypoint:   "python -m train",
			RayEvaluationEntrypoint: "python -m eval",
			RayPromotionEntrypoint:  "python -m promote",
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

	It("rejects a Ray poll interval that can miss Temporal heartbeats", func() {
		exec, err := newTrainingExecutor(trainingExecutorConfig{
			Provider:                "ray",
			RayJobsURL:              "http://ray.local",
			RayTrainingEntrypoint:   "python -m train",
			RayEvaluationEntrypoint: "python -m eval",
			RayPromotionEntrypoint:  "python -m promote",
			RayRequestTimeout:       time.Second,
			RayPollInterval:         2 * time.Minute,
		}, manifestReaderStub{})

		Expect(exec).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring("ray poll interval must be less than training activity heartbeat timeout")))
	})

	It("rejects a KubeRay poll interval that can miss Temporal heartbeats", func() {
		exec, err := newTrainingExecutor(trainingExecutorConfig{
			Provider:                "kuberay",
			RayPollInterval:         2 * time.Minute,
			KubeRayNamespace:        "default",
			KubeRayRayVersion:       "2.46.0",
			KubeRayImage:            "training-jobs:0.0.1",
			KubeRayCPU:              "1",
			KubeRayMemory:           "4Gi",
			RayTrainingEntrypoint:   "python -m training_jobs.train",
			RayEvaluationEntrypoint: "python -m training_jobs.evaluate",
			RayPromotionEntrypoint:  "python -m training_jobs.promotion_report",
		}, manifestReaderStub{})

		Expect(exec).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring("kuberay poll interval must be less than training activity heartbeat timeout")))
	})
})

var _ = Describe("newHealthCheckConfig", func() {
	It("maps health settings", func() {
		cfg := newHealthCheckConfig(healthConfig{
			CpuThresholdPercentage:                    70,
			MemFreeThresholdPercent:                   30,
			HealthCheckPort:                           5058,
			ServiceLatencyThreshold:                   3 * time.Second,
			MessageBrokerSubscriberMaxPollSilence:     30 * time.Second,
			MessageBrokerSubscriberMaxProgressSilence: 90 * time.Second,
			MessageBrokerSubscriberMaxLag:             100,
		})

		Expect(cfg.CpuThresholdPercentage).To(Equal(70))
		Expect(cfg.MemFreeThresholdPercentage).To(Equal(30))
		Expect(cfg.HealthCheckPort).To(Equal(5058))
		Expect(cfg.ServiceLatencyThresholdSec).To(Equal(3 * time.Second))
		Expect(cfg.MessageBrokerSubscriberMaxPollSilenceSec).To(Equal(30 * time.Second))
		Expect(cfg.MessageBrokerSubscriberMaxProgressSilenceSec).To(Equal(90 * time.Second))
		Expect(cfg.MessageBrokerSubscriberMaxLag).To(Equal(int64(100)))
	})
})
