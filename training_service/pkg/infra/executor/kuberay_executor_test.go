package executor_test

import (
	"context"
	"strings"
	"time"

	"training_service/pkg/domain/model"
	"training_service/pkg/infra/executor"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

var _ = Describe("KubeRayExecutor", func() {
	It("rejects missing boundary dependencies or config", func() {
		client := fake.NewSimpleDynamicClient(runtime.NewScheme())

		_, err := executor.NewKubeRayExecutorWithClient(kubeRayConfig(), nil, client)
		Expect(err).To(MatchError(ContainSubstring("manifest reader is required")))

		config := kubeRayConfig()
		config.PollInterval = 0
		_, err = executor.NewKubeRayExecutorWithClient(config, &manifestReaderStub{}, client)
		Expect(err).To(MatchError(ContainSubstring("poll interval must be greater than zero")))
	})

	It("creates a RayJob CR with the training image bound into head and worker pods", func() {
		reader := &manifestReaderStub{payloads: map[string]string{
			"s3://models/run-kube/artifact.json": `{"training_run_id":"run-kube","model_uri":"s3://models/run-kube","model_name":"ranker","model_version":"v1","base_model":"mistral","artifact_format":"HF_PEFT_ADAPTER","artifact_checksum":"sha256:kube","artifact_size_bytes":123}`,
		}, stats: map[string]model.ObjectInfo{
			"s3://models/run-kube": {Location: "s3://models/run-kube", SizeBytes: 123},
		}}
		var created *unstructured.Unstructured
		client := fake.NewSimpleDynamicClient(runtime.NewScheme())
		client.PrependReactor("create", "rayjobs", func(action ktesting.Action) (bool, runtime.Object, error) {
			create := action.(ktesting.CreateAction)
			created = create.GetObject().(*unstructured.Unstructured)
			return true, created, nil
		})
		client.PrependReactor("get", "rayjobs", func(action ktesting.Action) (bool, runtime.Object, error) {
			if created == nil {
				return true, nil, errors.NewNotFound(schema.GroupResource{Group: "ray.io", Resource: "rayjobs"}, action.(ktesting.GetAction).GetName())
			}
			out := created.DeepCopy()
			Expect(unstructured.SetNestedField(out.Object, "SUCCEEDED", "status", "jobStatus")).To(Succeed())
			return true, out, nil
		})
		kuberay, err := executor.NewKubeRayExecutorWithClient(kubeRayConfig(), reader, client)
		Expect(err).NotTo(HaveOccurred())

		artifact, err := kuberay.RunTrainingJob(context.Background(), model.TrainingJobSpec{
			TrainingRunID:        "run-kube",
			DatasetURI:           "s3://features/run-kube.parquet",
			ModelName:            "ranker",
			ModelVersion:         "v1",
			BaseModel:            "mistral",
			ModelURI:             "s3://models/run-kube",
			AdapterURI:           "s3://models/run-kube",
			ServingTarget:        "vllm-local",
			ServingModel:         "ranker-v1",
			ServingLoadStatus:    "NOT_LOADED",
			ArtifactFormat:       "HF_PEFT_ADAPTER",
			ArtifactManifestURI:  "s3://models/run-kube/artifact.json",
			ArtifactBucketRegion: "eu-west-1",
			AxolotlCommand:       "axolotl train",
			RecipeYAML:           "base_model: mistral",
			RecipeHash:           "hash",
			SubmissionID:         "train-run-kube-hash",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(artifact.ModelURI).To(Equal("s3://models/run-kube"))
		Expect(created).NotTo(BeNil())
		Expect(created.GetName()).To(Equal("train-run-kube-hash"))
		runtimeEnv, _, _ := unstructured.NestedString(created.Object, "spec", "runtimeEnvYAML")
		Expect(runtimeEnv).To(ContainSubstring(`TRAINING_RUN_ID: "run-kube"`))
		Expect(runtimeEnv).To(ContainSubstring(`TRAINING_AXOLOTL_COMMAND: "axolotl train"`))
		Expect(runtimeEnv).To(ContainSubstring(`TRAINING_ARTIFACT_FORMAT: "HF_PEFT_ADAPTER"`))
		headContainers, _, _ := unstructured.NestedSlice(created.Object, "spec", "rayClusterSpec", "headGroupSpec", "template", "spec", "containers")
		workerGroups, _, _ := unstructured.NestedSlice(created.Object, "spec", "rayClusterSpec", "workerGroupSpecs")
		Expect(headContainers[0].(map[string]any)["image"]).To(Equal("training-jobs:unit"))
		workerTemplate := workerGroups[0].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
		workerContainers := workerTemplate["containers"].([]any)
		Expect(workerContainers[0].(map[string]any)["image"]).To(Equal("training-jobs:unit"))
	})

	It("reattaches to an already completed RayJob", func() {
		reader := &manifestReaderStub{payloads: map[string]string{
			"s3://evals/run-eval.json": `{"training_run_id":"run-eval","report_uri":"s3://evals/run-eval.json","passed":true}`,
		}, stats: map[string]model.ObjectInfo{
			"s3://evals/run-eval.json": {Location: "s3://evals/run-eval.json", SizeBytes: 10},
		}}
		existing := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "ray.io/v1",
			"kind":       "RayJob",
			"metadata": map[string]any{
				"name":      "eval-run-eval-hash",
				"namespace": "ml",
			},
			"status": map[string]any{
				"jobStatus": "SUCCEEDED",
			},
		}}
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), existing)
		kuberay, err := executor.NewKubeRayExecutorWithClient(kubeRayConfig(), reader, client)
		Expect(err).NotTo(HaveOccurred())

		report, err := kuberay.EvaluateModel(context.Background(), model.EvaluationJobSpec{
			TrainingRunID:     "run-eval",
			ReportManifestURI: "s3://evals/run-eval.json",
			SubmissionID:      "eval-run-eval-hash",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(report.Passed).To(BeTrue())
	})

	It("treats KubeRay jobDeploymentStatus Complete as success", func() {
		reader := &manifestReaderStub{payloads: map[string]string{
			"s3://evals/run-complete.json": `{"training_run_id":"run-complete","report_uri":"s3://evals/run-complete.json","passed":true}`,
		}, stats: map[string]model.ObjectInfo{
			"s3://evals/run-complete.json": {Location: "s3://evals/run-complete.json", SizeBytes: 10},
		}}
		existing := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "ray.io/v1",
			"kind":       "RayJob",
			"metadata": map[string]any{
				"name":      "eval-run-complete-hash",
				"namespace": "ml",
			},
			"status": map[string]any{
				"jobDeploymentStatus": "Complete",
			},
		}}
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), existing)
		kuberay, err := executor.NewKubeRayExecutorWithClient(kubeRayConfig(), reader, client)
		Expect(err).NotTo(HaveOccurred())

		report, err := kuberay.EvaluateModel(context.Background(), model.EvaluationJobSpec{
			TrainingRunID:     "run-complete",
			ReportManifestURI: "s3://evals/run-complete.json",
			SubmissionID:      "eval-run-complete-hash",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(report.Passed).To(BeTrue())
	})

	It("recovers the manifest when a RayJob is missing after create", func() {
		reader := &lateManifestReader{
			location: "s3://models/run-gc/artifact.json",
			payload:  `{"training_run_id":"run-gc","model_uri":"s3://models/run-gc","model_name":"ranker","model_version":"v1","base_model":"mistral","artifact_format":"HF_PEFT_ADAPTER","artifact_checksum":"sha256:gc","artifact_size_bytes":123}`,
			stat:     model.ObjectInfo{Location: "s3://models/run-gc", SizeBytes: 123},
		}
		createCalls := 0
		client := fake.NewSimpleDynamicClient(runtime.NewScheme())
		client.PrependReactor("create", "rayjobs", func(action ktesting.Action) (bool, runtime.Object, error) {
			createCalls++
			return true, action.(ktesting.CreateAction).GetObject(), nil
		})
		client.PrependReactor("get", "rayjobs", func(action ktesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.NewNotFound(schema.GroupResource{Group: "ray.io", Resource: "rayjobs"}, action.(ktesting.GetAction).GetName())
		})
		kuberay, err := executor.NewKubeRayExecutorWithClient(kubeRayConfig(), reader, client)
		Expect(err).NotTo(HaveOccurred())

		artifact, err := kuberay.RunTrainingJob(context.Background(), model.TrainingJobSpec{
			TrainingRunID:       "run-gc",
			ArtifactManifestURI: "s3://models/run-gc/artifact.json",
			SubmissionID:        "train-run-gc-hash",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(artifact.ModelURI).To(Equal("s3://models/run-gc"))
		Expect(createCalls).To(Equal(1))
		Expect(reader.readCalls).To(Equal(2))
	})

	It("returns failed RayJob status as a training failure", func() {
		existing := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "ray.io/v1",
			"kind":       "RayJob",
			"metadata": map[string]any{
				"name":      "train-failed-hash",
				"namespace": "ml",
			},
			"status": map[string]any{
				"jobStatus": "FAILED",
				"message":   "pod failed",
			},
		}}
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), existing)
		kuberay, err := executor.NewKubeRayExecutorWithClient(kubeRayConfig(), &manifestReaderStub{}, client)
		Expect(err).NotTo(HaveOccurred())

		artifact, err := kuberay.RunTrainingJob(context.Background(), model.TrainingJobSpec{
			TrainingRunID:       "failed",
			ArtifactManifestURI: "s3://models/failed/artifact.json",
			SubmissionID:        "train-failed-hash",
		})

		Expect(artifact).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring("kuberay job failed: pod failed")))
	})

	It("deletes the RayJob when the activity context is canceled", func() {
		ctx, cancel := context.WithCancel(context.Background())
		var created *unstructured.Unstructured
		deleted := false
		statusCalls := 0
		client := fake.NewSimpleDynamicClient(runtime.NewScheme())
		client.PrependReactor("create", "rayjobs", func(action ktesting.Action) (bool, runtime.Object, error) {
			created = action.(ktesting.CreateAction).GetObject().(*unstructured.Unstructured)
			return true, created, nil
		})
		client.PrependReactor("get", "rayjobs", func(action ktesting.Action) (bool, runtime.Object, error) {
			statusCalls++
			if created == nil {
				return true, nil, errors.NewNotFound(schema.GroupResource{Group: "ray.io", Resource: "rayjobs"}, action.(ktesting.GetAction).GetName())
			}
			out := created.DeepCopy()
			Expect(unstructured.SetNestedField(out.Object, "RUNNING", "status", "jobStatus")).To(Succeed())
			if statusCalls == 1 {
				cancel()
			}
			return true, out, nil
		})
		client.PrependReactor("delete", "rayjobs", func(action ktesting.Action) (bool, runtime.Object, error) {
			deleted = true
			return true, nil, nil
		})
		kuberay, err := executor.NewKubeRayExecutorWithClient(kubeRayConfig(), &manifestReaderStub{}, client)
		Expect(err).NotTo(HaveOccurred())

		artifact, err := kuberay.RunTrainingJob(ctx, model.TrainingJobSpec{
			TrainingRunID:       "run-cancel",
			ArtifactManifestURI: "s3://models/run-cancel/artifact.json",
			SubmissionID:        "train-cancel-hash",
		})

		Expect(artifact).To(BeNil())
		Expect(err).To(HaveOccurred())
		Expect(deleted).To(BeTrue())
	})

	It("sanitizes deterministic submission ids into Kubernetes names", func() {
		name := executor.KubeRayJobName("Train_RUN_WITH_UPPERCASE_AND_SYMBOLS_" + strings.Repeat("abcdef", 20))

		Expect(name).To(MatchRegexp(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`))
		Expect(len(name)).To(BeNumerically("<=", 63))
	})
})

type lateManifestReader struct {
	location  string
	payload   string
	stat      model.ObjectInfo
	readCalls int
}

func (r *lateManifestReader) Read(_ context.Context, location string) ([]byte, error) {
	r.readCalls++
	if location != r.location || r.readCalls == 1 {
		return []byte{}, nil
	}
	return []byte(r.payload), nil
}

func (r *lateManifestReader) Stat(_ context.Context, location string) (model.ObjectInfo, error) {
	return r.stat, nil
}

func kubeRayConfig() executor.KubeRayExecutorConfig {
	return executor.KubeRayExecutorConfig{
		Namespace:               "ml",
		RayVersion:              "2.46.0",
		Image:                   "training-jobs:unit",
		ImagePullPolicy:         "IfNotPresent",
		ServiceAccountName:      "training-jobs",
		TTLSecondsAfterFinished: 3600,
		WorkerReplicas:          1,
		CPU:                     "1",
		Memory:                  "4Gi",
		GPUResource:             "nvidia.com/gpu",
		GPU:                     "1",
		TrainingEntrypoint:      "python -m training_jobs.train",
		EvaluationEntrypoint:    "python -m training_jobs.evaluate",
		PromotionEntrypoint:     "python -m training_jobs.promotion_report",
		PollInterval:            time.Millisecond,
	}
}
