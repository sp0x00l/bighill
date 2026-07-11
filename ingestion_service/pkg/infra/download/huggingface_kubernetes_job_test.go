package download_test

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"ingestion_service/pkg/domain/model"
	"ingestion_service/pkg/infra/download"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

var _ = Describe("HuggingFaceKubernetesJobDownloader", func() {
	It("creates a Kubernetes Job and reads the object-store manifest after success", func() {
		resourceID := uuid.New()
		manifest := map[string]any{
			"resource_id":         resourceID.String(),
			"storage_location":    "s3://bucket/models/" + resourceID.String() + "/snapshot",
			"manifest_location":   "s3://bucket/models/" + resourceID.String() + "/manifest.json",
			"artifact_type":       "BASE_MODEL",
			"artifact_format":     "HF_MODEL",
			"artifact_size_bytes": float64(12),
			"artifact_checksum":   "sha256:test",
			"model_name":          "llama",
			"model_version":       "1",
			"base_model":          "meta-llama/Llama",
			"source_uri":          "https://huggingface.co/meta-llama/Llama",
			"hf_repo_id":          "meta-llama/Llama",
			"hf_revision":         "main",
			"hf_commit_sha":       "abc",
		}
		manifestBytes, err := json.Marshal(manifest)
		Expect(err).NotTo(HaveOccurred())
		reader := &stubModelManifestReader{data: manifestBytes}
		client := fake.NewSimpleDynamicClient(kruntime.NewScheme())
		var created *unstructured.Unstructured
		client.PrependReactor("create", "jobs", func(action ktesting.Action) (bool, kruntime.Object, error) {
			created = action.(ktesting.CreateAction).GetObject().(*unstructured.Unstructured)
			return false, nil, nil
		})
		client.PrependReactor("get", "jobs", func(action ktesting.Action) (bool, kruntime.Object, error) {
			Expect(created).NotTo(BeNil())
			obj := created.DeepCopy()
			Expect(unstructured.SetNestedField(obj.Object, int64(1), "status", "succeeded")).To(Succeed())
			return true, obj, nil
		})
		downloader, err := download.NewHuggingFaceKubernetesJobDownloaderWithClient(download.HuggingFaceKubernetesJobDownloaderConfig{
			Namespace:               "ml-ops-test",
			Image:                   "training-jobs:test",
			ImagePullPolicy:         "IfNotPresent",
			ServiceAccountName:      "ingestion-service",
			Command:                 "python -m training_jobs.model_onboard",
			OutputURI:               "s3://bucket/models",
			TTLSecondsAfterFinished: 60,
			BackoffLimit:            0,
			CPU:                     "1",
			Memory:                  "1Gi",
			PollInterval:            time.Millisecond,
			Timeout:                 time.Second,
			EnvKeys:                 testHuggingFaceJobEnvKeys(),
		}, reader, client)
		Expect(err).NotTo(HaveOccurred())

		result, err := downloader.DownloadHuggingFaceModel(context.Background(), model.OnboardHuggingFaceModelRequest{
			ResourceID:       resourceID,
			RepoID:           "meta-llama/Llama",
			Revision:         "main",
			ModelName:        "llama",
			ModelVersion:     "1",
			BaseModel:        "meta-llama/Llama",
			HuggingFaceToken: "hf-token",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(result.ResourceID).To(Equal(resourceID))
		Expect(result.StorageLocation).To(Equal("s3://bucket/models/" + resourceID.String() + "/snapshot"))
		Expect(reader.location).To(Equal("s3://bucket/models/" + resourceID.String() + "/manifest.json"))
		Expect(created.GetNamespace()).To(Equal("ml-ops-test"))
		Expect(created.GetName()).To(HavePrefix("hf-model-"))
		Expect(created.GetAnnotations()).To(HaveKeyWithValue("bighill.io/request-hash", Not(BeEmpty())))
		container := firstJobContainer(created)
		Expect(container["image"]).To(Equal("training-jobs:test"))
		Expect(container["command"]).To(Equal([]any{"python", "-m", "training_jobs.model_onboard"}))
		env := containerEnv(container)
		Expect(env["INGESTION_SERVICE_MODEL_RESOURCE_ID"].(map[string]any)["value"]).To(Equal(resourceID.String()))
		Expect(env["INGESTION_SERVICE_HUGGINGFACE_TOKEN"].(map[string]any)["value"]).To(Equal("hf-token"))
	})

	It("maps PEFT adapter rank from the object-store manifest", func() {
		resourceID := uuid.New()
		manifest := map[string]any{
			"resource_id":         resourceID.String(),
			"storage_location":    "s3://bucket/models/" + resourceID.String() + "/snapshot",
			"manifest_location":   "s3://bucket/models/" + resourceID.String() + "/manifest.json",
			"artifact_type":       "LORA_ADAPTER",
			"artifact_format":     "HF_PEFT_ADAPTER",
			"artifact_size_bytes": float64(12),
			"artifact_checksum":   "sha256:test",
			"model_name":          "adapter",
			"model_version":       "1",
			"base_model":          "meta-llama/Llama",
			"adapter_rank":        float64(16),
			"source_uri":          "https://huggingface.co/org/adapter",
			"hf_repo_id":          "org/adapter",
			"hf_revision":         "main",
			"hf_commit_sha":       "abc",
		}
		manifestBytes, err := json.Marshal(manifest)
		Expect(err).NotTo(HaveOccurred())
		reader := &stubModelManifestReader{data: manifestBytes}
		client := fake.NewSimpleDynamicClient(kruntime.NewScheme())
		var created *unstructured.Unstructured
		client.PrependReactor("create", "jobs", func(action ktesting.Action) (bool, kruntime.Object, error) {
			created = action.(ktesting.CreateAction).GetObject().(*unstructured.Unstructured)
			return false, nil, nil
		})
		client.PrependReactor("get", "jobs", func(action ktesting.Action) (bool, kruntime.Object, error) {
			Expect(created).NotTo(BeNil())
			obj := created.DeepCopy()
			Expect(unstructured.SetNestedField(obj.Object, int64(1), "status", "succeeded")).To(Succeed())
			return true, obj, nil
		})
		downloader, err := download.NewHuggingFaceKubernetesJobDownloaderWithClient(testKubernetesDownloaderConfig(time.Millisecond, time.Second), reader, client)
		Expect(err).NotTo(HaveOccurred())

		result, err := downloader.DownloadHuggingFaceModel(context.Background(), model.OnboardHuggingFaceModelRequest{
			ResourceID:       resourceID,
			RepoID:           "org/adapter",
			Revision:         "main",
			ModelName:        "adapter",
			ModelVersion:     "1",
			BaseModel:        "meta-llama/Llama",
			AdapterRank:      16,
			ArtifactType:     "LORA_ADAPTER",
			ArtifactFormat:   "HF_PEFT_ADAPTER",
			HuggingFaceToken: "hf-token",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(result.ResourceID).To(Equal(resourceID))
		Expect(result.AdapterRank).To(Equal(16))
		Expect(result.ArtifactType).To(Equal("LORA_ADAPTER"))
		Expect(result.ArtifactFormat).To(Equal("HF_PEFT_ADAPTER"))
	})

	It("deletes the Kubernetes Job when the download times out", func() {
		resourceID := uuid.New()
		reader := &stubModelManifestReader{}
		client := fake.NewSimpleDynamicClient(kruntime.NewScheme())
		var created *unstructured.Unstructured
		deleted := false
		client.PrependReactor("create", "jobs", func(action ktesting.Action) (bool, kruntime.Object, error) {
			created = action.(ktesting.CreateAction).GetObject().(*unstructured.Unstructured)
			return false, nil, nil
		})
		client.PrependReactor("get", "jobs", func(action ktesting.Action) (bool, kruntime.Object, error) {
			Expect(created).NotTo(BeNil())
			return true, created.DeepCopy(), nil
		})
		client.PrependReactor("delete", "jobs", func(action ktesting.Action) (bool, kruntime.Object, error) {
			deleted = true
			return true, nil, nil
		})
		downloader, err := download.NewHuggingFaceKubernetesJobDownloaderWithClient(testKubernetesDownloaderConfig(time.Millisecond, 5*time.Millisecond), reader, client)
		Expect(err).NotTo(HaveOccurred())

		_, err = downloader.DownloadHuggingFaceModel(context.Background(), testOnboardRequest(resourceID, "main"))

		Expect(err).To(MatchError(ContainSubstring("wait for hugging face download job")))
		Expect(deleted).To(BeTrue())
	})

	It("rejects an existing Kubernetes Job with a different request hash", func() {
		resourceID := uuid.New()
		name := download.HuggingFaceJobName(resourceID)
		existing := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "batch/v1",
			"kind":       "Job",
			"metadata": map[string]any{
				"name":      name,
				"namespace": "ml-ops-test",
				"annotations": map[string]any{
					"bighill.io/request-hash": "different",
				},
			},
		}}
		client := fake.NewSimpleDynamicClient(kruntime.NewScheme(), existing)
		downloader, err := download.NewHuggingFaceKubernetesJobDownloaderWithClient(testKubernetesDownloaderConfig(time.Millisecond, time.Second), &stubModelManifestReader{}, client)
		Expect(err).NotTo(HaveOccurred())

		_, err = downloader.DownloadHuggingFaceModel(context.Background(), testOnboardRequest(resourceID, "main"))

		Expect(err).To(MatchError(ContainSubstring("existing hugging face download job has different request hash")))
	})
})

type stubModelManifestReader struct {
	location string
	data     []byte
	err      error
}

func (r *stubModelManifestReader) ReadManifest(_ context.Context, location string) ([]byte, error) {
	r.location = location
	return r.data, r.err
}

func firstJobContainer(obj *unstructured.Unstructured) map[string]any {
	GinkgoHelper()

	containers, found, err := unstructured.NestedSlice(obj.Object, "spec", "template", "spec", "containers")
	Expect(err).NotTo(HaveOccurred())
	Expect(found).To(BeTrue())
	Expect(containers).To(HaveLen(1))
	container, ok := containers[0].(map[string]any)
	Expect(ok).To(BeTrue())
	return container
}

func containerEnv(container map[string]any) map[string]any {
	GinkgoHelper()

	items, ok := container["env"].([]any)
	Expect(ok).To(BeTrue())
	out := map[string]any{}
	for _, item := range items {
		env, ok := item.(map[string]any)
		Expect(ok).To(BeTrue())
		name, ok := env["name"].(string)
		Expect(ok).To(BeTrue())
		if strings.TrimSpace(name) != "" {
			out[name] = env
		}
	}
	return out
}

func testKubernetesDownloaderConfig(pollInterval, timeout time.Duration) download.HuggingFaceKubernetesJobDownloaderConfig {
	return download.HuggingFaceKubernetesJobDownloaderConfig{
		Namespace:               "ml-ops-test",
		Image:                   "training-jobs:test",
		ImagePullPolicy:         "IfNotPresent",
		ServiceAccountName:      "ingestion-service",
		Command:                 "python -m training_jobs.model_onboard",
		OutputURI:               "s3://bucket/models",
		TTLSecondsAfterFinished: 60,
		BackoffLimit:            0,
		CPU:                     "1",
		Memory:                  "1Gi",
		PollInterval:            pollInterval,
		Timeout:                 timeout,
		EnvKeys:                 testHuggingFaceJobEnvKeys(),
	}
}

func testOnboardRequest(resourceID uuid.UUID, revision string) model.OnboardHuggingFaceModelRequest {
	return model.OnboardHuggingFaceModelRequest{
		ResourceID:       resourceID,
		RepoID:           "meta-llama/Llama",
		Revision:         revision,
		ModelName:        "llama",
		ModelVersion:     "1",
		BaseModel:        "meta-llama/Llama",
		HuggingFaceToken: "hf-token",
	}
}
