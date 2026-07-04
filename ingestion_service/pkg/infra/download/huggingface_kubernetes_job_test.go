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
		container := firstJobContainer(created)
		Expect(container["image"]).To(Equal("training-jobs:test"))
		Expect(container["command"]).To(Equal([]any{"python", "-m", "training_jobs.model_onboard"}))
		env := containerEnv(container)
		Expect(env["INGESTION_SERVICE_MODEL_RESOURCE_ID"].(map[string]any)["value"]).To(Equal(resourceID.String()))
		Expect(env["INGESTION_SERVICE_HUGGINGFACE_TOKEN"].(map[string]any)["value"]).To(Equal("hf-token"))
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
