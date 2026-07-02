package k8s_test

import (
	"context"
	"testing"
	"time"

	"model_registry_service/pkg/domain/model"
	registryk8s "model_registry_service/pkg/infra/network/k8s"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
)

func TestServedModelK8s(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Model registry served model k8s unit test suite")
}

var _ = Describe("ServedModelAdapter", func() {
	It("creates a ServedModel CR with model serving intent", func() {
		client := fake.NewSimpleDynamicClient(runtime.NewScheme())
		adapter, err := registryk8s.NewServedModelAdapterWithClient(servedModelConfig(), client)
		Expect(err).NotTo(HaveOccurred())
		modelRecord := validRegistryModel()

		Expect(adapter.EnsureServedModel(context.Background(), modelRecord)).To(Succeed())

		obj, err := client.Resource(servedModelGVR()).Namespace("ml").Get(context.Background(), registryk8s.ServedModelName(modelRecord.ModelID), metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.GetLabels()).To(HaveKeyWithValue("bighill.io/model-id", modelRecord.ModelID.String()))
		modelID, _, _ := unstructured.NestedString(obj.Object, "spec", "modelID")
		adapterURI, _, _ := unstructured.NestedString(obj.Object, "spec", "adapterURI")
		servingTarget, _, _ := unstructured.NestedString(obj.Object, "spec", "servingTarget")
		Expect(modelID).To(Equal(modelRecord.ModelID.String()))
		Expect(adapterURI).To(Equal(modelRecord.AdapterURI))
		Expect(servingTarget).To(Equal(modelRecord.ServingTarget))
	})

	It("updates existing ServedModel intent", func() {
		modelRecord := validRegistryModel()
		existing := servedModelObject(modelRecord)
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), existing)
		adapter, err := registryk8s.NewServedModelAdapterWithClient(servedModelConfig(), client)
		Expect(err).NotTo(HaveOccurred())
		modelRecord.AdapterURI = "s3://models/updated"

		Expect(adapter.EnsureServedModel(context.Background(), modelRecord)).To(Succeed())

		obj, err := client.Resource(servedModelGVR()).Namespace("ml").Get(context.Background(), registryk8s.ServedModelName(modelRecord.ModelID), metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		adapterURI, _, _ := unstructured.NestedString(obj.Object, "spec", "adapterURI")
		Expect(adapterURI).To(Equal("s3://models/updated"))
	})

	It("records loaded ServedModel status through the usecase boundary", func() {
		modelRecord := validRegistryModel()
		existing := servedModelObject(modelRecord)
		Expect(unstructured.SetNestedField(existing.Object, "LOADED", "status", "servingLoadStatus")).To(Succeed())
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), existing)
		adapter, err := registryk8s.NewServedModelAdapterWithClient(servedModelConfig(), client)
		Expect(err).NotTo(HaveOccurred())
		recorder := &servingStatusRecorderStub{}
		observer, err := registryk8s.NewServedModelStatusObserver(adapter, recorder, time.Millisecond)
		Expect(err).NotTo(HaveOccurred())

		Expect(observer.ProcessOnce(context.Background())).To(Succeed())

		Expect(recorder.status).NotTo(BeNil())
		Expect(recorder.status.ModelID).To(Equal(modelRecord.ModelID))
		Expect(recorder.status.ServingLoadStatus).To(Equal(model.ModelLoadStatusLoaded))
		Expect(recorder.status.ServingTarget).To(Equal(modelRecord.ServingTarget))
		Expect(recorder.idempotencyKey).NotTo(Equal(uuid.Nil))
	})
})

type servingStatusRecorderStub struct {
	status         *model.ServedModelStatus
	idempotencyKey uuid.UUID
}

func (s *servingStatusRecorderStub) RecordModelServingStatus(_ context.Context, status *model.ServedModelStatus, idempotencyKey uuid.UUID) (*model.Model, error) {
	s.status = status
	s.idempotencyKey = idempotencyKey
	return &model.Model{ModelID: status.ModelID}, nil
}

func servedModelConfig() registryk8s.ServedModelConfig {
	return registryk8s.ServedModelConfig{
		Namespace:    "ml",
		Group:        "serving.bighill.io",
		Version:      "v1alpha1",
		Resource:     "servedmodels",
		Kind:         "ServedModel",
		PollInterval: time.Millisecond,
	}
}

func servedModelGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "serving.bighill.io", Version: "v1alpha1", Resource: "servedmodels"}
}

func validRegistryModel() *model.Model {
	return &model.Model{
		ModelID:           uuid.New(),
		TrainingRunID:     uuid.New(),
		DatasetID:         uuid.New(),
		Name:              "movie-ranker",
		ModelVersion:      1,
		BaseModel:         "mistral-7b",
		ArtifactLocation:  "s3://models/run",
		ArtifactFormat:    "HF_PEFT_ADAPTER",
		ArtifactChecksum:  "sha256:abc",
		ArtifactSizeBytes: 123,
		AdapterURI:        "s3://models/run",
		ServingTarget:     "vllm-local",
		ServingModel:      "movie-ranker-v1",
		ServingLoadStatus: model.ModelLoadStatusNotLoaded,
		Status:            model.ModelStatusEvaluated,
	}
}

func servedModelObject(modelRecord *model.Model) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "serving.bighill.io/v1alpha1",
		"kind":       "ServedModel",
		"metadata": map[string]any{
			"name":      registryk8s.ServedModelName(modelRecord.ModelID),
			"namespace": "ml",
		},
		"spec": map[string]any{
			"modelID":       modelRecord.ModelID.String(),
			"adapterURI":    modelRecord.AdapterURI,
			"servingTarget": modelRecord.ServingTarget,
			"servingModel":  modelRecord.ServingModel,
		},
	}}
}
