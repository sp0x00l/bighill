package kubernetes_test

import (
	"context"
	stderrors "errors"
	"testing"
	"time"

	"model_registry_service/pkg/domain/model"
	registrykubernetes "model_registry_service/pkg/infra/network/kubernetes"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic/fake"
)

func TestServedModelK8s(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Model registry served model k8s unit test suite")
}

var _ = Describe("ServedModelAdapter", func() {
	It("creates a ServedModel CR with model serving intent", func() {
		client := fake.NewSimpleDynamicClient(runtime.NewScheme())
		adapter, err := registrykubernetes.NewServedModelAdapterWithClient(servedModelConfig(), client)
		Expect(err).NotTo(HaveOccurred())
		modelRecord := validRegistryModel()

		Expect(adapter.EnsureServedModel(context.Background(), modelRecord)).To(Succeed())

		obj, err := client.Resource(servedModelGVR()).Namespace("ml").Get(context.Background(), registrykubernetes.ServedModelName(modelRecord.ModelID, modelRecord.ModelVersion), metav1.GetOptions{})
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
		adapter, err := registrykubernetes.NewServedModelAdapterWithClient(servedModelConfig(), client)
		Expect(err).NotTo(HaveOccurred())
		modelRecord.AdapterURI = "s3://models/updated"

		Expect(adapter.EnsureServedModel(context.Background(), modelRecord)).To(Succeed())

		obj, err := client.Resource(servedModelGVR()).Namespace("ml").Get(context.Background(), registrykubernetes.ServedModelName(modelRecord.ModelID, modelRecord.ModelVersion), metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		adapterURI, _, _ := unstructured.NestedString(obj.Object, "spec", "adapterURI")
		Expect(adapterURI).To(Equal("s3://models/updated"))
	})

	It("creates distinct ServedModel CRs for different versions of the same model", func() {
		client := fake.NewSimpleDynamicClient(runtime.NewScheme())
		adapter, err := registrykubernetes.NewServedModelAdapterWithClient(servedModelConfig(), client)
		Expect(err).NotTo(HaveOccurred())
		v1 := validRegistryModel()
		v2 := *v1
		v2.ModelVersion = v1.ModelVersion + 1
		v2.ServingModel = "movie-ranker-v2"

		Expect(adapter.EnsureServedModel(context.Background(), v1)).To(Succeed())
		Expect(adapter.EnsureServedModel(context.Background(), &v2)).To(Succeed())

		_, err = client.Resource(servedModelGVR()).Namespace("ml").Get(context.Background(), registrykubernetes.ServedModelName(v1.ModelID, v1.ModelVersion), metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		_, err = client.Resource(servedModelGVR()).Namespace("ml").Get(context.Background(), registrykubernetes.ServedModelName(v2.ModelID, v2.ModelVersion), metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(registrykubernetes.ServedModelName(v1.ModelID, v1.ModelVersion)).NotTo(Equal(registrykubernetes.ServedModelName(v2.ModelID, v2.ModelVersion)))
	})

	It("records loaded ServedModel status through the usecase boundary", func() {
		modelRecord := validRegistryModel()
		existing := servedModelObject(modelRecord)
		Expect(unstructured.SetNestedField(existing.Object, "LOADED", "status", "servingLoadStatus")).To(Succeed())
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), existing)
		adapter, err := registrykubernetes.NewServedModelAdapterWithClient(servedModelConfig(), client)
		Expect(err).NotTo(HaveOccurred())
		recorder := &servingStatusRecorderStub{}
		observer, err := registrykubernetes.NewServedModelStatusObserver(adapter, recorder, time.Millisecond)
		Expect(err).NotTo(HaveOccurred())

		Expect(observer.ProcessOnce(context.Background())).To(Succeed())

		Expect(recorder.status).NotTo(BeNil())
		Expect(recorder.status.ModelID).To(Equal(modelRecord.ModelID))
		Expect(recorder.status.ServingLoadStatus).To(Equal(model.ModelLoadStatusLoaded))
		Expect(recorder.status.ServingTarget).To(Equal(modelRecord.ServingTarget))
		Expect(recorder.idempotencyKey).NotTo(Equal(uuid.Nil))
	})

	It("skips unchanged ServedModel status after a successful record", func() {
		modelRecord := validRegistryModel()
		existing := servedModelObject(modelRecord)
		Expect(unstructured.SetNestedField(existing.Object, "LOADED", "status", "servingLoadStatus")).To(Succeed())
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), existing)
		adapter, err := registrykubernetes.NewServedModelAdapterWithClient(servedModelConfig(), client)
		Expect(err).NotTo(HaveOccurred())
		recorder := &servingStatusRecorderStub{}
		observer, err := registrykubernetes.NewServedModelStatusObserver(adapter, recorder, time.Millisecond)
		Expect(err).NotTo(HaveOccurred())

		Expect(observer.ProcessOnce(context.Background())).To(Succeed())
		Expect(observer.ProcessOnce(context.Background())).To(Succeed())

		Expect(recorder.calls).To(Equal(1))
	})

	It("continues when a ServedModel has malformed status", func() {
		modelRecord := validRegistryModel()
		valid := servedModelObject(modelRecord)
		Expect(unstructured.SetNestedField(valid.Object, "LOADED", "status", "servingLoadStatus")).To(Succeed())
		malformed := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "serving.bighill.io/v1alpha1",
			"kind":       "ServedModel",
			"metadata": map[string]any{
				"name":      "bad-served-model",
				"namespace": "ml",
			},
			"spec": map[string]any{
				"modelID": "not-a-uuid",
			},
			"status": map[string]any{
				"servingLoadStatus": "LOADED",
			},
		}}
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), malformed, valid)
		adapter, err := registrykubernetes.NewServedModelAdapterWithClient(servedModelConfig(), client)
		Expect(err).NotTo(HaveOccurred())
		recorder := &servingStatusRecorderStub{}
		observer, err := registrykubernetes.NewServedModelStatusObserver(adapter, recorder, time.Millisecond)
		Expect(err).NotTo(HaveOccurred())

		Expect(observer.ProcessOnce(context.Background())).To(Succeed())

		Expect(recorder.status).NotTo(BeNil())
		Expect(recorder.status.ModelID).To(Equal(modelRecord.ModelID))
	})

	It("does not fail the poll when recording one ServedModel status fails", func() {
		modelRecord := validRegistryModel()
		existing := servedModelObject(modelRecord)
		Expect(unstructured.SetNestedField(existing.Object, "LOADED", "status", "servingLoadStatus")).To(Succeed())
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), existing)
		adapter, err := registrykubernetes.NewServedModelAdapterWithClient(servedModelConfig(), client)
		Expect(err).NotTo(HaveOccurred())
		recorder := &servingStatusRecorderStub{err: stderrors.New("database unavailable")}
		observer, err := registrykubernetes.NewServedModelStatusObserver(adapter, recorder, time.Millisecond)
		Expect(err).NotTo(HaveOccurred())

		Expect(observer.ProcessOnce(context.Background())).To(Succeed())

		Expect(recorder.calls).To(Equal(1))
	})

	It("records loaded ServedModel status from watch events", func() {
		modelRecord := validRegistryModel()
		existing := servedModelObject(modelRecord)
		Expect(unstructured.SetNestedField(existing.Object, "LOADED", "status", "servingLoadStatus")).To(Succeed())
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), existing)
		adapter, err := registrykubernetes.NewServedModelAdapterWithClient(servedModelConfig(), client)
		Expect(err).NotTo(HaveOccurred())
		recorder := &servingStatusRecorderStub{}
		observer, err := registrykubernetes.NewServedModelStatusObserver(adapter, recorder, time.Millisecond)
		Expect(err).NotTo(HaveOccurred())

		Expect(observer.ProcessWatchEvent(context.Background(), watch.Event{
			Type:   watch.Modified,
			Object: existing,
		})).To(Succeed())

		Expect(recorder.status).NotTo(BeNil())
		Expect(recorder.status.ModelID).To(Equal(modelRecord.ModelID))
		Expect(recorder.status.ServingLoadStatus).To(Equal(model.ModelLoadStatusLoaded))
		Expect(recorder.idempotencyKey).NotTo(Equal(uuid.Nil))
	})
})

type servingStatusRecorderStub struct {
	status         *model.ServedModelStatus
	idempotencyKey uuid.UUID
	err            error
	calls          int
}

func (s *servingStatusRecorderStub) RecordModelServingStatus(_ context.Context, status *model.ServedModelStatus, idempotencyKey uuid.UUID) (*model.Model, error) {
	s.calls++
	s.status = status
	s.idempotencyKey = idempotencyKey
	if s.err != nil {
		return nil, s.err
	}
	return &model.Model{ModelID: status.ModelID}, nil
}

func servedModelConfig() registrykubernetes.ServedModelConfig {
	return registrykubernetes.ServedModelConfig{
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
			"name":      registrykubernetes.ServedModelName(modelRecord.ModelID, modelRecord.ModelVersion),
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
