package k8s_test

import (
	"context"
	stderrors "errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"model_serving_service/pkg/app"
	"model_serving_service/pkg/domain/model"
	servingk8s "model_serving_service/pkg/infra/network/k8s"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestK8s(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Model serving k8s unit test suite")
}

var _ = Describe("ServedModelStore", func() {
	It("lists ServedModel CR specs", func() {
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), servedModelCR(validServedModel()))
		store, err := servingk8s.NewServedModelStore(storeConfig(), client)
		Expect(err).NotTo(HaveOccurred())

		servedModels, err := store.List(context.Background())

		Expect(err).NotTo(HaveOccurred())
		Expect(servedModels).To(HaveLen(1))
		Expect(servedModels[0].ModelID).To(Equal(validServedModel().ModelID))
		Expect(servedModels[0].BaseModel).To(Equal("mistral-7b"))
		Expect(servedModels[0].AdapterURI).To(Equal("s3://models/run-1"))
	})

	It("writes ServedModel status", func() {
		servedModel := validServedModel()
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), servedModelCR(servedModel))
		store, err := servingk8s.NewServedModelStore(storeConfig(), client)
		Expect(err).NotTo(HaveOccurred())

		Expect(store.UpdateStatus(context.Background(), servedModel.ResourceName, &model.ServedModelStatus{
			ServingLoadStatus:  model.ModelLoadStatusLoaded,
			ServingTarget:      "http://served-model.default.svc.cluster.local:8000",
			ServingModel:       "ranker-v1",
			ObservedGeneration: 7,
			ReadyReplicas:      1,
		})).To(Succeed())

		obj, err := client.Resource(servedModelGVR()).Namespace("default").Get(context.Background(), servedModel.ResourceName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		statusRaw, _, _ := unstructured.NestedString(obj.Object, "status", "servingLoadStatus")
		target, _, _ := unstructured.NestedString(obj.Object, "status", "servingTarget")
		Expect(statusRaw).To(Equal("LOADED"))
		Expect(target).To(Equal("http://served-model.default.svc.cluster.local:8000"))
	})

	It("returns status-subresource write errors instead of falling back to a spec update", func() {
		servedModel := validServedModel()
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), servedModelCR(servedModel))
		client.PrependReactor("update", "servedmodels", func(action ktesting.Action) (bool, runtime.Object, error) {
			if action.GetSubresource() != "status" {
				return false, nil, nil
			}
			return true, nil, apierrors.NewForbidden(schema.GroupResource{
				Group:    "serving.bighill.io",
				Resource: "servedmodels",
			}, servedModel.ResourceName, stderrors.New("status update forbidden"))
		})
		store, err := servingk8s.NewServedModelStore(storeConfig(), client)
		Expect(err).NotTo(HaveOccurred())

		err = store.UpdateStatus(context.Background(), servedModel.ResourceName, &model.ServedModelStatus{
			ServingLoadStatus:  model.ModelLoadStatusLoaded,
			ServingTarget:      "http://served-model.default.svc.cluster.local:8000",
			ServingModel:       "ranker-v1",
			ObservedGeneration: 7,
			ReadyReplicas:      1,
		})

		Expect(err).To(MatchError(ContainSubstring("update served model status")))
		Expect(apierrors.IsForbidden(err)).To(BeTrue())
	})

	It("keeps separate workload names for different versions of the same model", func() {
		v1 := validServedModel()
		v2 := validServedModel()
		v1.ResourceName = ""
		v2.ResourceName = ""
		v1.ModelVersion = 1
		v2.ModelVersion = 2

		Expect(servingk8s.WorkloadName(v1)).NotTo(Equal(servingk8s.WorkloadName(v2)))

		v1.ResourceName = "served-model-4f4b8258-f9af-49f8-b5a8-f84d75891f3b-v1"
		v2.ResourceName = "served-model-4f4b8258-f9af-49f8-b5a8-f84d75891f3b-v2"
		Expect(servingk8s.WorkloadName(v1)).NotTo(Equal(servingk8s.WorkloadName(v2)))
	})
})

var _ = Describe("VLLMRuntime", func() {
	It("creates a vLLM deployment and service for a ServedModel", func() {
		servedModel := validServedModel()
		client := fake.NewSimpleDynamicClient(runtime.NewScheme())
		runtimeAdapter, err := servingk8s.NewVLLMRuntime(runtimeConfig(), client)
		Expect(err).NotTo(HaveOccurred())

		state, err := runtimeAdapter.EnsureServedModel(context.Background(), servedModel)

		Expect(err).NotTo(HaveOccurred())
		Expect(state.Ready).To(BeFalse())
		Expect(state.ServingModel).To(Equal("ranker-v1"))
		Expect(state.ServingTarget).To(Equal("http://served-model-4f4b8258-f9af-49f8-b5a8-f84d75891f3b.default.svc.cluster.local:8000"))
		deployment, err := client.Resource(deploymentGVR()).Namespace("default").Get(context.Background(), servingk8s.WorkloadName(servedModel), metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		containers, _, _ := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "containers")
		container := containers[0].(map[string]any)
		Expect(container["image"]).To(Equal("vllm/vllm-openai:v-test"))
		Expect(container["args"]).To(ContainElement("--enable-lora"))
		Expect(container["args"]).To(ContainElement("ranker-v1=s3://models/run-1"))
		service, err := client.Resource(serviceGVR()).Namespace("default").Get(context.Background(), servingk8s.WorkloadName(servedModel), metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		ports, _, _ := unstructured.NestedSlice(service.Object, "spec", "ports")
		Expect(ports[0].(map[string]any)["port"]).To(Equal(int64(8000)))
	})

	It("reports loaded when the vLLM deployment is ready", func() {
		servedModel := validServedModel()
		servedModel.ServingTarget = "http://vllm.test"
		deployment := readyDeployment(servedModel)
		service := serviceObject(servedModel)
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), deployment, service)
		runtimeAdapter, err := servingk8s.NewVLLMRuntime(runtimeConfigWithModels("ranker-v1"), client)
		Expect(err).NotTo(HaveOccurred())

		state, err := runtimeAdapter.EnsureServedModel(context.Background(), servedModel)

		Expect(err).NotTo(HaveOccurred())
		Expect(state.Ready).To(BeTrue())
		Expect(state.ReadyReplicas).To(Equal(int32(1)))
	})

	It("does not report loaded when vLLM does not list the adapter model", func() {
		servedModel := validServedModel()
		servedModel.ServingTarget = "http://vllm.test"
		deployment := readyDeployment(servedModel)
		service := serviceObject(servedModel)
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), deployment, service)
		runtimeAdapter, err := servingk8s.NewVLLMRuntime(runtimeConfigWithModels("base-model"), client)
		Expect(err).NotTo(HaveOccurred())

		state, err := runtimeAdapter.EnsureServedModel(context.Background(), servedModel)

		Expect(err).NotTo(HaveOccurred())
		Expect(state.Ready).To(BeFalse())
		Expect(state.Failed).To(BeFalse())
		Expect(state.FailureReason).To(ContainSubstring("not listed by vllm"))
	})

	It("reports failed when the vLLM deployment exceeds its progress deadline", func() {
		servedModel := validServedModel()
		deployment := failedDeployment(servedModel)
		service := serviceObject(servedModel)
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), deployment, service)
		runtimeAdapter, err := servingk8s.NewVLLMRuntime(runtimeConfig(), client)
		Expect(err).NotTo(HaveOccurred())

		state, err := runtimeAdapter.EnsureServedModel(context.Background(), servedModel)

		Expect(err).NotTo(HaveOccurred())
		Expect(state.Ready).To(BeFalse())
		Expect(state.Failed).To(BeTrue())
		Expect(state.FailureReason).To(ContainSubstring("progress deadline"))
	})

	It("preserves allocated Service cluster IP fields across updates", func() {
		servedModel := validServedModel()
		deployment := readyDeployment(servedModel)
		service := serviceObject(servedModel)
		Expect(unstructured.SetNestedField(service.Object, "10.0.0.10", "spec", "clusterIP")).To(Succeed())
		Expect(unstructured.SetNestedStringSlice(service.Object, []string{"10.0.0.10"}, "spec", "clusterIPs")).To(Succeed())
		Expect(unstructured.SetNestedStringSlice(service.Object, []string{"IPv4"}, "spec", "ipFamilies")).To(Succeed())
		Expect(unstructured.SetNestedField(service.Object, "SingleStack", "spec", "ipFamilyPolicy")).To(Succeed())
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), deployment, service)
		runtimeAdapter, err := servingk8s.NewVLLMRuntime(runtimeConfigWithModels("ranker-v1"), client)
		Expect(err).NotTo(HaveOccurred())

		_, err = runtimeAdapter.EnsureServedModel(context.Background(), servedModel)

		Expect(err).NotTo(HaveOccurred())
		updated, err := client.Resource(serviceGVR()).Namespace("default").Get(context.Background(), servingk8s.WorkloadName(servedModel), metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		clusterIP, _, _ := unstructured.NestedString(updated.Object, "spec", "clusterIP")
		clusterIPs, _, _ := unstructured.NestedStringSlice(updated.Object, "spec", "clusterIPs")
		ipFamilies, _, _ := unstructured.NestedStringSlice(updated.Object, "spec", "ipFamilies")
		ipFamilyPolicy, _, _ := unstructured.NestedString(updated.Object, "spec", "ipFamilyPolicy")
		Expect(clusterIP).To(Equal("10.0.0.10"))
		Expect(clusterIPs).To(Equal([]string{"10.0.0.10"}))
		Expect(ipFamilies).To(Equal([]string{"IPv4"}))
		Expect(ipFamilyPolicy).To(Equal("SingleStack"))
	})

	It("fails when an adapter uri is missing", func() {
		servedModel := validServedModel()
		servedModel.AdapterURI = ""
		client := fake.NewSimpleDynamicClient(runtime.NewScheme())
		runtimeAdapter, err := servingk8s.NewVLLMRuntime(runtimeConfig(), client)
		Expect(err).NotTo(HaveOccurred())

		state, err := runtimeAdapter.EnsureServedModel(context.Background(), servedModel)

		Expect(state).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring("adapter uri is required")))
	})
})

var _ = Describe("ServedModelController", func() {
	It("reconciles every listed ServedModel", func() {
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), servedModelCR(validServedModel()))
		store, err := servingk8s.NewServedModelStore(storeConfig(), client)
		Expect(err).NotTo(HaveOccurred())
		runtimeAdapter, err := servingk8s.NewVLLMRuntime(runtimeConfig(), client)
		Expect(err).NotTo(HaveOccurred())
		reconciler := app.NewServedModelReconciler(runtimeAdapter, store)
		controller := servingk8s.NewServedModelController(store, reconciler, time.Millisecond)

		Expect(controller.ProcessOnce(context.Background())).To(Succeed())

		obj, err := client.Resource(servedModelGVR()).Namespace("default").Get(context.Background(), validServedModel().ResourceName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		statusRaw, _, _ := unstructured.NestedString(obj.Object, "status", "servingLoadStatus")
		Expect(statusRaw).To(Equal("NOT_LOADED"))
	})
})

func storeConfig() servingk8s.ServedModelStoreConfig {
	return servingk8s.ServedModelStoreConfig{
		Namespace: "default",
		Group:     "serving.bighill.io",
		Version:   "v1alpha1",
		Resource:  "servedmodels",
	}
}

func runtimeConfig() servingk8s.VLLMRuntimeConfig {
	return servingk8s.VLLMRuntimeConfig{
		Namespace:       "default",
		Image:           "vllm/vllm-openai:v-test",
		ImagePullPolicy: "IfNotPresent",
		Replicas:        1,
		Port:            8000,
		CPU:             "1",
		Memory:          "4Gi",
		GPUResource:     "nvidia.com/gpu",
		GPU:             "1",
	}
}

func runtimeConfigWithModels(modelIDs ...string) servingk8s.VLLMRuntimeConfig {
	config := runtimeConfig()
	config.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		Expect(req.URL.Path).To(Equal("/v1/models"))
		models := make([]string, 0, len(modelIDs))
		for _, modelID := range modelIDs {
			models = append(models, `{"id":"`+modelID+`"}`)
		}
		body := `{"data":[` + strings.Join(models, ",") + `]}`
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(body)),
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Request:    req,
		}, nil
	})}
	return config
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func validServedModel() *model.ServedModel {
	return &model.ServedModel{
		ResourceName:  "served-model-4f4b8258-f9af-49f8-b5a8-f84d75891f3b",
		Namespace:     "default",
		Generation:    7,
		ModelID:       uuid.MustParse("4f4b8258-f9af-49f8-b5a8-f84d75891f3b"),
		TrainingRunID: uuid.MustParse("76b4da89-7fdb-459a-a842-9f866152efad"),
		DatasetID:     uuid.MustParse("6629d88a-05af-411e-8439-7497620e41df"),
		Name:          "ranker",
		ModelVersion:  1,
		BaseModel:     "mistral-7b",
		AdapterURI:    "s3://models/run-1",
		ServingModel:  "ranker-v1",
	}
}

func servedModelCR(servedModel *model.ServedModel) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "serving.bighill.io/v1alpha1",
		"kind":       "ServedModel",
		"metadata": map[string]any{
			"name":      servedModel.ResourceName,
			"namespace": "default",
		},
		"spec": map[string]any{
			"modelID":       servedModel.ModelID.String(),
			"trainingRunID": servedModel.TrainingRunID.String(),
			"datasetID":     servedModel.DatasetID.String(),
			"name":          servedModel.Name,
			"modelVersion":  int64(servedModel.ModelVersion),
			"baseModel":     servedModel.BaseModel,
			"adapterURI":    servedModel.AdapterURI,
			"servingModel":  servedModel.ServingModel,
		},
	}}
	obj.SetGeneration(servedModel.Generation)
	return obj
}

func readyDeployment(servedModel *model.ServedModel) *unstructured.Unstructured {
	deployment := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name":      servingk8s.WorkloadName(servedModel),
			"namespace": "default",
		},
		"status": map[string]any{
			"readyReplicas":      int64(1),
			"observedGeneration": int64(5),
		},
	}}
	deployment.SetGeneration(5)
	return deployment
}

func failedDeployment(servedModel *model.ServedModel) *unstructured.Unstructured {
	deployment := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name":      servingk8s.WorkloadName(servedModel),
			"namespace": "default",
		},
		"status": map[string]any{
			"observedGeneration": int64(5),
			"conditions": []any{
				map[string]any{
					"type":    "Progressing",
					"status":  "False",
					"reason":  "ProgressDeadlineExceeded",
					"message": "vllm deployment exceeded progress deadline",
				},
			},
		},
	}}
	deployment.SetGeneration(5)
	return deployment
}

func serviceObject(servedModel *model.ServedModel) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata": map[string]any{
			"name":      servingk8s.WorkloadName(servedModel),
			"namespace": "default",
		},
	}}
}

func servedModelGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "serving.bighill.io", Version: "v1alpha1", Resource: "servedmodels"}
}

func deploymentGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
}

func serviceGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "services"}
}
