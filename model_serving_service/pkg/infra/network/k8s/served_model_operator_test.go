package kubernetes_test

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
	servingkubernetes "model_serving_service/pkg/infra/network/k8s"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
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
		store, err := servingkubernetes.NewServedModelStore(storeConfig(), client)
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
		store, err := servingkubernetes.NewServedModelStore(storeConfig(), client)
		Expect(err).NotTo(HaveOccurred())

		Expect(store.UpdateStatus(context.Background(), servedModel.ResourceName, &model.ServedModelStatus{
			ServingLoadStatus:  model.ModelLoadStatusLoaded,
			ServingTarget:      "http://served-model.default.svc.cluster.local:8000",
			ServingModel:       "ranker-v1",
			ServingProtocol:    model.ServingProtocolOpenAIChatCompletions,
			ObservedGeneration: 7,
			ReadyReplicas:      1,
		})).To(Succeed())

		obj, err := client.Resource(servedModelGVR()).Namespace("default").Get(context.Background(), servedModel.ResourceName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		statusRaw, _, _ := unstructured.NestedString(obj.Object, "status", "servingLoadStatus")
		target, _, _ := unstructured.NestedString(obj.Object, "status", "servingTarget")
		protocol, _, _ := unstructured.NestedString(obj.Object, "status", "servingProtocol")
		Expect(statusRaw).To(Equal("LOADED"))
		Expect(target).To(Equal("http://served-model.default.svc.cluster.local:8000"))
		Expect(protocol).To(Equal("OPENAI_CHAT_COMPLETIONS"))
	})

	It("skips ServedModel status writes when status has not changed", func() {
		servedModel := validServedModel()
		existing := servedModelCR(servedModel)
		Expect(unstructured.SetNestedField(existing.Object, "LOADED", "status", "servingLoadStatus")).To(Succeed())
		Expect(unstructured.SetNestedField(existing.Object, "http://served-model.default.svc.cluster.local:8000", "status", "servingTarget")).To(Succeed())
		Expect(unstructured.SetNestedField(existing.Object, "ranker-v1", "status", "servingModel")).To(Succeed())
		Expect(unstructured.SetNestedField(existing.Object, "OPENAI_CHAT_COMPLETIONS", "status", "servingProtocol")).To(Succeed())
		Expect(unstructured.SetNestedField(existing.Object, "", "status", "failureReason")).To(Succeed())
		Expect(unstructured.SetNestedField(existing.Object, int64(7), "status", "observedGeneration")).To(Succeed())
		Expect(unstructured.SetNestedField(existing.Object, int64(1), "status", "readyReplicas")).To(Succeed())
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), existing)
		updateCalls := 0
		client.PrependReactor("update", "servedmodels", func(action ktesting.Action) (bool, runtime.Object, error) {
			if action.GetSubresource() == "status" {
				updateCalls++
			}
			return false, nil, nil
		})
		store, err := servingkubernetes.NewServedModelStore(storeConfig(), client)
		Expect(err).NotTo(HaveOccurred())

		Expect(store.UpdateStatus(context.Background(), servedModel.ResourceName, &model.ServedModelStatus{
			ServingLoadStatus:  model.ModelLoadStatusLoaded,
			ServingTarget:      "http://served-model.default.svc.cluster.local:8000",
			ServingModel:       "ranker-v1",
			ServingProtocol:    model.ServingProtocolOpenAIChatCompletions,
			ObservedGeneration: 7,
			ReadyReplicas:      1,
		})).To(Succeed())

		Expect(updateCalls).To(Equal(0))
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
		store, err := servingkubernetes.NewServedModelStore(storeConfig(), client)
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

		Expect(servingkubernetes.WorkloadName(v1)).NotTo(Equal(servingkubernetes.WorkloadName(v2)))

		v1.ResourceName = "served-model-4f4b8258-f9af-49f8-b5a8-f84d75891f3b-v1"
		v2.ResourceName = "served-model-4f4b8258-f9af-49f8-b5a8-f84d75891f3b-v2"
		Expect(servingkubernetes.WorkloadName(v1)).NotTo(Equal(servingkubernetes.WorkloadName(v2)))
	})
})

var _ = Describe("VLLMRuntime", func() {
	It("creates a vLLM deployment and service for a ServedModel", func() {
		servedModel := validServedModel()
		client := fake.NewSimpleDynamicClient(runtime.NewScheme())
		runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(runtimeConfig(), client)
		Expect(err).NotTo(HaveOccurred())

		state, err := runtimeAdapter.EnsureServedModel(context.Background(), servedModel)

		Expect(err).NotTo(HaveOccurred())
		Expect(state.Ready).To(BeFalse())
		Expect(state.ServingModel).To(Equal("ranker-v1"))
		Expect(state.ServingTarget).To(Equal("http://served-model-4f4b8258-f9af-49f8-b5a8-f84d75891f3b.default.svc.cluster.local:8000"))
		Expect(state.ServingProtocol).To(Equal(model.ServingProtocolOpenAIChatCompletions))
		deployment, err := client.Resource(deploymentGVR()).Namespace("default").Get(context.Background(), servingkubernetes.WorkloadName(servedModel), metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		containers, _, _ := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "containers")
		container := containers[0].(map[string]any)
		Expect(container["image"]).To(Equal("vllm/vllm-openai:v-test"))
		Expect(container["args"]).To(ContainElement("--enable-lora"))
		Expect(container["args"]).To(ContainElement("ranker-v1=s3://models/run-1"))
		service, err := client.Resource(serviceGVR()).Namespace("default").Get(context.Background(), servingkubernetes.WorkloadName(servedModel), metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		ports, _, _ := unstructured.NestedSlice(service.Object, "spec", "ports")
		Expect(ports[0].(map[string]any)["port"]).To(Equal(int64(8000)))
	})

	It("treats major open model families as runtime data, not provider or protocol variants", func() {
		families := []string{
			"meta-llama/Llama-3-8B",
			"mistralai/Mistral-7B-Instruct-v0.3",
			"Qwen/Qwen2.5-7B-Instruct",
			"deepseek-ai/DeepSeek-R1-Distill-Qwen-7B",
			"google/gemma-2-9b-it",
		}

		for _, baseModel := range families {
			servedModel := validServedModel()
			servedModel.ModelID = uuid.New()
			servedModel.ResourceName = ""
			servedModel.BaseModel = baseModel
			servedModel.AdapterURI = ""
			client := fake.NewSimpleDynamicClient(runtime.NewScheme())
			runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(runtimeConfig(), client)
			Expect(err).NotTo(HaveOccurred())

			state, err := runtimeAdapter.EnsureServedModel(context.Background(), servedModel)

			Expect(err).NotTo(HaveOccurred())
			Expect(state.ServingProtocol).To(Equal(model.ServingProtocolOpenAIChatCompletions), baseModel)
			deployment, err := client.Resource(deploymentGVR()).Namespace("default").Get(context.Background(), servingkubernetes.WorkloadName(servedModel), metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			containers, _, _ := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "containers")
			args := containers[0].(map[string]any)["args"].([]any)
			Expect(args).To(ContainElement("--model"), baseModel)
			Expect(args).To(ContainElement(baseModel), baseModel)
			Expect(args).To(ContainElement("--served-model-name"), baseModel)
			Expect(args).To(ContainElement(servingkubernetes.ServingModelName(servedModel)), baseModel)
		}
	})

	It("uses a shared base runtime and dynamically loads adapters in multi-tenant mode", func() {
		servedModel := validServedModel()
		servedModel.ServingTarget = "http://vllm.test"
		sharedWorkloadName := servingkubernetes.SharedRuntimeWorkloadName(servedModel)
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), readyDeploymentWithName(sharedWorkloadName), serviceObjectWithName(sharedWorkloadName))
		loadRequests := 0
		config := runtimeConfig()
		config.MultiTenant = true
		config.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch {
			case req.Method == http.MethodGet && req.URL.Path == "/v1/models" && loadRequests == 0:
				return jsonResponse(req, `{"data":[{"id":"base-mistral-7b"}]}`), nil
			case req.Method == http.MethodPost && req.URL.Path == "/v1/load_lora_adapter":
				raw, err := io.ReadAll(req.Body)
				Expect(err).NotTo(HaveOccurred())
				Expect(string(raw)).To(MatchJSON(`{"lora_name":"ranker-v1","lora_path":"s3://models/run-1"}`))
				loadRequests++
				return jsonResponse(req, `{}`), nil
			case req.Method == http.MethodGet && req.URL.Path == "/v1/models" && loadRequests == 1:
				return jsonResponse(req, `{"data":[{"id":"base-mistral-7b"},{"id":"ranker-v1"}]}`), nil
			default:
				return nil, stderrors.New("unexpected vllm request " + req.Method + " " + req.URL.Path)
			}
		})}
		runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(config, client)
		Expect(err).NotTo(HaveOccurred())

		state, err := runtimeAdapter.EnsureServedModel(context.Background(), servedModel)

		Expect(err).NotTo(HaveOccurred())
		Expect(state.Ready).To(BeTrue())
		Expect(loadRequests).To(Equal(1))
		Expect(state.ServingTarget).To(Equal("http://vllm.test"))
		Expect(state.ServingModel).To(Equal("ranker-v1"))
		Expect(state.ServingProtocol).To(Equal(model.ServingProtocolOpenAIChatCompletions))
		deployment, err := client.Resource(deploymentGVR()).Namespace("default").Get(context.Background(), sharedWorkloadName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		containers, _, _ := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "containers")
		container := containers[0].(map[string]any)
		Expect(container["args"]).To(ContainElement("--enable-lora"))
		Expect(container["args"]).NotTo(ContainElement("--lora-modules"))
	})

	It("does not mark a multi-tenant adapter failed when vLLM reports it is already loaded", func() {
		servedModel := validServedModel()
		servedModel.ServingTarget = "http://vllm.test"
		sharedWorkloadName := servingkubernetes.SharedRuntimeWorkloadName(servedModel)
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), readyDeploymentWithName(sharedWorkloadName), serviceObjectWithName(sharedWorkloadName))
		loadRequests := 0
		config := runtimeConfig()
		config.MultiTenant = true
		config.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch {
			case req.Method == http.MethodGet && req.URL.Path == "/v1/models":
				return jsonResponse(req, `{"data":[{"id":"base-mistral-7b"}]}`), nil
			case req.Method == http.MethodPost && req.URL.Path == "/v1/load_lora_adapter":
				loadRequests++
				return &http.Response{
					StatusCode: http.StatusConflict,
					Body:       io.NopCloser(strings.NewReader(`{"error":"adapter already loaded"}`)),
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Request:    req,
				}, nil
			default:
				return nil, stderrors.New("unexpected vllm request " + req.Method + " " + req.URL.Path)
			}
		})}
		runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(config, client)
		Expect(err).NotTo(HaveOccurred())

		state, err := runtimeAdapter.EnsureServedModel(context.Background(), servedModel)

		Expect(err).NotTo(HaveOccurred())
		Expect(state.Ready).To(BeFalse())
		Expect(state.Failed).To(BeFalse())
		Expect(state.FailureReason).To(ContainSubstring("not listed by vllm"))
		Expect(loadRequests).To(Equal(1))
	})

	It("shares one workload for versions of the same base model when multi-tenant mode is enabled", func() {
		v1 := validServedModel()
		v2 := validServedModel()
		v1.ResourceName = ""
		v2.ResourceName = ""
		v1.ModelVersion = 1
		v1.ServingModel = "ranker-v1"
		v2.ModelVersion = 2
		v2.ServingModel = "ranker-v2"
		v2.AdapterURI = "s3://models/run-2"

		Expect(servingkubernetes.WorkloadName(v1)).NotTo(Equal(servingkubernetes.WorkloadName(v2)))
		Expect(servingkubernetes.SharedRuntimeWorkloadName(v1)).To(Equal(servingkubernetes.SharedRuntimeWorkloadName(v2)))
	})

	It("hashes shared runtime names so normalized base-model collisions stay separate", func() {
		first := validServedModel()
		second := validServedModel()
		first.BaseModel = "org/Model-A"
		second.BaseModel = "org-model-a"

		Expect(servingkubernetes.SharedRuntimeWorkloadName(first)).NotTo(Equal(servingkubernetes.SharedRuntimeWorkloadName(second)))
		Expect(servingkubernetes.SharedRuntimeServingModelName(first)).NotTo(Equal(servingkubernetes.SharedRuntimeServingModelName(second)))
	})

	It("reports loaded when the vLLM deployment is ready", func() {
		servedModel := validServedModel()
		servedModel.ServingTarget = "http://vllm.test"
		deployment := readyDeployment(servedModel)
		service := serviceObject(servedModel)
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), deployment, service)
		runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(runtimeConfigWithModels("ranker-v1"), client)
		Expect(err).NotTo(HaveOccurred())

		state, err := runtimeAdapter.EnsureServedModel(context.Background(), servedModel)

		Expect(err).NotTo(HaveOccurred())
		Expect(state.Ready).To(BeTrue())
		Expect(state.ReadyReplicas).To(Equal(int32(1)))
		Expect(state.ServingProtocol).To(Equal(model.ServingProtocolOpenAIChatCompletions))
	})

	It("does not report loaded when vLLM does not list the adapter model", func() {
		servedModel := validServedModel()
		servedModel.ServingTarget = "http://vllm.test"
		deployment := readyDeployment(servedModel)
		service := serviceObject(servedModel)
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), deployment, service)
		runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(runtimeConfigWithModels("base-model"), client)
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
		runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(runtimeConfig(), client)
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
		runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(runtimeConfigWithModels("ranker-v1"), client)
		Expect(err).NotTo(HaveOccurred())

		_, err = runtimeAdapter.EnsureServedModel(context.Background(), servedModel)

		Expect(err).NotTo(HaveOccurred())
		updated, err := client.Resource(serviceGVR()).Namespace("default").Get(context.Background(), servingkubernetes.WorkloadName(servedModel), metav1.GetOptions{})
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

	It("serves a base model without LoRA arguments when an adapter uri is missing", func() {
		servedModel := validServedModel()
		servedModel.AdapterURI = ""
		servedModel.ServingTarget = "http://vllm.test"
		deployment := readyDeployment(servedModel)
		service := serviceObject(servedModel)
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), deployment, service)
		runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(runtimeConfigWithModels("ranker-v1"), client)
		Expect(err).NotTo(HaveOccurred())

		state, err := runtimeAdapter.EnsureServedModel(context.Background(), servedModel)

		Expect(err).NotTo(HaveOccurred())
		Expect(state.Ready).To(BeTrue())
		Expect(state.ServingModel).To(Equal("ranker-v1"))
		updated, err := client.Resource(deploymentGVR()).Namespace("default").Get(context.Background(), servingkubernetes.WorkloadName(servedModel), metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		containers, _, _ := unstructured.NestedSlice(updated.Object, "spec", "template", "spec", "containers")
		container := containers[0].(map[string]any)
		Expect(container["args"]).NotTo(ContainElement("--enable-lora"))
		Expect(container["args"]).NotTo(ContainElement("--lora-modules"))
	})
})

var _ = Describe("ServedModelController", func() {
	It("reconciles every listed ServedModel", func() {
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), servedModelCR(validServedModel()))
		store, err := servingkubernetes.NewServedModelStore(storeConfig(), client)
		Expect(err).NotTo(HaveOccurred())
		runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(runtimeConfig(), client)
		Expect(err).NotTo(HaveOccurred())
		reconciler := app.NewServedModelReconciler(runtimeAdapter, store)
		controller := servingkubernetes.NewServedModelController(store, reconciler, time.Millisecond)

		Expect(controller.ProcessOnce(context.Background())).To(Succeed())

		obj, err := client.Resource(servedModelGVR()).Namespace("default").Get(context.Background(), validServedModel().ResourceName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		statusRaw, _, _ := unstructured.NestedString(obj.Object, "status", "servingLoadStatus")
		Expect(statusRaw).To(Equal("NOT_LOADED"))
	})

	It("reconciles ServedModel watch events", func() {
		servedModel := validServedModel()
		obj := servedModelCR(servedModel)
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), obj)
		store, err := servingkubernetes.NewServedModelStore(storeConfig(), client)
		Expect(err).NotTo(HaveOccurred())
		runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(runtimeConfig(), client)
		Expect(err).NotTo(HaveOccurred())
		reconciler := app.NewServedModelReconciler(runtimeAdapter, store)
		controller := servingkubernetes.NewServedModelController(store, reconciler, time.Millisecond)

		Expect(controller.ProcessWatchEvent(context.Background(), watch.Event{
			Type:   watch.Modified,
			Object: obj,
		})).To(Succeed())

		updated, err := client.Resource(servedModelGVR()).Namespace("default").Get(context.Background(), servedModel.ResourceName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		statusRaw, _, _ := unstructured.NestedString(updated.Object, "status", "servingLoadStatus")
		Expect(statusRaw).To(Equal("NOT_LOADED"))
	})

	It("does not requeue a served model that has already been deleted from the store", func() {
		servedModel := validServedModel()
		obj := servedModelCR(servedModel)
		client := fake.NewSimpleDynamicClient(runtime.NewScheme())
		store, err := servingkubernetes.NewServedModelStore(storeConfig(), client)
		Expect(err).NotTo(HaveOccurred())
		runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(runtimeConfig(), client)
		Expect(err).NotTo(HaveOccurred())
		reconciler := app.NewServedModelReconciler(runtimeAdapter, store)
		controller := servingkubernetes.NewServedModelController(store, reconciler, time.Millisecond)

		Expect(controller.ProcessWatchEvent(context.Background(), watch.Event{
			Type:   watch.Modified,
			Object: obj,
		})).To(Succeed())

		health := controller.Health()
		Expect(health.KnownServedModels).To(Equal(0))
		Expect(health.OutstandingServedModels).To(Equal(0))
		Expect(health.LastError).To(BeEmpty())
	})

})

func storeConfig() servingkubernetes.ServedModelStoreConfig {
	return servingkubernetes.ServedModelStoreConfig{
		Namespace: "default",
		Group:     "serving.bighill.io",
		Version:   "v1alpha1",
		Resource:  "servedmodels",
	}
}

func runtimeConfig() servingkubernetes.VLLMRuntimeConfig {
	return servingkubernetes.VLLMRuntimeConfig{
		Namespace:       "default",
		Image:           "vllm/vllm-openai:v-test",
		ImagePullPolicy: "IfNotPresent",
		Replicas:        1,
		Port:            8000,
		CPU:             "1",
		Memory:          "4Gi",
		GPUResource:     "nvidia.com/gpu",
		GPU:             "1",
		RequestTimeout:  time.Second,
	}
}

func runtimeConfigWithModels(modelIDs ...string) servingkubernetes.VLLMRuntimeConfig {
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

func jsonResponse(req *http.Request, body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Request:    req,
	}
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
		ModelKind:     "FINE_TUNED",
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
			"modelKind":     servedModel.ModelKind,
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
	return readyDeploymentWithName(servingkubernetes.WorkloadName(servedModel))
}

func readyDeploymentWithName(name string) *unstructured.Unstructured {
	deployment := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name":      name,
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
			"name":      servingkubernetes.WorkloadName(servedModel),
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
	return serviceObjectWithName(servingkubernetes.WorkloadName(servedModel))
}

func serviceObjectWithName(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata": map[string]any{
			"name":      name,
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
