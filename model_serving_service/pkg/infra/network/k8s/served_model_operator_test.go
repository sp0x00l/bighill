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
	"k8s.io/client-go/dynamic"
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
		Expect(servedModels[0].AdapterRank).To(Equal(16))
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

var _ = Describe("BaseRuntimeStore", func() {
	It("creates and reuses BaseRuntime resources by base model and pool key", func() {
		client := fake.NewSimpleDynamicClient(runtime.NewScheme())
		store, err := servingkubernetes.NewBaseRuntimeStore(baseRuntimeStoreConfig(), client)
		Expect(err).NotTo(HaveOccurred())
		spec := &model.BaseRuntime{
			BaseModel:   "mistral-7b",
			PoolKey:     "mistral-7b",
			MaxLoras:    8,
			MaxLoraRank: 16,
			GPU:         "1",
			Image:       "vllm/vllm-openai:v-test",
		}

		first, err := store.FindOrCreate(context.Background(), spec)
		Expect(err).NotTo(HaveOccurred())
		second, err := store.FindOrCreate(context.Background(), spec)
		Expect(err).NotTo(HaveOccurred())

		Expect(first.ResourceName).To(Equal(servingkubernetes.BaseRuntimeResourceName("mistral-7b", "mistral-7b")))
		Expect(second.ResourceName).To(Equal(first.ResourceName))
		obj, err := client.Resource(baseRuntimeGVR()).Namespace("default").Get(context.Background(), first.ResourceName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.GetName()).To(Equal(first.ResourceName))
		maxLoras, _, _ := unstructured.NestedInt64(obj.Object, "spec", "maxLoras")
		maxRank, _, _ := unstructured.NestedInt64(obj.Object, "spec", "maxLoraRank")
		Expect(maxLoras).To(Equal(int64(8)))
		Expect(maxRank).To(Equal(int64(16)))
	})

	It("writes BaseRuntime status", func() {
		client := fake.NewSimpleDynamicClient(runtime.NewScheme())
		store, err := servingkubernetes.NewBaseRuntimeStore(baseRuntimeStoreConfig(), client)
		Expect(err).NotTo(HaveOccurred())
		baseRuntime, err := store.FindOrCreate(context.Background(), &model.BaseRuntime{
			BaseModel:   "mistral-7b",
			PoolKey:     "mistral-7b",
			MaxLoras:    8,
			MaxLoraRank: 16,
			GPU:         "1",
			Image:       "vllm/vllm-openai:v-test",
		})
		Expect(err).NotTo(HaveOccurred())

		Expect(store.UpdateStatus(context.Background(), baseRuntime.ResourceName, "http://runtime.default.svc.cluster.local:8000", "Ready", 1)).To(Succeed())

		obj, err := client.Resource(baseRuntimeGVR()).Namespace("default").Get(context.Background(), baseRuntime.ResourceName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		endpoint, _, _ := unstructured.NestedString(obj.Object, "status", "endpoint")
		phase, _, _ := unstructured.NestedString(obj.Object, "status", "phase")
		readyReplicas, _, _ := unstructured.NestedInt64(obj.Object, "status", "readyReplicas")
		Expect(endpoint).To(Equal("http://runtime.default.svc.cluster.local:8000"))
		Expect(phase).To(Equal("Ready"))
		Expect(readyReplicas).To(Equal(int64(1)))
	})

	It("records and removes loaded adapters in BaseRuntime status", func() {
		now := time.Date(2026, 7, 11, 14, 0, 0, 0, time.UTC)
		client := fake.NewSimpleDynamicClient(runtime.NewScheme())
		store, err := servingkubernetes.NewBaseRuntimeStore(baseRuntimeStoreConfig(), client)
		Expect(err).NotTo(HaveOccurred())
		baseRuntime, err := store.FindOrCreate(context.Background(), &model.BaseRuntime{
			BaseModel:   "mistral-7b",
			PoolKey:     "mistral-7b",
			MaxLoras:    8,
			MaxLoraRank: 16,
			GPU:         "1",
			Image:       "vllm/vllm-openai:v-test",
		})
		Expect(err).NotTo(HaveOccurred())
		adapter := model.BaseRuntimeLoadedAdapter{
			ServingModel:            "ranker-v1",
			ServedModelResourceName: "served-model-ranker",
			ModelID:                 uuid.MustParse("4f4b8258-f9af-49f8-b5a8-f84d75891f3b"),
			ObservedGeneration:      7,
			LastUsedAt:              now,
			Pinned:                  true,
		}

		Expect(store.RecordAdapterLoaded(context.Background(), baseRuntime.ResourceName, adapter)).To(Succeed())
		read, err := store.Read(context.Background(), baseRuntime.ResourceName)
		Expect(err).NotTo(HaveOccurred())
		Expect(read.LoadedAdapters).To(HaveLen(1))
		Expect(read.LoadedAdapters[0]).To(Equal(adapter))

		Expect(store.RemoveLoadedAdapter(context.Background(), baseRuntime.ResourceName, "ranker-v1")).To(Succeed())
		read, err = store.Read(context.Background(), baseRuntime.ResourceName)
		Expect(err).NotTo(HaveOccurred())
		Expect(read.LoadedAdapters).To(BeEmpty())
	})

	It("updates mutable BaseRuntime spec without changing existing capacity", func() {
		resourceName := servingkubernetes.BaseRuntimeResourceName("mistral-7b", "mistral-7b")
		existing := baseRuntimeCR(resourceName, 1)
		Expect(unstructured.SetNestedField(existing.Object, int64(4), "spec", "maxLoras")).To(Succeed())
		Expect(unstructured.SetNestedField(existing.Object, int64(8), "spec", "maxLoraRank")).To(Succeed())
		Expect(unstructured.SetNestedField(existing.Object, "old-gpu", "spec", "gpu")).To(Succeed())
		Expect(unstructured.SetNestedField(existing.Object, "vllm/vllm-openai:old", "spec", "image")).To(Succeed())
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), existing)
		store, err := servingkubernetes.NewBaseRuntimeStore(baseRuntimeStoreConfig(), client)
		Expect(err).NotTo(HaveOccurred())

		updated, err := store.FindOrCreate(context.Background(), &model.BaseRuntime{
			BaseModel:   "mistral-7b",
			PoolKey:     "mistral-7b",
			MaxLoras:    8,
			MaxLoraRank: 16,
			GPU:         "1",
			Image:       "vllm/vllm-openai:v-test",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(updated.MaxLoras).To(Equal(4))
		Expect(updated.MaxLoraRank).To(Equal(8))
		Expect(updated.GPU).To(Equal("1"))
		Expect(updated.Image).To(Equal("vllm/vllm-openai:v-test"))
		obj, err := client.Resource(baseRuntimeGVR()).Namespace("default").Get(context.Background(), resourceName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		maxLoras, _, _ := unstructured.NestedInt64(obj.Object, "spec", "maxLoras")
		maxRank, _, _ := unstructured.NestedInt64(obj.Object, "spec", "maxLoraRank")
		image, _, _ := unstructured.NestedString(obj.Object, "spec", "image")
		Expect(maxLoras).To(Equal(int64(4)))
		Expect(maxRank).To(Equal(int64(8)))
		Expect(image).To(Equal("vllm/vllm-openai:v-test"))
	})
})

var _ = Describe("VLLMRuntime", func() {
	It("creates a vLLM deployment and service for a ServedModel", func() {
		servedModel := validServedModel()
		client := fake.NewSimpleDynamicClient(runtime.NewScheme())
		runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(runtimeConfig(client), client)
		Expect(err).NotTo(HaveOccurred())

		state, err := runtimeAdapter.EnsureServedModel(context.Background(), servedModel)

		Expect(err).NotTo(HaveOccurred())
		Expect(state.Ready).To(BeFalse())
		Expect(state.ServingModel).To(Equal("ranker-v1"))
		sharedWorkloadName := servingkubernetes.SharedRuntimeWorkloadName(servedModel)
		Expect(state.ServingTarget).To(Equal("http://" + sharedWorkloadName + ".default.svc.cluster.local:8000"))
		Expect(state.ServingProtocol).To(Equal(model.ServingProtocolOpenAIChatCompletions))
		deployment, err := client.Resource(deploymentGVR()).Namespace("default").Get(context.Background(), sharedWorkloadName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		containers, _, _ := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "containers")
		container := containers[0].(map[string]any)
		Expect(container["image"]).To(Equal("vllm/vllm-openai:v-test"))
		Expect(container["args"]).To(ContainElement("--enable-lora"))
		Expect(container["args"]).To(ContainElement("--max-loras"))
		Expect(container["args"]).To(ContainElement("8"))
		Expect(container["args"]).To(ContainElement("--max-lora-rank"))
		Expect(container["args"]).To(ContainElement("16"))
		Expect(container["args"]).NotTo(ContainElement("--lora-modules"))
		env, ok := container["env"].([]any)
		Expect(ok).To(BeTrue())
		Expect(env).To(ContainElement(SatisfyAll(
			HaveKeyWithValue("name", "VLLM_ALLOW_RUNTIME_LORA_UPDATING"),
			HaveKeyWithValue("value", "1"),
		)))
		baseRuntime, err := client.Resource(baseRuntimeGVR()).Namespace("default").Get(context.Background(), sharedWorkloadName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		poolKey, _, _ := unstructured.NestedString(baseRuntime.Object, "spec", "poolKey")
		maxLoras, _, _ := unstructured.NestedInt64(baseRuntime.Object, "spec", "maxLoras")
		Expect(poolKey).To(Equal(servedModel.BaseModel))
		Expect(maxLoras).To(Equal(int64(8)))
		service, err := client.Resource(serviceGVR()).Namespace("default").Get(context.Background(), sharedWorkloadName, metav1.GetOptions{})
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
			servedModel.ModelKind = "BASE"
			servedModel.AdapterURI = ""
			client := fake.NewSimpleDynamicClient(runtime.NewScheme())
			runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(runtimeConfig(client), client)
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

	It("uses a shared base runtime and dynamically loads adapters by default", func() {
		servedModel := validServedModel()
		servedModel.ServingTarget = "http://vllm.test"
		sharedWorkloadName := servingkubernetes.SharedRuntimeWorkloadName(servedModel)
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), readyDeploymentWithName(sharedWorkloadName), serviceObjectWithName(sharedWorkloadName))
		loadRequests := 0
		config := runtimeConfig(client)
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
		baseRuntime, err := client.Resource(baseRuntimeGVR()).Namespace("default").Get(context.Background(), sharedWorkloadName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		loadedAdapters, _, _ := unstructured.NestedSlice(baseRuntime.Object, "status", "loadedAdapters")
		Expect(loadedAdapters).To(HaveLen(1))
		Expect(loadedAdapters[0]).To(SatisfyAll(
			HaveKeyWithValue("servingModel", "ranker-v1"),
			HaveKeyWithValue("servedModelResourceName", servedModel.ResourceName),
			HaveKeyWithValue("modelID", servedModel.ModelID.String()),
		))
	})

	It("evicts the least recently used unpinned adapter before loading a new adapter", func() {
		servedModel := validServedModel()
		servedModel.ServingTarget = "http://vllm.test"
		sharedWorkloadName := servingkubernetes.SharedRuntimeWorkloadName(servedModel)
		victim := validServedModel()
		victim.ResourceName = "served-model-victim"
		victim.ModelID = uuid.MustParse("d84d7589-f3ba-4f4b-8258-f9af49f8b5a8")
		victim.ServingModel = "victim-v1"
		victimCR := servedModelCR(victim)
		Expect(unstructured.SetNestedField(victimCR.Object, "LOADED", "status", "servingLoadStatus")).To(Succeed())
		Expect(unstructured.SetNestedField(victimCR.Object, "http://vllm.test", "status", "servingTarget")).To(Succeed())
		Expect(unstructured.SetNestedField(victimCR.Object, "victim-v1", "status", "servingModel")).To(Succeed())
		Expect(unstructured.SetNestedField(victimCR.Object, "OPENAI_CHAT_COMPLETIONS", "status", "servingProtocol")).To(Succeed())
		baseRuntime := baseRuntimeCR(sharedWorkloadName, 3)
		Expect(unstructured.SetNestedField(baseRuntime.Object, int64(1), "spec", "maxLoras")).To(Succeed())
		Expect(setLoadedAdapters(baseRuntime, []map[string]any{{
			"servingModel":            "victim-v1",
			"servedModelResourceName": victim.ResourceName,
			"modelID":                 victim.ModelID.String(),
			"observedGeneration":      victim.Generation,
			"lastUsedAt":              "2026-07-11T12:00:00Z",
			"pinned":                  false,
		}})).To(Succeed())
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), baseRuntime, readyDeploymentWithName(sharedWorkloadName), serviceObjectWithName(sharedWorkloadName), victimCR)
		store, err := servingkubernetes.NewServedModelStore(storeConfig(), client)
		Expect(err).NotTo(HaveOccurred())
		unloaded := false
		loaded := false
		config := runtimeConfig(client)
		config.ServedModelStatusWriter = store
		config.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch {
			case req.Method == http.MethodGet && req.URL.Path == "/v1/models" && !loaded:
				return jsonResponse(req, `{"data":[{"id":"base-mistral-7b"}]}`), nil
			case req.Method == http.MethodPost && req.URL.Path == "/v1/unload_lora_adapter":
				raw, err := io.ReadAll(req.Body)
				Expect(err).NotTo(HaveOccurred())
				Expect(string(raw)).To(MatchJSON(`{"lora_name":"victim-v1"}`))
				unloaded = true
				return jsonResponse(req, `{}`), nil
			case req.Method == http.MethodPost && req.URL.Path == "/v1/load_lora_adapter":
				Expect(unloaded).To(BeTrue())
				loaded = true
				return jsonResponse(req, `{}`), nil
			case req.Method == http.MethodGet && req.URL.Path == "/v1/models" && loaded:
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
		Expect(unloaded).To(BeTrue())
		victimUpdated, err := client.Resource(servedModelGVR()).Namespace("default").Get(context.Background(), victim.ResourceName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		statusRaw, _, _ := unstructured.NestedString(victimUpdated.Object, "status", "servingLoadStatus")
		failureReason, _, _ := unstructured.NestedString(victimUpdated.Object, "status", "failureReason")
		Expect(statusRaw).To(Equal("NOT_LOADED"))
		Expect(failureReason).To(Equal("capacity_evicted"))
		updatedBaseRuntime, err := client.Resource(baseRuntimeGVR()).Namespace("default").Get(context.Background(), sharedWorkloadName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		loadedAdapters, _, _ := unstructured.NestedSlice(updatedBaseRuntime.Object, "status", "loadedAdapters")
		Expect(loadedAdapters).To(HaveLen(1))
		Expect(loadedAdapters[0]).To(HaveKeyWithValue("servingModel", "ranker-v1"))
	})

	It("fails closed when a full base runtime has only pinned adapters", func() {
		servedModel := validServedModel()
		sharedWorkloadName := servingkubernetes.SharedRuntimeWorkloadName(servedModel)
		baseRuntime := baseRuntimeCR(sharedWorkloadName, 3)
		Expect(unstructured.SetNestedField(baseRuntime.Object, int64(1), "spec", "maxLoras")).To(Succeed())
		Expect(setLoadedAdapters(baseRuntime, []map[string]any{{
			"servingModel":            "pinned-v1",
			"servedModelResourceName": "served-model-pinned",
			"modelID":                 uuid.NewString(),
			"observedGeneration":      int64(1),
			"lastUsedAt":              "2026-07-11T12:00:00Z",
			"pinned":                  true,
		}})).To(Succeed())
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), baseRuntime, readyDeploymentWithName(sharedWorkloadName), serviceObjectWithName(sharedWorkloadName))
		config := runtimeConfig(client)
		config.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodGet && req.URL.Path == "/v1/models" {
				return jsonResponse(req, `{"data":[{"id":"base-mistral-7b"}]}`), nil
			}
			Fail("vLLM load/unload must not be called when all adapters are pinned")
			return nil, nil
		})}
		runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(config, client)
		Expect(err).NotTo(HaveOccurred())

		state, err := runtimeAdapter.EnsureServedModel(context.Background(), servedModel)

		Expect(err).NotTo(HaveOccurred())
		Expect(state.Ready).To(BeFalse())
		Expect(state.Failed).To(BeTrue())
		Expect(state.FailureReason).To(ContainSubstring("base runtime at capacity"))
	})

	It("unloads an adapter when a shared ServedModel is deleted", func() {
		servedModel := validServedModel()
		servedModel.ServingTarget = "http://vllm.test"
		sharedWorkloadName := servingkubernetes.SharedRuntimeWorkloadName(servedModel)
		baseRuntime := baseRuntimeCR(sharedWorkloadName, 3)
		Expect(unstructured.SetNestedField(baseRuntime.Object, "http://vllm.test", "status", "endpoint")).To(Succeed())
		Expect(setLoadedAdapters(baseRuntime, []map[string]any{{
			"servingModel":            "ranker-v1",
			"servedModelResourceName": servedModel.ResourceName,
			"modelID":                 servedModel.ModelID.String(),
			"observedGeneration":      servedModel.Generation,
			"lastUsedAt":              "2026-07-11T12:00:00Z",
			"pinned":                  false,
		}})).To(Succeed())
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), baseRuntime)
		unloaded := false
		config := runtimeConfig(client)
		config.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			Expect(req.Method).To(Equal(http.MethodPost))
			Expect(req.URL.Path).To(Equal("/v1/unload_lora_adapter"))
			unloaded = true
			return jsonResponse(req, `{}`), nil
		})}
		runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(config, client)
		Expect(err).NotTo(HaveOccurred())

		Expect(runtimeAdapter.DeleteServedModel(context.Background(), servedModel)).To(Succeed())

		Expect(unloaded).To(BeTrue())
		updatedBaseRuntime, err := client.Resource(baseRuntimeGVR()).Namespace("default").Get(context.Background(), sharedWorkloadName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		loadedAdapters, _, _ := unstructured.NestedSlice(updatedBaseRuntime.Object, "status", "loadedAdapters")
		Expect(loadedAdapters).To(BeEmpty())
	})

	It("uses an org-scoped pool key for dedicated runtime isolation", func() {
		shared := validServedModel()
		dedicated := validServedModel()
		dedicated.RuntimeIsolation = model.RuntimeIsolationDedicated

		Expect(servingkubernetes.RuntimePoolKey(shared)).To(Equal(shared.BaseModel))
		Expect(servingkubernetes.RuntimePoolKey(dedicated)).To(Equal(dedicated.OrgID.String()))
		Expect(servingkubernetes.SharedRuntimeWorkloadName(dedicated)).NotTo(Equal(servingkubernetes.SharedRuntimeWorkloadName(shared)))
	})

	It("does not mark an adapter failed when vLLM reports it is already loaded", func() {
		servedModel := validServedModel()
		servedModel.ServingTarget = "http://vllm.test"
		sharedWorkloadName := servingkubernetes.SharedRuntimeWorkloadName(servedModel)
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), readyDeploymentWithName(sharedWorkloadName), serviceObjectWithName(sharedWorkloadName))
		loadRequests := 0
		config := runtimeConfig(client)
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

	It("shares one workload for adapter versions of the same base model by default", func() {
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
		sharedWorkloadName := servingkubernetes.SharedRuntimeWorkloadName(servedModel)
		deployment := readyDeploymentWithName(sharedWorkloadName)
		service := serviceObjectWithName(sharedWorkloadName)
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), deployment, service)
		runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(runtimeConfigWithModels(client, "ranker-v1"), client)
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
		sharedWorkloadName := servingkubernetes.SharedRuntimeWorkloadName(servedModel)
		deployment := readyDeploymentWithName(sharedWorkloadName)
		service := serviceObjectWithName(sharedWorkloadName)
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), deployment, service)
		loadRequests := 0
		config := runtimeConfig(client)
		config.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch {
			case req.Method == http.MethodGet && req.URL.Path == "/v1/models":
				return jsonResponse(req, `{"data":[{"id":"base-model"}]}`), nil
			case req.Method == http.MethodPost && req.URL.Path == "/v1/load_lora_adapter":
				loadRequests++
				return jsonResponse(req, `{}`), nil
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

	It("reports failed when the vLLM deployment exceeds its progress deadline", func() {
		servedModel := validServedModel()
		sharedWorkloadName := servingkubernetes.SharedRuntimeWorkloadName(servedModel)
		deployment := failedDeploymentWithName(sharedWorkloadName)
		service := serviceObjectWithName(sharedWorkloadName)
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), deployment, service)
		runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(runtimeConfig(client), client)
		Expect(err).NotTo(HaveOccurred())

		state, err := runtimeAdapter.EnsureServedModel(context.Background(), servedModel)

		Expect(err).NotTo(HaveOccurred())
		Expect(state.Ready).To(BeFalse())
		Expect(state.Failed).To(BeTrue())
		Expect(state.FailureReason).To(ContainSubstring("progress deadline"))
	})

	It("preserves allocated Service cluster IP fields across updates", func() {
		servedModel := validServedModel()
		sharedWorkloadName := servingkubernetes.SharedRuntimeWorkloadName(servedModel)
		deployment := readyDeploymentWithName(sharedWorkloadName)
		service := serviceObjectWithName(sharedWorkloadName)
		Expect(unstructured.SetNestedField(service.Object, "10.0.0.10", "spec", "clusterIP")).To(Succeed())
		Expect(unstructured.SetNestedStringSlice(service.Object, []string{"10.0.0.10"}, "spec", "clusterIPs")).To(Succeed())
		Expect(unstructured.SetNestedStringSlice(service.Object, []string{"IPv4"}, "spec", "ipFamilies")).To(Succeed())
		Expect(unstructured.SetNestedField(service.Object, "SingleStack", "spec", "ipFamilyPolicy")).To(Succeed())
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), deployment, service)
		runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(runtimeConfigWithModels(client, "ranker-v1"), client)
		Expect(err).NotTo(HaveOccurred())

		_, err = runtimeAdapter.EnsureServedModel(context.Background(), servedModel)

		Expect(err).NotTo(HaveOccurred())
		updated, err := client.Resource(serviceGVR()).Namespace("default").Get(context.Background(), sharedWorkloadName, metav1.GetOptions{})
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

	It("serves a base model without LoRA arguments", func() {
		servedModel := validServedModel()
		servedModel.ModelKind = "BASE"
		servedModel.AdapterURI = ""
		servedModel.ServingTarget = "http://vllm.test"
		deployment := readyDeployment(servedModel)
		service := serviceObject(servedModel)
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), deployment, service)
		runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(runtimeConfigWithModels(client, "ranker-v1"), client)
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

	It("fails closed when a fine-tuned model has no adapter uri", func() {
		servedModel := validServedModel()
		servedModel.AdapterURI = ""
		client := fake.NewSimpleDynamicClient(runtime.NewScheme())
		runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(runtimeConfig(client), client)
		Expect(err).NotTo(HaveOccurred())

		state, err := runtimeAdapter.EnsureServedModel(context.Background(), servedModel)

		Expect(err).NotTo(HaveOccurred())
		Expect(state.Ready).To(BeFalse())
		Expect(state.Failed).To(BeTrue())
		Expect(state.FailureReason).To(ContainSubstring("fine-tuned model has no adapter URI"))
	})

	It("fails closed when adapter rank is unknown", func() {
		servedModel := validServedModel()
		servedModel.AdapterRank = 0
		client := fake.NewSimpleDynamicClient(runtime.NewScheme())
		config := runtimeConfigWithModels(client, "base-mistral-7b")
		loadRequests := 0
		config.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodPost && req.URL.Path == "/v1/load_lora_adapter" {
				loadRequests++
			}
			return jsonResponse(req, `{"data":[{"id":"base-mistral-7b"}]}`), nil
		})}
		runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(config, client)
		Expect(err).NotTo(HaveOccurred())

		state, err := runtimeAdapter.EnsureServedModel(context.Background(), servedModel)

		Expect(err).NotTo(HaveOccurred())
		Expect(state.Ready).To(BeFalse())
		Expect(state.Failed).To(BeTrue())
		Expect(state.FailureReason).To(ContainSubstring("unknown adapter rank"))
		Expect(loadRequests).To(Equal(0))
		_, err = client.Resource(deploymentGVR()).Namespace("default").Get(context.Background(), servingkubernetes.SharedRuntimeWorkloadName(servedModel), metav1.GetOptions{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
	})

	It("fails closed when adapter rank exceeds the configured max", func() {
		servedModel := validServedModel()
		servedModel.AdapterRank = 32
		client := fake.NewSimpleDynamicClient(runtime.NewScheme())
		config := runtimeConfigWithModels(client, "base-mistral-7b")
		loadRequests := 0
		config.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodPost && req.URL.Path == "/v1/load_lora_adapter" {
				loadRequests++
			}
			return jsonResponse(req, `{"data":[{"id":"base-mistral-7b"}]}`), nil
		})}
		runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(config, client)
		Expect(err).NotTo(HaveOccurred())

		state, err := runtimeAdapter.EnsureServedModel(context.Background(), servedModel)

		Expect(err).NotTo(HaveOccurred())
		Expect(state.Ready).To(BeFalse())
		Expect(state.Failed).To(BeTrue())
		Expect(state.FailureReason).To(ContainSubstring("adapter rank 32 exceeds max lora rank 16"))
		Expect(loadRequests).To(Equal(0))
		_, err = client.Resource(deploymentGVR()).Namespace("default").Get(context.Background(), servingkubernetes.SharedRuntimeWorkloadName(servedModel), metav1.GetOptions{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
	})

	It("fails closed when adapter rank exceeds an existing base runtime limit", func() {
		servedModel := validServedModel()
		servedModel.AdapterRank = 12
		sharedWorkloadName := servingkubernetes.SharedRuntimeWorkloadName(servedModel)
		baseRuntime := baseRuntimeCR(sharedWorkloadName, 3)
		Expect(unstructured.SetNestedField(baseRuntime.Object, int64(8), "spec", "maxLoraRank")).To(Succeed())
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), baseRuntime)
		config := runtimeConfigWithModels(client, "base-mistral-7b")
		config.MaxLoraRank = 16
		loadRequests := 0
		config.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodPost && req.URL.Path == "/v1/load_lora_adapter" {
				loadRequests++
			}
			return jsonResponse(req, `{"data":[{"id":"base-mistral-7b"}]}`), nil
		})}
		runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(config, client)
		Expect(err).NotTo(HaveOccurred())

		state, err := runtimeAdapter.EnsureServedModel(context.Background(), servedModel)

		Expect(err).NotTo(HaveOccurred())
		Expect(state.Ready).To(BeFalse())
		Expect(state.Failed).To(BeTrue())
		Expect(state.FailureReason).To(ContainSubstring("adapter rank 12 exceeds max lora rank 8"))
		Expect(loadRequests).To(Equal(0))
		_, err = client.Resource(deploymentGVR()).Namespace("default").Get(context.Background(), sharedWorkloadName, metav1.GetOptions{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
		updatedBaseRuntime, err := client.Resource(baseRuntimeGVR()).Namespace("default").Get(context.Background(), sharedWorkloadName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		maxRank, _, _ := unstructured.NestedInt64(updatedBaseRuntime.Object, "spec", "maxLoraRank")
		Expect(maxRank).To(Equal(int64(8)))
	})
})

var _ = Describe("ServedModelController", func() {
	It("reconciles every listed ServedModel", func() {
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), servedModelCR(validServedModel()))
		store, err := servingkubernetes.NewServedModelStore(storeConfig(), client)
		Expect(err).NotTo(HaveOccurred())
		runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(runtimeConfig(client), client)
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
		runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(runtimeConfig(client), client)
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

	It("requeues served models when BaseRuntime spec generation changes", func() {
		servedModel := validServedModel()
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), servedModelCR(servedModel))
		store, err := servingkubernetes.NewServedModelStore(storeConfig(), client)
		Expect(err).NotTo(HaveOccurred())
		reconciler := &countingReconciler{}
		controller := servingkubernetes.NewServedModelController(store, reconciler, time.Millisecond)
		baseRuntime := baseRuntimeCR(servingkubernetes.SharedRuntimeWorkloadName(servedModel), 7)

		Expect(controller.ProcessBaseRuntimeWatchEvent(context.Background(), watch.Event{
			Type:   watch.Modified,
			Object: baseRuntime,
		})).To(Succeed())
		Expect(controller.ProcessBaseRuntimeWatchEvent(context.Background(), watch.Event{
			Type:   watch.Modified,
			Object: baseRuntime,
		})).To(Succeed())

		Expect(reconciler.calls).To(Equal(1))
	})

	It("does not requeue a served model that has already been deleted from the store", func() {
		servedModel := validServedModel()
		obj := servedModelCR(servedModel)
		client := fake.NewSimpleDynamicClient(runtime.NewScheme())
		store, err := servingkubernetes.NewServedModelStore(storeConfig(), client)
		Expect(err).NotTo(HaveOccurred())
		runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(runtimeConfig(client), client)
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

type countingReconciler struct {
	calls int
}

func (r *countingReconciler) Reconcile(_ context.Context, servedModel *model.ServedModel) (*model.ServedModelStatus, error) {
	r.calls++
	return &model.ServedModelStatus{
		ServingLoadStatus:  model.ModelLoadStatusNotLoaded,
		ServingTarget:      servedModel.ServingTarget,
		ServingModel:       servedModel.ServingModel,
		ServingProtocol:    servedModel.ServingProtocol,
		ObservedGeneration: servedModel.Generation,
	}, nil
}

func (r *countingReconciler) Delete(context.Context, *model.ServedModel) error {
	return nil
}

func storeConfig() servingkubernetes.ServedModelStoreConfig {
	return servingkubernetes.ServedModelStoreConfig{
		Namespace: "default",
		Group:     "serving.bighill.io",
		Version:   "v1alpha1",
		Resource:  "servedmodels",
	}
}

func baseRuntimeStoreConfig() servingkubernetes.BaseRuntimeStoreConfig {
	return servingkubernetes.BaseRuntimeStoreConfig{
		Namespace: "default",
		Group:     "serving.bighill.io",
		Version:   "v1alpha1",
		Resource:  "baseruntimes",
	}
}

func runtimeConfig(client dynamic.Interface) servingkubernetes.VLLMRuntimeConfig {
	baseRuntimeStore, err := servingkubernetes.NewBaseRuntimeStore(baseRuntimeStoreConfig(), client)
	Expect(err).NotTo(HaveOccurred())
	return servingkubernetes.VLLMRuntimeConfig{
		Namespace:        "default",
		Image:            "vllm/vllm-openai:v-test",
		ImagePullPolicy:  "IfNotPresent",
		Replicas:         1,
		Port:             8000,
		CPU:              "1",
		Memory:           "4Gi",
		GPUResource:      "nvidia.com/gpu",
		GPU:              "1",
		MaxLoras:         8,
		MaxLoraRank:      16,
		RequestTimeout:   time.Second,
		BaseRuntimeStore: baseRuntimeStore,
	}
}

func runtimeConfigWithModels(client dynamic.Interface, modelIDs ...string) servingkubernetes.VLLMRuntimeConfig {
	config := runtimeConfig(client)
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
		OrgID:         uuid.MustParse("f7e1f9d9-0777-4a6e-801a-a432901f2522"),
		TrainingRunID: uuid.MustParse("76b4da89-7fdb-459a-a842-9f866152efad"),
		DatasetID:     uuid.MustParse("6629d88a-05af-411e-8439-7497620e41df"),
		ModelKind:     "FINE_TUNED",
		Name:          "ranker",
		ModelVersion:  1,
		BaseModel:     "mistral-7b",
		AdapterURI:    "s3://models/run-1",
		AdapterRank:   16,
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
			"modelID":          servedModel.ModelID.String(),
			"orgID":            servedModel.OrgID.String(),
			"trainingRunID":    servedModel.TrainingRunID.String(),
			"datasetID":        servedModel.DatasetID.String(),
			"modelKind":        servedModel.ModelKind,
			"name":             servedModel.Name,
			"modelVersion":     int64(servedModel.ModelVersion),
			"baseModel":        servedModel.BaseModel,
			"adapterURI":       servedModel.AdapterURI,
			"adapterRank":      int64(servedModel.AdapterRank),
			"runtimeIsolation": servedModel.RuntimeIsolation,
			"pinned":           servedModel.Pinned,
			"servingModel":     servedModel.ServingModel,
		},
	}}
	obj.SetGeneration(servedModel.Generation)
	return obj
}

func baseRuntimeCR(resourceName string, generation int64) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "serving.bighill.io/v1alpha1",
		"kind":       "BaseRuntime",
		"metadata": map[string]any{
			"name":      resourceName,
			"namespace": "default",
		},
		"spec": map[string]any{
			"baseModel":   "mistral-7b",
			"poolKey":     "mistral-7b",
			"maxLoras":    int64(8),
			"maxLoraRank": int64(16),
		},
	}}
	obj.SetGeneration(generation)
	return obj
}

func setLoadedAdapters(obj *unstructured.Unstructured, adapters []map[string]any) error {
	raw := make([]any, 0, len(adapters))
	for _, adapter := range adapters {
		raw = append(raw, adapter)
	}
	return unstructured.SetNestedSlice(obj.Object, raw, "status", "loadedAdapters")
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
	return failedDeploymentWithName(servingkubernetes.WorkloadName(servedModel))
}

func failedDeploymentWithName(name string) *unstructured.Unstructured {
	deployment := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name":      name,
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

func baseRuntimeGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "serving.bighill.io", Version: "v1alpha1", Resource: "baseruntimes"}
}

func deploymentGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
}

func serviceGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "services"}
}
