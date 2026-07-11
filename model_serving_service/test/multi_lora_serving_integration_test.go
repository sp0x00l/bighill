package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"model_serving_service/pkg/domain/model"
	servingkubernetes "model_serving_service/pkg/infra/network/k8s"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
)

var _ = Describe("Multi-LoRA serving integration", func() {
	It("loads a fine-tuned adapter on a shared vLLM base runtime", func() {
		servedModel := integrationServedModel(16)
		workloadName := servingkubernetes.SharedRuntimeWorkloadName(servedModel)
		loaded := false
		loadRequests := 0
		httpClient := &http.Client{Transport: integrationRoundTripFunc(func(r *http.Request) (*http.Response, error) {
			switch {
			case r.Method == http.MethodGet && r.URL.Path == "/v1/models":
				models := []map[string]string{{"id": servingkubernetes.SharedRuntimeServingModelName(servedModel)}}
				if loaded {
					models = append(models, map[string]string{"id": servingkubernetes.ServingModelName(servedModel)})
				}
				return integrationJSONResponse(r, map[string]any{"data": models}), nil
			case r.Method == http.MethodPost && r.URL.Path == "/v1/load_lora_adapter":
				defer r.Body.Close()
				raw, err := io.ReadAll(r.Body)
				Expect(err).NotTo(HaveOccurred())
				Expect(string(raw)).To(MatchJSON(`{"lora_name":"ranker-v1","lora_path":"s3://models/run-1"}`))
				loadRequests++
				loaded = true
				return integrationJSONResponse(r, map[string]any{}), nil
			default:
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(strings.NewReader(`{}`)),
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Request:    r,
				}, nil
			}
		})}
		servedModel.ServingTarget = "http://vllm.test"
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), integrationReadyDeployment(workloadName), integrationService(workloadName))
		config := integrationRuntimeConfig(client)
		config.HTTPClient = httpClient
		runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(config, client)
		Expect(err).NotTo(HaveOccurred())

		state, err := runtimeAdapter.EnsureServedModel(context.Background(), servedModel)

		Expect(err).NotTo(HaveOccurred())
		Expect(state.Ready).To(BeTrue())
		Expect(state.Failed).To(BeFalse())
		Expect(state.ServingModel).To(Equal("ranker-v1"))
		Expect(state.ServingTarget).To(Equal("http://vllm.test"))
		Expect(loadRequests).To(Equal(1))
		deployment, err := client.Resource(integrationDeploymentGVR()).Namespace("default").Get(context.Background(), workloadName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		containers, _, _ := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "containers")
		container := containers[0].(map[string]any)
		Expect(container["args"]).To(ContainElement("--enable-lora"))
		Expect(container["args"]).To(ContainElement("--max-loras"))
		Expect(container["args"]).To(ContainElement("8"))
		Expect(container["args"]).To(ContainElement("--max-lora-rank"))
		Expect(container["args"]).To(ContainElement("16"))
		Expect(container["args"]).NotTo(ContainElement("--lora-modules"))
		Expect(container["env"]).To(ContainElement(SatisfyAll(
			HaveKeyWithValue("name", "VLLM_ALLOW_RUNTIME_LORA_UPDATING"),
			HaveKeyWithValue("value", "1"),
		)))
		baseRuntime, err := client.Resource(integrationBaseRuntimeGVR()).Namespace("default").Get(context.Background(), workloadName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		endpoint, _, _ := unstructured.NestedString(baseRuntime.Object, "status", "endpoint")
		phase, _, _ := unstructured.NestedString(baseRuntime.Object, "status", "phase")
		Expect(endpoint).To(Equal("http://" + workloadName + ".default.svc.cluster.local:8000"))
		Expect(phase).To(Equal("Ready"))
	})

	It("fails closed when the adapter rank exceeds the existing base runtime limit", func() {
		servedModel := integrationServedModel(12)
		workloadName := servingkubernetes.SharedRuntimeWorkloadName(servedModel)
		baseRuntime := integrationBaseRuntime(workloadName, 8)
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), baseRuntime)
		config := integrationRuntimeConfig(client)
		config.MaxLoraRank = 16
		config.HTTPClient = &http.Client{Transport: integrationRoundTripFunc(func(_ *http.Request) (*http.Response, error) {
			Fail("vLLM HTTP client must not be called when adapter compatibility fails")
			return nil, nil
		})}
		runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(config, client)
		Expect(err).NotTo(HaveOccurred())

		state, err := runtimeAdapter.EnsureServedModel(context.Background(), servedModel)

		Expect(err).NotTo(HaveOccurred())
		Expect(state.Ready).To(BeFalse())
		Expect(state.Failed).To(BeTrue())
		Expect(state.FailureReason).To(ContainSubstring("adapter rank 12 exceeds max lora rank 8"))
		_, err = client.Resource(integrationDeploymentGVR()).Namespace("default").Get(context.Background(), workloadName, metav1.GetOptions{})
		Expect(err).To(HaveOccurred())
		updatedBaseRuntime, err := client.Resource(integrationBaseRuntimeGVR()).Namespace("default").Get(context.Background(), workloadName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		maxRank, _, _ := unstructured.NestedInt64(updatedBaseRuntime.Object, "spec", "maxLoraRank")
		Expect(maxRank).To(Equal(int64(8)))
	})

	It("packs multiple tenant adapters onto one shared base runtime", func() {
		first := integrationServedModelVariant("ranker-a", 1, uuid.New(), uuid.New(), model.RuntimeIsolationShared)
		second := integrationServedModelVariant("ranker-b", 1, uuid.New(), uuid.New(), model.RuntimeIsolationShared)
		second.BaseModel = first.BaseModel
		workloadName := servingkubernetes.SharedRuntimeWorkloadName(first)
		loaded := map[string]bool{servingkubernetes.SharedRuntimeServingModelName(first): true}
		loadRequests := 0
		httpClient := integrationVLLMClient(loaded, &loadRequests, nil)
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), integrationReadyDeployment(workloadName), integrationService(workloadName))
		config := integrationRuntimeConfig(client)
		config.HTTPClient = httpClient
		runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(config, client)
		Expect(err).NotTo(HaveOccurred())

		firstState, err := runtimeAdapter.EnsureServedModel(context.Background(), first)
		Expect(err).NotTo(HaveOccurred())
		secondState, err := runtimeAdapter.EnsureServedModel(context.Background(), second)
		Expect(err).NotTo(HaveOccurred())

		Expect(firstState.Ready).To(BeTrue())
		Expect(secondState.Ready).To(BeTrue())
		Expect(firstState.ServingTarget).To(Equal(secondState.ServingTarget))
		Expect(firstState.ServingModel).NotTo(Equal(secondState.ServingModel))
		Expect(loadRequests).To(Equal(2))
		baseRuntime, err := client.Resource(integrationBaseRuntimeGVR()).Namespace("default").Get(context.Background(), workloadName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		adapters, _, _ := unstructured.NestedSlice(baseRuntime.Object, "status", "loadedAdapters")
		Expect(adapters).To(ConsistOf(
			HaveKeyWithValue("servingModel", servingkubernetes.ServingModelName(first)),
			HaveKeyWithValue("servingModel", servingkubernetes.ServingModelName(second)),
		))
		deployments, err := client.Resource(integrationDeploymentGVR()).Namespace("default").List(context.Background(), metav1.ListOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(deployments.Items).To(HaveLen(1))
	})

	It("evicts the LRU adapter and reloads it on demand", func() {
		oldTime := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		victim := integrationServedModelVariant("ranker-lru", 1, uuid.New(), uuid.New(), model.RuntimeIsolationShared)
		newcomer := integrationServedModelVariant("ranker-new", 1, uuid.New(), uuid.New(), model.RuntimeIsolationShared)
		newcomer.BaseModel = victim.BaseModel
		workloadName := servingkubernetes.SharedRuntimeWorkloadName(victim)
		baseRuntime := integrationBaseRuntime(workloadName, 16)
		Expect(unstructured.SetNestedField(baseRuntime.Object, int64(1), "spec", "maxLoras")).To(Succeed())
		Expect(unstructured.SetNestedSlice(baseRuntime.Object, []any{map[string]any{
			"servingModel":            servingkubernetes.ServingModelName(victim),
			"servedModelResourceName": victim.ResourceName,
			"modelID":                 victim.ModelID.String(),
			"observedGeneration":      int64(victim.Generation),
			"lastUsedAt":              oldTime.Format(time.RFC3339Nano),
			"pinned":                  false,
		}}, "status", "loadedAdapters")).To(Succeed())
		loaded := map[string]bool{
			servingkubernetes.SharedRuntimeServingModelName(victim): true,
			servingkubernetes.ServingModelName(victim):              true,
		}
		loadRequests := 0
		unloadRequests := 0
		httpClient := integrationVLLMClient(loaded, &loadRequests, &unloadRequests)
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(),
			baseRuntime,
			integrationReadyDeployment(workloadName),
			integrationService(workloadName),
			integrationServedModelObject(victim, model.ModelLoadStatusLoaded, ""),
			integrationServedModelObject(newcomer, model.ModelLoadStatusNotLoaded, ""),
		)
		config := integrationRuntimeConfig(client)
		config.MaxLoras = 1
		config.HTTPClient = httpClient
		statusWriter, err := servingkubernetes.NewServedModelStore(integrationServedModelStoreConfig(), client)
		Expect(err).NotTo(HaveOccurred())
		config.ServedModelStatusWriter = statusWriter
		runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(config, client)
		Expect(err).NotTo(HaveOccurred())

		newState, err := runtimeAdapter.EnsureServedModel(context.Background(), newcomer)
		Expect(err).NotTo(HaveOccurred())

		Expect(newState.Ready).To(BeTrue())
		Expect(loadRequests).To(Equal(1))
		Expect(unloadRequests).To(Equal(1))
		victimUpdated, err := client.Resource(integrationServedModelGVR()).Namespace("default").Get(context.Background(), victim.ResourceName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		status, _, _ := unstructured.NestedString(victimUpdated.Object, "status", "servingLoadStatus")
		reason, _, _ := unstructured.NestedString(victimUpdated.Object, "status", "failureReason")
		Expect(status).To(Equal(model.ModelLoadStatusNotLoaded.String()))
		Expect(reason).To(Equal(model.NotLoadedReasonCapacityEvicted))

		reloadedState, err := runtimeAdapter.EnsureServedModel(context.Background(), victim)
		Expect(err).NotTo(HaveOccurred())

		Expect(reloadedState.Ready).To(BeTrue())
		Expect(loaded[servingkubernetes.ServingModelName(victim)]).To(BeTrue())
		Expect(loaded[servingkubernetes.ServingModelName(newcomer)]).To(BeFalse())
		Expect(loadRequests).To(Equal(2))
		Expect(unloadRequests).To(Equal(2))
	})

	It("keeps a dedicated tenant adapter on an isolated base runtime pool", func() {
		shared := integrationServedModelVariant("ranker-shared", 1, uuid.New(), uuid.New(), model.RuntimeIsolationShared)
		dedicated := integrationServedModelVariant("ranker-dedicated", 1, uuid.New(), uuid.New(), model.RuntimeIsolationDedicated)
		dedicated.BaseModel = shared.BaseModel
		sharedWorkload := servingkubernetes.SharedRuntimeWorkloadName(shared)
		dedicatedWorkload := servingkubernetes.SharedRuntimeWorkloadName(dedicated)
		Expect(dedicatedWorkload).NotTo(Equal(sharedWorkload))
		loaded := map[string]bool{
			servingkubernetes.SharedRuntimeServingModelName(shared):    true,
			servingkubernetes.SharedRuntimeServingModelName(dedicated): true,
		}
		loadRequests := 0
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(),
			integrationReadyDeployment(sharedWorkload),
			integrationService(sharedWorkload),
			integrationReadyDeployment(dedicatedWorkload),
			integrationService(dedicatedWorkload),
		)
		config := integrationRuntimeConfig(client)
		config.HTTPClient = integrationVLLMClient(loaded, &loadRequests, nil)
		runtimeAdapter, err := servingkubernetes.NewVLLMRuntime(config, client)
		Expect(err).NotTo(HaveOccurred())

		sharedState, err := runtimeAdapter.EnsureServedModel(context.Background(), shared)
		Expect(err).NotTo(HaveOccurred())
		dedicatedState, err := runtimeAdapter.EnsureServedModel(context.Background(), dedicated)
		Expect(err).NotTo(HaveOccurred())

		Expect(sharedState.Ready).To(BeTrue())
		Expect(dedicatedState.Ready).To(BeTrue())
		Expect(sharedState.ServingTarget).NotTo(Equal(dedicatedState.ServingTarget))
		sharedRuntime, err := client.Resource(integrationBaseRuntimeGVR()).Namespace("default").Get(context.Background(), sharedWorkload, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		dedicatedRuntime, err := client.Resource(integrationBaseRuntimeGVR()).Namespace("default").Get(context.Background(), dedicatedWorkload, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		sharedPool, _, _ := unstructured.NestedString(sharedRuntime.Object, "spec", "poolKey")
		dedicatedPool, _, _ := unstructured.NestedString(dedicatedRuntime.Object, "spec", "poolKey")
		Expect(sharedPool).To(Equal(shared.BaseModel))
		Expect(dedicatedPool).To(Equal(dedicated.OrgID.String()))
	})
})

func integrationServedModel(adapterRank int) *model.ServedModel {
	return &model.ServedModel{
		ResourceName:  "served-model-4f4b8258-f9af-49f8-b5a8-f84d75891f3b",
		Namespace:     "default",
		Generation:    7,
		ModelID:       uuid.MustParse("4f4b8258-f9af-49f8-b5a8-f84d75891f3b"),
		OrgID:         uuid.MustParse("3bc4f810-ae40-47b7-a0b2-c980f04e2687"),
		TrainingRunID: uuid.MustParse("76b4da89-7fdb-459a-a842-9f866152efad"),
		DatasetID:     uuid.MustParse("6629d88a-05af-411e-8439-7497620e41df"),
		ModelKind:     "FINE_TUNED",
		Name:          "ranker",
		ModelVersion:  1,
		BaseModel:     "mistral-7b",
		AdapterURI:    "s3://models/run-1",
		AdapterRank:   adapterRank,
		ServingModel:  "ranker-v1",
	}
}

func integrationServedModelVariant(name string, version int, modelID uuid.UUID, orgID uuid.UUID, isolation string) *model.ServedModel {
	servedModel := integrationServedModel(16)
	servedModel.ResourceName = "served-model-" + modelID.String() + "-v" + strconv.Itoa(version)
	servedModel.ModelID = modelID
	servedModel.OrgID = orgID
	servedModel.TrainingRunID = uuid.New()
	servedModel.DatasetID = uuid.New()
	servedModel.Name = name
	servedModel.ModelVersion = version
	servedModel.AdapterURI = "s3://models/" + name
	servedModel.ServingModel = name + "-v" + strconv.Itoa(version)
	servedModel.RuntimeIsolation = isolation
	return servedModel
}

func integrationRuntimeConfig(client *fake.FakeDynamicClient) servingkubernetes.VLLMRuntimeConfig {
	baseRuntimeStore, err := servingkubernetes.NewBaseRuntimeStore(servingkubernetes.BaseRuntimeStoreConfig{
		Namespace: "default",
		Group:     "serving.bighill.io",
		Version:   "v1alpha1",
		Resource:  "baseruntimes",
	}, client)
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
		HTTPClient:       http.DefaultClient,
		BaseRuntimeStore: baseRuntimeStore,
	}
}

func integrationServedModelStoreConfig() servingkubernetes.ServedModelStoreConfig {
	return servingkubernetes.ServedModelStoreConfig{
		Namespace: "default",
		Group:     "serving.bighill.io",
		Version:   "v1alpha1",
		Resource:  "servedmodels",
	}
}

func integrationReadyDeployment(name string) *unstructured.Unstructured {
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

func integrationService(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata": map[string]any{
			"name":      name,
			"namespace": "default",
		},
	}}
}

func integrationBaseRuntime(name string, maxRank int64) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "serving.bighill.io/v1alpha1",
		"kind":       "BaseRuntime",
		"metadata": map[string]any{
			"name":      name,
			"namespace": "default",
		},
		"spec": map[string]any{
			"baseModel":   "mistral-7b",
			"poolKey":     "mistral-7b",
			"maxLoras":    int64(4),
			"maxLoraRank": maxRank,
			"gpu":         strings.TrimSpace("1"),
			"image":       "vllm/vllm-openai:v-test",
		},
	}}
	obj.SetGeneration(3)
	return obj
}

func integrationServedModelObject(servedModel *model.ServedModel, status model.ModelLoadStatus, reason string) *unstructured.Unstructured {
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
			"artifactLocation": servedModel.ArtifactLocation,
			"artifactFormat":   servedModel.ArtifactFormat,
			"artifactChecksum": servedModel.ArtifactChecksum,
			"adapterURI":       servedModel.AdapterURI,
			"adapterRank":      int64(servedModel.AdapterRank),
			"runtimeIsolation": servedModel.RuntimeIsolation,
			"pinned":           servedModel.Pinned,
			"servingTarget":    servedModel.ServingTarget,
			"servingModel":     servedModel.ServingModel,
			"servingProtocol":  "OPENAI_CHAT_COMPLETIONS",
		},
		"status": map[string]any{
			"servingLoadStatus":  status.String(),
			"servingTarget":      "http://vllm.test",
			"servingModel":       servingkubernetes.ServingModelName(servedModel),
			"servingProtocol":    "OPENAI_CHAT_COMPLETIONS",
			"failureReason":      reason,
			"observedGeneration": int64(servedModel.Generation),
			"readyReplicas":      int64(1),
		},
	}}
	obj.SetGeneration(servedModel.Generation)
	return obj
}

func integrationDeploymentGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
}

func integrationBaseRuntimeGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "serving.bighill.io", Version: "v1alpha1", Resource: "baseruntimes"}
}

func integrationServedModelGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "serving.bighill.io", Version: "v1alpha1", Resource: "servedmodels"}
}

type integrationRoundTripFunc func(*http.Request) (*http.Response, error)

func (f integrationRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func integrationJSONResponse(req *http.Request, payload any) *http.Response {
	raw, err := json.Marshal(payload)
	Expect(err).NotTo(HaveOccurred())
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(string(raw))),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Request:    req,
	}
}

func integrationVLLMClient(loaded map[string]bool, loadRequests *int, unloadRequests *int) *http.Client {
	return &http.Client{Transport: integrationRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/models":
			models := []map[string]string{}
			for name, isLoaded := range loaded {
				if isLoaded {
					models = append(models, map[string]string{"id": name})
				}
			}
			return integrationJSONResponse(r, map[string]any{"data": models}), nil
		case r.Method == http.MethodPost && r.URL.Path == "/v1/load_lora_adapter":
			defer r.Body.Close()
			var payload map[string]string
			Expect(json.NewDecoder(r.Body).Decode(&payload)).To(Succeed())
			loaded[payload["lora_name"]] = true
			if loadRequests != nil {
				*loadRequests++
			}
			return integrationJSONResponse(r, map[string]any{}), nil
		case r.Method == http.MethodPost && r.URL.Path == "/v1/unload_lora_adapter":
			defer r.Body.Close()
			var payload map[string]string
			Expect(json.NewDecoder(r.Body).Decode(&payload)).To(Succeed())
			loaded[payload["lora_name"]] = false
			if unloadRequests != nil {
				*unloadRequests++
			}
			return integrationJSONResponse(r, map[string]any{}), nil
		default:
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(strings.NewReader(`{}`)),
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Request:    r,
			}, nil
		}
	})}
}
