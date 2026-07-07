package localserving

import (
	"context"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	localstore "lib/shared_lib/servedmodel"
	"model_serving_service/pkg/domain"
	"model_serving_service/pkg/domain/model"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/watch"
)

func TestLocalServing(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Model serving local serving unit test suite")
}

var _ = Describe("Runtime", func() {
	It("returns a ready local runtime state for base-backed served models", func() {
		modelID := uuid.New()
		runtime := NewRuntime("default", 8080, "http://ollama.local")
		runtime.client = &http.Client{Transport: newLocalOllamaTagsTransport(`{"models":[{"name":"llama3.1:8b"}]}`, func(req *http.Request) {
			Expect(req.URL.Path).To(Equal("/api/tags"))
		})}

		state, err := runtime.EnsureServedModel(context.Background(), &model.ServedModel{
			ModelID:      modelID,
			ModelKind:    "BASE",
			Name:         "llama",
			ModelVersion: 2,
			BaseModel:    "llama3.1:8b",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(state.Ready).To(BeTrue())
		Expect(state.ServingTarget).To(Equal("http://ollama.local"))
		Expect(state.ServingModel).To(Equal("llama3.1:8b"))
		Expect(state.ReadyReplicas).To(Equal(int32(1)))
	})

	It("matches Ollama tags that omit the explicit latest suffix", func() {
		runtime := NewRuntime("default", 8080, "http://ollama.local")
		runtime.client = &http.Client{Transport: newLocalOllamaTagsTransport(`{"models":[{"name":"llama3.1:latest"}]}`, nil)}

		state, err := runtime.EnsureServedModel(context.Background(), &model.ServedModel{
			ModelID:      uuid.New(),
			ModelKind:    "BASE",
			Name:         "llama",
			ModelVersion: 1,
			BaseModel:    "llama3.1",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(state.ServingModel).To(Equal("llama3.1"))
	})

	It("treats major local model families as runtime data, not provider or protocol variants", func() {
		families := []string{
			"llama3.1:8b",
			"mistral:7b",
			"qwen2.5:7b",
			"deepseek-r1:7b",
			"gemma3:4b",
		}

		for _, baseModel := range families {
			runtime := NewRuntime("default", 8080, "http://ollama.local")
			runtime.client = &http.Client{Transport: newLocalOllamaTagsTransport(`{"models":[{"name":"`+baseModel+`"}]}`, nil)}

			state, err := runtime.EnsureServedModel(context.Background(), &model.ServedModel{
				ModelID:      uuid.New(),
				ModelKind:    "BASE",
				Name:         "base",
				ModelVersion: 1,
				BaseModel:    baseModel,
			})

			Expect(err).NotTo(HaveOccurred(), baseModel)
			Expect(state.ServingModel).To(Equal(baseModel), baseModel)
			Expect(state.ServingProtocol).To(Equal(model.ServingProtocolOpenAIChatCompletions), baseModel)
		}
	})

	It("does not default non-base models to the base model", func() {
		_, err := NewRuntime("default", 8080, "http://ollama.local").EnsureServedModel(context.Background(), &model.ServedModel{
			ModelID:      uuid.New(),
			ModelKind:    "FINE_TUNED",
			Name:         "fine-tune",
			ModelVersion: 1,
			BaseModel:    "llama3.1:8b",
		})

		Expect(err).To(MatchError(ContainSubstring("serving model is required for non-base local served models")))
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
	})

	It("rejects base models that are not loaded in local Ollama", func() {
		runtime := NewRuntime("default", 8080, "http://ollama.local")
		runtime.client = &http.Client{Transport: newLocalOllamaTagsTransport(`{"models":[{"name":"other-model"}]}`, nil)}

		_, err := runtime.EnsureServedModel(context.Background(), &model.ServedModel{
			ModelID:      uuid.New(),
			ModelKind:    "BASE",
			Name:         "llama",
			ModelVersion: 1,
			BaseModel:    "llama3.1:8b",
		})

		Expect(err).To(MatchError(ContainSubstring(`local ollama model "llama3.1:8b" is not available`)))
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
	})

	It("rejects served models with no base model", func() {
		_, err := NewRuntime("default", 8080, "http://ollama.local").EnsureServedModel(context.Background(), &model.ServedModel{})

		Expect(err).To(MatchError(ContainSubstring("base model is required")))
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
	})
})

type localOllamaTagsTransport struct {
	payload string
	assert  func(*http.Request)
}

func newLocalOllamaTagsTransport(payload string, assert func(*http.Request)) localOllamaTagsTransport {
	return localOllamaTagsTransport{payload: payload, assert: assert}
}

func (t localOllamaTagsTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.assert != nil {
		t.assert(req)
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(t.payload)),
		Header:     make(http.Header),
	}, nil
}

var _ = Describe("Store record conversion", func() {
	It("converts local store records to served models", func() {
		modelID := uuid.New()
		trainingRunID := uuid.New()
		datasetID := uuid.New()
		record := localstore.Record{
			Name:       "served-model",
			Namespace:  "default",
			Generation: 2,
			Spec: localstore.Spec{
				ModelID:       modelID.String(),
				TrainingRunID: trainingRunID.String(),
				DatasetID:     datasetID.String(),
				ModelKind:     "BASE",
				Name:          "llama",
				ModelVersion:  1,
				BaseModel:     "meta-llama/Llama",
			},
			Status: localstore.Status{
				ServingLoadStatus:  model.ModelLoadStatusLoaded.String(),
				ServingTarget:      "http://runtime",
				ServingModel:       "llama",
				ObservedGeneration: 2,
				ReadyReplicas:      1,
			},
		}

		served, err := recordToServedModel(record)

		Expect(err).NotTo(HaveOccurred())
		Expect(served.ModelID).To(Equal(modelID))
		Expect(served.TrainingRunID).To(Equal(trainingRunID))
		Expect(served.DatasetID).To(Equal(datasetID))
		Expect(served.ModelKind).To(Equal("BASE"))
		Expect(served.Status.ServingLoadStatus).To(Equal(model.ModelLoadStatusLoaded))
	})

	It("rejects invalid local store IDs and statuses", func() {
		_, err := recordToServedModel(localstore.Record{Spec: localstore.Spec{ModelID: "bad"}})
		Expect(err).To(HaveOccurred())

		_, err = recordToServedModel(localstore.Record{Spec: localstore.Spec{ModelID: uuid.NewString(), TrainingRunID: "bad"}})
		Expect(err).To(HaveOccurred())

		_, err = recordToServedModel(localstore.Record{
			Spec:   localstore.Spec{ModelID: uuid.NewString()},
			Status: localstore.Status{ServingLoadStatus: "WARMING"},
		})
		Expect(err).To(HaveOccurred())
	})

	It("handles optional UUIDs and Kubernetes watch objects", func() {
		id, err := parseOptionalUUID("")
		Expect(err).NotTo(HaveOccurred())
		Expect(id).To(Equal(uuid.Nil))

		_, err = parseOptionalUUID("bad")
		Expect(err).To(HaveOccurred())

		record := localstore.Record{Name: "served-model", Namespace: "default", Spec: localstore.Spec{ModelID: uuid.NewString()}}
		Expect(recordsByName([]localstore.Record{record})).To(HaveKey("served-model"))
		Expect(deletedObject("served-model").GetName()).To(Equal("served-model"))
		Expect(recordToObject(record).GetKind()).To(Equal("ServedModel"))
	})
})

var _ = Describe("Store", func() {
	It("reads and lists served models from the local store", func() {
		path := filepath.Join(GinkgoT().TempDir(), "served_models.json")
		store, err := NewStore("default", path)
		Expect(err).NotTo(HaveOccurred())
		modelID := uuid.New()
		name := localstore.ResourceName(modelID.String(), 1)
		Expect(store.store.UpsertSpec(name, "default", localstore.Spec{
			ModelID:      modelID.String(),
			Name:         "llama",
			ModelVersion: 1,
			BaseModel:    "meta-llama/Llama",
		})).To(Succeed())

		served, err := store.Read(context.Background(), name)
		Expect(err).NotTo(HaveOccurred())
		Expect(served.ModelID).To(Equal(modelID))

		list, resourceVersion, err := store.ListWithResourceVersion(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(list).To(HaveLen(1))
		Expect(resourceVersion).NotTo(BeEmpty())
		Expect(store.Namespace()).To(Equal("default"))
	})

	It("returns model serve errors for missing records", func() {
		store, err := NewStore("default", filepath.Join(GinkgoT().TempDir(), "served_models.json"))
		Expect(err).NotTo(HaveOccurred())

		_, err = store.Read(context.Background(), "missing")

		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrModelServe)).To(BeTrue())
	})

	It("updates local status records", func() {
		path := filepath.Join(GinkgoT().TempDir(), "served_models.json")
		store, err := NewStore("default", path)
		Expect(err).NotTo(HaveOccurred())
		modelID := uuid.New()
		name := localstore.ResourceName(modelID.String(), 1)
		Expect(store.store.UpsertSpec(name, "default", localstore.Spec{ModelID: modelID.String(), Name: "llama", ModelVersion: 1, BaseModel: "meta-llama/Llama"})).To(Succeed())

		Expect(store.UpdateStatus(context.Background(), name, &model.ServedModelStatus{
			ServingLoadStatus:  model.ModelLoadStatusLoaded,
			ServingTarget:      "http://runtime",
			ServingModel:       "llama",
			ObservedGeneration: 1,
			ReadyReplicas:      1,
		})).To(Succeed())

		served, err := store.Read(context.Background(), name)
		Expect(err).NotTo(HaveOccurred())
		Expect(served.Status.ServingLoadStatus).To(Equal(model.ModelLoadStatusLoaded))
	})

	It("does not block sending watch events after cancellation", func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		events := make(chan watch.Event)

		Expect(sendWatchEvent(ctx, events, watch.Event{Type: watch.Added})).To(BeFalse())
	})
})
