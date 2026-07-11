package kubernetes_test

import (
	"context"
	"errors"
	"time"

	"model_serving_service/pkg/domain"
	"model_serving_service/pkg/domain/model"
	servingkubernetes "model_serving_service/pkg/infra/network/k8s"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/fake"
)

var _ = Describe("BaseRuntimeStore loaded adapters", func() {
	It("round-trips loaded adapter status with model id, generation, last-used time, and pin state", func() {
		client := fake.NewSimpleDynamicClient(runtime.NewScheme())
		store, err := servingkubernetes.NewBaseRuntimeStore(baseRuntimeStoreConfig(), client)
		Expect(err).NotTo(HaveOccurred())
		baseRuntime, err := store.FindOrCreate(context.Background(), &model.BaseRuntime{
			BaseModel:   "mistral-7b",
			PoolKey:     "mistral-7b",
			MaxLoras:    2,
			MaxLoraRank: 16,
			GPU:         "1",
			Image:       "vllm/vllm-openai:v-test",
		})
		Expect(err).NotTo(HaveOccurred())
		lastUsed := time.Date(2026, 7, 11, 14, 30, 15, 123456789, time.FixedZone("UTC+2", 2*60*60))
		adapter := model.BaseRuntimeLoadedAdapter{
			ServingModel:            "tenant-ranker-v1",
			ServedModelResourceName: "served-model-tenant-ranker",
			ModelID:                 uuid.MustParse("4f4b8258-f9af-49f8-b5a8-f84d75891f3b"),
			ObservedGeneration:      12,
			LastUsedAt:              lastUsed,
			Pinned:                  true,
		}

		Expect(store.RecordAdapterLoaded(context.Background(), baseRuntime.ResourceName, adapter)).To(Succeed())
		read, err := store.Read(context.Background(), baseRuntime.ResourceName)
		Expect(err).NotTo(HaveOccurred())

		Expect(read.LoadedAdapters).To(HaveLen(1))
		Expect(read.LoadedAdapters[0].ServingModel).To(Equal("tenant-ranker-v1"))
		Expect(read.LoadedAdapters[0].ServedModelResourceName).To(Equal("served-model-tenant-ranker"))
		Expect(read.LoadedAdapters[0].ModelID).To(Equal(uuid.MustParse("4f4b8258-f9af-49f8-b5a8-f84d75891f3b")))
		Expect(read.LoadedAdapters[0].ObservedGeneration).To(Equal(int64(12)))
		Expect(read.LoadedAdapters[0].LastUsedAt).To(BeTemporally("==", lastUsed.UTC()))
		Expect(read.LoadedAdapters[0].Pinned).To(BeTrue())
	})

	It("replaces an existing loaded adapter by serving model instead of duplicating it", func() {
		client := fake.NewSimpleDynamicClient(runtime.NewScheme())
		store, err := servingkubernetes.NewBaseRuntimeStore(baseRuntimeStoreConfig(), client)
		Expect(err).NotTo(HaveOccurred())
		baseRuntime, err := store.FindOrCreate(context.Background(), &model.BaseRuntime{
			BaseModel:   "mistral-7b",
			PoolKey:     "mistral-7b",
			MaxLoras:    2,
			MaxLoraRank: 16,
			GPU:         "1",
			Image:       "vllm/vllm-openai:v-test",
		})
		Expect(err).NotTo(HaveOccurred())
		firstSeen := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
		secondSeen := firstSeen.Add(5 * time.Minute)

		Expect(store.RecordAdapterLoaded(context.Background(), baseRuntime.ResourceName, model.BaseRuntimeLoadedAdapter{
			ServingModel:            "tenant-ranker-v1",
			ServedModelResourceName: "served-model-old",
			ModelID:                 uuid.MustParse("4f4b8258-f9af-49f8-b5a8-f84d75891f3b"),
			ObservedGeneration:      3,
			LastUsedAt:              firstSeen,
			Pinned:                  false,
		})).To(Succeed())
		Expect(store.RecordAdapterLoaded(context.Background(), baseRuntime.ResourceName, model.BaseRuntimeLoadedAdapter{
			ServingModel:            "tenant-ranker-v1",
			ServedModelResourceName: "served-model-new",
			ModelID:                 uuid.MustParse("c3553483-2b78-4ebf-91cf-50d0bd5d7b91"),
			ObservedGeneration:      4,
			LastUsedAt:              secondSeen,
			Pinned:                  true,
		})).To(Succeed())

		read, err := store.Read(context.Background(), baseRuntime.ResourceName)
		Expect(err).NotTo(HaveOccurred())
		Expect(read.LoadedAdapters).To(HaveLen(1))
		Expect(read.LoadedAdapters[0].ServedModelResourceName).To(Equal("served-model-new"))
		Expect(read.LoadedAdapters[0].ModelID).To(Equal(uuid.MustParse("c3553483-2b78-4ebf-91cf-50d0bd5d7b91")))
		Expect(read.LoadedAdapters[0].ObservedGeneration).To(Equal(int64(4)))
		Expect(read.LoadedAdapters[0].LastUsedAt).To(BeTemporally("==", secondSeen))
		Expect(read.LoadedAdapters[0].Pinned).To(BeTrue())
	})

	It("removes only the requested loaded adapter", func() {
		client := fake.NewSimpleDynamicClient(runtime.NewScheme())
		store, err := servingkubernetes.NewBaseRuntimeStore(baseRuntimeStoreConfig(), client)
		Expect(err).NotTo(HaveOccurred())
		baseRuntime, err := store.FindOrCreate(context.Background(), &model.BaseRuntime{
			BaseModel:   "mistral-7b",
			PoolKey:     "mistral-7b",
			MaxLoras:    2,
			MaxLoraRank: 16,
			GPU:         "1",
			Image:       "vllm/vllm-openai:v-test",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(store.RecordAdapterLoaded(context.Background(), baseRuntime.ResourceName, model.BaseRuntimeLoadedAdapter{
			ServingModel:            "adapter-one",
			ServedModelResourceName: "served-model-one",
			ModelID:                 uuid.MustParse("4f4b8258-f9af-49f8-b5a8-f84d75891f3b"),
			ObservedGeneration:      1,
			LastUsedAt:              time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC),
		})).To(Succeed())
		Expect(store.RecordAdapterLoaded(context.Background(), baseRuntime.ResourceName, model.BaseRuntimeLoadedAdapter{
			ServingModel:            "adapter-two",
			ServedModelResourceName: "served-model-two",
			ModelID:                 uuid.MustParse("c3553483-2b78-4ebf-91cf-50d0bd5d7b91"),
			ObservedGeneration:      1,
			LastUsedAt:              time.Date(2026, 7, 11, 13, 0, 0, 0, time.UTC),
		})).To(Succeed())

		Expect(store.RemoveLoadedAdapter(context.Background(), baseRuntime.ResourceName, "adapter-one")).To(Succeed())
		read, err := store.Read(context.Background(), baseRuntime.ResourceName)
		Expect(err).NotTo(HaveOccurred())

		Expect(read.LoadedAdapters).To(HaveLen(1))
		Expect(read.LoadedAdapters[0].ServingModel).To(Equal("adapter-two"))
	})

	It("rejects malformed loaded adapter timestamps at the DTO boundary", func() {
		resourceName := servingkubernetes.BaseRuntimeResourceName("mistral-7b", "mistral-7b")
		baseRuntime := baseRuntimeCR(resourceName, 1)
		Expect(setLoadedAdapters(baseRuntime, []map[string]any{{
			"servingModel":            "adapter-one",
			"servedModelResourceName": "served-model-one",
			"modelID":                 "4f4b8258-f9af-49f8-b5a8-f84d75891f3b",
			"observedGeneration":      int64(1),
			"lastUsedAt":              "not-a-timestamp",
			"pinned":                  false,
		}})).To(Succeed())
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), baseRuntime)
		store, err := servingkubernetes.NewBaseRuntimeStore(baseRuntimeStoreConfig(), client)
		Expect(err).NotTo(HaveOccurred())

		_, err = store.Read(context.Background(), resourceName)

		Expect(err).To(MatchError(ContainSubstring("invalid loaded adapter last used time")))
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
	})

	It("rejects malformed loaded adapter model ids at the DTO boundary", func() {
		resourceName := servingkubernetes.BaseRuntimeResourceName("mistral-7b", "mistral-7b")
		baseRuntime := baseRuntimeCR(resourceName, 1)
		Expect(setLoadedAdapters(baseRuntime, []map[string]any{{
			"servingModel":            "adapter-one",
			"servedModelResourceName": "served-model-one",
			"modelID":                 "not-a-uuid",
			"observedGeneration":      int64(1),
			"lastUsedAt":              time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
			"pinned":                  false,
		}})).To(Succeed())
		client := fake.NewSimpleDynamicClient(runtime.NewScheme(), baseRuntime)
		store, err := servingkubernetes.NewBaseRuntimeStore(baseRuntimeStoreConfig(), client)
		Expect(err).NotTo(HaveOccurred())

		_, err = store.Read(context.Background(), resourceName)

		Expect(err).To(MatchError(ContainSubstring("invalid loaded adapter model id")))
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
	})
})
