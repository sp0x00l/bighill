package servedmodel_test

import (
	"errors"
	"path/filepath"
	"testing"

	"lib/shared_lib/servedmodel"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestServedModelStore(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Local served model store suite")
}

var _ = Describe("Store", func() {
	It("shares ServedModel intent and status through a file-backed store", func() {
		path := filepath.Join(GinkgoT().TempDir(), "served_models.json")
		registryStore, err := servedmodel.NewStore(path)
		Expect(err).NotTo(HaveOccurred())
		servingStore, err := servedmodel.NewStore(path)
		Expect(err).NotTo(HaveOccurred())

		Expect(registryStore.UpsertSpec("served-model-one", "default", servedmodel.Spec{
			ModelID:      "4f4b8258-f9af-49f8-b5a8-f84d75891f3b",
			Name:         "ranker",
			ModelVersion: 1,
			BaseModel:    "mistral",
			AdapterURI:   "s3://models/run",
		})).To(Succeed())

		records, _, err := servingStore.List("default")
		Expect(err).NotTo(HaveOccurred())
		Expect(records).To(HaveLen(1))
		Expect(records[0].Spec.AdapterURI).To(Equal("s3://models/run"))

		Expect(servingStore.UpdateStatus("served-model-one", servedmodel.Status{
			ServingLoadStatus:  "LOADED",
			ServingTarget:      "http://local-model-serving.default.local:8000",
			ServingModel:       "ranker-v1",
			ObservedGeneration: records[0].Generation,
			ReadyReplicas:      1,
		})).To(Succeed())

		record, ok, err := registryStore.Read("served-model-one")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(record.Status.ServingLoadStatus).To(Equal("LOADED"))
	})

	It("rejects status writes for stale generations", func() {
		path := filepath.Join(GinkgoT().TempDir(), "served_models.json")
		store, err := servedmodel.NewStore(path)
		Expect(err).NotTo(HaveOccurred())

		Expect(store.UpsertSpec("served-model-one", "default", servedmodel.Spec{
			ModelID:      "4f4b8258-f9af-49f8-b5a8-f84d75891f3b",
			Name:         "ranker",
			ModelVersion: 1,
			BaseModel:    "mistral",
		})).To(Succeed())
		Expect(store.UpsertSpec("served-model-one", "default", servedmodel.Spec{
			ModelID:      "4f4b8258-f9af-49f8-b5a8-f84d75891f3b",
			Name:         "ranker",
			ModelVersion: 1,
			BaseModel:    "llama",
		})).To(Succeed())

		err = store.UpdateStatus("served-model-one", servedmodel.Status{
			ServingLoadStatus:  "LOADED",
			ObservedGeneration: 1,
		})

		Expect(errors.Is(err, servedmodel.ErrStaleGeneration)).To(BeTrue())
	})

	It("surfaces namespace mismatches instead of returning an empty list", func() {
		path := filepath.Join(GinkgoT().TempDir(), "served_models.json")
		store, err := servedmodel.NewStore(path)
		Expect(err).NotTo(HaveOccurred())

		Expect(store.UpsertSpec("served-model-one", "registry", servedmodel.Spec{
			ModelID:      "4f4b8258-f9af-49f8-b5a8-f84d75891f3b",
			Name:         "ranker",
			ModelVersion: 1,
		})).To(Succeed())

		_, _, err = store.List("serving")

		Expect(errors.Is(err, servedmodel.ErrNamespaceMismatch)).To(BeTrue())
	})
})
