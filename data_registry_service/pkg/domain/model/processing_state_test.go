package model_test

import (
	"data_registry_service/pkg/domain/model"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestModel(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Data registry model unit test suite")
}

var _ = Describe("ProcessingState", func() {
	It("converts known states", func() {
		state, err := model.ToProcessingState("FEATURE_MATERIALIZED")

		Expect(err).NotTo(HaveOccurred())
		Expect(state).To(Equal(model.DatasetProcessingFeatureMaterialized))
		Expect(model.DatasetProcessingEmbeddingsMaterialized.String()).To(Equal("EMBEDDINGS_MATERIALIZED"))
	})

	It("defaults empty values and unknown string renderings to pending", func() {
		state, err := model.ToProcessingState("")

		Expect(err).NotTo(HaveOccurred())
		Expect(state).To(Equal(model.DatasetProcessingPending))
		Expect(model.ProcessingState(99).String()).To(Equal("PENDING"))
	})

	It("rejects unknown states", func() {
		_, err := model.ToProcessingState("UNKNOWN")

		Expect(err).To(HaveOccurred())
	})

	It("only advances forward", func() {
		Expect(model.AdvanceProcessingState(model.DatasetProcessingRawMaterialized, model.DatasetProcessingFeatureMaterialized)).To(Equal(model.DatasetProcessingFeatureMaterialized))
		Expect(model.AdvanceProcessingState(model.DatasetProcessingEmbeddingsMaterialized, model.DatasetProcessingRawMaterialized)).To(Equal(model.DatasetProcessingEmbeddingsMaterialized))
	})
})

var _ = Describe("ProcessingProfile", func() {
	It("converts known profiles and defaults empty values to generic parquet", func() {
		profile, err := model.ToProcessingProfile("")
		Expect(err).NotTo(HaveOccurred())
		Expect(profile).To(Equal(model.GenericParquetProfile))

		profile, err = model.ToProcessingProfile("TEXT_RAG")
		Expect(err).NotTo(HaveOccurred())
		Expect(profile).To(Equal(model.TextRAGProfile))
		Expect(model.InstructionTuningProfile.String()).To(Equal("INSTRUCTION_TUNING"))
		Expect(model.ProcessingProfile(99).String()).To(Equal("GENERIC_PARQUET"))
	})

	It("rejects unknown profiles", func() {
		_, err := model.ToProcessingProfile("CUSTOM")
		Expect(err).To(HaveOccurred())
	})
})
