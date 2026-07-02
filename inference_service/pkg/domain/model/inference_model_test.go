package model_test

import (
	"testing"

	"inference_service/pkg/domain/model"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestModel(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Inference service model unit test suite")
}

var _ = Describe("ModelStatus", func() {
	It("converts known model statuses", func() {
		status, err := model.ToModelStatus("READY")

		Expect(err).NotTo(HaveOccurred())
		Expect(status).To(Equal(model.ModelStatusReady))
		Expect(model.ModelStatusPending.String()).To(Equal("PENDING"))
		Expect(model.ModelStatusCandidate.String()).To(Equal("CANDIDATE"))
		Expect(model.ModelStatusEvaluated.String()).To(Equal("EVALUATED"))
		Expect(model.ModelStatusFailed.String()).To(Equal("FAILED"))
	})

	It("rejects unknown model statuses", func() {
		_, err := model.ToModelStatus("UNKNOWN")

		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("ModelLoadStatus", func() {
	It("converts known load statuses", func() {
		status, err := model.ToModelLoadStatus("LOADED")

		Expect(err).NotTo(HaveOccurred())
		Expect(status).To(Equal(model.ModelLoadStatusLoaded))
		Expect(model.ModelLoadStatusNotLoaded.String()).To(Equal("NOT_LOADED"))
		Expect(model.ModelLoadStatusFailed.String()).To(Equal("FAILED"))
	})

	It("rejects unknown load statuses", func() {
		_, err := model.ToModelLoadStatus("UNKNOWN")

		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("DatasetProcessingState", func() {
	It("converts known dataset processing states", func() {
		status, err := model.ToDatasetProcessingState("EMBEDDINGS_MATERIALIZED")

		Expect(err).NotTo(HaveOccurred())
		Expect(status).To(Equal(model.DatasetProcessingEmbeddingsMaterialized))
		Expect(model.DatasetProcessingPending.String()).To(Equal("PENDING"))
		Expect(model.DatasetProcessingFailed.String()).To(Equal("FAILED"))
	})

	It("rejects unknown dataset processing states", func() {
		_, err := model.ToDatasetProcessingState("UNKNOWN")

		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("InferenceDataset", func() {
	It("reports RAG readiness only for active embedding metadata", func() {
		dataset := &model.InferenceDataset{
			ProcessingState:     model.DatasetProcessingEmbeddingsMaterialized,
			EmbeddingSnapshotID: uuid.New(),
			EmbeddingDimensions: 384,
			EmbeddingCount:      1,
		}

		Expect(dataset.IsRAGReady()).To(BeTrue())

		dataset.EmbeddingCount = 0
		Expect(dataset.IsRAGReady()).To(BeFalse())
	})
})
