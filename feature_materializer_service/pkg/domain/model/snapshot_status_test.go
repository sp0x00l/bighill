package model_test

import (
	"testing"

	"feature_materializer_service/pkg/domain/model"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestModel(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Feature materializer model unit test suite")
}

var _ = Describe("SnapshotStatus", func() {
	It("converts known statuses", func() {
		status, err := model.ToSnapshotStatus("PENDING")
		Expect(err).NotTo(HaveOccurred())
		Expect(status).To(Equal(model.SnapshotStatusPending))
		Expect(model.SnapshotStatusReady.String()).To(Equal("READY"))
		Expect(model.SnapshotStatusFailed.String()).To(Equal("FAILED"))
	})

	It("rejects unknown statuses", func() {
		_, err := model.ToSnapshotStatus("UNKNOWN")
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("EmbeddingStrategy", func() {
	It("normalizes defaults and provider casing", func() {
		strategy := model.NormalizeEmbeddingStrategy(model.EmbeddingStrategy{
			EmbeddingProvider: " OLLAMA ",
			ChunkOverlap:      -1,
		})

		Expect(strategy.StrategyVersion).To(Equal(model.DefaultEmbeddingStrategyVersion))
		Expect(strategy.ExtractorName).To(Equal(model.DefaultExtractorName))
		Expect(strategy.CleanerName).To(Equal(model.DefaultCleanerName))
		Expect(strategy.ChunkerName).To(Equal(model.DefaultChunkerName))
		Expect(strategy.ChunkOverlap).To(Equal(0))
		Expect(strategy.EmbeddingProvider).To(Equal("ollama"))
		Expect(strategy.EmbeddingDimensions).To(Equal(model.DefaultEmbeddingDimensions))
	})

	It("includes chunking and model choices in the canonical key", func() {
		first := model.NormalizeEmbeddingStrategy(model.EmbeddingStrategy{EmbeddingModel: "bge-small-en-v1.5", ChunkSize: 512})
		second := model.NormalizeEmbeddingStrategy(model.EmbeddingStrategy{EmbeddingModel: "bge-m3", ChunkSize: 512})

		Expect(first.CanonicalKey()).To(ContainSubstring("embedding_model=bge-small-en-v1.5"))
		Expect(first.CanonicalKey()).To(ContainSubstring("extractor=go-document-extractor-suite"))
		Expect(first.CanonicalKey()).To(ContainSubstring("cleaner=go-basic-text-cleaner"))
		Expect(first.CanonicalKey()).NotTo(Equal(second.CanonicalKey()))
	})

	It("changes the canonical key when extractor or cleaner versions change", func() {
		first := model.NormalizeEmbeddingStrategy(model.EmbeddingStrategy{ExtractorVersion: "v1", CleanerVersion: "v1"})
		second := model.NormalizeEmbeddingStrategy(model.EmbeddingStrategy{ExtractorVersion: "v2", CleanerVersion: "v1"})
		third := model.NormalizeEmbeddingStrategy(model.EmbeddingStrategy{ExtractorVersion: "v1", CleanerVersion: "v2"})

		Expect(first.CanonicalKey()).NotTo(Equal(second.CanonicalKey()))
		Expect(first.CanonicalKey()).NotTo(Equal(third.CanonicalKey()))
	})
})

var _ = Describe("ProcessingProfile", func() {
	It("converts known profiles", func() {
		profile, err := model.ToProcessingProfile("GENERIC_PARQUET_PROCESSING_PROFILE")
		Expect(err).NotTo(HaveOccurred())
		Expect(profile).To(Equal(model.ProcessingProfileGenericParquet))
		Expect(profile.RequiresEmbeddings()).To(BeFalse())

		profile, err = model.ToProcessingProfile("TEXT_RAG_PROCESSING_PROFILE")
		Expect(err).NotTo(HaveOccurred())
		Expect(profile).To(Equal(model.ProcessingProfileTextRAG))
		Expect(profile.RequiresEmbeddings()).To(BeTrue())
	})

	It("rejects unknown profiles", func() {
		_, err := model.ToProcessingProfile("CUSTOM")
		Expect(err).To(HaveOccurred())
	})

	It("rejects empty profiles instead of defaulting", func() {
		_, err := model.ToProcessingProfile("")
		Expect(err).To(HaveOccurred())
	})
})
