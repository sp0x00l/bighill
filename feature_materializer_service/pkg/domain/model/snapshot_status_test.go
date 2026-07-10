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
		status, err := model.ToSnapshotStatus(" pending ")
		Expect(err).NotTo(HaveOccurred())
		Expect(status).To(Equal(model.SnapshotStatusPending))
		Expect(model.SnapshotStatusReady.String()).To(Equal("READY"))
		Expect(model.SnapshotStatusFailed.String()).To(Equal("FAILED"))
	})

	It("renders unknown status values without panicking", func() {
		Expect(model.SnapshotStatus(99).String()).To(Equal("UNKNOWN"))
	})

	It("rejects unknown statuses", func() {
		_, err := model.ToSnapshotStatus("UNKNOWN")
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("EmbeddingStrategy", func() {
	It("normalizes text fields without applying defaults", func() {
		strategy := model.NormalizeEmbeddingStrategy(model.EmbeddingStrategy{
			StrategyVersion:     " rag-v2 ",
			ExtractorName:       " extractor ",
			ExtractorVersion:    " v9 ",
			CleanerName:         " cleaner ",
			CleanerVersion:      " v8 ",
			ChunkerName:         " chunker ",
			ChunkerVersion:      " v7 ",
			ChunkSize:           256,
			ChunkOverlap:        32,
			EmbeddingProvider:   " OLLAMA ",
			EmbeddingModel:      " bge-m3 ",
			EmbeddingDimensions: 1024,
		})

		Expect(strategy.StrategyVersion).To(Equal("rag-v2"))
		Expect(strategy.ExtractorName).To(Equal("extractor"))
		Expect(strategy.ExtractorVersion).To(Equal("v9"))
		Expect(strategy.CleanerName).To(Equal("cleaner"))
		Expect(strategy.CleanerVersion).To(Equal("v8"))
		Expect(strategy.ChunkerName).To(Equal("chunker"))
		Expect(strategy.ChunkerVersion).To(Equal("v7"))
		Expect(strategy.EmbeddingProvider).To(Equal("ollama"))
		Expect(strategy.EmbeddingModel).To(Equal("bge-m3"))
		Expect(strategy.ChunkSize).To(Equal(256))
		Expect(strategy.ChunkOverlap).To(Equal(32))
		Expect(strategy.EmbeddingDimensions).To(Equal(1024))
	})

	It("applies defaults and provider casing at the boundary", func() {
		strategy := model.ApplyEmbeddingStrategyDefaults(model.EmbeddingStrategy{
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

	It("keeps configured strategy values instead of overwriting them with defaults", func() {
		strategy := model.ApplyEmbeddingStrategyDefaults(model.EmbeddingStrategy{
			StrategyVersion:     "rag-v2",
			ExtractorName:       "custom-extractor",
			ExtractorVersion:    "v9",
			CleanerName:         "custom-cleaner",
			CleanerVersion:      "v8",
			ChunkerName:         "custom-chunker",
			ChunkerVersion:      "v7",
			ChunkSize:           1024,
			ChunkOverlap:        128,
			EmbeddingProvider:   "tei",
			EmbeddingModel:      "bge-m3",
			EmbeddingDimensions: 1024,
		})

		Expect(strategy.StrategyVersion).To(Equal("rag-v2"))
		Expect(strategy.ExtractorName).To(Equal("custom-extractor"))
		Expect(strategy.CleanerName).To(Equal("custom-cleaner"))
		Expect(strategy.ChunkerName).To(Equal("custom-chunker"))
		Expect(strategy.ChunkSize).To(Equal(1024))
		Expect(strategy.ChunkOverlap).To(Equal(128))
		Expect(strategy.EmbeddingModel).To(Equal("bge-m3"))
		Expect(strategy.EmbeddingDimensions).To(Equal(1024))
	})

	It("bounds defaulted chunk overlap below chunk size", func() {
		strategy := model.ApplyEmbeddingStrategyDefaults(model.EmbeddingStrategy{
			EmbeddingProvider: "tei",
			ChunkSize:         8,
			ChunkOverlap:      99,
		})

		Expect(strategy.ChunkOverlap).To(Equal(2))
		Expect(model.ValidateEmbeddingStrategy(strategy)).To(Succeed())
	})

	It("includes chunking and model choices in the canonical key", func() {
		first := model.ApplyEmbeddingStrategyDefaults(model.EmbeddingStrategy{EmbeddingProvider: "tei", EmbeddingModel: "bge-small-en-v1.5", ChunkSize: 512})
		second := model.ApplyEmbeddingStrategyDefaults(model.EmbeddingStrategy{EmbeddingProvider: "tei", EmbeddingModel: "bge-m3", ChunkSize: 512})

		Expect(first.CanonicalKey()).To(ContainSubstring("embedding_model=bge-small-en-v1.5"))
		Expect(first.CanonicalKey()).To(ContainSubstring("extractor=go-document-extractor-suite"))
		Expect(first.CanonicalKey()).To(ContainSubstring("cleaner=go-basic-text-cleaner"))
		Expect(first.CanonicalKey()).NotTo(Equal(second.CanonicalKey()))
	})

	It("canonicalizes equivalent strategy values", func() {
		first := model.ApplyEmbeddingStrategyDefaults(model.EmbeddingStrategy{
			StrategyVersion:   " rag-v1 ",
			EmbeddingProvider: " TEI ",
			EmbeddingModel:    " bge-small-en-v1.5 ",
		})
		second := model.ApplyEmbeddingStrategyDefaults(model.EmbeddingStrategy{
			StrategyVersion:   "rag-v1",
			EmbeddingProvider: "tei",
			EmbeddingModel:    "bge-small-en-v1.5",
		})

		Expect(first.CanonicalKey()).To(Equal(second.CanonicalKey()))
	})

	It("changes the canonical key when extractor or cleaner versions change", func() {
		first := model.ApplyEmbeddingStrategyDefaults(model.EmbeddingStrategy{EmbeddingProvider: "tei", ExtractorVersion: "v1", CleanerVersion: "v1"})
		second := model.ApplyEmbeddingStrategyDefaults(model.EmbeddingStrategy{EmbeddingProvider: "tei", ExtractorVersion: "v2", CleanerVersion: "v1"})
		third := model.ApplyEmbeddingStrategyDefaults(model.EmbeddingStrategy{EmbeddingProvider: "tei", ExtractorVersion: "v1", CleanerVersion: "v2"})

		Expect(first.CanonicalKey()).NotTo(Equal(second.CanonicalKey()))
		Expect(first.CanonicalKey()).NotTo(Equal(third.CanonicalKey()))
	})

	It("rejects unsupported embedding providers", func() {
		strategy := model.ApplyEmbeddingStrategyDefaults(model.EmbeddingStrategy{EmbeddingProvider: "unknown"})

		err := model.ValidateEmbeddingStrategy(strategy)

		Expect(err).To(MatchError(ContainSubstring("embedding_provider")))
	})

	DescribeTable("rejects incomplete or invalid strategy fields",
		func(mutator func(model.EmbeddingStrategy) model.EmbeddingStrategy, expected string) {
			strategy := model.ApplyEmbeddingStrategyDefaults(model.EmbeddingStrategy{EmbeddingProvider: "tei"})
			err := model.ValidateEmbeddingStrategy(mutator(strategy))

			Expect(err).To(MatchError(ContainSubstring(expected)))
		},
		Entry("strategy version", func(s model.EmbeddingStrategy) model.EmbeddingStrategy { s.StrategyVersion = ""; return s }, "strategy_version"),
		Entry("extractor name", func(s model.EmbeddingStrategy) model.EmbeddingStrategy { s.ExtractorName = ""; return s }, "extractor_name"),
		Entry("extractor version", func(s model.EmbeddingStrategy) model.EmbeddingStrategy { s.ExtractorVersion = ""; return s }, "extractor_version"),
		Entry("cleaner name", func(s model.EmbeddingStrategy) model.EmbeddingStrategy { s.CleanerName = ""; return s }, "cleaner_name"),
		Entry("cleaner version", func(s model.EmbeddingStrategy) model.EmbeddingStrategy { s.CleanerVersion = ""; return s }, "cleaner_version"),
		Entry("chunker name", func(s model.EmbeddingStrategy) model.EmbeddingStrategy { s.ChunkerName = ""; return s }, "chunker_name"),
		Entry("chunker version", func(s model.EmbeddingStrategy) model.EmbeddingStrategy { s.ChunkerVersion = ""; return s }, "chunker_version"),
		Entry("embedding model", func(s model.EmbeddingStrategy) model.EmbeddingStrategy { s.EmbeddingModel = ""; return s }, "embedding_model"),
		Entry("chunk size", func(s model.EmbeddingStrategy) model.EmbeddingStrategy { s.ChunkSize = 0; return s }, "chunk_size"),
		Entry("negative overlap", func(s model.EmbeddingStrategy) model.EmbeddingStrategy { s.ChunkOverlap = -1; return s }, "chunk_overlap"),
		Entry("overlap equals size", func(s model.EmbeddingStrategy) model.EmbeddingStrategy { s.ChunkOverlap = s.ChunkSize; return s }, "chunk_overlap"),
		Entry("embedding dimensions", func(s model.EmbeddingStrategy) model.EmbeddingStrategy { s.EmbeddingDimensions = 0; return s }, "embedding_dimensions"),
	)

	DescribeTable("recognizes supported providers after normalization",
		func(provider string, supported bool) {
			Expect(model.IsSupportedEmbeddingProvider(provider)).To(Equal(supported))
		},
		Entry("tei", " tei ", true),
		Entry("ollama", "OLLAMA", true),
		Entry("unknown", "openai", false),
		Entry("empty", "", false),
	)
})

var _ = Describe("ProcessingProfile", func() {
	It("converts known profiles", func() {
		profile, err := model.ToProcessingProfile(" generic_parquet_processing_profile ")
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

	It("does not require embeddings for instruction tuning data", func() {
		profile, err := model.ToProcessingProfile("INSTRUCTION_TUNING_PROCESSING_PROFILE")

		Expect(err).NotTo(HaveOccurred())
		Expect(profile).To(Equal(model.ProcessingProfileInstructionTuning))
		Expect(profile.RequiresEmbeddings()).To(BeFalse())
	})

	It("renders unknown profile values without panicking", func() {
		Expect(model.ProcessingProfile(99).String()).To(Equal("UNKNOWN"))
	})
})
