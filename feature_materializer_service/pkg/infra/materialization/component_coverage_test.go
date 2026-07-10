package materialization_test

import (
	"context"
	"errors"
	"time"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"
	"feature_materializer_service/pkg/infra/materialization"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Materialization components", func() {
	It("constructs HTTP embedding providers from config and exposes dimensions", func() {
		provider := materialization.NewHTTPEmbeddingProvider(" TEI ", "http://embedding-service/", "bge-small", 384, time.Second)

		Expect(provider.Dimensions()).To(Equal(384))
	})

	It("validates flight reader configuration before dialing data stream", func() {
		reader := materialization.NewFlightDataStreamReaderWithConfig(materialization.FlightDataStreamReaderConfig{
			Timeout:  time.Second,
			Insecure: true,
		})

		artifact, err := reader.ReadParquet(context.Background(), materialization.DataStreamReadRequest{})

		Expect(artifact).To(BeNil())
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
		Expect(err).To(MatchError(ContainSubstring("data stream flight address is required")))
	})

	It("reports PDF extractor identity and wraps invalid PDF extraction errors", func() {
		extractor := materialization.NewPDFDocumentExtractor()

		Expect(extractor.Name()).NotTo(BeEmpty())
		Expect(extractor.Version()).NotTo(BeEmpty())

		extraction, err := extractor.ExtractText(context.Background(), []byte("not a pdf"))

		Expect(extraction).To(BeNil())
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrArtifactRead)).To(BeTrue())
	})

	It("cleans whitespace and control characters deterministically", func() {
		cleaner := materialization.NewBasicTextCleaner()

		cleaned, err := cleaner.Clean(context.Background(), "  first\x00\n\t second  ")

		Expect(err).NotTo(HaveOccurred())
		Expect(cleaned).To(Equal("first second"))
		Expect(cleaner.Name()).To(Equal(model.DefaultCleanerName))
		Expect(cleaner.Version()).To(Equal(model.DefaultCleanerVersion))
	})

	It("reports support predicates for feature and embedding processors", func() {
		builder := materialization.NewFeatureSnapshotBuilder(newMemoryArtifactStore())
		Expect(builder.SupportsFeatureSnapshot(nil)).To(BeFalse())
		Expect(builder.SupportsFeatureSnapshot(validRawSnapshot(validDatasetFile()))).To(BeTrue())

		strategy := model.ApplyEmbeddingStrategyDefaults(model.EmbeddingStrategy{
			EmbeddingProvider:   "tei",
			EmbeddingModel:      "test-model",
			EmbeddingDimensions: 4,
		})
		writer := materialization.NewEmbeddingWriter(newMemoryArtifactStore(), &recordingEmbeddingProvider{dimensions: 4}, nil, strategy, "pgvector", 10)

		generic := validFeatureSnapshot(validRawSnapshot(validDatasetFile()))
		generic.ProcessingProfile = model.ProcessingProfileGenericParquet
		rag := validFeatureSnapshot(validRawSnapshot(validDatasetFile()))
		rag.ProcessingProfile = model.ProcessingProfileTextRAG

		Expect(writer.SupportsEmbeddings(nil)).To(BeFalse())
		Expect(writer.SupportsEmbeddings(generic)).To(BeFalse())
		Expect(writer.SupportsEmbeddings(rag)).To(BeTrue())
	})
})
