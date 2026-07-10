package materialization_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"
	"feature_materializer_service/pkg/infra/materialization"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type failingArtifactStore struct {
	readErr  error
	writeErr error
}

func (s failingArtifactStore) Read(context.Context, string) ([]byte, error) {
	if s.readErr != nil {
		return nil, s.readErr
	}
	return []byte("title\nIntro\n"), nil
}

func (s failingArtifactStore) Write(context.Context, string, string, []byte) (string, error) {
	if s.writeErr != nil {
		return "", s.writeErr
	}
	return "s3://local-dev-bucket/output.parquet", nil
}

type recordingFeatureSnapshotProcessor struct {
	profile  model.ProcessingProfile
	selected bool
}

func (p *recordingFeatureSnapshotProcessor) SupportsFeatureSnapshot(rawSnapshot *model.RawSnapshot) bool {
	return rawSnapshot != nil && rawSnapshot.ProcessingProfile == p.profile
}

func (p *recordingFeatureSnapshotProcessor) BuildFeatureSnapshot(_ context.Context, _ *model.RawSnapshot, featureSnapshot *model.FeatureSnapshot) (*model.FeatureSnapshot, error) {
	p.selected = true
	out := *featureSnapshot
	out.StorageLocation = "s3://local-dev-bucket/lakehouse/features/selected.parquet"
	return &out, nil
}

var _ = Describe("Materialization boundaries", func() {
	It("rejects empty artifact write keys before uploading", func() {
		store, err := materialization.NewObjectArtifactStore(context.Background(), "local-dev-bucket", "local-dev", 10*1024*1024)
		Expect(err).NotTo(HaveOccurred())

		location, err := store.Write(context.Background(), " / ", "text/plain", []byte("body"))

		Expect(location).To(BeEmpty())
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
		Expect(err).To(MatchError(ContainSubstring("artifact key is required")))
	})

	It("keeps raw snapshot uploaded-file and connector support mutually exclusive", func() {
		uploaded := validDatasetFile()
		connector := validDatasetFile()
		connector.SourceConnectorID = uuid.New()

		uploadedWriter := materialization.NewRawSnapshotWriter(newMemoryArtifactStore())
		connectorWriter := materialization.NewDataStreamRawSnapshotWriter(newMemoryArtifactStore(), &recordingDataStreamReader{})

		Expect(uploadedWriter.SupportsRawSnapshot(nil)).To(BeFalse())
		Expect(uploadedWriter.SupportsRawSnapshot(uploaded)).To(BeTrue())
		Expect(uploadedWriter.SupportsRawSnapshot(connector)).To(BeFalse())
		Expect(connectorWriter.SupportsRawSnapshot(nil)).To(BeFalse())
		Expect(connectorWriter.SupportsRawSnapshot(uploaded)).To(BeFalse())
		Expect(connectorWriter.SupportsRawSnapshot(connector)).To(BeTrue())
	})

	It("propagates artifact read and write failures from raw snapshot writing", func() {
		datasetFile := validDatasetFile()
		rawSnapshot := validRawSnapshot(datasetFile)

		_, err := materialization.NewRawSnapshotWriter(failingArtifactStore{readErr: domain.ErrArtifactRead}).WriteRawSnapshot(context.Background(), datasetFile, rawSnapshot)
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrArtifactRead)).To(BeTrue())

		_, err = materialization.NewRawSnapshotWriter(failingArtifactStore{writeErr: domain.ErrArtifactWrite}).WriteRawSnapshot(context.Background(), datasetFile, rawSnapshot)
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrArtifactWrite)).To(BeTrue())
	})

	It("rejects connector raw snapshot writes without a reader, raw snapshot, or table reference", func() {
		datasetFile := validDatasetFile()
		datasetFile.SourceConnectorID = uuid.New()
		rawSnapshot := validRawSnapshot(datasetFile)

		_, err := materialization.NewDataStreamRawSnapshotWriter(newMemoryArtifactStore(), nil).WriteRawSnapshot(context.Background(), datasetFile, rawSnapshot)
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrRawSnapshotMaterialize)).To(BeTrue())

		_, err = materialization.NewDataStreamRawSnapshotWriter(newMemoryArtifactStore(), &recordingDataStreamReader{}).WriteRawSnapshot(context.Background(), datasetFile, nil)
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())

		datasetFile.TableName = ""
		_, err = materialization.NewDataStreamRawSnapshotWriter(newMemoryArtifactStore(), &recordingDataStreamReader{}).WriteRawSnapshot(context.Background(), datasetFile, rawSnapshot)
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
	})

	It("requires an Iceberg writer for Polaris feature snapshots", func() {
		ctx := context.Background()
		store := newMemoryArtifactStore()
		datasetFile := validDatasetFile()
		datasetFile.TableFormat = model.TableFormatIceberg
		datasetFile.CatalogProvider = model.CatalogProviderPolaris
		rawSnapshot := validRawSnapshot(datasetFile)
		rawSnapshot.StorageLocation = "s3://local/raw/snapshot.parquet"
		rawArtifact, err := materialization.NormalizeArtifactToParquet(ctx, []byte("title\nIntro\n"), "text/csv", "csv")
		Expect(err).NotTo(HaveOccurred())
		store.objects[rawSnapshot.StorageLocation] = rawArtifact.Data

		_, err = materialization.NewFeatureSnapshotBuilder(store).BuildFeatureSnapshot(ctx, rawSnapshot, validFeatureSnapshot(rawSnapshot))

		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrCatalogRegister)).To(BeTrue())
		Expect(err).To(MatchError(ContainSubstring("iceberg table writer is required")))
	})

	It("selects feature snapshot processors by processing profile", func() {
		rawSnapshot := validRawSnapshot(validDatasetFile())
		rawSnapshot.ProcessingProfile = model.ProcessingProfileTextRAG
		featureSnapshot := validFeatureSnapshot(rawSnapshot)
		generic := &recordingFeatureSnapshotProcessor{profile: model.ProcessingProfileGenericParquet}
		rag := &recordingFeatureSnapshotProcessor{profile: model.ProcessingProfileTextRAG}
		dispatcher := materialization.NewFeatureSnapshotBuilderDispatcher(generic, rag)

		result, err := dispatcher.BuildFeatureSnapshot(context.Background(), rawSnapshot, featureSnapshot)

		Expect(err).NotTo(HaveOccurred())
		Expect(result.StorageLocation).To(ContainSubstring("selected.parquet"))
		Expect(generic.selected).To(BeFalse())
		Expect(rag.selected).To(BeTrue())
	})

	It("rejects raw snapshot dispatch when no processor supports the profile", func() {
		dispatcher := materialization.NewRawSnapshotWriterDispatcher(nil)

		result, err := dispatcher.WriteRawSnapshot(context.Background(), validDatasetFile(), validRawSnapshot(validDatasetFile()))

		Expect(result).To(BeNil())
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrRawSnapshotMaterialize)).To(BeTrue())
	})

	It("selects embedding processors by processing profile", func() {
		featureSnapshot := validFeatureSnapshot(validRawSnapshot(validDatasetFile()))
		featureSnapshot.ProcessingProfile = model.ProcessingProfileTextRAG
		processor := &recordingEmbeddingProcessor{}
		dispatcher := materialization.NewEmbeddingWriterDispatcher(processor)

		result, _, err := dispatcher.MaterializeEmbeddings(context.Background(), featureSnapshot, validEmbeddingSnapshot(featureSnapshot))

		Expect(err).NotTo(HaveOccurred())
		Expect(result.VectorStore).To(Equal("pgvector"))
		Expect(processor.selected).To(BeTrue())
	})

	It("fails embedding materialization for unsupported chunkers", func() {
		ctx := context.Background()
		store := newMemoryArtifactStore()
		rawArtifact, err := materialization.NormalizeArtifactToParquet(ctx, []byte("title\nIntro\n"), "text/csv", "csv")
		Expect(err).NotTo(HaveOccurred())
		featureSnapshot := validFeatureSnapshot(validRawSnapshot(validDatasetFile()))
		featureSnapshot.StorageLocation = "s3://local/features/unsupported-chunker.parquet"
		store.objects[featureSnapshot.StorageLocation] = rawArtifact.Data
		strategy := model.ApplyEmbeddingStrategyDefaults(model.EmbeddingStrategy{
			ChunkerName:         "not-a-real-chunker",
			EmbeddingProvider:   "tei",
			EmbeddingModel:      "test-model",
			EmbeddingDimensions: 4,
		})
		writer := materialization.NewEmbeddingWriter(store, &recordingEmbeddingProvider{dimensions: 4}, nil, strategy, "pgvector", 10)

		_, _, err = writer.MaterializeEmbeddings(ctx, featureSnapshot, validEmbeddingSnapshot(featureSnapshot))

		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrEmbeddingMaterialize)).To(BeTrue())
		Expect(err).To(MatchError(ContainSubstring("unsupported embedding chunker")))
	})

	It("handles empty embedding inputs without calling the HTTP service", func() {
		called := false
		provider := materialization.NewHTTPEmbeddingProviderWithClient("tei", "http://embedding-service", "bge-small", 2, &http.Client{
			Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				called = true
				return embeddingHTTPResponse(http.StatusOK, "[]"), nil
			}),
			Timeout: time.Second,
		})

		vectors, err := provider.Embed(context.Background(), nil)

		Expect(err).NotTo(HaveOccurred())
		Expect(vectors).To(BeEmpty())
		Expect(called).To(BeFalse())
	})

	It("normalizes JSON object and JSONL artifacts to Parquet", func() {
		ctx := context.Background()
		objectArtifact, err := materialization.NormalizeArtifactToParquet(ctx, []byte(`{"Title":"Intro","nested":{"chapter":1}}`), "application/json", "json")
		Expect(err).NotTo(HaveOccurred())
		Expect(objectArtifact.SchemaMetadata).To(ContainSubstring(`"source_format":"json"`))
		objectRows, err := materialization.ExtractTextRowsFromParquet(ctx, objectArtifact.Data, 10)
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.Join(objectRows, " ")).To(ContainSubstring(`{"chapter":1}`))

		jsonlArtifact, err := materialization.NormalizeArtifactToParquet(ctx, []byte("{\"title\":\"First\"}\n{\"title\":\"Second\"}\n"), "application/jsonl", "jsonl")
		Expect(err).NotTo(HaveOccurred())
		Expect(jsonlArtifact.RowCount).To(Equal(int64(2)))
		jsonlRows, err := materialization.ExtractTextRowsFromParquet(ctx, jsonlArtifact.Data, 10)
		Expect(err).NotTo(HaveOccurred())
		Expect(jsonlRows).To(Equal([]string{"First", "Second"}))
	})

	It("rejects malformed or scalar raw artifacts", func() {
		_, err := materialization.NormalizeArtifactToParquet(context.Background(), []byte("[]"), "application/octet-stream", "bin")
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())

		_, err = materialization.NormalizeArtifactToParquet(context.Background(), []byte(`["not-object"]`), "application/json", "json")
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())

		_, err = materialization.NormalizeArtifactToParquet(context.Background(), []byte("   \n"), "text/plain", "txt")
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
	})

	It("merges source schema metadata and rejects malformed metadata", func() {
		merged, err := materialization.MergeSourceSchemaMetadata(
			`{"format":"arrow","rows":1}`,
			`{"source_format":"pdf","source_page_count":3,"extractor_name":"test-extractor","ignored":"x"}`,
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(merged).To(ContainSubstring(`"source_format":"pdf"`))
		Expect(merged).To(ContainSubstring(`"source_page_count":3`))
		Expect(merged).To(ContainSubstring(`"extractor_name":"test-extractor"`))
		Expect(merged).NotTo(ContainSubstring("ignored"))

		_, err = materialization.MergeSourceSchemaMetadata(`{`, `{"source_format":"pdf"}`)
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrArtifactRead)).To(BeTrue())

		_, err = materialization.MergeSourceSchemaMetadata(`{"format":"arrow"}`, `{`)
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrArtifactRead)).To(BeTrue())
	})

	It("surfaces Iceberg writer command output decoding failures", func() {
		writer := materialization.NewExternalIcebergTableWriter(materialization.ExternalIcebergTableWriterConfig{
			BinaryPath:        "/bin/echo",
			Timeout:           time.Second,
			PolarisBaseURL:    "http://polaris",
			PolarisCatalog:    "bighill",
			PolarisWarehouse:  "s3://warehouse/",
			PolarisCredential: "client:secret",
			PolarisScope:      "PRINCIPAL_ROLE:ALL",
			S3Endpoint:        "http://object-store",
			S3AccessKeyID:     "access",
			S3SecretAccessKey: "secret",
			S3Region:          "local-dev",
			S3PathStyle:       true,
		})

		_, err := writer.WriteTable(context.Background(), materialization.IcebergTableWriteRequest{
			Namespace:   "features",
			Table:       "movies",
			ParquetData: []byte("PAR1fakePAR1"),
		})

		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrCatalogRegister)).To(BeTrue())
		Expect(err).To(MatchError(ContainSubstring("decode iceberg writer result")))
	})

})
