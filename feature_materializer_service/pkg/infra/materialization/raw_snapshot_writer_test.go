package materialization_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"
	"feature_materializer_service/pkg/infra/materialization"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMaterialization(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Feature materializer materialization unit test suite")
}

type memoryArtifactStore struct {
	objects map[string][]byte
	writes  map[string][]byte
}

func newMemoryArtifactStore() *memoryArtifactStore {
	return &memoryArtifactStore{
		objects: map[string][]byte{},
		writes:  map[string][]byte{},
	}
}

func (s *memoryArtifactStore) Read(_ context.Context, storageLocation string) ([]byte, error) {
	return s.objects[storageLocation], nil
}

func (s *memoryArtifactStore) Write(_ context.Context, key, _ string, body []byte) (string, error) {
	location := "s3://local-dev-bucket/" + key
	s.writes[location] = body
	return location, nil
}

type recordingEmbeddingRecordRepository struct {
	records []model.EmbeddingRecord
}

func (r *recordingEmbeddingRecordRepository) SaveEmbeddingRecords(_ context.Context, records []model.EmbeddingRecord) error {
	r.records = records
	return nil
}

type recordingRawSnapshotProcessor struct {
	profile  model.ProcessingProfile
	selected bool
}

func (p *recordingRawSnapshotProcessor) SupportsRawSnapshot(datasetFile *model.DatasetFile) bool {
	return datasetFile != nil && datasetFile.ProcessingProfile == p.profile
}

func (p *recordingRawSnapshotProcessor) WriteRawSnapshot(_ context.Context, _ *model.DatasetFile, rawSnapshot *model.RawSnapshot) (*model.RawSnapshot, error) {
	p.selected = true
	out := *rawSnapshot
	out.StorageLocation = "s3://local-dev-bucket/lakehouse/raw/selected.parquet"
	return &out, nil
}

type recordingEmbeddingProcessor struct {
	selected bool
}

func (p *recordingEmbeddingProcessor) SupportsEmbeddings(featureSnapshot *model.FeatureSnapshot) bool {
	return featureSnapshot != nil && featureSnapshot.ProcessingProfile == model.ProcessingProfileTextRAG
}

func (p *recordingEmbeddingProcessor) MaterializeEmbeddings(_ context.Context, _ *model.FeatureSnapshot, embeddingSnapshot *model.EmbeddingSnapshot) (*model.EmbeddingSnapshot, error) {
	p.selected = true
	out := *embeddingSnapshot
	out.VectorStore = "pgvector"
	return &out, nil
}

type fakeDocumentExtractor struct{}

func (e fakeDocumentExtractor) Name() string {
	return "fake-pdf-extractor"
}

func (e fakeDocumentExtractor) Version() string {
	return "test-v1"
}

func (e fakeDocumentExtractor) ExtractText(context.Context, []byte) (*materialization.DocumentExtraction, error) {
	return &materialization.DocumentExtraction{
		Text:      " Hello   PDF\n\ncontent ",
		PageCount: 1,
	}, nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

var _ = Describe("Materialization adapters", func() {
	It("normalizes CSV artifacts to Parquet with schema metadata", func() {
		artifact, err := materialization.NormalizeArtifactToParquet(context.Background(), []byte("title,views\nIntro,10\n"), "text/csv", "csv")

		Expect(err).NotTo(HaveOccurred())
		Expect(artifact.Data).NotTo(BeEmpty())
		Expect(artifact.SchemaVersion).To(Equal(1))
		Expect(artifact.SchemaMetadata).To(ContainSubstring("title"))
		Expect(artifact.RowCount).To(Equal(int64(1)))
	})

	It("normalizes PDF artifacts to source text Parquet with extractor metadata", func() {
		ctx := context.Background()

		artifact, err := materialization.NormalizeArtifactToParquetWithProcessors(ctx, []byte("%PDF-1.4"), "application/pdf", "pdf", fakeDocumentExtractor{}, materialization.NewBasicTextCleaner())

		Expect(err).NotTo(HaveOccurred())
		Expect(artifact.Data).NotTo(BeEmpty())
		Expect(artifact.SchemaMetadata).To(ContainSubstring("source_text"))
		Expect(artifact.SchemaMetadata).To(ContainSubstring("fake-pdf-extractor"))
		Expect(artifact.SchemaMetadata).To(ContainSubstring("go-basic-text-cleaner"))
		Expect(artifact.RowCount).To(Equal(int64(1)))

		rows, err := materialization.ExtractTextRowsFromParquet(ctx, artifact.Data, 10)
		Expect(err).NotTo(HaveOccurred())
		Expect(rows).To(Equal([]string{"Hello PDF content"}))
	})

	It("normalizes HTML artifacts to source text Parquet with extractor metadata", func() {
		ctx := context.Background()
		html := []byte("<!doctype html><html><head><style>.hidden{}</style><script>alert('x')</script></head><body><main><h1>Guide</h1><p>Clean HTML content.</p></main></body></html>")

		artifact, err := materialization.NormalizeArtifactToParquet(ctx, html, "text/html", "html")

		Expect(err).NotTo(HaveOccurred())
		Expect(artifact.Data).NotTo(BeEmpty())
		Expect(artifact.SchemaMetadata).To(ContainSubstring(`"source_format":"html"`))
		Expect(artifact.SchemaMetadata).To(ContainSubstring("go-html-text-extractor"))
		Expect(artifact.SchemaMetadata).To(ContainSubstring("go-basic-text-cleaner"))
		rows, err := materialization.ExtractTextRowsFromParquet(ctx, artifact.Data, 10)
		Expect(err).NotTo(HaveOccurred())
		Expect(rows).To(Equal([]string{"Guide Clean HTML content."}))
	})

	It("normalizes markdown artifacts with source metadata", func() {
		ctx := context.Background()

		artifact, err := materialization.NormalizeArtifactToParquet(ctx, []byte("# Guide\n\nA markdown document."), "text/markdown", "md")

		Expect(err).NotTo(HaveOccurred())
		Expect(artifact.SchemaMetadata).To(ContainSubstring(`"source_format":"markdown"`))
		rows, err := materialization.ExtractTextRowsFromParquet(ctx, artifact.Data, 10)
		Expect(err).NotTo(HaveOccurred())
		Expect(rows).To(Equal([]string{"# Guide A markdown document."}))
	})

	It("reads and writes artifacts through the local object store boundary", func() {
		ctx := context.Background()
		store, err := materialization.NewObjectArtifactStore(ctx, "local-dev-bucket", "local-dev", 10*1024*1024)
		Expect(err).NotTo(HaveOccurred())

		path := filepath.Join(GinkgoT().TempDir(), "input.csv")
		Expect(os.WriteFile(path, []byte("title\nIntro\n"), 0600)).To(Succeed())

		data, err := store.Read(ctx, path)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(data)).To(Equal("title\nIntro\n"))

		location, err := store.Write(ctx, "feature-materializer-test/object.csv", "text/csv", data)
		Expect(err).NotTo(HaveOccurred())
		Expect(location).To(Equal("s3://local-dev-bucket/feature-materializer-test/object.csv"))
	})

	It("writes a ready raw snapshot from an uploaded dataset file", func() {
		ctx := context.Background()
		store := newMemoryArtifactStore()
		datasetFile := validDatasetFile()
		rawSnapshot := validRawSnapshot(datasetFile)
		store.objects[datasetFile.StorageLocation] = []byte("title,views\nIntro,10\n")

		writer := materialization.NewRawSnapshotWriter(store)

		result, err := writer.WriteRawSnapshot(ctx, datasetFile, rawSnapshot)

		Expect(err).NotTo(HaveOccurred())
		Expect(result.Status).To(Equal(model.SnapshotStatusReady))
		Expect(result.StorageLocation).To(ContainSubstring("lakehouse/raw/"))
		Expect(store.writes[result.StorageLocation]).NotTo(BeEmpty())
	})

	It("builds a feature snapshot artifact", func() {
		ctx := context.Background()
		store := newMemoryArtifactStore()
		datasetFile := validDatasetFile()
		rawSnapshot := validRawSnapshot(datasetFile)
		rawSnapshot.StorageLocation = "s3://local/raw/snapshot.parquet"
		rawArtifact, err := materialization.NormalizeArtifactToParquet(ctx, []byte("title,views\nIntro,10\n"), "text/csv", "csv")
		Expect(err).NotTo(HaveOccurred())
		rawSnapshot.SchemaMetadata = rawArtifact.SchemaMetadata
		store.objects[rawSnapshot.StorageLocation] = rawArtifact.Data
		builder := materialization.NewFeatureSnapshotBuilder(store)
		featureSnapshot := validFeatureSnapshot(rawSnapshot)

		result, err := builder.BuildFeatureSnapshot(ctx, rawSnapshot, featureSnapshot)

		Expect(err).NotTo(HaveOccurred())
		Expect(result.Status).To(Equal(model.SnapshotStatusReady))
		Expect(result.StorageLocation).To(ContainSubstring("lakehouse/features/"))
		Expect(result.SchemaMetadata).To(ContainSubstring(`"source_format":"csv"`))
	})

	It("preserves source extraction metadata when building feature snapshots", func() {
		ctx := context.Background()
		store := newMemoryArtifactStore()
		datasetFile := validDatasetFile()
		rawSnapshot := validRawSnapshot(datasetFile)
		rawSnapshot.StorageLocation = "s3://local/raw/pdf-snapshot.parquet"
		rawArtifact, err := materialization.NormalizeArtifactToParquetWithProcessors(ctx, []byte("%PDF-1.4"), "application/pdf", "pdf", fakeDocumentExtractor{}, materialization.NewBasicTextCleaner())
		Expect(err).NotTo(HaveOccurred())
		rawSnapshot.SchemaMetadata = rawArtifact.SchemaMetadata
		store.objects[rawSnapshot.StorageLocation] = rawArtifact.Data
		builder := materialization.NewFeatureSnapshotBuilder(store)
		featureSnapshot := validFeatureSnapshot(rawSnapshot)

		result, err := builder.BuildFeatureSnapshot(ctx, rawSnapshot, featureSnapshot)

		Expect(err).NotTo(HaveOccurred())
		Expect(result.SchemaMetadata).To(ContainSubstring(`"source_format":"pdf"`))
		Expect(result.SchemaMetadata).To(ContainSubstring(`"source_page_count":1`))
		Expect(result.SchemaMetadata).To(ContainSubstring("fake-pdf-extractor"))
	})

	It("materializes embeddings into the pgvector repository boundary", func() {
		ctx := context.Background()
		store := newMemoryArtifactStore()
		repo := &recordingEmbeddingRecordRepository{}
		provider := materialization.NewDeterministicEmbeddingProvider(8)
		strategy := model.NormalizeEmbeddingStrategy(model.EmbeddingStrategy{
			StrategyVersion:     "rag-v1",
			ChunkerName:         "go-token-window",
			ChunkerVersion:      "v1",
			ChunkSize:           512,
			ChunkOverlap:        64,
			EmbeddingProvider:   "deterministic",
			EmbeddingModel:      "deterministic-test",
			EmbeddingDimensions: 8,
		})
		writer := materialization.NewEmbeddingWriter(store, repo, provider, nil, strategy, "pgvector", 10)
		rawArtifact, err := materialization.NormalizeArtifactToParquet(ctx, []byte("title,views\nIntro,10\nNext,20\n"), "text/csv", "csv")
		Expect(err).NotTo(HaveOccurred())
		featureSnapshot := validFeatureSnapshot(validRawSnapshot(validDatasetFile()))
		featureSnapshot.StorageLocation = "s3://local/features/snapshot.parquet"
		store.objects[featureSnapshot.StorageLocation] = rawArtifact.Data
		embeddingSnapshot := &model.EmbeddingSnapshot{EmbeddingSnapshotID: uuid.New(), FeatureSnapshotID: featureSnapshot.FeatureSnapshotID}

		result, err := writer.MaterializeEmbeddings(ctx, featureSnapshot, embeddingSnapshot)

		Expect(err).NotTo(HaveOccurred())
		Expect(result.Status).To(Equal(model.SnapshotStatusReady))
		Expect(result.EmbeddingDimensions).To(Equal(8))
		Expect(result.EmbeddingCount).To(Equal(int64(2)))
		Expect(result.StrategyVersion).To(Equal(strategy.StrategyVersion))
		Expect(result.EmbeddingProvider).To(Equal("deterministic"))
		Expect(result.ChunkSize).To(Equal(512))
		Expect(repo.records).To(HaveLen(2))
		Expect(repo.records[0].Vector).To(HaveLen(8))
		Expect(repo.records[0].ChunkIndex).To(Equal(0))
		Expect(repo.records[1].ChunkIndex).To(Equal(1))
	})

	It("chunks feature rows with a Go token window", func() {
		strategy := model.EmbeddingStrategy{ChunkSize: 3, ChunkOverlap: 1}
		chunker := materialization.NewTokenWindowChunker(strategy)

		chunks, err := chunker.Chunk(context.Background(), []string{"one two three four five"})

		Expect(err).NotTo(HaveOccurred())
		Expect(chunks).To(Equal([]materialization.TextChunk{
			{ChunkIndex: 0, Text: "one two three"},
			{ChunkIndex: 1, Text: "three four five"},
		}))
	})

	It("embeds through a TEI-compatible HTTP service", func() {
		provider := materialization.NewHTTPEmbeddingProviderWithClient("tei", "http://embedding-service", "bge-small", 2, &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				Expect(req.URL.Path).To(Equal("/embed"))
				body, err := json.Marshal([][]float32{{0.1, 0.2}, {0.3, 0.4}})
				Expect(err).NotTo(HaveOccurred())
				return embeddingHTTPResponse(http.StatusOK, string(body)), nil
			}),
			Timeout: time.Second,
		})

		vectors, err := provider.Embed(context.Background(), []string{"first", "second"})

		Expect(err).NotTo(HaveOccurred())
		Expect(vectors).To(Equal([][]float32{{0.1, 0.2}, {0.3, 0.4}}))
	})

	It("embeds through an Ollama-compatible HTTP service", func() {
		provider := materialization.NewHTTPEmbeddingProviderWithClient("ollama", "http://embedding-service", "bge-small", 2, &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				Expect(req.URL.Path).To(Equal("/api/embed"))
				body, err := json.Marshal(map[string]any{
					"embeddings": [][]float32{{0.1, 0.2}},
				})
				Expect(err).NotTo(HaveOccurred())
				return embeddingHTTPResponse(http.StatusOK, string(body)), nil
			}),
			Timeout: time.Second,
		})

		vectors, err := provider.Embed(context.Background(), []string{"first"})

		Expect(err).NotTo(HaveOccurred())
		Expect(vectors).To(Equal([][]float32{{0.1, 0.2}}))
	})

	It("rejects embedding service dimension mismatches", func() {
		provider := materialization.NewHTTPEmbeddingProviderWithClient("tei", "http://embedding-service", "bge-small", 2, &http.Client{
			Transport: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
				body, err := json.Marshal([][]float32{{0.1}})
				Expect(err).NotTo(HaveOccurred())
				return embeddingHTTPResponse(http.StatusOK, string(body)), nil
			}),
			Timeout: time.Second,
		})

		_, err := provider.Embed(context.Background(), []string{"first"})

		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrEmbeddingMaterialize)).To(BeTrue())
	})

	It("dispatches raw snapshot writes by processing profile", func() {
		datasetFile := validDatasetFile()
		datasetFile.ProcessingProfile = model.ProcessingProfileTextRAG
		rawSnapshot := validRawSnapshot(datasetFile)
		generic := &recordingRawSnapshotProcessor{profile: model.ProcessingProfileGenericParquet}
		rag := &recordingRawSnapshotProcessor{profile: model.ProcessingProfileTextRAG}
		dispatcher := materialization.NewRawSnapshotWriterDispatcher(generic, rag)

		result, err := dispatcher.WriteRawSnapshot(context.Background(), datasetFile, rawSnapshot)

		Expect(err).NotTo(HaveOccurred())
		Expect(result.StorageLocation).To(ContainSubstring("selected.parquet"))
		Expect(generic.selected).To(BeFalse())
		Expect(rag.selected).To(BeTrue())
	})

	It("rejects embedding materialization for profiles without an embedding processor", func() {
		dispatcher := materialization.NewEmbeddingWriterDispatcher(&recordingEmbeddingProcessor{})
		featureSnapshot := validFeatureSnapshot(validRawSnapshot(validDatasetFile()))
		featureSnapshot.ProcessingProfile = model.ProcessingProfileGenericParquet

		_, err := dispatcher.MaterializeEmbeddings(context.Background(), featureSnapshot, validEmbeddingSnapshot(featureSnapshot))

		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrEmbeddingMaterialize)).To(BeTrue())
	})
})

func embeddingHTTPResponse(statusCode int, body string) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func validDatasetFile() *model.DatasetFile {
	return &model.DatasetFile{
		DatasetID:         uuid.New(),
		UserID:            uuid.New(),
		StorageLocation:   "s3://local/raw/file.csv",
		ContentType:       "text/csv",
		FileExtension:     "csv",
		TableNamespace:    "features",
		TableName:         "movies",
		TableFormat:       "PARQUET",
		CatalogProvider:   "LOCAL",
		ProcessingProfile: model.ProcessingProfileGenericParquet,
	}
}

func validRawSnapshot(datasetFile *model.DatasetFile) *model.RawSnapshot {
	return &model.RawSnapshot{
		RawSnapshotID:     uuid.New(),
		DatasetID:         datasetFile.DatasetID,
		UserID:            datasetFile.UserID,
		StorageLocation:   datasetFile.StorageLocation,
		ContentType:       datasetFile.ContentType,
		FileExtension:     datasetFile.FileExtension,
		TableNamespace:    datasetFile.TableNamespace,
		TableName:         datasetFile.TableName,
		TableFormat:       datasetFile.TableFormat,
		CatalogProvider:   datasetFile.CatalogProvider,
		ProcessingProfile: datasetFile.ProcessingProfile,
		SchemaVersion:     1,
		SchemaMetadata:    "{}",
		Status:            model.SnapshotStatusPending,
	}
}

func validFeatureSnapshot(rawSnapshot *model.RawSnapshot) *model.FeatureSnapshot {
	return &model.FeatureSnapshot{
		FeatureSnapshotID: uuid.New(),
		RawSnapshotID:     rawSnapshot.RawSnapshotID,
		DatasetID:         rawSnapshot.DatasetID,
		UserID:            rawSnapshot.UserID,
		TableNamespace:    rawSnapshot.TableNamespace,
		TableName:         rawSnapshot.TableName,
		TableFormat:       rawSnapshot.TableFormat,
		CatalogProvider:   rawSnapshot.CatalogProvider,
		ProcessingProfile: rawSnapshot.ProcessingProfile,
		SchemaVersion:     rawSnapshot.SchemaVersion,
		SchemaMetadata:    rawSnapshot.SchemaMetadata,
		Status:            model.SnapshotStatusPending,
	}
}

func validEmbeddingSnapshot(featureSnapshot *model.FeatureSnapshot) *model.EmbeddingSnapshot {
	return &model.EmbeddingSnapshot{
		EmbeddingSnapshotID: uuid.New(),
		FeatureSnapshotID:   featureSnapshot.FeatureSnapshotID,
		DatasetID:           featureSnapshot.DatasetID,
		UserID:              featureSnapshot.UserID,
		Status:              model.SnapshotStatusPending,
	}
}
