package materialization_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

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

var _ = Describe("Materialization adapters", func() {
	It("normalizes CSV artifacts to Parquet with schema metadata", func() {
		artifact, err := materialization.NormalizeArtifactToParquet(context.Background(), []byte("title,views\nIntro,10\n"), "text/csv", "csv")

		Expect(err).NotTo(HaveOccurred())
		Expect(artifact.Data).NotTo(BeEmpty())
		Expect(artifact.SchemaVersion).To(Equal(1))
		Expect(artifact.SchemaMetadata).To(ContainSubstring("title"))
		Expect(artifact.RowCount).To(Equal(int64(1)))
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
		store.objects[rawSnapshot.StorageLocation] = rawArtifact.Data
		builder := materialization.NewFeatureSnapshotBuilder(store)
		featureSnapshot := validFeatureSnapshot(rawSnapshot)

		result, err := builder.BuildFeatureSnapshot(ctx, rawSnapshot, featureSnapshot)

		Expect(err).NotTo(HaveOccurred())
		Expect(result.Status).To(Equal(model.SnapshotStatusReady))
		Expect(result.StorageLocation).To(ContainSubstring("lakehouse/features/"))
	})

	It("materializes embeddings into the pgvector repository boundary", func() {
		ctx := context.Background()
		store := newMemoryArtifactStore()
		repo := &recordingEmbeddingRecordRepository{}
		provider := materialization.NewDeterministicEmbeddingProvider(8)
		writer := materialization.NewEmbeddingWriter(store, repo, provider, "pgvector", 10)
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
		Expect(repo.records).To(HaveLen(2))
		Expect(repo.records[0].Vector).To(HaveLen(8))
	})

})

func validDatasetFile() *model.DatasetFile {
	return &model.DatasetFile{
		DatasetID:       uuid.New(),
		UserID:          uuid.New(),
		StorageLocation: "s3://local/raw/file.csv",
		ContentType:     "text/csv",
		FileExtension:   "csv",
		TableNamespace:  "features",
		TableName:       "movies",
		TableFormat:     "PARQUET",
		CatalogProvider: "LOCAL",
	}
}

func validRawSnapshot(datasetFile *model.DatasetFile) *model.RawSnapshot {
	return &model.RawSnapshot{
		RawSnapshotID:   uuid.New(),
		DatasetID:       datasetFile.DatasetID,
		UserID:          datasetFile.UserID,
		StorageLocation: datasetFile.StorageLocation,
		ContentType:     datasetFile.ContentType,
		FileExtension:   datasetFile.FileExtension,
		TableNamespace:  datasetFile.TableNamespace,
		TableName:       datasetFile.TableName,
		TableFormat:     datasetFile.TableFormat,
		CatalogProvider: datasetFile.CatalogProvider,
		SchemaVersion:   1,
		SchemaMetadata:  "{}",
		Status:          model.SnapshotStatusPending,
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
		SchemaVersion:     rawSnapshot.SchemaVersion,
		SchemaMetadata:    rawSnapshot.SchemaMetadata,
		Status:            model.SnapshotStatusPending,
	}
}
