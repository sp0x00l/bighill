package temporalworker_test

import (
	"context"
	"errors"
	"testing"

	usecase "feature_materializer_service/pkg/app"
	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"
	featuretemporal "feature_materializer_service/pkg/infra/temporalworker"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestTemporalWorker(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Feature materializer Temporal worker unit test suite")
}

type rawUsecaseStub struct {
	rawSnapshot *model.RawSnapshot
	err         error
}

func (s rawUsecaseStub) MaterializeRawSnapshot(context.Context, *model.DatasetFile, uuid.UUID) (*model.RawSnapshot, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.rawSnapshot, nil
}

type featureUsecaseStub struct {
	featureSnapshot *model.FeatureSnapshot
	err             error
}

func (s featureUsecaseStub) BuildFeatureSnapshot(context.Context, uuid.UUID, uuid.UUID) (*model.FeatureSnapshot, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.featureSnapshot, nil
}

type embeddingUsecaseStub struct {
	embeddingSnapshot *model.EmbeddingSnapshot
	err               error
}

func (s embeddingUsecaseStub) MaterializeEmbeddings(context.Context, uuid.UUID, uuid.UUID, model.EmbeddingStrategy) (*model.EmbeddingSnapshot, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.embeddingSnapshot, nil
}

var _ = Describe("MaterializationActivities", func() {
	It("returns existing raw snapshot records for idempotent activity replays", func() {
		existing := validRawSnapshot()
		activities := featuretemporal.NewMaterializationActivities(
			rawUsecaseStub{err: &domain.RawSnapshotAlreadyMaterializedError{Record: existing}},
			nil,
			nil,
		)

		rawSnapshot, err := activities.MaterializeRawSnapshot(context.Background(), usecase.MaterializeRawSnapshotActivityInput{
			DatasetFile:    validDatasetFile(),
			IdempotencyKey: uuid.New(),
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(rawSnapshot).To(Equal(existing))
	})

	It("returns transient raw snapshot errors for Temporal retry", func() {
		expectedErr := errors.New("object store unavailable")
		activities := featuretemporal.NewMaterializationActivities(rawUsecaseStub{err: expectedErr}, nil, nil)

		rawSnapshot, err := activities.MaterializeRawSnapshot(context.Background(), usecase.MaterializeRawSnapshotActivityInput{
			DatasetFile:    validDatasetFile(),
			IdempotencyKey: uuid.New(),
		})

		Expect(rawSnapshot).To(BeNil())
		Expect(errors.Is(err, expectedErr)).To(BeTrue())
	})
})

func validDatasetFile() model.DatasetFile {
	return model.DatasetFile{
		DatasetID:       uuid.New(),
		UserID:          uuid.New(),
		StorageLocation: "s3://local-dev-bucket/raw/file.csv",
		ContentType:     "text/csv",
		FileExtension:   "csv",
		TableNamespace:  "features",
		TableName:       "movies",
		TableFormat:     "PARQUET",
		CatalogProvider: "LOCAL",
	}
}

func validRawSnapshot() *model.RawSnapshot {
	return &model.RawSnapshot{
		RawSnapshotID:   uuid.New(),
		DatasetID:       uuid.New(),
		UserID:          uuid.New(),
		StorageLocation: "s3://local-dev-bucket/lakehouse/raw/data.parquet",
		TableNamespace:  "features",
		TableName:       "movies",
		TableFormat:     "PARQUET",
		CatalogProvider: "LOCAL",
		Status:          model.SnapshotStatusReady,
	}
}

func validFeatureSnapshot(rawSnapshotID uuid.UUID) *model.FeatureSnapshot {
	return &model.FeatureSnapshot{
		FeatureSnapshotID: uuid.New(),
		RawSnapshotID:     rawSnapshotID,
		DatasetID:         uuid.New(),
		UserID:            uuid.New(),
		StorageLocation:   "s3://local-dev-bucket/lakehouse/features/data.parquet",
		TableNamespace:    "features",
		TableName:         "movies",
		TableFormat:       "PARQUET",
		CatalogProvider:   "LOCAL",
		Status:            model.SnapshotStatusReady,
	}
}

func validEmbeddingSnapshot(featureSnapshotID uuid.UUID) *model.EmbeddingSnapshot {
	return &model.EmbeddingSnapshot{
		EmbeddingSnapshotID: uuid.New(),
		FeatureSnapshotID:   featureSnapshotID,
		DatasetID:           uuid.New(),
		UserID:              uuid.New(),
		VectorStore:         "pgvector",
		CollectionName:      "movies",
		Status:              model.SnapshotStatusReady,
	}
}
