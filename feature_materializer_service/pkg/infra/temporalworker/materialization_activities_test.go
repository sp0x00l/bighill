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
	called      bool
}

func (s *rawUsecaseStub) MaterializeRawSnapshot(context.Context, *model.DatasetFile, uuid.UUID) (*model.RawSnapshot, error) {
	s.called = true
	if s.err != nil {
		return nil, s.err
	}
	return s.rawSnapshot, nil
}

type featureUsecaseStub struct {
	featureSnapshot *model.FeatureSnapshot
	err             error
	called          bool
}

func (s *featureUsecaseStub) BuildFeatureSnapshot(context.Context, uuid.UUID, uuid.UUID) (*model.FeatureSnapshot, error) {
	s.called = true
	if s.err != nil {
		return nil, s.err
	}
	return s.featureSnapshot, nil
}

type embeddingUsecaseStub struct {
	embeddingSnapshot *model.EmbeddingSnapshot
	err               error
	called            bool
	strategy          model.EmbeddingStrategy
}

func (s *embeddingUsecaseStub) MaterializeEmbeddings(_ context.Context, _ uuid.UUID, _ uuid.UUID, strategy model.EmbeddingStrategy) (*model.EmbeddingSnapshot, error) {
	s.called = true
	s.strategy = strategy
	if s.err != nil {
		return nil, s.err
	}
	return s.embeddingSnapshot, nil
}

type graphUsecaseStub struct {
	graphSnapshot *model.GraphSnapshot
	err           error
	called        bool
	strategy      model.GraphExtractionStrategy
}

func (s *graphUsecaseStub) MaterializeGraph(_ context.Context, _ uuid.UUID, _ uuid.UUID, strategy model.GraphExtractionStrategy) (*model.GraphSnapshot, error) {
	s.called = true
	s.strategy = strategy
	if s.err != nil {
		return nil, s.err
	}
	return s.graphSnapshot, nil
}

var _ = Describe("MaterializationActivities", func() {
	It("returns existing raw snapshot records for idempotent activity replays", func() {
		existing := validRawSnapshot()
		rawUsecase := &rawUsecaseStub{err: &domain.RawSnapshotAlreadyMaterializedError{Record: existing}}
		activities := featuretemporal.NewMaterializationActivities(
			rawUsecase,
			nil,
			nil,
			nil,
		)

		rawSnapshot, err := activities.MaterializeRawSnapshot(context.Background(), usecase.MaterializeRawSnapshotActivityInput{
			DatasetFile:    validDatasetFile(),
			IdempotencyKey: uuid.New(),
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(rawSnapshot).To(Equal(existing))
		Expect(rawUsecase.called).To(BeTrue())
	})

	It("returns transient raw snapshot errors for Temporal retry", func() {
		expectedErr := errors.New("object store unavailable")
		rawUsecase := &rawUsecaseStub{err: expectedErr}
		activities := featuretemporal.NewMaterializationActivities(rawUsecase, nil, nil, nil)

		rawSnapshot, err := activities.MaterializeRawSnapshot(context.Background(), usecase.MaterializeRawSnapshotActivityInput{
			DatasetFile:    validDatasetFile(),
			IdempotencyKey: uuid.New(),
		})

		Expect(rawSnapshot).To(BeNil())
		Expect(errors.Is(err, expectedErr)).To(BeTrue())
		Expect(rawUsecase.called).To(BeTrue())
	})

	It("rejects invalid raw snapshot activity input at the Temporal boundary", func() {
		rawUsecase := &rawUsecaseStub{rawSnapshot: validRawSnapshot()}
		activities := featuretemporal.NewMaterializationActivities(rawUsecase, nil, nil, nil)
		input := usecase.MaterializeRawSnapshotActivityInput{
			DatasetFile:    validDatasetFile(),
			IdempotencyKey: uuid.New(),
		}
		input.DatasetFile.OrgID = uuid.Nil

		rawSnapshot, err := activities.MaterializeRawSnapshot(context.Background(), input)

		Expect(rawSnapshot).To(BeNil())
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
		Expect(err.Error()).To(ContainSubstring("org_id is required"))
		Expect(rawUsecase.called).To(BeFalse())
	})

	It("rejects invalid feature snapshot activity input at the Temporal boundary", func() {
		featureUsecase := &featureUsecaseStub{featureSnapshot: validFeatureSnapshot(uuid.New())}
		activities := featuretemporal.NewMaterializationActivities(nil, featureUsecase, nil, nil)

		featureSnapshot, err := activities.BuildFeatureSnapshot(context.Background(), usecase.BuildFeatureSnapshotActivityInput{
			RawSnapshotID:  uuid.Nil,
			UserID:         uuid.New(),
			OrgID:          uuid.New(),
			IdempotencyKey: uuid.New(),
		})

		Expect(featureSnapshot).To(BeNil())
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
		Expect(err.Error()).To(ContainSubstring("raw_snapshot_id is required"))
		Expect(featureUsecase.called).To(BeFalse())
	})

	It("normalizes and validates embedding activity strategy at the Temporal boundary", func() {
		embeddingUsecase := &embeddingUsecaseStub{embeddingSnapshot: validEmbeddingSnapshot(uuid.New())}
		activities := featuretemporal.NewMaterializationActivities(nil, nil, embeddingUsecase, nil)
		featureSnapshotID := uuid.New()

		embeddingSnapshot, err := activities.MaterializeEmbeddings(context.Background(), usecase.MaterializeEmbeddingsActivityInput{
			FeatureSnapshotID: featureSnapshotID,
			UserID:            uuid.New(),
			OrgID:             uuid.New(),
			IdempotencyKey:    uuid.New(),
			Strategy: model.ApplyEmbeddingStrategyDefaults(model.EmbeddingStrategy{
				EmbeddingProvider: " TEI ",
				EmbeddingModel:    "bge-small-en-v1.5",
			}),
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(embeddingSnapshot).To(Equal(embeddingUsecase.embeddingSnapshot))
		Expect(embeddingUsecase.called).To(BeTrue())
		Expect(embeddingUsecase.strategy.EmbeddingProvider).To(Equal("tei"))
	})

	It("rejects invalid embedding activity strategy before invoking the usecase", func() {
		embeddingUsecase := &embeddingUsecaseStub{embeddingSnapshot: validEmbeddingSnapshot(uuid.New())}
		activities := featuretemporal.NewMaterializationActivities(nil, nil, embeddingUsecase, nil)

		embeddingSnapshot, err := activities.MaterializeEmbeddings(context.Background(), usecase.MaterializeEmbeddingsActivityInput{
			FeatureSnapshotID: uuid.New(),
			UserID:            uuid.New(),
			OrgID:             uuid.New(),
			IdempotencyKey:    uuid.New(),
			Strategy:          model.EmbeddingStrategy{EmbeddingProvider: "tei"},
		})

		Expect(embeddingSnapshot).To(BeNil())
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
		Expect(err.Error()).To(ContainSubstring("strategy_version is required"))
		Expect(embeddingUsecase.called).To(BeFalse())
	})
})

func validDatasetFile() model.DatasetFile {
	return model.DatasetFile{
		DatasetID:       uuid.New(),
		UserID:          uuid.New(),
		OrgID:           uuid.New(),
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
		OrgID:           uuid.New(),
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
		OrgID:             uuid.New(),
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
		OrgID:               uuid.New(),
		VectorStore:         "pgvector",
		CollectionName:      "movies",
		Status:              model.SnapshotStatusReady,
	}
}
