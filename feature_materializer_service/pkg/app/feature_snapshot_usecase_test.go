package app_test

import (
	"context"
	"errors"

	usecase "feature_materializer_service/pkg/app"
	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type featureSnapshotRepoStub struct {
	featureSnapshot *model.FeatureSnapshot
	saveErr         error
	readyID         uuid.UUID
	failedID        uuid.UUID
	failure         string
	failedErr       error
}

func (s *featureSnapshotRepoStub) SavePendingFeatureSnapshot(_ context.Context, _ pgx.Tx, rawSnapshotID, _ uuid.UUID) (*model.FeatureSnapshot, error) {
	if s.saveErr != nil {
		return nil, s.saveErr
	}
	if s.featureSnapshot != nil {
		return s.featureSnapshot, nil
	}
	return validFeatureSnapshot(rawSnapshotID), nil
}

func (s *featureSnapshotRepoStub) MarkFeatureReady(_ context.Context, _ pgx.Tx, featureSnapshot *model.FeatureSnapshot) error {
	s.readyID = featureSnapshot.FeatureSnapshotID
	featureSnapshot.MaterializationEventSeq = 1
	return nil
}

func (s *featureSnapshotRepoStub) MarkFeatureFailed(_ context.Context, _ pgx.Tx, featureSnapshotID uuid.UUID, reason string) error {
	s.failedID = featureSnapshotID
	s.failure = reason
	return s.failedErr
}

func (s *featureSnapshotRepoStub) ReadFeatureByIdempotencyKey(context.Context, uuid.UUID) (*model.FeatureSnapshot, error) {
	return s.featureSnapshot, nil
}

type rawSnapshotReaderStub struct {
	rawSnapshot *model.RawSnapshot
	err         error
}

func (s rawSnapshotReaderStub) ReadRawSnapshot(context.Context, uuid.UUID) (*model.RawSnapshot, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.rawSnapshot, nil
}

type featureSnapshotBuilderStub struct {
	featureSnapshot *model.FeatureSnapshot
	err             error
}

func (s featureSnapshotBuilderStub) BuildFeatureSnapshot(_ context.Context, _ *model.RawSnapshot, featureSnapshot *model.FeatureSnapshot) (*model.FeatureSnapshot, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.featureSnapshot != nil {
		return s.featureSnapshot, nil
	}
	featureSnapshot.StorageLocation = "s3://lakehouse/features/snapshot.parquet"
	return featureSnapshot, nil
}

var _ = Describe("FeatureSnapshotUsecase", func() {
	It("builds and marks a feature snapshot ready", func() {
		rawSnapshot := validRawSnapshot()
		featureSnapshot := validFeatureSnapshot(rawSnapshot.RawSnapshotID)
		repo := &featureSnapshotRepoStub{featureSnapshot: featureSnapshot}
		uc := usecase.NewFeatureSnapshotUsecase(repo, &snapshotUnitOfWorkStub{}, snapshotEventBuilderStub{}, rawSnapshotReaderStub{rawSnapshot: rawSnapshot}, featureSnapshotBuilderStub{})

		result, err := uc.BuildFeatureSnapshot(context.Background(), rawSnapshot.RawSnapshotID, uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Expect(repo.readyID).To(Equal(featureSnapshot.FeatureSnapshotID))
		Expect(result.StorageLocation).To(Equal("s3://lakehouse/features/snapshot.parquet"))
	})

	It("returns replay records from repository idempotency errors", func() {
		existing := validFeatureSnapshot(uuid.New())
		repo := &featureSnapshotRepoStub{saveErr: &domain.FeatureSnapshotAlreadyBuiltError{Record: existing}}
		uc := usecase.NewFeatureSnapshotUsecase(repo, &snapshotUnitOfWorkStub{}, snapshotEventBuilderStub{}, nil, nil)

		featureSnapshot, err := uc.BuildFeatureSnapshot(context.Background(), uuid.New(), uuid.New())

		Expect(err).To(HaveOccurred())
		Expect(featureSnapshot).To(Equal(existing))
	})

	It("marks failed when the builder fails", func() {
		expectedErr := errors.New("builder failed")
		rawSnapshot := validRawSnapshot()
		repo := &featureSnapshotRepoStub{featureSnapshot: validFeatureSnapshot(rawSnapshot.RawSnapshotID)}
		uc := usecase.NewFeatureSnapshotUsecase(repo, &snapshotUnitOfWorkStub{}, snapshotEventBuilderStub{}, rawSnapshotReaderStub{rawSnapshot: rawSnapshot}, featureSnapshotBuilderStub{err: expectedErr})

		result, err := uc.BuildFeatureSnapshot(context.Background(), rawSnapshot.RawSnapshotID, uuid.New())

		Expect(result).To(BeNil())
		Expect(errors.Is(err, expectedErr)).To(BeTrue())
		Expect(errors.Is(err, domain.ErrFeatureSnapshotBuild)).To(BeTrue())
		Expect(repo.failedID).To(Equal(repo.featureSnapshot.FeatureSnapshotID))
	})

	It("returns the failure-state write error when marking a feature snapshot failed is unsuccessful", func() {
		builderErr := errors.New("builder failed")
		markErr := errors.New("mark failed")
		rawSnapshot := validRawSnapshot()
		repo := &featureSnapshotRepoStub{featureSnapshot: validFeatureSnapshot(rawSnapshot.RawSnapshotID), failedErr: markErr}
		uc := usecase.NewFeatureSnapshotUsecase(repo, &snapshotUnitOfWorkStub{}, snapshotEventBuilderStub{}, rawSnapshotReaderStub{rawSnapshot: rawSnapshot}, featureSnapshotBuilderStub{err: builderErr})

		result, err := uc.BuildFeatureSnapshot(context.Background(), rawSnapshot.RawSnapshotID, uuid.New())

		Expect(result).To(BeNil())
		Expect(errors.Is(err, builderErr)).To(BeTrue())
		Expect(errors.Is(err, markErr)).To(BeTrue())
		Expect(errors.Is(err, domain.ErrFeatureSnapshotBuild)).To(BeTrue())
	})
})

func validFeatureSnapshot(rawSnapshotID uuid.UUID) *model.FeatureSnapshot {
	return &model.FeatureSnapshot{
		FeatureSnapshotID: uuid.New(),
		RawSnapshotID:     rawSnapshotID,
		DatasetID:         uuid.New(),
		UserID:            uuid.New(),
		OrgID:             uuid.New(),
		TableNamespace:    "default",
		TableName:         "dataset_movies",
		TableFormat:       "PARQUET",
		CatalogProvider:   "LOCAL",
		Status:            model.SnapshotStatusPending,
	}
}
