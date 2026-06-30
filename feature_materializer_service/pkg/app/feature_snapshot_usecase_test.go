package app_test

import (
	"context"
	"errors"

	usecase "feature_materializer_service/pkg/app"
	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type featureSnapshotRepoStub struct {
	featureSnapshot *model.FeatureSnapshot
	saveErr         error
	readyID         uuid.UUID
	failedID        uuid.UUID
	failure         string
}

func (s *featureSnapshotRepoStub) SavePendingFeatureSnapshot(_ context.Context, rawSnapshotID, _ uuid.UUID) (*model.FeatureSnapshot, error) {
	if s.saveErr != nil {
		return nil, s.saveErr
	}
	if s.featureSnapshot != nil {
		return s.featureSnapshot, nil
	}
	return validFeatureSnapshot(rawSnapshotID), nil
}

func (s *featureSnapshotRepoStub) MarkFeatureReady(_ context.Context, featureSnapshot *model.FeatureSnapshot) error {
	s.readyID = featureSnapshot.FeatureSnapshotID
	return nil
}

func (s *featureSnapshotRepoStub) MarkFeatureFailed(_ context.Context, featureSnapshotID uuid.UUID, reason string) error {
	s.failedID = featureSnapshotID
	s.failure = reason
	return nil
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
	It("saves a pending feature snapshot when no builder is configured", func() {
		repo := &featureSnapshotRepoStub{}
		uc := usecase.NewFeatureSnapshotUsecase(repo, nil, nil)

		featureSnapshot, err := uc.BuildFeatureSnapshot(context.Background(), uuid.New(), uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Expect(featureSnapshot.Status).To(Equal(model.SnapshotStatusPending))
	})

	It("builds and marks a feature snapshot ready", func() {
		rawSnapshot := validRawSnapshot()
		featureSnapshot := validFeatureSnapshot(rawSnapshot.RawSnapshotID)
		repo := &featureSnapshotRepoStub{featureSnapshot: featureSnapshot}
		uc := usecase.NewFeatureSnapshotUsecase(repo, rawSnapshotReaderStub{rawSnapshot: rawSnapshot}, featureSnapshotBuilderStub{})

		result, err := uc.BuildFeatureSnapshot(context.Background(), rawSnapshot.RawSnapshotID, uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Expect(repo.readyID).To(Equal(featureSnapshot.FeatureSnapshotID))
		Expect(result.StorageLocation).To(Equal("s3://lakehouse/features/snapshot.parquet"))
	})

	It("returns replay records from repository idempotency errors", func() {
		existing := validFeatureSnapshot(uuid.New())
		repo := &featureSnapshotRepoStub{saveErr: &domain.FeatureSnapshotAlreadyBuiltError{Record: existing}}
		uc := usecase.NewFeatureSnapshotUsecase(repo, nil, nil)

		featureSnapshot, err := uc.BuildFeatureSnapshot(context.Background(), uuid.New(), uuid.New())

		Expect(err).To(HaveOccurred())
		Expect(featureSnapshot).To(Equal(existing))
	})

	It("marks failed when the builder fails", func() {
		expectedErr := errors.New("builder failed")
		rawSnapshot := validRawSnapshot()
		repo := &featureSnapshotRepoStub{featureSnapshot: validFeatureSnapshot(rawSnapshot.RawSnapshotID)}
		uc := usecase.NewFeatureSnapshotUsecase(repo, rawSnapshotReaderStub{rawSnapshot: rawSnapshot}, featureSnapshotBuilderStub{err: expectedErr})

		result, err := uc.BuildFeatureSnapshot(context.Background(), rawSnapshot.RawSnapshotID, uuid.New())

		Expect(result).To(BeNil())
		Expect(errors.Is(err, expectedErr)).To(BeTrue())
		Expect(errors.Is(err, domain.ErrFeatureSnapshotBuild)).To(BeTrue())
		Expect(repo.failedID).To(Equal(repo.featureSnapshot.FeatureSnapshotID))
	})
})

func validFeatureSnapshot(rawSnapshotID uuid.UUID) *model.FeatureSnapshot {
	return &model.FeatureSnapshot{
		FeatureSnapshotID: uuid.New(),
		RawSnapshotID:     rawSnapshotID,
		DatasetID:         uuid.New(),
		TableNamespace:    "default",
		TableName:         "dataset_movies",
		TableFormat:       "PARQUET",
		CatalogProvider:   "LOCAL",
		Status:            model.SnapshotStatusPending,
	}
}
