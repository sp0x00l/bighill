package app_test

import (
	"context"
	"errors"
	"testing"

	usecase "feature_materializer_service/pkg/app"
	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestApp(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Feature materializer app unit test suite")
}

type rawSnapshotRepoStub struct {
	rawSnapshot *model.RawSnapshot
	saveErr     error
	readyID     uuid.UUID
	failedID    uuid.UUID
	failure     string
}

func (s *rawSnapshotRepoStub) SavePendingRawSnapshot(_ context.Context, _ *model.DatasetFile, _ uuid.UUID) (*model.RawSnapshot, error) {
	if s.saveErr != nil {
		return nil, s.saveErr
	}
	if s.rawSnapshot != nil {
		return s.rawSnapshot, nil
	}
	return validRawSnapshot(), nil
}

func (s *rawSnapshotRepoStub) MarkRawReady(_ context.Context, rawSnapshot *model.RawSnapshot) error {
	s.readyID = rawSnapshot.RawSnapshotID
	return nil
}

func (s *rawSnapshotRepoStub) MarkRawFailed(_ context.Context, rawSnapshotID uuid.UUID, reason string) error {
	s.failedID = rawSnapshotID
	s.failure = reason
	return nil
}

func (s *rawSnapshotRepoStub) ReadRawByIdempotencyKey(context.Context, uuid.UUID) (*model.RawSnapshot, error) {
	return s.rawSnapshot, nil
}

type rawSnapshotWriterStub struct {
	rawSnapshot *model.RawSnapshot
	err         error
}

func (s *rawSnapshotWriterStub) WriteRawSnapshot(_ context.Context, _ *model.DatasetFile, rawSnapshot *model.RawSnapshot) (*model.RawSnapshot, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.rawSnapshot != nil {
		return s.rawSnapshot, nil
	}
	rawSnapshot.StorageLocation = "s3://lakehouse/raw/snapshot.parquet"
	return rawSnapshot, nil
}

var _ = Describe("RawSnapshotUsecase", func() {
	It("saves a pending raw snapshot when no writer is configured", func() {
		repo := &rawSnapshotRepoStub{}
		uc := usecase.NewRawSnapshotUsecase(repo, nil)

		rawSnapshot, err := uc.MaterializeRawSnapshot(context.Background(), validDatasetFile(), uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Expect(rawSnapshot.Status).To(Equal(model.SnapshotStatusPending))
	})

	It("writes and marks a raw snapshot ready", func() {
		repo := &rawSnapshotRepoStub{rawSnapshot: validRawSnapshot()}
		writer := &rawSnapshotWriterStub{}
		uc := usecase.NewRawSnapshotUsecase(repo, writer)

		rawSnapshot, err := uc.MaterializeRawSnapshot(context.Background(), validDatasetFile(), uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Expect(repo.readyID).To(Equal(rawSnapshot.RawSnapshotID))
		Expect(rawSnapshot.StorageLocation).To(Equal("s3://lakehouse/raw/snapshot.parquet"))
	})

	It("returns replay records from repository idempotency errors", func() {
		existing := validRawSnapshot()
		repo := &rawSnapshotRepoStub{saveErr: &domain.RawSnapshotAlreadyMaterializedError{Record: existing}}
		uc := usecase.NewRawSnapshotUsecase(repo, nil)

		rawSnapshot, err := uc.MaterializeRawSnapshot(context.Background(), validDatasetFile(), uuid.New())

		Expect(err).To(HaveOccurred())
		Expect(rawSnapshot).To(Equal(existing))
	})

	It("marks failed when the writer fails", func() {
		expectedErr := errors.New("writer failed")
		repo := &rawSnapshotRepoStub{rawSnapshot: validRawSnapshot()}
		uc := usecase.NewRawSnapshotUsecase(repo, &rawSnapshotWriterStub{err: expectedErr})

		rawSnapshot, err := uc.MaterializeRawSnapshot(context.Background(), validDatasetFile(), uuid.New())

		Expect(rawSnapshot).To(BeNil())
		Expect(errors.Is(err, expectedErr)).To(BeTrue())
		Expect(errors.Is(err, domain.ErrRawSnapshotMaterialize)).To(BeTrue())
		Expect(repo.failedID).To(Equal(repo.rawSnapshot.RawSnapshotID))
		Expect(repo.failure).To(Equal(expectedErr.Error()))
	})
})

func validDatasetFile() *model.DatasetFile {
	return &model.DatasetFile{
		DatasetID:       uuid.New(),
		UserID:          uuid.New(),
		StorageLocation: "s3://local-dev-bucket/raw/file.csv",
		ContentType:     "text/csv",
		FileExtension:   "csv",
		TableNamespace:  "default",
		TableName:       "dataset_movies",
		TableFormat:     "PARQUET",
		CatalogProvider: "LOCAL",
	}
}

func validRawSnapshot() *model.RawSnapshot {
	datasetID := uuid.New()
	return &model.RawSnapshot{
		RawSnapshotID:   uuid.New(),
		DatasetID:       datasetID,
		UserID:          uuid.New(),
		StorageLocation: "s3://local-dev-bucket/raw/file.csv",
		ContentType:     "text/csv",
		FileExtension:   "csv",
		TableNamespace:  "default",
		TableName:       "dataset_movies",
		TableFormat:     "PARQUET",
		CatalogProvider: "LOCAL",
		Status:          model.SnapshotStatusPending,
	}
}
