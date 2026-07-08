package app_test

import (
	"context"
	"errors"

	usecase "ingestion_service/pkg/app"
	"ingestion_service/pkg/domain/model"
	"lib/shared_lib/ctxutil"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type stubDatasetRepository struct {
	upsertDataset *model.Dataset
	upsertErr     error

	blacklistDatasetID uuid.UUID
	blacklistUserID    uuid.UUID
	blacklistErr       error

	deleteDatasetID uuid.UUID
	deleteUserID    uuid.UUID
	deleteErr       error

	readForUploadDatasetID uuid.UUID
	readForUploadUserID    uuid.UUID
	readForUploadDataset   *model.Dataset
	readForUploadErr       error
}

func (s *stubDatasetRepository) Upsert(_ context.Context, dataset *model.Dataset) error {
	s.upsertDataset = dataset
	return s.upsertErr
}

func (s *stubDatasetRepository) BlacklistDataset(_ context.Context, datasetID, userID uuid.UUID) error {
	s.blacklistDatasetID = datasetID
	s.blacklistUserID = userID
	return s.blacklistErr
}

func (s *stubDatasetRepository) DeleteDataset(_ context.Context, datasetID, userID uuid.UUID) error {
	s.deleteDatasetID = datasetID
	s.deleteUserID = userID
	return s.deleteErr
}

func (s *stubDatasetRepository) ReadForUpload(_ context.Context, datasetID, userID uuid.UUID) (*model.Dataset, error) {
	s.readForUploadDatasetID = datasetID
	s.readForUploadUserID = userID
	return s.readForUploadDataset, s.readForUploadErr
}

var _ = Describe("DatasetUsecase", func() {
	var (
		ctx       context.Context
		repo      *stubDatasetRepository
		uc        *usecase.DatasetUsecase
		datasetID uuid.UUID
		userID    uuid.UUID
		orgID     uuid.UUID
	)

	BeforeEach(func() {
		ctx = context.Background()
		repo = &stubDatasetRepository{}
		uc = usecase.NewDatasetUseCase(repo)
		datasetID = uuid.New()
		userID = uuid.New()
		orgID = uuid.New()
	})

	It("adds a dataset by upserting the local projection", func() {
		dataset := &model.Dataset{DatasetID: datasetID, UserID: userID, OrgID: orgID}

		Expect(uc.AddDataset(ctx, dataset)).To(Succeed())
		Expect(repo.upsertDataset).To(Equal(dataset))
	})

	It("returns repository errors when adding a dataset projection fails", func() {
		expectedErr := errors.New("upsert failed")
		repo.upsertErr = expectedErr

		Expect(uc.AddDataset(ctx, &model.Dataset{DatasetID: datasetID, UserID: userID, OrgID: orgID})).To(MatchError(expectedErr))
	})

	It("updates a dataset through the repository", func() {
		dataset := &model.Dataset{DatasetID: datasetID, UserID: userID, OrgID: orgID}

		Expect(uc.UpdateDataset(ctx, dataset)).To(Succeed())
		Expect(repo.upsertDataset).To(Equal(dataset))
	})

	It("reads dataset metadata for upload", func() {
		expected := &model.Dataset{DatasetID: datasetID, UserID: userID}
		repo.readForUploadDataset = expected

		got, err := uc.DatasetForUpload(ctxutil.WithActorOrg(ctx, userID, orgID), datasetID, userID)

		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(expected))
		Expect(repo.readForUploadDatasetID).To(Equal(datasetID))
		Expect(repo.readForUploadUserID).To(Equal(userID))
	})

	It("rejects dataset upload reads without org context", func() {
		got, err := uc.DatasetForUpload(ctx, datasetID, userID)

		Expect(got).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring("org id is required")))
		Expect(repo.readForUploadDatasetID).To(Equal(uuid.Nil))
	})

	It("blacklists a dataset through the repository", func() {
		Expect(uc.BlacklistDataset(ctxutil.WithActorOrg(ctx, userID, orgID), datasetID, userID)).To(Succeed())
		Expect(repo.blacklistDatasetID).To(Equal(datasetID))
		Expect(repo.blacklistUserID).To(Equal(userID))
	})

	It("rejects dataset blacklist without org context", func() {
		err := uc.BlacklistDataset(ctx, datasetID, userID)

		Expect(err).To(MatchError(ContainSubstring("org id is required")))
		Expect(repo.blacklistDatasetID).To(Equal(uuid.Nil))
	})

	It("deletes a dataset through the repository", func() {
		Expect(uc.DeleteDataset(ctxutil.WithActorOrg(ctx, userID, orgID), datasetID, userID)).To(Succeed())
		Expect(repo.deleteDatasetID).To(Equal(datasetID))
		Expect(repo.deleteUserID).To(Equal(userID))
	})

	It("rejects dataset delete without org context", func() {
		err := uc.DeleteDataset(ctx, datasetID, userID)

		Expect(err).To(MatchError(ContainSubstring("org id is required")))
		Expect(repo.deleteDatasetID).To(Equal(uuid.Nil))
	})
})
