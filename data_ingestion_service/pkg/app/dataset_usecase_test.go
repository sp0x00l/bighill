package app_test

import (
	"context"
	"errors"

	usecase "data_ingestion_service/pkg/app"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type stubDatasetRepository struct {
	saveDatasetID uuid.UUID
	saveUserID    uuid.UUID
	saveErr       error

	isValidDatasetID uuid.UUID
	isValidUserID    uuid.UUID
	isValidValue     bool
	isValidErr       error

	blacklistDatasetID uuid.UUID
	blacklistUserID    uuid.UUID
	blacklistErr       error

	deleteDatasetID uuid.UUID
	deleteUserID    uuid.UUID
	deleteErr       error
}

func (s *stubDatasetRepository) Save(_ context.Context, datasetID, userID uuid.UUID) error {
	s.saveDatasetID = datasetID
	s.saveUserID = userID
	return s.saveErr
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

func (s *stubDatasetRepository) IsValid(_ context.Context, datasetID, userID uuid.UUID) (bool, error) {
	s.isValidDatasetID = datasetID
	s.isValidUserID = userID
	return s.isValidValue, s.isValidErr
}

var _ = Describe("DatasetUsecase", func() {
	var (
		ctx       context.Context
		repo      *stubDatasetRepository
		uc        *usecase.DatasetUsecase
		datasetID uuid.UUID
		userID    uuid.UUID
	)

	BeforeEach(func() {
		ctx = context.Background()
		repo = &stubDatasetRepository{}
		uc = usecase.NewDatasetUseCase(repo)
		datasetID = uuid.New()
		userID = uuid.New()
	})

	It("adds a dataset through the repository", func() {
		Expect(uc.AddDataset(ctx, datasetID, userID)).To(Succeed())
		Expect(repo.saveDatasetID).To(Equal(datasetID))
		Expect(repo.saveUserID).To(Equal(userID))
	})

	It("returns repository errors when adding a dataset fails", func() {
		expectedErr := errors.New("save failed")
		repo.saveErr = expectedErr

		Expect(uc.AddDataset(ctx, datasetID, userID)).To(MatchError(expectedErr))
	})

	It("checks whether a dataset can be uploaded", func() {
		repo.isValidValue = true

		valid, err := uc.IsValidForUpload(ctx, datasetID, userID)

		Expect(err).NotTo(HaveOccurred())
		Expect(valid).To(BeTrue())
		Expect(repo.isValidDatasetID).To(Equal(datasetID))
		Expect(repo.isValidUserID).To(Equal(userID))
	})

	It("blacklists a dataset through the repository", func() {
		Expect(uc.BlacklistDataset(ctx, datasetID, userID)).To(Succeed())
		Expect(repo.blacklistDatasetID).To(Equal(datasetID))
		Expect(repo.blacklistUserID).To(Equal(userID))
	})

	It("deletes a dataset through the repository", func() {
		Expect(uc.DeleteDataset(ctx, datasetID, userID)).To(Succeed())
		Expect(repo.deleteDatasetID).To(Equal(datasetID))
		Expect(repo.deleteUserID).To(Equal(userID))
	})
})
