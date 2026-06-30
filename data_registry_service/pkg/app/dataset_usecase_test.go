package usecase_test

import (
	"context"
	"errors"

	usecase "data_registry_service/pkg/app"
	"data_registry_service/pkg/domain/model"
	core "lib/shared_lib/transport"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type stubDatasetRepository struct {
	createDataset        *model.Dataset
	createIdempotencyKey uuid.UUID
	createErr            error

	readPublishedDatasets []*model.Dataset
	readPublishedTotal    int
	readPublishedErr      error

	readPublishedByIDDatasetID uuid.UUID
	readPublishedByIDDataset   *model.Dataset
	readPublishedByIDErr       error

	readDatasetID uuid.UUID
	readUserID    uuid.UUID
	readDataset   *model.Dataset
	readErr       error

	deleteDatasetID uuid.UUID
	deleteUserID    uuid.UUID
	deleteErr       error

	publishDatasetID uuid.UUID
	publishUserID    uuid.UUID
	publishErr       error

	updateProcessingDatasetID uuid.UUID
	updateProcessingUserID    uuid.UUID
	updateProcessingState     model.ProcessingState
	updateProcessingResult    *model.Dataset
	updateProcessingErr       error

	updateMaterializationDataset *model.Dataset
	updateMaterializationResult  *model.Dataset
	updateMaterializationErr     error

	replaceDataset *model.Dataset
	replaceResult  *model.Dataset
	replaceErr     error
}

func (s *stubDatasetRepository) Close() {}

func (s *stubDatasetRepository) Create(_ context.Context, dataset *model.Dataset, idempotencyKey uuid.UUID) error {
	s.createDataset = dataset
	s.createIdempotencyKey = idempotencyKey
	return s.createErr
}

func (s *stubDatasetRepository) ReadPublished(_ context.Context, _ core.Pagination, _ []model.Filter) ([]*model.Dataset, int, error) {
	return s.readPublishedDatasets, s.readPublishedTotal, s.readPublishedErr
}

func (s *stubDatasetRepository) ReadPublishedByID(_ context.Context, datasetID uuid.UUID) (*model.Dataset, error) {
	s.readPublishedByIDDatasetID = datasetID
	return s.readPublishedByIDDataset, s.readPublishedByIDErr
}

func (s *stubDatasetRepository) ReadPublishedByUserID(_ context.Context, _ uuid.UUID, _ core.Pagination, _ []model.Filter) ([]*model.Dataset, int, error) {
	return s.readPublishedDatasets, s.readPublishedTotal, s.readPublishedErr
}

func (s *stubDatasetRepository) Read(_ context.Context, _ uuid.UUID, _ core.Pagination, _ []model.Filter) ([]*model.Dataset, int, error) {
	return s.readPublishedDatasets, s.readPublishedTotal, s.readPublishedErr
}

func (s *stubDatasetRepository) ReadByID(_ context.Context, datasetID, userID uuid.UUID) (*model.Dataset, error) {
	s.readDatasetID = datasetID
	s.readUserID = userID
	return s.readDataset, s.readErr
}

func (s *stubDatasetRepository) Delete(_ context.Context, datasetID, userID uuid.UUID) error {
	s.deleteDatasetID = datasetID
	s.deleteUserID = userID
	return s.deleteErr
}

func (s *stubDatasetRepository) UpdatePublishedState(_ context.Context, datasetID, userID uuid.UUID) error {
	s.publishDatasetID = datasetID
	s.publishUserID = userID
	return s.publishErr
}

func (s *stubDatasetRepository) UpdateProcessingState(_ context.Context, datasetID, userID uuid.UUID, state model.ProcessingState) (*model.Dataset, error) {
	s.updateProcessingDatasetID = datasetID
	s.updateProcessingUserID = userID
	s.updateProcessingState = state
	return s.updateProcessingResult, s.updateProcessingErr
}

func (s *stubDatasetRepository) UpdateMaterializationMetadata(_ context.Context, dataset *model.Dataset) (*model.Dataset, error) {
	s.updateMaterializationDataset = dataset
	return s.updateMaterializationResult, s.updateMaterializationErr
}

func (s *stubDatasetRepository) Replace(_ context.Context, dataset *model.Dataset) (*model.Dataset, error) {
	s.replaceDataset = dataset
	return s.replaceResult, s.replaceErr
}

var _ = Describe("DatasetUsecase", func() {
	var (
		ctx       context.Context
		repo      *stubDatasetRepository
		uc        usecase.DatasetUsecase
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

	It("creates a dataset through the repository", func() {
		dataset := &model.Dataset{ID: datasetID, UserID: userID}
		idempotencyKey := uuid.New()

		Expect(uc.CreateDataset(ctx, dataset, idempotencyKey)).To(Succeed())
		Expect(repo.createDataset).To(Equal(dataset))
		Expect(repo.createIdempotencyKey).To(Equal(idempotencyKey))
	})

	It("returns repository create errors", func() {
		expectedErr := errors.New("create failed")
		repo.createErr = expectedErr

		Expect(uc.CreateDataset(ctx, &model.Dataset{}, uuid.New())).To(MatchError(expectedErr))
	})

	It("reads a published dataset by ID", func() {
		expected := &model.Dataset{ID: datasetID}
		repo.readPublishedByIDDataset = expected

		got, err := uc.ReadPublishedDatasetByID(ctx, datasetID)

		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(expected))
		Expect(repo.readPublishedByIDDatasetID).To(Equal(datasetID))
	})

	It("reads a user's dataset by ID", func() {
		expected := &model.Dataset{ID: datasetID, UserID: userID}
		repo.readDataset = expected

		got, err := uc.ReadDatasetForUser(ctx, datasetID, userID)

		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(expected))
		Expect(repo.readDatasetID).To(Equal(datasetID))
		Expect(repo.readUserID).To(Equal(userID))
	})

	It("deletes a dataset through the repository", func() {
		Expect(uc.DeleteDataset(ctx, datasetID, userID)).To(Succeed())
		Expect(repo.deleteDatasetID).To(Equal(datasetID))
		Expect(repo.deleteUserID).To(Equal(userID))
	})

	It("publishes a dataset through the repository", func() {
		Expect(uc.PublishDataset(ctx, datasetID, userID)).To(Succeed())
		Expect(repo.publishDatasetID).To(Equal(datasetID))
		Expect(repo.publishUserID).To(Equal(userID))
	})

	It("replaces a dataset through the repository", func() {
		replacement := &model.Dataset{ID: datasetID, UserID: userID, Title: "updated"}
		repo.replaceResult = replacement

		got, err := uc.ReplaceDataset(ctx, replacement)

		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(replacement))
		Expect(repo.replaceDataset).To(Equal(replacement))
	})

	It("advances dataset processing state through the repository", func() {
		existing := &model.Dataset{ID: datasetID, UserID: userID, ProcessingState: model.DatasetProcessingPending}
		updated := &model.Dataset{ID: datasetID, UserID: userID, ProcessingState: model.DatasetProcessingRawMaterialized}
		repo.readDataset = existing
		repo.updateProcessingResult = updated

		got, err := uc.AdvanceDatasetProcessingState(ctx, datasetID, userID, model.DatasetProcessingRawMaterialized)

		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(updated))
		Expect(repo.updateProcessingDatasetID).To(Equal(datasetID))
		Expect(repo.updateProcessingUserID).To(Equal(userID))
		Expect(repo.updateProcessingState).To(Equal(model.DatasetProcessingRawMaterialized))
	})

	It("does not downgrade dataset processing state for late events", func() {
		existing := &model.Dataset{ID: datasetID, UserID: userID, ProcessingState: model.DatasetProcessingEmbeddingsMaterialized}
		repo.readDataset = existing

		got, err := uc.AdvanceDatasetProcessingState(ctx, datasetID, userID, model.DatasetProcessingRawMaterialized)

		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(existing))
		Expect(repo.updateProcessingDatasetID).To(Equal(uuid.Nil))
	})

	It("records feature materialization metadata and advances processing state", func() {
		existing := &model.Dataset{ID: datasetID, UserID: userID, Title: "movies", ProcessingState: model.DatasetProcessingRawMaterialized}
		repo.readDataset = existing
		repo.updateMaterializationResult = existing
		materialized := &model.Dataset{
			ID:              datasetID,
			UserID:          userID,
			Location:        "s3://local-dev-bucket/lakehouse/features/data.parquet",
			TableNamespace:  "features",
			TableName:       "movies",
			TableFormat:     model.Parquet,
			CatalogProvider: model.LocalCatalog,
			SchemaVersion:   2,
			SchemaMetadata:  `{"columns":["title"]}`,
		}

		_, err := uc.RecordDatasetMaterialization(ctx, materialized, model.DatasetProcessingFeatureMaterialized)

		Expect(err).NotTo(HaveOccurred())
		Expect(repo.updateMaterializationDataset.Location).To(Equal(materialized.Location))
		Expect(repo.updateMaterializationDataset.TableNamespace).To(Equal("features"))
		Expect(repo.updateMaterializationDataset.ProcessingState).To(Equal(model.DatasetProcessingFeatureMaterialized))
		Expect(repo.updateMaterializationDataset.Title).To(Equal("movies"))
	})
})
