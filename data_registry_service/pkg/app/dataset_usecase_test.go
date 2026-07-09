package usecase_test

import (
	"context"
	"errors"
	"testing"

	usecase "data_registry_service/pkg/app"
	"data_registry_service/pkg/domain/model"
	registrymessaging "data_registry_service/pkg/infra/network/messaging"
	datasetpb "lib/data_contracts_lib/data_registry"
	"lib/shared_lib/ctxutil"
	msgConn "lib/shared_lib/messaging"
	core "lib/shared_lib/transport"
	shareduow "lib/shared_lib/uow"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
)

func TestAppUseCases(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Data registry app unit test suite")
}

type stubDatasetRepository struct {
	createDataset        *model.Dataset
	createIdempotencyKey uuid.UUID
	createErr            error

	readDatasetID      uuid.UUID
	readUserID         uuid.UUID
	readDataset        *model.Dataset
	readManyUserID     uuid.UUID
	readManyPagination core.Pagination
	readManyFilters    []model.Filter
	readManyDatasets   []*model.Dataset
	readManyCount      int
	readErr            error

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
	updateProcessingChanged   bool
	updateProcessingErr       error

	updateMaterializationDataset *model.Dataset
	updateMaterializationState   model.ProcessingState
	updateMaterializationSeq     int64
	updateMaterializationResult  *model.Dataset
	updateMaterializationErr     error

	replaceDataset *model.Dataset
	replaceResult  *model.Dataset
	replaceErr     error
}

type stubDatasetTableCatalog struct {
	dataset *model.Dataset
	err     error
}

func (s *stubDatasetTableCatalog) ValidateDatasetTable(_ context.Context, dataset *model.Dataset) error {
	s.dataset = dataset
	return s.err
}

type stubDatasetUnitOfWork struct {
	calls      int
	messages   []msgConn.OutboundMessage
	enqueueErr error
}

func (s *stubDatasetUnitOfWork) Do(ctx context.Context, fn shareduow.TxFunc) error {
	s.calls++
	return fn(ctx, nil, func(message msgConn.OutboundMessage) error {
		if s.enqueueErr != nil {
			return s.enqueueErr
		}
		s.messages = append(s.messages, message)
		return nil
	})
}

func (s *stubDatasetRepository) Close() {}

func (s *stubDatasetRepository) Create(_ context.Context, _ pgx.Tx, dataset *model.Dataset, idempotencyKey uuid.UUID) error {
	s.createDataset = dataset
	s.createIdempotencyKey = idempotencyKey
	return s.createErr
}

func (s *stubDatasetRepository) Read(_ context.Context, userID uuid.UUID, pagination core.Pagination, filters []model.Filter) ([]*model.Dataset, int, error) {
	s.readManyUserID = userID
	s.readManyPagination = pagination
	s.readManyFilters = filters
	return s.readManyDatasets, s.readManyCount, s.readErr
}

func (s *stubDatasetRepository) ReadByID(_ context.Context, datasetID, userID uuid.UUID) (*model.Dataset, error) {
	s.readDatasetID = datasetID
	s.readUserID = userID
	return s.readDataset, s.readErr
}

func (s *stubDatasetRepository) Delete(_ context.Context, _ pgx.Tx, datasetID, userID uuid.UUID) error {
	s.deleteDatasetID = datasetID
	s.deleteUserID = userID
	return s.deleteErr
}

func (s *stubDatasetRepository) UpdatePublishedState(_ context.Context, _ pgx.Tx, datasetID, userID uuid.UUID) error {
	s.publishDatasetID = datasetID
	s.publishUserID = userID
	return s.publishErr
}

func (s *stubDatasetRepository) UpdateProcessingState(_ context.Context, _ pgx.Tx, datasetID, userID uuid.UUID, state model.ProcessingState) (*model.Dataset, bool, error) {
	s.updateProcessingDatasetID = datasetID
	s.updateProcessingUserID = userID
	s.updateProcessingState = state
	return s.updateProcessingResult, s.updateProcessingChanged, s.updateProcessingErr
}

func (s *stubDatasetRepository) RecordMaterialization(_ context.Context, _ pgx.Tx, dataset *model.Dataset, state model.ProcessingState, eventSeq int64) (*model.Dataset, bool, error) {
	s.updateMaterializationDataset = dataset
	s.updateMaterializationState = state
	s.updateMaterializationSeq = eventSeq
	return s.updateMaterializationResult, s.updateMaterializationResult != nil, s.updateMaterializationErr
}

func (s *stubDatasetRepository) Replace(_ context.Context, _ pgx.Tx, dataset *model.Dataset) (*model.Dataset, error) {
	s.replaceDataset = dataset
	return s.replaceResult, s.replaceErr
}

var _ = Describe("DatasetUsecase", func() {
	var (
		ctx       context.Context
		repo      *stubDatasetRepository
		work      *stubDatasetUnitOfWork
		uc        usecase.DatasetUsecase
		datasetID uuid.UUID
		userID    uuid.UUID
		orgID     uuid.UUID
	)

	BeforeEach(func() {
		userID = uuid.New()
		orgID = uuid.New()
		ctx = ctxutil.WithActorOrg(context.Background(), userID, orgID)
		repo = &stubDatasetRepository{}
		work = &stubDatasetUnitOfWork{}
		uc = usecase.NewDatasetUseCase(repo, work, registrymessaging.NewDatasetEventBuilder("data_registry"))
		datasetID = uuid.New()
	})

	It("creates a dataset through the repository", func() {
		dataset := &model.Dataset{ID: datasetID, UserID: userID}
		idempotencyKey := uuid.New()

		Expect(uc.CreateDataset(ctx, dataset, idempotencyKey)).To(Succeed())
		Expect(repo.createDataset).To(Equal(dataset))
		Expect(repo.createIdempotencyKey).To(Equal(idempotencyKey))
		Expect(work.calls).To(Equal(1))
		Expect(work.messages).To(HaveLen(1))
		Expect(work.messages[0].Topic).To(Equal("data_registry"))
		Expect(work.messages[0].Message.MsgType).To(Equal(msgConn.MsgTypeDatasetCreated))
		Expect(work.messages[0].Message.ResourceKey).To(Equal(dataset.ID))
		var event datasetpb.DatasetCreatedEvent
		Expect(proto.Unmarshal(work.messages[0].Message.Payload, &event)).To(Succeed())
		Expect(event.DatasetId).To(Equal(dataset.ID.String()))
		Expect(event.UserId).To(Equal(userID.String()))
		Expect(event.OrgId).To(Equal(orgID.String()))
	})

	It("returns repository create errors", func() {
		expectedErr := errors.New("create failed")
		repo.createErr = expectedErr

		Expect(uc.CreateDataset(ctx, &model.Dataset{}, uuid.New())).To(MatchError(expectedErr))
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

	It("reads a user's datasets with pagination and filters", func() {
		expected := []*model.Dataset{{ID: datasetID, UserID: userID, Title: "movies"}}
		repo.readManyDatasets = expected
		repo.readManyCount = 1
		filter := model.CategoryFilter{Values: []string{"movies"}}
		pagination := core.Pagination{Page: 2, Limit: 10}

		got, total, err := uc.ReadDatasetsForUser(ctx, userID, pagination, []model.Filter{filter})

		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(expected))
		Expect(total).To(Equal(1))
		Expect(repo.readManyUserID).To(Equal(userID))
		Expect(repo.readManyPagination).To(Equal(pagination))
		Expect(repo.readManyFilters).To(Equal([]model.Filter{filter}))
	})

	It("returns repository read-many errors", func() {
		expectedErr := errors.New("read failed")
		repo.readErr = expectedErr

		_, _, err := uc.ReadDatasetsForUser(ctx, userID, core.Pagination{Page: 1, Limit: 10}, nil)

		Expect(err).To(MatchError(expectedErr))
	})

	It("reads materialized dataset table metadata", func() {
		expected := &model.Dataset{
			ID:              datasetID,
			UserID:          userID,
			Location:        "s3://local-dev-bucket/lakehouse/features/data.parquet",
			TableNamespace:  "features",
			TableName:       "movies",
			ProcessingState: model.DatasetProcessingFeatureMaterialized,
		}
		repo.readDataset = expected

		got, err := uc.ReadDatasetTable(ctx, datasetID, userID, "")

		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(expected))
		Expect(repo.readDatasetID).To(Equal(datasetID))
		Expect(repo.readUserID).To(Equal(userID))
	})

	It("rejects dataset table reads before materialization", func() {
		repo.readDataset = &model.Dataset{
			ID:              datasetID,
			UserID:          userID,
			Location:        "s3://local-dev-bucket/lakehouse/raw/data.parquet",
			TableNamespace:  "raw",
			TableName:       "movies",
			ProcessingState: model.DatasetProcessingRawMaterialized,
		}

		_, err := uc.ReadDatasetTable(ctx, datasetID, userID, "")

		Expect(err).To(MatchError(ContainSubstring("dataset table is not materialized")))
	})

	It("reads materialized dataset table metadata pinned to the feature snapshot", func() {
		featureSnapshotID := uuid.New()
		expected := &model.Dataset{
			ID:                datasetID,
			UserID:            userID,
			Location:          "s3://local-dev-bucket/lakehouse/features/data.parquet",
			TableNamespace:    "features",
			TableName:         "movies",
			ProcessingState:   model.DatasetProcessingFeatureMaterialized,
			FeatureSnapshotID: featureSnapshotID,
		}
		repo.readDataset = expected

		got, err := uc.ReadDatasetTable(ctx, datasetID, userID, featureSnapshotID.String())

		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(expected))
		Expect(repo.readDatasetID).To(Equal(datasetID))
		Expect(repo.readUserID).To(Equal(userID))
	})

	It("rejects invalid snapshot-pinned dataset table reads", func() {
		repo.readDataset = &model.Dataset{
			ID:              datasetID,
			UserID:          userID,
			Location:        "s3://local-dev-bucket/lakehouse/features/data.parquet",
			TableNamespace:  "features",
			TableName:       "movies",
			ProcessingState: model.DatasetProcessingFeatureMaterialized,
		}

		_, err := uc.ReadDatasetTable(ctx, datasetID, userID, "123")

		Expect(err).To(MatchError(ContainSubstring("snapshot_id is invalid")))
		Expect(repo.readDatasetID).To(Equal(datasetID))
	})

	It("rejects unknown snapshot-pinned dataset table reads", func() {
		repo.readDataset = &model.Dataset{
			ID:                datasetID,
			UserID:            userID,
			Location:          "s3://local-dev-bucket/lakehouse/features/data.parquet",
			TableNamespace:    "features",
			TableName:         "movies",
			ProcessingState:   model.DatasetProcessingFeatureMaterialized,
			FeatureSnapshotID: uuid.New(),
		}

		_, err := uc.ReadDatasetTable(ctx, datasetID, userID, uuid.New().String())

		Expect(err).To(MatchError(ContainSubstring("dataset snapshot was not found")))
	})

	It("deletes a dataset through the repository", func() {
		Expect(uc.DeleteDataset(ctx, datasetID, userID)).To(Succeed())
		Expect(repo.deleteDatasetID).To(Equal(datasetID))
		Expect(repo.deleteUserID).To(Equal(userID))
		Expect(work.messages).To(HaveLen(1))
		Expect(work.messages[0].Message.MsgType).To(Equal(msgConn.MsgTypeDatasetDeleted))
		var event datasetpb.DatasetDeletedEvent
		Expect(proto.Unmarshal(work.messages[0].Message.Payload, &event)).To(Succeed())
		Expect(event.DatasetId).To(Equal(datasetID.String()))
		Expect(event.UserId).To(Equal(userID.String()))
		Expect(event.OrgId).To(Equal(orgID.String()))
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
		Expect(work.messages).To(HaveLen(1))
		Expect(work.messages[0].Message.MsgType).To(Equal(msgConn.MsgTypeDatasetUpdated))
		var event datasetpb.DatasetUpdatedEvent
		Expect(proto.Unmarshal(work.messages[0].Message.Payload, &event)).To(Succeed())
		Expect(event.DatasetId).To(Equal(datasetID.String()))
		Expect(event.UserId).To(Equal(userID.String()))
		Expect(event.OrgId).To(Equal(orgID.String()))
	})

	It("advances dataset processing state through the repository", func() {
		existing := &model.Dataset{ID: datasetID, UserID: userID, ProcessingState: model.DatasetProcessingPending}
		updated := &model.Dataset{ID: datasetID, UserID: userID, ProcessingState: model.DatasetProcessingRawMaterialized}
		repo.readDataset = existing
		repo.updateProcessingResult = updated
		repo.updateProcessingChanged = true

		got, err := uc.AdvanceDatasetProcessingState(ctx, datasetID, userID, model.DatasetProcessingRawMaterialized)

		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(updated))
		Expect(repo.updateProcessingDatasetID).To(Equal(datasetID))
		Expect(repo.updateProcessingUserID).To(Equal(userID))
		Expect(repo.updateProcessingState).To(Equal(model.DatasetProcessingRawMaterialized))
		Expect(work.messages).To(HaveLen(1))
		Expect(work.messages[0].Message.MsgType).To(Equal(msgConn.MsgTypeDatasetUpdated))
	})

	It("does not downgrade dataset processing state for late events", func() {
		existing := &model.Dataset{ID: datasetID, UserID: userID, ProcessingState: model.DatasetProcessingEmbeddingsMaterialized}
		repo.updateProcessingResult = existing

		got, err := uc.AdvanceDatasetProcessingState(ctx, datasetID, userID, model.DatasetProcessingRawMaterialized)

		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(existing))
		Expect(repo.updateProcessingDatasetID).To(Equal(datasetID))
		Expect(work.messages).To(BeEmpty())
	})

	It("records feature materialization metadata and advances processing state", func() {
		updated := &model.Dataset{ID: datasetID, UserID: userID, OrgID: orgID, Title: "movies", ProcessingState: model.DatasetProcessingFeatureMaterialized}
		repo.updateMaterializationResult = updated
		materialized := &model.Dataset{
			ID:                datasetID,
			UserID:            userID,
			Location:          "s3://local-dev-bucket/lakehouse/features/data.parquet",
			TableNamespace:    "features",
			TableName:         "movies",
			TableFormat:       model.Parquet,
			CatalogProvider:   model.LocalCatalog,
			SchemaVersion:     2,
			SchemaMetadata:    `{"columns":["title"]}`,
			RawSnapshotID:     uuid.New(),
			FeatureSnapshotID: uuid.New(),
		}
		updated.RawSnapshotID = materialized.RawSnapshotID
		updated.FeatureSnapshotID = materialized.FeatureSnapshotID
		updated.TableNamespace = materialized.TableNamespace
		updated.TableName = materialized.TableName
		updated.Location = materialized.Location

		got, err := uc.RecordDatasetMaterialization(ctx, materialized, model.DatasetProcessingFeatureMaterialized, 2)

		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(updated))
		Expect(repo.updateMaterializationState).To(Equal(model.DatasetProcessingFeatureMaterialized))
		Expect(repo.updateMaterializationSeq).To(Equal(int64(2)))
		Expect(repo.updateMaterializationDataset.Location).To(Equal(materialized.Location))
		Expect(repo.updateMaterializationDataset.TableNamespace).To(Equal("features"))
		Expect(repo.updateMaterializationDataset.RawSnapshotID).To(Equal(materialized.RawSnapshotID))
		Expect(repo.updateMaterializationDataset.FeatureSnapshotID).To(Equal(materialized.FeatureSnapshotID))
		Expect(work.messages).To(HaveLen(1))
		Expect(work.messages[0].Message.MsgType).To(Equal(msgConn.MsgTypeDatasetUpdated))
		var event datasetpb.DatasetUpdatedEvent
		Expect(proto.Unmarshal(work.messages[0].Message.Payload, &event)).To(Succeed())
		Expect(event.DatasetId).To(Equal(datasetID.String()))
		Expect(event.UserId).To(Equal(userID.String()))
		Expect(event.OrgId).To(Equal(orgID.String()))
		Expect(event.RawSnapshotId).To(Equal(materialized.RawSnapshotID.String()))
		Expect(event.FeatureSnapshotId).To(Equal(materialized.FeatureSnapshotID.String()))
		Expect(event.TableNamespace).To(Equal("features"))
	})

	It("records materialization updates through the repository transaction boundary", func() {
		updated := &model.Dataset{ID: datasetID, UserID: userID, ProcessingState: model.DatasetProcessingEmbeddingsMaterialized}
		repo.updateMaterializationResult = updated

		_, err := uc.RecordDatasetMaterialization(ctx, &model.Dataset{
			ID:                  datasetID,
			UserID:              userID,
			EmbeddingSnapshotID: uuid.New(),
			VectorStore:         "pgvector",
			CollectionName:      "movies",
			EmbeddingDimensions: 384,
			EmbeddingCount:      2,
		}, model.DatasetProcessingEmbeddingsMaterialized, 3)

		Expect(err).NotTo(HaveOccurred())
		Expect(repo.updateMaterializationState).To(Equal(model.DatasetProcessingEmbeddingsMaterialized))
		Expect(repo.updateMaterializationSeq).To(Equal(int64(3)))
	})

	It("records catalog-backed materialization without synchronous catalog validation", func() {
		tableCatalog := &stubDatasetTableCatalog{err: errors.New("catalog should not be consulted")}
		uc = usecase.NewDatasetUseCase(repo, work, registrymessaging.NewDatasetEventBuilder("data_registry"), usecase.WithDatasetTableCatalog(tableCatalog))
		updated := &model.Dataset{ID: datasetID, UserID: userID, ProcessingState: model.DatasetProcessingFeatureMaterialized}
		repo.updateMaterializationResult = updated
		materialized := &model.Dataset{
			ID:              datasetID,
			UserID:          userID,
			Location:        "s3://warehouse/features/movies",
			TableNamespace:  "features",
			TableName:       "movies",
			TableFormat:     model.Iceberg,
			CatalogProvider: model.PolarisCatalog,
		}

		got, err := uc.RecordDatasetMaterialization(ctx, materialized, model.DatasetProcessingFeatureMaterialized, 2)

		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(updated))
		Expect(tableCatalog.dataset).To(BeNil())
		Expect(repo.updateMaterializationDataset).To(Equal(materialized))
	})

	It("does not validate catalog-backed table metadata for raw materialization events", func() {
		tableCatalog := &stubDatasetTableCatalog{err: errors.New("catalog should not be consulted")}
		uc = usecase.NewDatasetUseCase(repo, work, registrymessaging.NewDatasetEventBuilder("data_registry"), usecase.WithDatasetTableCatalog(tableCatalog))
		updated := &model.Dataset{ID: datasetID, UserID: userID, ProcessingState: model.DatasetProcessingRawMaterialized}
		repo.updateMaterializationResult = updated
		materialized := &model.Dataset{
			ID:              datasetID,
			UserID:          userID,
			Location:        "s3://local-dev-bucket/lakehouse/raw/data.parquet",
			TableNamespace:  "features",
			TableName:       "movies",
			TableFormat:     model.Iceberg,
			CatalogProvider: model.PolarisCatalog,
		}

		got, err := uc.RecordDatasetMaterialization(ctx, materialized, model.DatasetProcessingRawMaterialized, 1)

		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(updated))
		Expect(tableCatalog.dataset).To(BeNil())
		Expect(repo.updateMaterializationDataset).To(Equal(materialized))
	})

})
