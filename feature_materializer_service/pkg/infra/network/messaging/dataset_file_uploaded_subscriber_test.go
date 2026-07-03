package messaging_test

import (
	"context"
	"feature_materializer_service/pkg/domain/model"
	featuremessaging "feature_materializer_service/pkg/infra/network/messaging"
	"testing"

	dataingestionpb "lib/data_contracts_lib/data_ingestion"
	dataregistrypb "lib/data_contracts_lib/data_registry"
	sharedMessaging "lib/shared_lib/messaging"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMessaging(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Feature materializer messaging test suite")
}

type recordingRawSnapshotUsecase struct {
	datasetFile    *model.DatasetFile
	idempotencyKey uuid.UUID
	err            error
}

func (r *recordingRawSnapshotUsecase) StartMaterializationWorkflow(_ context.Context, datasetFile *model.DatasetFile, idempotencyKey uuid.UUID) error {
	r.datasetFile = datasetFile
	r.idempotencyKey = idempotencyKey
	return r.err
}

var _ = Describe("DatasetFileUploadedEventListener", func() {
	It("exposes the dataset file uploaded message type", func() {
		listener := featuremessaging.NewDatasetFileUploadedEventListener(&recordingRawSnapshotUsecase{})

		Expect(listener.MsgType()).To(Equal(sharedMessaging.MsgTypeDatasetFileUploaded))
		Expect(listener.NewMessage()).To(Equal(&dataingestionpb.DatasetFileUploadedEvent{}))
	})

	It("maps the protobuf event into the materialization workflow starter", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		uc := &recordingRawSnapshotUsecase{}
		listener := featuremessaging.NewDatasetFileUploadedEventListener(uc)

		err := listener.Handle(context.Background(), datasetID, &dataingestionpb.DatasetFileUploadedEvent{
			DatasetId:         datasetID.String(),
			UserId:            userID.String(),
			StorageLocation:   "s3://local-dev-bucket/raw/user/dataset/file.csv",
			ContentType:       "text/csv",
			FileExtension:     "csv",
			TableNamespace:    "features",
			TableName:         "movies",
			TableFormat:       "PARQUET",
			CatalogProvider:   "LOCAL",
			ProcessingProfile: "TEXT_RAG",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.datasetFile.DatasetID).To(Equal(datasetID))
		Expect(uc.datasetFile.UserID).To(Equal(userID))
		Expect(uc.datasetFile.TableNamespace).To(Equal("features"))
		Expect(uc.datasetFile.TableName).To(Equal("movies"))
		Expect(uc.datasetFile.ProcessingProfile).To(Equal(model.ProcessingProfileTextRAG))
		Expect(uc.idempotencyKey).NotTo(Equal(uuid.Nil))
	})

	It("returns non-retryable errors when table metadata is missing", func() {
		datasetID := uuid.New()
		uc := &recordingRawSnapshotUsecase{}
		listener := featuremessaging.NewDatasetFileUploadedEventListener(uc)

		err := listener.Handle(context.Background(), datasetID, &dataingestionpb.DatasetFileUploadedEvent{
			DatasetId:         datasetID.String(),
			UserId:            uuid.NewString(),
			StorageLocation:   "s3://local-dev-bucket/raw/user/dataset/file.csv",
			ContentType:       "text/csv",
			FileExtension:     "csv",
			ProcessingProfile: "TEXT_RAG",
		})

		Expect(err).To(HaveOccurred())
		Expect(sharedMessaging.IsNonRetryable(err)).To(BeTrue())
	})

	It("uses a deterministic idempotency key for the same dataset file", func() {
		datasetID := uuid.New()
		first := &recordingRawSnapshotUsecase{}
		second := &recordingRawSnapshotUsecase{}
		event := &dataingestionpb.DatasetFileUploadedEvent{
			DatasetId:         datasetID.String(),
			UserId:            uuid.NewString(),
			StorageLocation:   "s3://local-dev-bucket/raw/user/dataset/file.csv",
			ContentType:       "text/csv",
			FileExtension:     "csv",
			TableNamespace:    "features",
			TableName:         "movies",
			TableFormat:       "PARQUET",
			CatalogProvider:   "LOCAL",
			ProcessingProfile: "TEXT_RAG",
		}

		Expect(featuremessaging.NewDatasetFileUploadedEventListener(first).Handle(context.Background(), datasetID, event)).To(Succeed())
		Expect(featuremessaging.NewDatasetFileUploadedEventListener(second).Handle(context.Background(), datasetID, event)).To(Succeed())

		Expect(first.idempotencyKey).To(Equal(second.idempotencyKey))
	})

	It("returns non-retryable errors for invalid wire payloads", func() {
		datasetID := uuid.New()
		listener := featuremessaging.NewDatasetFileUploadedEventListener(&recordingRawSnapshotUsecase{})

		err := listener.Handle(context.Background(), datasetID, &dataingestionpb.DatasetFileUploadedEvent{
			DatasetId:         uuid.NewString(),
			UserId:            uuid.NewString(),
			StorageLocation:   "s3://local-dev-bucket/raw/user/dataset/file.csv",
			ContentType:       "text/csv",
			FileExtension:     "csv",
			TableNamespace:    "features",
			TableName:         "movies",
			TableFormat:       "PARQUET",
			CatalogProvider:   "LOCAL",
			ProcessingProfile: "TEXT_RAG",
		})

		Expect(err).To(HaveOccurred())
		Expect(sharedMessaging.IsNonRetryable(err)).To(BeTrue())
	})

	It("returns non-retryable errors when required file metadata is missing", func() {
		datasetID := uuid.New()
		listener := featuremessaging.NewDatasetFileUploadedEventListener(&recordingRawSnapshotUsecase{})

		err := listener.Handle(context.Background(), datasetID, &dataingestionpb.DatasetFileUploadedEvent{
			DatasetId: datasetID.String(),
			UserId:    uuid.NewString(),
		})

		Expect(err).To(HaveOccurred())
		Expect(sharedMessaging.IsNonRetryable(err)).To(BeTrue())
	})

	It("maps connector-backed dataset created events into the materialization workflow starter", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		connectorID := uuid.New()
		uc := &recordingRawSnapshotUsecase{}
		listener := featuremessaging.NewDatasetCreatedEventListener(uc)

		err := listener.Handle(context.Background(), datasetID, &dataregistrypb.DatasetCreatedEvent{
			DatasetId:         datasetID.String(),
			UserId:            userID.String(),
			DatasetVersion:    3,
			TableNamespace:    "features",
			TableName:         "movies",
			TableFormat:       "PARQUET",
			CatalogProvider:   "LOCAL",
			ProcessingProfile: "TEXT_RAG",
			SourceType:        "POSTGRES",
			SourceConnectorId: connectorID.String(),
			SourceQuery:       "SELECT title FROM movies",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(listener.MsgType()).To(Equal(sharedMessaging.MsgTypeDatasetCreated))
		Expect(listener.NewMessage()).To(Equal(&dataregistrypb.DatasetCreatedEvent{}))
		Expect(uc.datasetFile.DatasetID).To(Equal(datasetID))
		Expect(uc.datasetFile.UserID).To(Equal(userID))
		Expect(uc.datasetFile.SourceConnectorID).To(Equal(connectorID))
		Expect(uc.datasetFile.SourceType).To(Equal("postgres"))
		Expect(uc.datasetFile.SourceQuery).To(Equal("SELECT title FROM movies"))
		Expect(uc.datasetFile.ContentType).To(Equal("application/vnd.apache.parquet"))
		Expect(uc.datasetFile.FileExtension).To(Equal("parquet"))
		Expect(uc.idempotencyKey).NotTo(Equal(uuid.Nil))
	})

	It("ignores uploaded-object dataset created events without a source connector", func() {
		datasetID := uuid.New()
		uc := &recordingRawSnapshotUsecase{}
		listener := featuremessaging.NewDatasetCreatedEventListener(uc)

		err := listener.Handle(context.Background(), datasetID, &dataregistrypb.DatasetCreatedEvent{
			DatasetId:         datasetID.String(),
			UserId:            uuid.NewString(),
			DatasetVersion:    1,
			TableNamespace:    "features",
			TableName:         "movies",
			TableFormat:       "PARQUET",
			CatalogProvider:   "LOCAL",
			ProcessingProfile: "TEXT_RAG",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.datasetFile).To(BeNil())
		Expect(uc.idempotencyKey).To(Equal(uuid.Nil))
	})

	It("maps connector-backed dataset updated events into deterministic materialization starts", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		connectorID := uuid.New()
		event := &dataregistrypb.DatasetUpdatedEvent{
			DatasetId:         datasetID.String(),
			UserId:            userID.String(),
			DatasetVersion:    4,
			TableNamespace:    "features",
			TableName:         "movies",
			TableFormat:       "PARQUET",
			CatalogProvider:   "LOCAL",
			ProcessingProfile: "TEXT_RAG",
			SourceType:        "MONGO",
			SourceConnectorId: connectorID.String(),
			SourceDatabase:    "catalog",
			SourceCollection:  "movies",
		}
		first := &recordingRawSnapshotUsecase{}
		second := &recordingRawSnapshotUsecase{}

		Expect(featuremessaging.NewDatasetUpdatedEventListener(first).Handle(context.Background(), datasetID, event)).To(Succeed())
		Expect(featuremessaging.NewDatasetUpdatedEventListener(second).Handle(context.Background(), datasetID, event)).To(Succeed())

		Expect(first.idempotencyKey).To(Equal(second.idempotencyKey))
		Expect(first.datasetFile.SourceType).To(Equal("mongo"))
		Expect(first.datasetFile.SourceDatabase).To(Equal("catalog"))
		Expect(first.datasetFile.SourceCollection).To(Equal("movies"))
	})
})
