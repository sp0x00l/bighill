package messaging_test

import (
	"context"
	"feature_materializer_service/pkg/domain/model"
	featuremessaging "feature_materializer_service/pkg/infra/network/messaging"
	"testing"

	datasetpb "lib/data_contracts_lib/dataset"
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
		Expect(listener.NewMessage()).To(Equal(&datasetpb.DatasetFileUploadedEvent{}))
	})

	It("maps the protobuf event into the materialization workflow starter", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		uc := &recordingRawSnapshotUsecase{}
		listener := featuremessaging.NewDatasetFileUploadedEventListener(uc)

		err := listener.Handle(context.Background(), datasetID, &datasetpb.DatasetFileUploadedEvent{
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

	It("defaults table metadata at the infra entry point", func() {
		datasetID := uuid.New()
		uc := &recordingRawSnapshotUsecase{}
		listener := featuremessaging.NewDatasetFileUploadedEventListener(uc)

		err := listener.Handle(context.Background(), datasetID, &datasetpb.DatasetFileUploadedEvent{
			DatasetId:       datasetID.String(),
			UserId:          uuid.NewString(),
			StorageLocation: "s3://local-dev-bucket/raw/user/dataset/file.csv",
			ContentType:     "text/csv",
			FileExtension:   "csv",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.datasetFile.TableNamespace).To(Equal("default"))
		Expect(uc.datasetFile.TableName).To(HavePrefix("dataset_"))
		Expect(uc.datasetFile.TableFormat).To(Equal("PARQUET"))
		Expect(uc.datasetFile.CatalogProvider).To(Equal("LOCAL"))
		Expect(uc.datasetFile.ProcessingProfile).To(Equal(model.ProcessingProfileGenericParquet))
	})

	It("uses a deterministic idempotency key for the same dataset file", func() {
		datasetID := uuid.New()
		first := &recordingRawSnapshotUsecase{}
		second := &recordingRawSnapshotUsecase{}
		event := &datasetpb.DatasetFileUploadedEvent{
			DatasetId:       datasetID.String(),
			UserId:          uuid.NewString(),
			StorageLocation: "s3://local-dev-bucket/raw/user/dataset/file.csv",
			ContentType:     "text/csv",
			FileExtension:   "csv",
		}

		Expect(featuremessaging.NewDatasetFileUploadedEventListener(first).Handle(context.Background(), datasetID, event)).To(Succeed())
		Expect(featuremessaging.NewDatasetFileUploadedEventListener(second).Handle(context.Background(), datasetID, event)).To(Succeed())

		Expect(first.idempotencyKey).To(Equal(second.idempotencyKey))
	})

	It("returns non-retryable errors for invalid wire payloads", func() {
		datasetID := uuid.New()
		listener := featuremessaging.NewDatasetFileUploadedEventListener(&recordingRawSnapshotUsecase{})

		err := listener.Handle(context.Background(), datasetID, &datasetpb.DatasetFileUploadedEvent{
			DatasetId:       uuid.NewString(),
			UserId:          uuid.NewString(),
			StorageLocation: "s3://local-dev-bucket/raw/user/dataset/file.csv",
			ContentType:     "text/csv",
			FileExtension:   "csv",
		})

		Expect(err).To(HaveOccurred())
		Expect(sharedMessaging.IsNonRetryable(err)).To(BeTrue())
	})

	It("returns non-retryable errors when required file metadata is missing", func() {
		datasetID := uuid.New()
		listener := featuremessaging.NewDatasetFileUploadedEventListener(&recordingRawSnapshotUsecase{})

		err := listener.Handle(context.Background(), datasetID, &datasetpb.DatasetFileUploadedEvent{
			DatasetId: datasetID.String(),
			UserId:    uuid.NewString(),
		})

		Expect(err).To(HaveOccurred())
		Expect(sharedMessaging.IsNonRetryable(err)).To(BeTrue())
	})
})
