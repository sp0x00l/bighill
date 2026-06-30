package app_test

import (
	"context"
	"errors"

	usecase "data_ingestion_service/pkg/app"
	"data_ingestion_service/pkg/domain/model"
	shared "lib/shared_lib/messaging"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
)

type stubBlobRepository struct {
	receivedUpload *model.DataFile
	location       string
	saveErr        error
}

func (s *stubBlobRepository) Save(_ context.Context, upload *model.DataFile) (string, error) {
	s.receivedUpload = upload
	if s.location == "" {
		s.location = "s3://local-dev-bucket/raw/file.csv"
	}
	return s.location, s.saveErr
}

type stubEventPublisher struct {
	topic   string
	message shared.Message
	payload proto.Message
	err     error
}

func (s *stubEventPublisher) Publish(_ context.Context, topic string, message shared.Message, payload proto.Message) error {
	s.topic = topic
	s.message = message
	s.payload = payload
	return s.err
}

var _ = Describe("DataUploadUseCase", func() {
	It("uploads a file through the blob repository", func() {
		repo := &stubBlobRepository{}
		uc := usecase.NewDataUploadUseCase(repo)
		upload := &model.DataFile{
			DatasetID:   uuid.New(),
			UserID:      uuid.New(),
			ContentType: "text/csv",
			Extension:   ".csv",
		}

		Expect(uc.UploadFile(context.Background(), upload)).To(Succeed())
		Expect(repo.receivedUpload).To(Equal(upload))
	})

	It("returns repository errors", func() {
		expectedErr := errors.New("upload failed")
		repo := &stubBlobRepository{saveErr: expectedErr}
		uc := usecase.NewDataUploadUseCase(repo)

		Expect(uc.UploadFile(context.Background(), &model.DataFile{})).To(MatchError(expectedErr))
	})

	It("publishes a dataset file uploaded event after storage succeeds", func() {
		repo := &stubBlobRepository{location: "s3://bucket/raw/file.csv"}
		publisher := &stubEventPublisher{}
		uc := usecase.NewDataUploadUseCase(repo, usecase.WithUploadEventPublisher(publisher, "dataset_file_uploaded"))
		upload := &model.DataFile{
			DatasetID:   uuid.New(),
			UserID:      uuid.New(),
			ContentType: "text/csv",
			Extension:   "csv",
		}

		Expect(uc.UploadFile(context.Background(), upload)).To(Succeed())

		Expect(publisher.topic).To(Equal("dataset_file_uploaded"))
		Expect(publisher.message.ResourceKey).To(Equal(upload.DatasetID))
		Expect(publisher.message.MsgType).To(Equal(shared.MsgTypeDatasetFileUploaded))
	})
})
