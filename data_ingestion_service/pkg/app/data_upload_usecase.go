package app

import (
	"context"
	"data_ingestion_service/pkg/domain/model"
	"fmt"
	datasetpb "lib/data_contracts_lib/dataset"
	messaging "lib/shared_lib/messaging"
	usecasetrace "lib/shared_lib/usecasetrace"
	"strings"

	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

type dataUploadUseCase struct {
	bucket      BlobRepositoryAdapter
	publisher   EventPublisher
	uploadTopic string
}

type DataUploadOption func(*dataUploadUseCase)

func WithUploadEventPublisher(publisher EventPublisher, topic string) DataUploadOption {
	return func(u *dataUploadUseCase) {
		u.publisher = publisher
		u.uploadTopic = topic
	}
}

func NewDataUploadUseCase(bucket BlobRepositoryAdapter, opts ...DataUploadOption) *dataUploadUseCase {
	log.Trace("NewDataUploadUseCase")

	u := &dataUploadUseCase{
		bucket: bucket,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(u)
		}
	}
	return u
}

// UploadFile uploads the file to the raw object store and emits a materialization event.
func (u *dataUploadUseCase) UploadFile(ctx context.Context, upload *model.DataFile) (err error) {
	log.Trace("DataUploadUseCase UploadFile")
	var attrs []attribute.KeyValue
	if upload != nil {
		attrs = append(attrs,
			attribute.String("dataset_id", upload.DatasetID.String()),
			attribute.String("user_id", upload.UserID.String()),
			attribute.String("content_type", upload.ContentType),
			attribute.String("extension", upload.Extension),
		)
	}
	ctx, span := usecasetrace.StartSpan(ctx, "data_ingestion_service/app", "data_upload.upload_file", attrs...)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	storageLocation, err := u.bucket.Save(ctx, upload)
	if err != nil {
		return err
	}

	if u.publisher == nil || strings.TrimSpace(u.uploadTopic) == "" {
		return nil
	}

	payload := &datasetpb.DatasetFileUploadedEvent{
		DatasetId:       upload.DatasetID.String(),
		UserId:          upload.UserID.String(),
		StorageLocation: storageLocation,
		ContentType:     upload.ContentType,
		FileExtension:   upload.Extension,
	}
	message := messaging.Message{
		ResourceKey: upload.DatasetID,
		MsgType:     messaging.MsgTypeDatasetFileUploaded,
	}
	if err := u.publisher.Publish(ctx, u.uploadTopic, message, payload); err != nil {
		return fmt.Errorf("publish dataset file uploaded event: %w", err)
	}
	return nil
}
