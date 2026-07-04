package bucket

import (
	"context"
	"fmt"
	"ingestion_service/pkg/domain/model"
	"io"
	coreBucket "lib/shared_lib/bucket"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

type Bucket interface {
	Upload(ctx context.Context, bucket, key, contentType string, body io.Reader) error
	SignUploadPost(ctx context.Context, bucket, key, contentType string, maxBytes int64, timeout time.Duration) (*coreBucket.PresignedPost, error)
	HeadObject(ctx context.Context, bucket, key string) (*coreBucket.ObjectInfo, error)
	ReadObjectPrefix(ctx context.Context, bucket, key string, maxBytes int64) ([]byte, error)
	ReadObjectRange(ctx context.Context, bucket, key string, offset, maxBytes int64) ([]byte, error)
	CopyObject(ctx context.Context, bucket, sourceKey, destinationKey, contentType string) error
	DeleteObject(ctx context.Context, bucket, key string) error
}

type DataBucket struct {
	bucketName string
	bucket     Bucket
}

func NewDataBucket(bucketName string, bucket Bucket) *DataBucket {
	log.Trace("bucket NewDataBucket")

	return &DataBucket{
		bucketName: bucketName,
		bucket:     bucket,
	}
}

// Save saves the dataset file to the bucket and returns its object URI.
func (b *DataBucket) Save(ctx context.Context, file *model.DataFile) (string, error) {
	log.Trace("DataBucket Save")

	extension := strings.TrimPrefix(file.Extension, ".")
	uploadKey := fmt.Sprintf(
		"raw/%s/%s/%d.%s",
		file.UserID.String(),
		file.DatasetID.String(),
		time.Now().UTC().UnixNano(),
		extension,
	)
	if err := b.bucket.Upload(ctx, b.bucketName, uploadKey, file.ContentType, file.File); err != nil {
		return "", err
	}

	log.WithContext(ctx).Infof("Dataset file uploaded for ID : %s", file.DatasetID.String())
	return fmt.Sprintf("s3://%s/%s", b.bucketName, uploadKey), nil
}

func (b *DataBucket) SignUploadPost(ctx context.Context, session *model.UploadSession, maxBytes int64, ttl time.Duration) (*model.PresignedUploadPost, error) {
	log.Trace("DataBucket SignUploadPost")

	post, err := b.bucket.SignUploadPost(ctx, b.bucketName, session.StagingKey, session.DeclaredContentType, maxBytes, ttl)
	if err != nil {
		return nil, err
	}
	return &model.PresignedUploadPost{
		URL:       post.URL,
		Fields:    post.Fields,
		ExpiresAt: session.ExpiresAt,
	}, nil
}

func (b *DataBucket) HeadStagingObject(ctx context.Context, session *model.UploadSession) (*model.ObjectInfo, error) {
	log.Trace("DataBucket HeadStagingObject")

	info, err := b.bucket.HeadObject(ctx, b.bucketName, session.StagingKey)
	if err != nil {
		return nil, err
	}
	return &model.ObjectInfo{
		Size:        info.Size,
		ContentType: info.ContentType,
		Checksum:    info.Checksum,
	}, nil
}

func (b *DataBucket) ReadStagingRange(ctx context.Context, session *model.UploadSession, offset, maxBytes int64) ([]byte, error) {
	log.Trace("DataBucket ReadStagingRange")

	return b.bucket.ReadObjectRange(ctx, b.bucketName, session.StagingKey, offset, maxBytes)
}

func (b *DataBucket) PromoteStagedObject(ctx context.Context, session *model.UploadSession, contentType string) (string, error) {
	log.Trace("DataBucket PromoteStagedObject")

	if err := b.bucket.CopyObject(ctx, b.bucketName, session.StagingKey, session.FinalKey, contentType); err != nil {
		return "", err
	}
	return fmt.Sprintf("s3://%s/%s", b.bucketName, session.FinalKey), nil
}

func (b *DataBucket) DeleteStagedObject(ctx context.Context, session *model.UploadSession) error {
	log.Trace("DataBucket DeleteStagedObject")

	return b.bucket.DeleteObject(ctx, b.bucketName, session.StagingKey)
}
