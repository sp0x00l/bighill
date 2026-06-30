package bucket

import (
	"context"
	"data_ingestion_service/pkg/domain/model"
	"fmt"
	"io"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

type Bucket interface {
	Upload(ctx context.Context, bucket, key, contentType string, body io.Reader) error
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
