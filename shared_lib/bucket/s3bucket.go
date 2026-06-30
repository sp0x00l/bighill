package s3bucket

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	log "github.com/sirupsen/logrus"
)

const LocalDevS3Region = "local-dev"

type s3Uploader interface {
	Upload(context.Context, *s3.PutObjectInput, ...func(*manager.Uploader)) (*manager.UploadOutput, error)
}
type s3Signer interface {
	PresignGetObject(context.Context, *s3.GetObjectInput, ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error)
}
type s3Client interface {
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	DeleteObjects(context.Context, *s3.DeleteObjectsInput, ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
}

type S3Bucket struct {
	region   string
	uploader s3Uploader
	signer   s3Signer
	client   s3Client
}

func NewBucket(ctx context.Context, region string, uploadPartSize int64) *S3Bucket {
	log.Trace("bucket NewBucket")

	var (
		uploader s3Uploader
		signer   s3Signer
		client   s3Client
	)
	if region == LocalDevS3Region {
		localS3 := genLocalS3UploadComponents()
		uploader, signer, client = localS3, localS3, localS3
	} else {
		uploader, signer, client = genS3Components(ctx, region, uploadPartSize)
		if uploader == nil || signer == nil {
			return nil
		}
	}

	return NewS3Bucket(region, uploader, signer, client)
}

func genS3Components(ctx context.Context, region string, uploadPartSize int64) (s3Uploader, s3Signer, s3Client) {
	log.Trace("S3Bucket genS3Components")

	s3Config, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to load S3 config")
		return nil, nil, nil
	}

	s3Client := s3.NewFromConfig(s3Config)
	uploader := manager.NewUploader(s3Client, func(u *manager.Uploader) { u.PartSize = uploadPartSize })
	presigner := s3.NewPresignClient(s3Client)
	return uploader, presigner, s3Client
}

func NewS3Bucket(region string, uploader s3Uploader, signer s3Signer, client s3Client) *S3Bucket {
	log.Trace("S3Bucket NewS3Bucket")

	return &S3Bucket{
		region:   region,
		uploader: uploader,
		signer:   signer,
		client:   client,
	}
}

func (b *S3Bucket) Upload(ctx context.Context, bucket, key, contentType string, body io.Reader) error {
	log.Trace("S3Bucket Upload")

	uploadObj := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   body,
	}
	if contentType != "" {
		uploadObj.ContentType = aws.String(contentType)
	}

	if _, err := b.uploader.Upload(ctx, uploadObj); err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to upload file to bucket")
		return fmt.Errorf("failed to upload file to bucket: %w", err)
	}

	return nil
}

func (bu *S3Bucket) Sign(ctx context.Context, bucket, key string, timeoutMins time.Duration) (string, error) {
	log.Trace("S3Bucket Sign")

	getObject := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	req, err := bu.signer.PresignGetObject(ctx, getObject, s3.WithPresignExpires(timeoutMins))
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to generate pre-signed URL")
		return "", fmt.Errorf("failed to generate pre-signed URL: %w", err)
	}

	return req.URL, nil
}

func (b *S3Bucket) GetKeysWithPrefix(ctx context.Context, bucket, prefix string) ([]string, error) {
	log.Trace("S3Bucket GetKeysWithPrefix")

	listObj := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}

	resp, err := b.client.ListObjectsV2(ctx, listObj)
	if err != nil {
		log.WithContext(ctx).WithError(err).Errorf("failed to list objects in bucket %s", bucket)
		return nil, fmt.Errorf("failed to list objects in bucket `%s`: %w", bucket, err)
	}

	keys := make([]string, len(resp.Contents))
	for i, obj := range resp.Contents {
		keys[i] = *obj.Key
	}

	return keys, nil
}

func (b *S3Bucket) DeleteObjects(ctx context.Context, bucket string, keys []string) error {
	log.Trace("S3Bucket DeleteObjects")

	if len(keys) == 0 {
		return nil
	}

	objects := make([]s3types.ObjectIdentifier, len(keys))
	for i, key := range keys {
		objects[i] = s3types.ObjectIdentifier{Key: aws.String(key)}
	}

	objectsToDelete := &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &s3types.Delete{
			Objects: objects,
			Quiet:   aws.Bool(true), // Doesn't return the list of deleted objects
		},
	}

	if _, err := b.client.DeleteObjects(ctx, objectsToDelete); err != nil {
		log.WithContext(ctx).WithError(err).Errorf("failed to delete from bucket `%s`", bucket)
		return fmt.Errorf("failed to delete from bucket `%s`: %w", bucket, err)
	}

	return nil
}
