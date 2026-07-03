package s3bucket

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
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
	PresignPostObject(context.Context, *s3.PutObjectInput, ...func(*s3.PresignPostOptions)) (*s3.PresignedPostRequest, error)
}
type s3Client interface {
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	DeleteObjects(context.Context, *s3.DeleteObjectsInput, ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	CopyObject(context.Context, *s3.CopyObjectInput, ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
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

type PresignedPost struct {
	URL    string
	Fields map[string]string
}

type ObjectInfo struct {
	Size        int64
	ContentType string
	Checksum    string
}

func (bu *S3Bucket) SignUploadPost(ctx context.Context, bucket, key, contentType string, maxBytes int64, timeout time.Duration) (*PresignedPost, error) {
	log.Trace("S3Bucket SignUploadPost")

	if maxBytes <= 0 {
		return nil, fmt.Errorf("pre-signed POST max bytes must be greater than zero")
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("pre-signed POST timeout must be greater than zero")
	}

	putObject := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	if contentType != "" {
		putObject.ContentType = aws.String(contentType)
	}
	conditions := []any{
		map[string]string{"bucket": bucket},
		map[string]string{"key": key},
		[]any{"content-length-range", 1, maxBytes},
	}
	if contentType != "" {
		conditions = append(conditions, map[string]string{"Content-Type": contentType})
	}

	req, err := bu.signer.PresignPostObject(ctx, putObject, func(opts *s3.PresignPostOptions) {
		opts.Expires = timeout
		opts.Conditions = conditions
	})
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to generate pre-signed POST")
		return nil, fmt.Errorf("failed to generate pre-signed POST: %w", err)
	}
	return &PresignedPost{URL: req.URL, Fields: req.Values}, nil
}

func (b *S3Bucket) HeadObject(ctx context.Context, bucket, key string) (*ObjectInfo, error) {
	log.Trace("S3Bucket HeadObject")

	out, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to head object")
		return nil, fmt.Errorf("failed to head object: %w", err)
	}
	info := &ObjectInfo{Size: aws.ToInt64(out.ContentLength)}
	if out.ContentType != nil {
		info.ContentType = *out.ContentType
	}
	if out.ETag != nil {
		info.Checksum = *out.ETag
	}
	return info, nil
}

func (b *S3Bucket) ReadObjectPrefix(ctx context.Context, bucket, key string, maxBytes int64) ([]byte, error) {
	log.Trace("S3Bucket ReadObjectPrefix")

	return b.ReadObjectRange(ctx, bucket, key, 0, maxBytes)
}

func (b *S3Bucket) ReadObjectRange(ctx context.Context, bucket, key string, offset, maxBytes int64) ([]byte, error) {
	log.Trace("S3Bucket ReadObjectRange")

	if maxBytes <= 0 {
		return nil, nil
	}
	if offset < 0 {
		return nil, fmt.Errorf("object range offset must be non-negative")
	}
	out, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+maxBytes-1)),
	})
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to read object range")
		return nil, fmt.Errorf("failed to read object range: %w", err)
	}
	defer out.Body.Close()
	data, err := io.ReadAll(io.LimitReader(out.Body, maxBytes))
	if err != nil {
		return nil, fmt.Errorf("read object range: %w", err)
	}
	return data, nil
}

func (b *S3Bucket) CopyObject(ctx context.Context, bucket, sourceKey, destinationKey, contentType string) error {
	log.Trace("S3Bucket CopyObject")

	copySource := bucket + "/" + strings.ReplaceAll(url.PathEscape(sourceKey), "%2F", "/")
	input := &s3.CopyObjectInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(destinationKey),
		CopySource: aws.String(copySource),
	}
	if contentType != "" {
		input.ContentType = aws.String(contentType)
		input.MetadataDirective = s3types.MetadataDirectiveReplace
	}
	if _, err := b.client.CopyObject(ctx, input); err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to copy object")
		return fmt.Errorf("failed to copy object: %w", err)
	}
	return nil
}

func (b *S3Bucket) DeleteObject(ctx context.Context, bucket, key string) error {
	log.Trace("S3Bucket DeleteObject")

	if _, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}); err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to delete object")
		return fmt.Errorf("failed to delete object: %w", err)
	}
	return nil
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
