package materialization

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"feature_materializer_service/pkg/domain"
	corebucket "lib/shared_lib/bucket"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	log "github.com/sirupsen/logrus"
)

type s3Downloader interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

type ObjectArtifactStore struct {
	bucketName string
	region     string
	bucket     *corebucket.S3Bucket
	downloader s3Downloader
}

func NewObjectArtifactStore(ctx context.Context, bucketName, region string, uploadPartSize int64) (*ObjectArtifactStore, error) {
	log.Trace("NewObjectArtifactStore")

	if strings.TrimSpace(bucketName) == "" {
		return nil, domain.ErrValidationFailed.Extend("artifact bucket name is required")
	}
	if strings.TrimSpace(region) == "" {
		return nil, domain.ErrValidationFailed.Extend("artifact bucket region is required")
	}

	uploader := corebucket.NewBucket(ctx, region, uploadPartSize)
	var downloader s3Downloader
	if region != corebucket.LocalDevS3Region {
		awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
		if err != nil {
			return nil, fmt.Errorf("load aws config: %w", err)
		}
		downloader = s3.NewFromConfig(awsCfg)
	}

	return &ObjectArtifactStore{
		bucketName: bucketName,
		region:     region,
		bucket:     uploader,
		downloader: downloader,
	}, nil
}

func (s *ObjectArtifactStore) Read(ctx context.Context, storageLocation string) ([]byte, error) {
	log.Trace("ObjectArtifactStore Read")

	parsed, err := url.Parse(strings.TrimSpace(storageLocation))
	if err != nil {
		return nil, fmt.Errorf("%w: parse storage location: %w", domain.ErrArtifactRead, err)
	}

	switch parsed.Scheme {
	case "s3":
		return s.readS3(ctx, parsed.Host, strings.TrimPrefix(parsed.Path, "/"))
	case "file":
		return readFileArtifact(parsed.Path)
	case "":
		return readFileArtifact(storageLocation)
	default:
		return nil, domain.ErrArtifactRead.Extend("unsupported artifact storage scheme " + parsed.Scheme)
	}
}

func (s *ObjectArtifactStore) Write(ctx context.Context, key, contentType string, body []byte) (string, error) {
	log.Trace("ObjectArtifactStore Write")

	key = strings.TrimPrefix(strings.TrimSpace(key), "/")
	if key == "" {
		return "", domain.ErrValidationFailed.Extend("artifact key is required")
	}

	if err := s.bucket.Upload(ctx, s.bucketName, key, contentType, bytes.NewReader(body)); err != nil {
		return "", fmt.Errorf("%w: upload artifact: %w", domain.ErrArtifactWrite, err)
	}
	return fmt.Sprintf("s3://%s/%s", s.bucketName, key), nil
}

func (s *ObjectArtifactStore) readS3(ctx context.Context, bucketName, key string) ([]byte, error) {
	log.Trace("ObjectArtifactStore readS3")

	if s.region == corebucket.LocalDevS3Region {
		return readFileArtifact(filepath.Join(corebucket.StorageDir, bucketName, key))
	}
	if s.downloader == nil {
		return nil, domain.ErrArtifactRead.Extend("s3 downloader is not configured")
	}

	output, err := s.downloader.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("%w: get s3 object: %w", domain.ErrArtifactRead, err)
	}
	defer output.Body.Close()

	data, err := io.ReadAll(output.Body)
	if err != nil {
		return nil, fmt.Errorf("%w: read s3 object: %w", domain.ErrArtifactRead, err)
	}
	return data, nil
}

func readFileArtifact(path string) ([]byte, error) {
	log.Trace("readFileArtifact")

	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("%w: read file artifact: %w", domain.ErrArtifactRead, err)
	}
	return data, nil
}
