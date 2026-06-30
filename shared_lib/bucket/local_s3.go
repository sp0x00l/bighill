package s3bucket

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	log "github.com/sirupsen/logrus"
)

var StorageDir string

func initStorageDir() {
	log.Trace("localS3 initStorageDir")

	rootDir, err := findRoot()
	if err != nil {
		log.Warnf("Failed to create Local S3 storage for local-dev deployment: %v", err)
		rootDir = os.TempDir()
	}
	StorageDir = filepath.Join(rootDir, "tmp/local_s3_storage")
	os.MkdirAll(StorageDir, os.ModePerm)
}

// write the file into the root temp directory
// need to do this as the path is relative to execution location
func findRoot() (string, error) {
	log.Trace("localS3 findRoot")

	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}

	for {
		// find the shared_lib, this is the root
		if _, err := os.Stat(filepath.Join(dir, "shared_lib")); err == nil {
			return dir, nil
		}
		parentDir := filepath.Dir(dir)
		if parentDir == dir {
			break
		}
		dir = parentDir
	}
	return "", os.ErrNotExist
}

type LocalS3Bucket struct {
	mu sync.Mutex
}

func genLocalS3UploadComponents() *LocalS3Bucket {
	log.Trace("localS3 genLocalS3UploadComponents")

	initStorageDir()
	os.MkdirAll(StorageDir, os.ModePerm)
	return &LocalS3Bucket{}
}

func (m *LocalS3Bucket) Upload(ctx context.Context, input *s3.PutObjectInput, _ ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
	log.Trace("LocalS3Bucket Upload")

	m.mu.Lock()
	defer m.mu.Unlock()

	bucketDir := filepath.Join(StorageDir, *input.Bucket)
	if err := os.MkdirAll(bucketDir, os.ModePerm); err != nil {
		return nil, err
	}

	filePath := filepath.Join(bucketDir, *input.Key)
	if err := os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
		return nil, err
	}

	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	if _, err := io.Copy(file, input.Body); err != nil {
		return nil, err
	}

	return &manager.UploadOutput{}, nil
}

func (m *LocalS3Bucket) PresignGetObject(ctx context.Context, params *s3.GetObjectInput, _ ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error) {
	log.Trace("LocalS3Bucket PresignGetObject")

	// https://docs.aws.amazon.com/STS/latest/APIReference/API_GetAccessKeyInfo.html
	httpPath := fmt.Sprintf("/%s/%s", *params.Bucket, *params.Key)
	algorithm := "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Feu-west-1%2Fs3%2Faws4_request&X-Amz-Date="
	signature := "&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"

	signedReq := &v4.PresignedHTTPRequest{
		URL: fmt.Sprintf("%s?%s%s%s", httpPath, algorithm, time.Now().UTC().Format("20060102T150405Z"), signature),
	}
	return signedReq, nil
}

func (m *LocalS3Bucket) DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, _ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	log.Trace("LocalS3Bucket DeleteObjects")

	m.mu.Lock()
	defer m.mu.Unlock()

	bucketDir := filepath.Join(StorageDir, *params.Bucket)
	for _, key := range params.Delete.Objects {
		filePath := filepath.Join(bucketDir, *key.Key)
		if err := os.Remove(filePath); err != nil {
			return nil, err
		}
	}

	return &s3.DeleteObjectsOutput{}, nil
}

func (m *LocalS3Bucket) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	log.Trace("LocalS3Bucket ListObjectsV2")

	m.mu.Lock()
	defer m.mu.Unlock()

	bucketDir := filepath.Join(StorageDir, *params.Bucket)
	files, err := os.ReadDir(bucketDir)
	if err != nil {
		return nil, err
	}

	var objects []s3types.Object
	for _, file := range files {
		fileName := file.Name()
		if !strings.HasPrefix(fileName, *params.Prefix) {
			continue
		}
		objects = append(objects, s3types.Object{
			Key: &fileName,
		})
	}

	return &s3.ListObjectsV2Output{
		Contents: objects,
	}, nil
}
