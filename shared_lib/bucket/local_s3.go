package s3bucket

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
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

type localObjectMetadata struct {
	ContentType string `json:"content_type"`
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
	if input.ContentType != nil {
		_ = writeLocalObjectMetadata(filePath, localObjectMetadata{ContentType: *input.ContentType})
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

func (m *LocalS3Bucket) PresignPostObject(ctx context.Context, params *s3.PutObjectInput, _ ...func(*s3.PresignPostOptions)) (*s3.PresignedPostRequest, error) {
	log.Trace("LocalS3Bucket PresignPostObject")

	values := map[string]string{
		"key": *params.Key,
	}
	if params.ContentType != nil {
		values["Content-Type"] = *params.ContentType
	}
	return &s3.PresignedPostRequest{
		URL:    fmt.Sprintf("local-s3://%s", *params.Bucket),
		Values: values,
	}, nil
}

func (m *LocalS3Bucket) HeadObject(ctx context.Context, params *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	log.Trace("LocalS3Bucket HeadObject")

	m.mu.Lock()
	defer m.mu.Unlock()

	filePath := filepath.Join(StorageDir, *params.Bucket, *params.Key)
	info, err := os.Stat(filePath)
	if err != nil {
		return nil, err
	}
	contentType := readLocalObjectMetadata(filePath).ContentType
	if strings.TrimSpace(contentType) == "" {
		contentType = mime.TypeByExtension(filepath.Ext(filePath))
	}
	checksum := ""
	if data, err := os.ReadFile(filePath); err == nil {
		sum := sha256.Sum256(data)
		checksum = fmt.Sprintf("%x", sum)
	}
	return &s3.HeadObjectOutput{
		ContentLength: aws.Int64(info.Size()),
		ContentType:   aws.String(contentType),
		ETag:          aws.String(checksum),
	}, nil
}

func (m *LocalS3Bucket) GetObject(ctx context.Context, params *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	log.Trace("LocalS3Bucket GetObject")

	m.mu.Lock()
	defer m.mu.Unlock()

	filePath := filepath.Join(StorageDir, *params.Bucket, *params.Key)
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	if params.Range != nil {
		start, end, ok := parseLocalRange(*params.Range)
		if ok {
			if _, err := file.Seek(start, io.SeekStart); err != nil {
				_ = file.Close()
				return nil, err
			}
			return &s3.GetObjectOutput{
				Body: localReadCloser{Reader: io.LimitReader(file, end-start+1), close: file.Close},
			}, nil
		}
	}
	return &s3.GetObjectOutput{Body: file}, nil
}

type localReadCloser struct {
	io.Reader
	close func() error
}

func (r localReadCloser) Close() error {
	if r.close == nil {
		return nil
	}
	return r.close()
}

func (m *LocalS3Bucket) CopyObject(ctx context.Context, params *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	log.Trace("LocalS3Bucket CopyObject")

	m.mu.Lock()
	defer m.mu.Unlock()

	source := strings.TrimPrefix(aws.ToString(params.CopySource), "/")
	if decoded, err := url.PathUnescape(source); err == nil {
		source = decoded
	}
	parts := strings.SplitN(source, "/", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid local copy source %q", source)
	}
	sourcePath := filepath.Join(StorageDir, parts[0], parts[1])
	destinationPath := filepath.Join(StorageDir, *params.Bucket, *params.Key)
	if err := os.MkdirAll(filepath.Dir(destinationPath), os.ModePerm); err != nil {
		return nil, err
	}
	in, err := os.Open(sourcePath)
	if err != nil {
		return nil, err
	}
	defer in.Close()
	out, err := os.Create(destinationPath)
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return nil, err
	}
	if err := out.Close(); err != nil {
		return nil, err
	}
	metadata := readLocalObjectMetadata(sourcePath)
	if params.ContentType != nil {
		metadata.ContentType = *params.ContentType
	}
	if strings.TrimSpace(metadata.ContentType) != "" {
		_ = writeLocalObjectMetadata(destinationPath, metadata)
	}
	return &s3.CopyObjectOutput{}, nil
}

func (m *LocalS3Bucket) DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	log.Trace("LocalS3Bucket DeleteObject")

	m.mu.Lock()
	defer m.mu.Unlock()

	filePath := filepath.Join(StorageDir, *params.Bucket, *params.Key)
	if err := os.Remove(filePath); err != nil {
		return nil, err
	}
	_ = os.Remove(localMetadataPath(filePath))
	return &s3.DeleteObjectOutput{}, nil
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
		_ = os.Remove(localMetadataPath(filePath))
	}

	return &s3.DeleteObjectsOutput{}, nil
}

func localMetadataPath(filePath string) string {
	return filePath + ".metadata.json"
}

func writeLocalObjectMetadata(filePath string, metadata localObjectMetadata) error {
	raw, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	return os.WriteFile(localMetadataPath(filePath), raw, 0600)
}

func readLocalObjectMetadata(filePath string) localObjectMetadata {
	raw, err := os.ReadFile(localMetadataPath(filePath))
	if err != nil {
		return localObjectMetadata{}
	}
	var metadata localObjectMetadata
	_ = json.Unmarshal(raw, &metadata)
	return metadata
}

func parseLocalRange(raw string) (int64, int64, bool) {
	raw = strings.TrimPrefix(strings.TrimSpace(raw), "bytes=")
	parts := strings.SplitN(raw, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	end, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || end < start {
		return 0, 0, false
	}
	return start, end, true
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
