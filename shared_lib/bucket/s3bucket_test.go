package s3bucket_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	bucket "shared_lib/bucket"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type mockS3Components struct {
	UploadCalled            bool
	PresignGetObjectCalled  bool
	PresignPostObjectCalled bool
	DeleteObjectsCalled     bool
	DeleteObjectCalled      bool
	ListObjectsV2Called     bool
	HeadObjectCalled        bool
	GetObjectCalled         bool
	CopyObjectCalled        bool

	LastInput             *s3.PutObjectInput
	LastParams            *s3.GetObjectInput
	LastPostInput         *s3.PutObjectInput
	LastDeleteInput       *s3.DeleteObjectsInput
	LastDeleteObjectInput *s3.DeleteObjectInput
	LastListObjectInput   *s3.ListObjectsV2Input
	LastHeadObjectInput   *s3.HeadObjectInput
	LastGetObjectInput    *s3.GetObjectInput
	LastCopyObjectInput   *s3.CopyObjectInput
	LastOptFns            []func(*s3.PresignOptions)
	LastPostOptFns        []func(*s3.PresignPostOptions)

	NextError         error
	NextPresignOutput *v4.PresignedHTTPRequest
	NextPostOutput    *s3.PresignedPostRequest
	NextListObjects   *s3.ListObjectsV2Output
	NextHeadObject    *s3.HeadObjectOutput
	NextGetObject     *s3.GetObjectOutput
}

func (m *mockS3Components) Upload(_ context.Context, input *s3.PutObjectInput, _ ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
	m.UploadCalled = true
	m.LastInput = input

	return nil, m.NextError
}

func (m *mockS3Components) PresignGetObject(_ context.Context, params *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error) {
	m.PresignGetObjectCalled = true
	m.LastParams = params
	m.LastOptFns = optFns

	return m.NextPresignOutput, m.NextError
}

func (m *mockS3Components) PresignPostObject(_ context.Context, input *s3.PutObjectInput, optFns ...func(*s3.PresignPostOptions)) (*s3.PresignedPostRequest, error) {
	m.PresignPostObjectCalled = true
	m.LastPostInput = input
	m.LastPostOptFns = optFns

	return m.NextPostOutput, m.NextError
}

func (m *mockS3Components) DeleteObjects(_ context.Context, input *s3.DeleteObjectsInput, _ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	m.DeleteObjectsCalled = true
	m.LastDeleteInput = input

	return nil, m.NextError
}

func (m *mockS3Components) DeleteObject(_ context.Context, input *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	m.DeleteObjectCalled = true
	m.LastDeleteObjectInput = input

	return nil, m.NextError
}

func (m *mockS3Components) ListObjectsV2(_ context.Context, input *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	m.ListObjectsV2Called = true
	m.LastListObjectInput = input

	return m.NextListObjects, m.NextError
}

func (m *mockS3Components) HeadObject(_ context.Context, input *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	m.HeadObjectCalled = true
	m.LastHeadObjectInput = input

	return m.NextHeadObject, m.NextError
}

func (m *mockS3Components) GetObject(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	m.GetObjectCalled = true
	m.LastGetObjectInput = input

	return m.NextGetObject, m.NextError
}

func (m *mockS3Components) CopyObject(_ context.Context, input *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	m.CopyObjectCalled = true
	m.LastCopyObjectInput = input

	return nil, m.NextError
}

func TestSign(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "AWS S3 Bucket Suite")
}

var _ = Describe("NewS3Bucket unit tests", func() {
	var (
		ctx        context.Context
		s3Mock     *mockS3Components
		s3bucket   *bucket.S3Bucket
		key        string
		body       io.ReadCloser
		bucketName string
	)

	BeforeEach(func() {
		ctx = context.Background()
		bucketName = "test-bucket"
		s3Mock = &mockS3Components{}
		s3bucket = bucket.NewS3Bucket("us-east-1", s3Mock, s3Mock, s3Mock)
		key = "key"
		body = io.NopCloser(strings.NewReader("mock body"))
	})

	Describe("Upload to S3", func() {
		It("should upload file to S3 with content type", func() {
			err := s3bucket.Upload(ctx, bucketName, key, "test-type", body)

			Expect(err).To(BeNil())
			Expect(s3Mock.UploadCalled).To(BeTrue())
			Expect(s3Mock.LastInput).NotTo(BeNil())
			Expect(s3Mock.LastInput.Bucket).ToNot(BeNil())
			Expect(*s3Mock.LastInput.Bucket).To(Equal(bucketName))
			Expect(s3Mock.LastInput.Key).ToNot(BeNil())
			Expect(*s3Mock.LastInput.Key).To(Equal("key"))
			Expect(s3Mock.LastInput.Body).To(Equal(body))
			Expect(s3Mock.LastInput.ContentType).ToNot(BeNil())
			Expect(*s3Mock.LastInput.ContentType).To(Equal("test-type"))
		})

		It("should upload file to S3 without content type", func() {
			_ = s3bucket.Upload(ctx, bucketName, key, "", body)

			Expect(s3Mock.LastInput.ContentType).To(BeNil())
		})

		It("should return error when failed to upload file to S3", func() {
			s3Mock.NextError = fmt.Errorf("mock AWS error")

			err := s3bucket.Upload(ctx, bucketName, key, "", body)

			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(Equal("failed to upload file to bucket: mock AWS error"))
		})
	})

	Describe("Upload to local S3", func() {
		It("creates directories for nested object keys", func() {
			localBucketName := "test-local-" + fmt.Sprint(time.Now().UnixNano())
			localKey := "raw/user-id/dataset-id/file.csv"
			localBucket := bucket.NewBucket(ctx, bucket.LocalDevS3Region, 10*1024*1024)

			err := localBucket.Upload(ctx, localBucketName, localKey, "text/csv", strings.NewReader("title\nMetropolis\n"))

			Expect(err).To(BeNil())
			filePath := filepath.Join(bucket.StorageDir, localBucketName, localKey)
			DeferCleanup(func() {
				_ = os.RemoveAll(filepath.Join(bucket.StorageDir, localBucketName))
			})
			Expect(filePath).To(BeAnExistingFile())
		})
	})

	Describe("Sign URL", func() {
		var timeoutMins time.Duration
		BeforeEach(func() {
			timeoutMins = time.Duration(5)
		})

		It("should return a generated signed URL", func() {
			s3Mock.NextPresignOutput = &v4.PresignedHTTPRequest{
				URL: "https://bucket.s3.us-east-1.amazonaws.com/key",
			}

			url, err := s3bucket.Sign(ctx, bucketName, key, timeoutMins)

			Expect(err).To(BeNil())
			Expect(url).To(Equal("https://bucket.s3.us-east-1.amazonaws.com/key"))

			Expect(s3Mock.PresignGetObjectCalled).To(BeTrue())
			Expect(s3Mock.LastParams).NotTo(BeNil())
			Expect(s3Mock.LastParams.Bucket).ToNot(BeNil())
			Expect(*s3Mock.LastParams.Bucket).To(Equal(bucketName))
			Expect(s3Mock.LastParams.Key).ToNot(BeNil())
			Expect(*s3Mock.LastParams.Key).To(Equal("key"))
			Expect(s3Mock.LastOptFns).ToNot(BeNil())
			Expect(s3Mock.LastOptFns).To(HaveLen(1))

			opts := &s3.PresignOptions{}
			s3Mock.LastOptFns[0](opts)
			Expect(opts.Expires).To(Equal(timeoutMins))
		})

		It("should return the sign error", func() {
			s3Mock.NextError = fmt.Errorf("mock AWS error")

			url, err := s3bucket.Sign(ctx, bucketName, key, timeoutMins)

			Expect(err).ToNot(BeNil())
			Expect(err.Error()).To(Equal("failed to generate pre-signed URL: mock AWS error"))
			Expect(url).To(BeEmpty())
		})
	})

	Describe("Sign upload POST", func() {
		It("returns a generated POST policy scoped to key, content type, and size", func() {
			s3Mock.NextPostOutput = &s3.PresignedPostRequest{
				URL:    "https://bucket.s3.us-east-1.amazonaws.com",
				Values: map[string]string{"key": key, "policy": "encoded"},
			}

			post, err := s3bucket.SignUploadPost(ctx, bucketName, key, "text/csv", 1024, 15*time.Minute)

			Expect(err).To(BeNil())
			Expect(post.URL).To(Equal("https://bucket.s3.us-east-1.amazonaws.com"))
			Expect(post.Fields).To(HaveKeyWithValue("policy", "encoded"))
			Expect(s3Mock.PresignPostObjectCalled).To(BeTrue())
			Expect(*s3Mock.LastPostInput.Bucket).To(Equal(bucketName))
			Expect(*s3Mock.LastPostInput.Key).To(Equal(key))
			Expect(*s3Mock.LastPostInput.ContentType).To(Equal("text/csv"))
			Expect(s3Mock.LastPostOptFns).To(HaveLen(1))
			opts := &s3.PresignPostOptions{}
			s3Mock.LastPostOptFns[0](opts)
			Expect(opts.Expires).To(Equal(15 * time.Minute))
			Expect(opts.Conditions).NotTo(BeEmpty())
		})

		It("rejects invalid POST policy sizes", func() {
			post, err := s3bucket.SignUploadPost(ctx, bucketName, key, "text/csv", 0, 15*time.Minute)

			Expect(post).To(BeNil())
			Expect(err).To(MatchError(ContainSubstring("max bytes must be greater than zero")))
			Expect(s3Mock.PresignPostObjectCalled).To(BeFalse())
		})
	})

	Describe("Delete from S3", func() {
		It("should remove objects from S3", func() {
			err := s3bucket.DeleteObjects(ctx, bucketName, []string{"key1", "key2"})

			Expect(err).To(BeNil())
			Expect(s3Mock.DeleteObjectsCalled).To(BeTrue())
			Expect(s3Mock.LastDeleteInput).NotTo(BeNil())
			Expect(s3Mock.LastDeleteInput.Bucket).ToNot(BeNil())
			Expect(*s3Mock.LastDeleteInput.Bucket).To(Equal(bucketName))
			Expect(s3Mock.LastDeleteInput.Delete.Objects).ToNot(BeNil())
			Expect(s3Mock.LastDeleteInput.Delete.Objects).To(HaveLen(2))
			Expect(*s3Mock.LastDeleteInput.Delete.Objects[0].Key).To(Equal("key1"))
			Expect(*s3Mock.LastDeleteInput.Delete.Objects[1].Key).To(Equal("key2"))
		})

		It("should not call delete on empty keys slice", func() {
			err := s3bucket.DeleteObjects(ctx, bucketName, []string{})

			Expect(err).To(BeNil())
			Expect(s3Mock.DeleteObjectsCalled).To(BeFalse())
		})

		It("should return error when failed to delete file from S3", func() {
			s3Mock.NextError = fmt.Errorf("mock AWS error")

			err := s3bucket.DeleteObjects(ctx, bucketName, []string{key})

			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(Equal("failed to delete from bucket `test-bucket`: mock AWS error"))
		})
	})

	Describe("GetKeysWithPrefix from S3", func() {
		It("should return the keys from S3", func() {
			s3Mock.NextListObjects = &s3.ListObjectsV2Output{
				Contents: []s3types.Object{
					{Key: &key},
					{Key: aws.String("key2")},
				},
			}
			res, err := s3bucket.GetKeysWithPrefix(ctx, bucketName, key)

			Expect(err).To(BeNil())
			Expect(s3Mock.ListObjectsV2Called).To(BeTrue())
			Expect(s3Mock.LastListObjectInput).NotTo(BeNil())
			Expect(s3Mock.LastListObjectInput.Bucket).ToNot(BeNil())
			Expect(*s3Mock.LastListObjectInput.Bucket).To(Equal(bucketName))
			Expect(s3Mock.LastListObjectInput.Prefix).ToNot(BeNil())
			Expect(*s3Mock.LastListObjectInput.Prefix).To(Equal(key))

			Expect(res).To(HaveLen(2))
			Expect(res[0]).To(Equal(key))
			Expect(res[1]).To(Equal("key2"))
		})

		It("should return empty slice when no keys found", func() {
			s3Mock.NextListObjects = &s3.ListObjectsV2Output{}

			res, err := s3bucket.GetKeysWithPrefix(ctx, bucketName, key)

			Expect(err).To(BeNil())
			Expect(res).To(BeEmpty())
			Expect(s3Mock.ListObjectsV2Called).To(BeTrue())
		})

		It("should return error when failed to list objects", func() {
			s3Mock.NextError = fmt.Errorf("mock AWS error")

			res, err := s3bucket.GetKeysWithPrefix(ctx, bucketName, key)

			Expect(s3Mock.ListObjectsV2Called).To(BeTrue())
			Expect(res).To(BeEmpty())
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(Equal("failed to list objects in bucket `test-bucket`: mock AWS error"))
		})
	})

	Describe("Object operations", func() {
		It("heads, reads, copies, and deletes objects", func() {
			s3Mock.NextHeadObject = &s3.HeadObjectOutput{
				ContentLength: aws.Int64(12),
				ContentType:   aws.String("text/csv"),
			}
			s3Mock.NextGetObject = &s3.GetObjectOutput{
				Body: io.NopCloser(strings.NewReader("abcdef")),
			}

			info, err := s3bucket.HeadObject(ctx, bucketName, key)
			Expect(err).To(BeNil())
			Expect(info.Size).To(Equal(int64(12)))
			Expect(info.ContentType).To(Equal("text/csv"))

			data, err := s3bucket.ReadObjectPrefix(ctx, bucketName, key, 3)
			Expect(err).To(BeNil())
			Expect(string(data)).To(Equal("abc"))
			Expect(*s3Mock.LastGetObjectInput.Range).To(Equal("bytes=0-2"))

			data, err = s3bucket.ReadObjectRange(ctx, bucketName, key, 4, 2)
			Expect(err).To(BeNil())
			Expect(string(data)).To(Equal("de"))
			Expect(*s3Mock.LastGetObjectInput.Range).To(Equal("bytes=4-5"))

			sourceKey := "staging/my report.csv"
			Expect(s3bucket.CopyObject(ctx, bucketName, sourceKey, "raw/key", "text/csv")).To(Succeed())
			Expect(s3Mock.CopyObjectCalled).To(BeTrue())
			Expect(*s3Mock.LastCopyObjectInput.CopySource).To(Equal(bucketName + "/staging/my%20report.csv"))
			Expect(*s3Mock.LastCopyObjectInput.Key).To(Equal("raw/key"))

			Expect(s3bucket.DeleteObject(ctx, bucketName, key)).To(Succeed())
			Expect(s3Mock.DeleteObjectCalled).To(BeTrue())
		})

		It("promotes objects in local S3 without using AWS credentials", func() {
			localBucketName := "test-local-promote-" + fmt.Sprint(time.Now().UnixNano())
			localBucket := bucket.NewBucket(ctx, bucket.LocalDevS3Region, 10*1024*1024)
			DeferCleanup(func() {
				_ = os.RemoveAll(filepath.Join(bucket.StorageDir, localBucketName))
			})

			Expect(localBucket.Upload(ctx, localBucketName, "staging/my report.csv", "text/csv", strings.NewReader("title\nMetropolis\n"))).To(Succeed())
			post, err := localBucket.SignUploadPost(ctx, localBucketName, "staging/my report.csv", "text/csv", 1024, time.Minute)
			Expect(err).NotTo(HaveOccurred())
			Expect(post.URL).To(Equal("local-s3://" + localBucketName))
			Expect(post.Fields).To(HaveKeyWithValue("key", "staging/my report.csv"))

			info, err := localBucket.HeadObject(ctx, localBucketName, "staging/my report.csv")
			Expect(err).NotTo(HaveOccurred())
			Expect(info.Size).To(BeNumerically(">", 0))
			Expect(info.ContentType).To(Equal("text/csv"))

			prefix, err := localBucket.ReadObjectPrefix(ctx, localBucketName, "staging/my report.csv", 5)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(prefix)).To(Equal("title"))

			tail, err := localBucket.ReadObjectRange(ctx, localBucketName, "staging/my report.csv", 6, 10)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(tail)).To(Equal("Metropolis"))

			Expect(localBucket.CopyObject(ctx, localBucketName, "staging/my report.csv", "raw/my report.csv", "text/csv")).To(Succeed())
			Expect(filepath.Join(bucket.StorageDir, localBucketName, "raw/my report.csv")).To(BeAnExistingFile())
			Expect(localBucket.DeleteObject(ctx, localBucketName, "staging/my report.csv")).To(Succeed())
			Expect(filepath.Join(bucket.StorageDir, localBucketName, "staging/my report.csv")).NotTo(BeAnExistingFile())
		})
	})
})
