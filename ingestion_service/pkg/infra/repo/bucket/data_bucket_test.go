package bucket

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"ingestion_service/pkg/domain/model"
	coreBucket "lib/shared_lib/bucket"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestBucket(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Ingestion bucket unit test suite")
}

type fakeBucket struct {
	uploadBucket      string
	uploadKey         string
	uploadContentType string
	uploadErr         error

	signBucket      string
	signKey         string
	signContentType string
	signMaxBytes    int64
	signTTL         time.Duration
	signErr         error

	headErr    error
	rangeBytes []byte
	rangeErr   error

	copySource      string
	copyDestination string
	copyErr         error

	deleteKey string
	deleteErr error
}

func (f *fakeBucket) Upload(_ context.Context, bucket, key, contentType string, body io.Reader) error {
	f.uploadBucket = bucket
	f.uploadKey = key
	f.uploadContentType = contentType
	_, _ = io.ReadAll(body)
	return f.uploadErr
}

func (f *fakeBucket) SignUploadPost(_ context.Context, bucket, key, contentType string, maxBytes int64, timeout time.Duration) (*coreBucket.PresignedPost, error) {
	f.signBucket = bucket
	f.signKey = key
	f.signContentType = contentType
	f.signMaxBytes = maxBytes
	f.signTTL = timeout
	if f.signErr != nil {
		return nil, f.signErr
	}
	return &coreBucket.PresignedPost{URL: "https://s3.local/upload", Fields: map[string]string{"key": key}}, nil
}

func (f *fakeBucket) HeadObject(context.Context, string, string) (*coreBucket.ObjectInfo, error) {
	if f.headErr != nil {
		return nil, f.headErr
	}
	return &coreBucket.ObjectInfo{Size: 42, ContentType: "text/csv", Checksum: "sha256"}, nil
}

func (f *fakeBucket) ReadObjectPrefix(context.Context, string, string, int64) ([]byte, error) {
	return nil, nil
}

func (f *fakeBucket) ReadObjectRange(_ context.Context, _ string, _ string, _ int64, _ int64) ([]byte, error) {
	return f.rangeBytes, f.rangeErr
}

func (f *fakeBucket) CopyObject(_ context.Context, _ string, sourceKey, destinationKey, _ string) error {
	f.copySource = sourceKey
	f.copyDestination = destinationKey
	return f.copyErr
}

func (f *fakeBucket) DeleteObject(_ context.Context, _ string, key string) error {
	f.deleteKey = key
	return f.deleteErr
}

var _ = Describe("DataBucket", func() {
	var (
		ctx     context.Context
		backend *fakeBucket
		bucket  *DataBucket
		session *model.UploadSession
	)

	BeforeEach(func() {
		ctx = context.Background()
		backend = &fakeBucket{rangeBytes: []byte("tail")}
		bucket = NewDataBucket("mlops-artifacts", backend)
		session = &model.UploadSession{
			UploadID:            uuid.New(),
			UserID:              uuid.New(),
			StagingKey:          "staging/file.csv",
			FinalKey:            "raw/file.csv",
			DeclaredContentType: "text/csv",
			ExpiresAt:           time.Now().UTC().Add(15 * time.Minute),
		}
	})

	It("saves direct uploads under the raw tenant path", func() {
		file := &model.DataFile{
			DatasetID:   uuid.New(),
			UserID:      uuid.New(),
			File:        nopMultipartFile{Reader: bytes.NewReader([]byte("id,title\n1,movie"))},
			ContentType: "text/csv",
			Extension:   ".csv",
		}

		location, err := bucket.Save(ctx, file)

		Expect(err).NotTo(HaveOccurred())
		Expect(location).To(HavePrefix("s3://mlops-artifacts/raw/" + file.UserID.String() + "/" + file.DatasetID.String() + "/"))
		Expect(location).To(HaveSuffix(".csv"))
		Expect(backend.uploadContentType).To(Equal("text/csv"))
		Expect(strings.HasPrefix(backend.uploadKey, "raw/")).To(BeTrue())
	})

	It("propagates direct upload failures", func() {
		backend.uploadErr = errors.New("upload failed")

		_, err := bucket.Save(ctx, &model.DataFile{
			DatasetID: uuid.New(),
			UserID:    uuid.New(),
			File:      nopMultipartFile{Reader: bytes.NewReader([]byte("data"))},
			Extension: "csv",
		})

		Expect(err).To(MatchError("upload failed"))
	})

	It("signs, inspects, reads, promotes, and deletes staged objects", func() {
		post, err := bucket.SignUploadPost(ctx, session, 1024, time.Minute)
		Expect(err).NotTo(HaveOccurred())
		Expect(post.URL).To(Equal("https://s3.local/upload"))
		Expect(post.ExpiresAt).To(Equal(session.ExpiresAt))
		Expect(backend.signMaxBytes).To(Equal(int64(1024)))

		info, err := bucket.HeadStagingObject(ctx, session)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.Size).To(Equal(int64(42)))

		tail, err := bucket.ReadStagingRange(ctx, session, 10, 4)
		Expect(err).NotTo(HaveOccurred())
		Expect(tail).To(Equal([]byte("tail")))

		location, err := bucket.PromoteStagedObject(ctx, session, "text/csv")
		Expect(err).NotTo(HaveOccurred())
		Expect(location).To(Equal("s3://mlops-artifacts/raw/file.csv"))
		Expect(backend.copySource).To(Equal(session.StagingKey))
		Expect(backend.copyDestination).To(Equal(session.FinalKey))

		Expect(bucket.DeleteStagedObject(ctx, session)).To(Succeed())
		Expect(backend.deleteKey).To(Equal(session.StagingKey))
	})

	It("propagates staged object operation failures", func() {
		backend.signErr = errors.New("sign failed")
		_, err := bucket.SignUploadPost(ctx, session, 1024, time.Minute)
		Expect(err).To(MatchError("sign failed"))

		backend.copyErr = errors.New("copy failed")
		_, err = bucket.PromoteStagedObject(ctx, session, "text/csv")
		Expect(err).To(MatchError("copy failed"))

		backend.deleteErr = errors.New("delete failed")
		Expect(bucket.DeleteStagedObject(ctx, session)).To(MatchError("delete failed"))
	})
})

type nopMultipartFile struct {
	*bytes.Reader
}

func (f nopMultipartFile) Close() error {
	return nil
}
