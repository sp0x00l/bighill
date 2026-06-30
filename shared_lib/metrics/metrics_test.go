package metrics_test

import (
	"context"
	"errors"
	"os"
	"testing"

	metrics "lib/shared_lib/metrics"

	"github.com/jackc/pgx/v5/pgconn"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMetrics(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Metrics Suite")
}

type stubRecorder struct {
	requests         int
	errors           int
	durations        int
	lag              int
	consumedMessages int
	consumerLag      int
}

func (s *stubRecorder) RecordError(_ context.Context, _ metrics.Boundary, _ string, _ metrics.ErrorClass, _ string) {
	s.errors++
}

func (s *stubRecorder) RecordRequest(_ context.Context, _ metrics.Boundary, _ string, _ string) {
	s.requests++
}

func (s *stubRecorder) RecordDuration(_ context.Context, _ metrics.Boundary, _ string, _ string, _ float64) {
	s.durations++
}

func (s *stubRecorder) RecordKafkaLag(_ context.Context, _ string, _ float64) {
	s.lag++
}

func (s *stubRecorder) RecordKafkaMessageConsumed(_ context.Context, _, _ string, _ int32, _ string) {
	s.consumedMessages++
}

func (s *stubRecorder) RecordKafkaConsumerLag(_ context.Context, _, _ string, _ int32, _ int64) {
	s.consumerLag++
}

var _ = Describe("Metrics", func() {
	It("initializes without OTLP endpoint", func() {
		DeferCleanup(func() {
			_ = os.Unsetenv("OTEL_EXPORTER_OTLP_ENDPOINT")
		})
		Expect(os.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")).To(Succeed())
		shutdown := metrics.Init(context.Background(), "test-service", "v1")
		Expect(shutdown).NotTo(BeNil())
		shutdown()
	})

	It("allows default recorder to be overridden", func() {
		original := metrics.Default()
		DeferCleanup(func() {
			metrics.SetDefault(original)
		})

		stub := &stubRecorder{}
		metrics.SetDefault(stub)
		metrics.Default().RecordRequest(context.Background(), metrics.BoundaryKafka, "publish", "OK")
		metrics.Default().RecordError(context.Background(), metrics.BoundaryKafka, "publish", metrics.ErrorClassInternal, "INTERNAL")
		metrics.Default().RecordDuration(context.Background(), metrics.BoundaryKafka, "publish", "OK", 0.01)
		metrics.Default().RecordKafkaLag(context.Background(), "topic-a", 0.2)
		metrics.Default().RecordKafkaMessageConsumed(context.Background(), "group-a", "topic-a", 0, "message-a")
		metrics.Default().RecordKafkaConsumerLag(context.Background(), "group-a", "topic-a", 0, 5)

		Expect(stub.requests).To(Equal(1))
		Expect(stub.errors).To(Equal(1))
		Expect(stub.durations).To(Equal(1))
		Expect(stub.lag).To(Equal(1))
		Expect(stub.consumedMessages).To(Equal(1))
		Expect(stub.consumerLag).To(Equal(1))
	})
})

var _ = Describe("Classifiers", func() {
	It("classifies gRPC errors", func() {
		class, statusLabel := metrics.ClassifyGRPC(status.Error(codes.DeadlineExceeded, "timeout"))
		Expect(class).To(Equal(metrics.ErrorClassTimeout))
		Expect(statusLabel).To(Equal(codes.DeadlineExceeded.String()))

		class, statusLabel = metrics.ClassifyGRPC(status.Error(codes.AlreadyExists, "conflict"))
		Expect(class).To(Equal(metrics.ErrorClassConflict))
		Expect(statusLabel).To(Equal(codes.AlreadyExists.String()))

		class, statusLabel = metrics.ClassifyGRPC(errors.New("plain error"))
		Expect(class).To(Equal(metrics.ErrorClassUnknown))
		Expect(statusLabel).To(Equal(""))
	})

	It("classifies HTTP status codes", func() {
		Expect(metrics.ClassifyHTTPStatus(408)).To(Equal(metrics.ErrorClassTimeout))
		Expect(metrics.ClassifyHTTPStatus(429)).To(Equal(metrics.ErrorClassRateLimit))
		Expect(metrics.ClassifyHTTPStatus(401)).To(Equal(metrics.ErrorClassAuth))
		Expect(metrics.ClassifyHTTPStatus(403)).To(Equal(metrics.ErrorClassPermission))
		Expect(metrics.ClassifyHTTPStatus(404)).To(Equal(metrics.ErrorClassNotFound))
		Expect(metrics.ClassifyHTTPStatus(409)).To(Equal(metrics.ErrorClassConflict))
		Expect(metrics.ClassifyHTTPStatus(500)).To(Equal(metrics.ErrorClassInternal))
		Expect(metrics.ClassifyHTTPStatus(200)).To(Equal(metrics.ErrorClassUnknown))
	})

	It("classifies DB errors", func() {
		Expect(metrics.ClassifyDB(context.DeadlineExceeded)).To(Equal(metrics.ErrorClassTimeout))
		Expect(metrics.ClassifyDB(context.Canceled)).To(Equal(metrics.ErrorClassCanceled))
		Expect(metrics.ClassifyDB(&pgconn.PgError{Code: "23505"})).To(Equal(metrics.ErrorClassConflict))
		Expect(metrics.ClassifyDB(&pgconn.PgError{Code: "28P01"})).To(Equal(metrics.ErrorClassAuth))
		Expect(metrics.ClassifyDB(errors.New("other"))).To(Equal(metrics.ErrorClassDB))
	})

	It("classifies Redis errors", func() {
		Expect(metrics.ClassifyRedis(context.DeadlineExceeded)).To(Equal(metrics.ErrorClassTimeout))
		Expect(metrics.ClassifyRedis(context.Canceled)).To(Equal(metrics.ErrorClassCanceled))
		Expect(metrics.ClassifyRedis(errors.New("other"))).To(Equal(metrics.ErrorClassNetwork))
	})
})
