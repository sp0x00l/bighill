package rpc_test

import (
	"context"
	"errors"
	"testing"

	metrics "lib/shared_lib/metrics"
	rpc "lib/shared_lib/rpc"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRPC(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "RPC Metrics Suite")
}

type stubRecorder struct {
	requests  int
	errors    int
	durations int
	lag       int
	lastReq   record
	lastErr   record
}

type record struct {
	boundary  metrics.Boundary
	operation string
	class     metrics.ErrorClass
	status    string
}

func (s *stubRecorder) RecordError(_ context.Context, boundary metrics.Boundary, operation string, class metrics.ErrorClass, status string) {
	s.errors++
	s.lastErr = record{boundary: boundary, operation: operation, class: class, status: status}
}

func (s *stubRecorder) RecordRequest(_ context.Context, boundary metrics.Boundary, operation string, status string) {
	s.requests++
	s.lastReq = record{boundary: boundary, operation: operation, status: status}
}

func (s *stubRecorder) RecordDuration(_ context.Context, boundary metrics.Boundary, operation string, status string, _ float64) {
	s.durations++
}

func (s *stubRecorder) RecordKafkaLag(_ context.Context, _ string, _ float64) {
	s.lag++
}

func (s *stubRecorder) RecordKafkaMessageConsumed(_ context.Context, _, _ string, _ int32, _ string) {
}

func (s *stubRecorder) RecordKafkaConsumerLag(_ context.Context, _, _ string, _ int32, _ int64) {}

type streamStub struct {
	ctx context.Context
	grpc.ServerStream
}

func (s *streamStub) Context() context.Context {
	return s.ctx
}

var _ = Describe("gRPC metrics interceptors", func() {
	var (
		recorder *stubRecorder
		original metrics.Recorder
	)

	BeforeEach(func() {
		original = metrics.Default()
		recorder = &stubRecorder{}
		metrics.SetDefault(recorder)
	})

	AfterEach(func() {
		metrics.SetDefault(original)
	})

	It("records unary requests and errors", func() {
		interceptor := rpc.MetricsUnaryServerInterceptor()
		info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/Unary"}

		resp, err := interceptor(context.Background(), "req", info, func(ctx context.Context, req any) (any, error) {
			return "ok", nil
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(resp).To(Equal("ok"))
		Expect(recorder.requests).To(Equal(1))
		Expect(recorder.errors).To(Equal(0))
		Expect(recorder.durations).To(Equal(1))
		Expect(recorder.lastReq.boundary).To(Equal(metrics.BoundaryGrpcServer))
		Expect(recorder.lastReq.operation).To(Equal(info.FullMethod))
		Expect(recorder.lastReq.status).To(Equal(codes.OK.String()))

		boom := status.Error(codes.Internal, "boom")
		_, err = interceptor(context.Background(), "req", info, func(ctx context.Context, req any) (any, error) {
			return nil, boom
		})
		Expect(err).To(MatchError(boom))
		Expect(recorder.requests).To(Equal(2))
		Expect(recorder.errors).To(Equal(1))
		Expect(recorder.durations).To(Equal(2))
		Expect(recorder.lastErr.boundary).To(Equal(metrics.BoundaryGrpcServer))
		Expect(recorder.lastErr.operation).To(Equal(info.FullMethod))
		Expect(recorder.lastErr.class).To(Equal(metrics.ErrorClassInternal))
		Expect(recorder.lastErr.status).To(Equal(codes.Internal.String()))
	})

	It("records stream requests and errors", func() {
		interceptor := rpc.MetricsStreamServerInterceptor()
		info := &grpc.StreamServerInfo{FullMethod: "/test.Service/Stream"}
		stream := &streamStub{ctx: context.Background()}

		err := interceptor(nil, stream, info, func(srv any, stream grpc.ServerStream) error {
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(recorder.requests).To(Equal(1))
		Expect(recorder.errors).To(Equal(0))
		Expect(recorder.durations).To(Equal(1))
		Expect(recorder.lastReq.boundary).To(Equal(metrics.BoundaryGrpcServer))
		Expect(recorder.lastReq.operation).To(Equal(info.FullMethod))
		Expect(recorder.lastReq.status).To(Equal(codes.OK.String()))

		fail := errors.New("stream boom")
		err = interceptor(nil, stream, info, func(srv any, stream grpc.ServerStream) error {
			return fail
		})
		Expect(err).To(MatchError(fail))
		Expect(recorder.requests).To(Equal(2))
		Expect(recorder.errors).To(Equal(1))
		Expect(recorder.durations).To(Equal(2))
		Expect(recorder.lastErr.boundary).To(Equal(metrics.BoundaryGrpcServer))
		Expect(recorder.lastErr.operation).To(Equal(info.FullMethod))
		Expect(recorder.lastErr.class).To(Equal(metrics.ErrorClassUnknown))
		Expect(recorder.lastErr.status).To(Equal(codes.Unknown.String()))
	})
})
