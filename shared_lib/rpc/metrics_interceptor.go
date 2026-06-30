package rpc

import (
	"context"
	"time"

	metrics "lib/shared_lib/metrics"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func MetricsUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		statusLabel := codes.OK.String()
		resp, err := handler(ctx, req)
		if err != nil {
			class, statusCode := metrics.ClassifyGRPC(err)
			if statusCode != "" {
				statusLabel = statusCode
			} else {
				statusLabel = codes.Unknown.String()
			}
			metrics.Default().RecordError(ctx, metrics.BoundaryGrpcServer, info.FullMethod, class, statusLabel)
		}
		metrics.Default().RecordRequest(ctx, metrics.BoundaryGrpcServer, info.FullMethod, statusLabel)
		metrics.Default().RecordDuration(ctx, metrics.BoundaryGrpcServer, info.FullMethod, statusLabel, time.Since(start).Seconds())
		return resp, err
	}
}

func MetricsStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		statusLabel := codes.OK.String()
		err := handler(srv, stream)
		if err != nil {
			class, statusCode := metrics.ClassifyGRPC(err)
			if statusCode != "" {
				statusLabel = statusCode
			} else {
				statusLabel = codes.Unknown.String()
			}
			metrics.Default().RecordError(stream.Context(), metrics.BoundaryGrpcServer, info.FullMethod, class, statusLabel)
		}
		metrics.Default().RecordRequest(stream.Context(), metrics.BoundaryGrpcServer, info.FullMethod, statusLabel)
		metrics.Default().RecordDuration(stream.Context(), metrics.BoundaryGrpcServer, info.FullMethod, statusLabel, time.Since(start).Seconds())
		return err
	}
}
