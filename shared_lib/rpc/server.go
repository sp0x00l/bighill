package rpc

import (
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

func NewServer(opts ...grpc.ServerOption) *grpc.Server {
	base := []grpc.ServerOption{
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	}
	return grpc.NewServer(append(base, opts...)...)
}
