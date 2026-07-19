package grpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"tool_execution_service/pkg/app"
	"tool_execution_service/pkg/domain"

	toolspb "lib/data_contracts_lib/tools"
	rpcLib "lib/shared_lib/rpc"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

type ToolServer struct {
	toolspb.UnimplementedToolServiceServer
	usecase    app.ToolUsecase
	adapter    *ToolDTOAdapter
	grpcServer *grpc.Server
	ready      atomic.Bool
}

func NewToolServer(usecase app.ToolUsecase, adapter *ToolDTOAdapter) *ToolServer {
	log.Trace("NewToolServer")

	if usecase == nil {
		log.Fatal("tool usecase is required")
	}
	if adapter == nil {
		log.Fatal("tool dto adapter is required")
	}
	return &ToolServer{
		usecase: usecase,
		adapter: adapter,
	}
}

func (s *ToolServer) Connect(port int) error {
	log.Trace("ToolServer Connect")

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.WithError(err).WithField("port", port).Error("ToolServer failed to listen")
		return fmt.Errorf("failed to open gRPC port %d: %w", port, err)
	}
	return s.Serve(lis)
}

func (s *ToolServer) Serve(lis net.Listener) error {
	log.Trace("ToolServer Serve")

	s.grpcServer = rpcLib.NewServer(
		grpc.ChainUnaryInterceptor(rpcLib.MetricsUnaryServerInterceptor()),
		grpc.ChainStreamInterceptor(rpcLib.MetricsStreamServerInterceptor()),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             60 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    2 * time.Minute,
			Timeout: 20 * time.Second,
		}),
	)
	toolspb.RegisterToolServiceServer(s.grpcServer, s)

	s.ready.Store(true)
	defer s.ready.Store(false)
	if err := s.grpcServer.Serve(lis); err != nil {
		if errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}
		log.WithError(err).Error("ToolServer failed to serve")
		return fmt.Errorf("failed to serve gRPC: %w", err)
	}
	return nil
}

func (s *ToolServer) Close() {
	log.Trace("ToolServer Close")

	if s.grpcServer == nil {
		return
	}
	s.ready.Store(false)
	s.grpcServer.Stop()
}

func (s *ToolServer) Shutdown(ctx context.Context) error {
	log.Trace("ToolServer Shutdown")

	if s.grpcServer == nil {
		return nil
	}
	s.ready.Store(false)
	done := make(chan struct{})
	go func() {
		s.grpcServer.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.grpcServer.Stop()
		return ctx.Err()
	}
}

func (s *ToolServer) Ready() bool {
	log.Trace("ToolServer Ready")

	return s.ready.Load()
}

func (s *ToolServer) ListAvailableTools(ctx context.Context, req *toolspb.ListAvailableToolsRequest) (*toolspb.ListAvailableToolsResponse, error) {
	log.Trace("ToolServer ListAvailableTools")

	command, err := s.adapter.FromListAvailableToolsRequest(req)
	if err != nil {
		return nil, toolStatusError(err)
	}
	tools, err := s.usecase.ListAvailableTools(ctx, command)
	if err != nil {
		return nil, toolStatusError(err)
	}
	return s.adapter.ToListAvailableToolsResponse(tools), nil
}

func (s *ToolServer) Invoke(ctx context.Context, req *toolspb.InvokeToolRequest) (*toolspb.InvokeToolResponse, error) {
	log.Trace("ToolServer Invoke")

	command, err := s.adapter.FromInvokeToolRequest(req)
	if err != nil {
		return nil, toolStatusError(err)
	}
	result, err := s.usecase.Invoke(ctx, command)
	if err != nil {
		return nil, toolStatusError(err)
	}
	return s.adapter.ToInvokeToolResponse(result), nil
}

func toolStatusError(err error) error {
	log.Trace("toolStatusError")

	switch {
	case errors.Is(err, domain.ErrValidationFailed):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, domain.ErrToolNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, domain.ErrToolDenied), errors.Is(err, domain.ErrToolPolicy):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, domain.ErrToolExecution):
		return status.Error(codes.Unavailable, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
