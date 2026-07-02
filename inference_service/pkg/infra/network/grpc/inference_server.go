package grpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	usecase "inference_service/pkg/app"
	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"
	inferencepb "lib/data_contracts_lib/inference"
	rpcLib "lib/shared_lib/rpc"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

type InferenceServer struct {
	inferencepb.UnimplementedInferenceServiceServer
	usecase    usecase.InferenceUsecase
	grpcServer *grpc.Server
}

func NewInferenceGrpcServer(usecase usecase.InferenceUsecase) *InferenceServer {
	log.Trace("NewInferenceGrpcServer")

	if usecase == nil {
		log.Fatal("NewInferenceGrpcServer: usecase is required")
	}
	return &InferenceServer{
		usecase: usecase,
	}
}

func (s *InferenceServer) Connect(port int) error {
	log.Trace("InferenceServer Connect")

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.WithError(err).WithField("port", port).Error("InferenceServer failed to listen")
		return fmt.Errorf("failed to open gRPC port %d: %w", port, err)
	}
	return s.Serve(lis)
}

func (s *InferenceServer) Serve(lis net.Listener) error {
	log.Trace("InferenceServer Serve")

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
	inferencepb.RegisterInferenceServiceServer(s.grpcServer, s)

	if err := s.grpcServer.Serve(lis); err != nil {
		if errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}
		log.WithError(err).Error("InferenceServer failed to serve")
		return fmt.Errorf("failed to serve gRPC: %w", err)
	}
	return nil
}

func (s *InferenceServer) Close() {
	log.Trace("InferenceServer Close")

	if s.grpcServer == nil {
		return
	}
	s.grpcServer.Stop()
}

func (s *InferenceServer) Generate(ctx context.Context, req *inferencepb.GenerateRequest) (*inferencepb.GenerateResponse, error) {
	log.Trace("InferenceServer Generate")

	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "generate request is required")
	}
	datasetID, err := uuid.Parse(req.GetDatasetId())
	if err != nil || datasetID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid dataset_id")
	}
	modelID, err := uuid.Parse(req.GetModelId())
	if err != nil || modelID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid model_id")
	}
	requestID, err := uuid.Parse(req.GetRequestId())
	if err != nil || requestID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid request_id")
	}
	queryText := strings.TrimSpace(req.GetQueryText())
	if queryText == "" {
		return nil, status.Error(codes.InvalidArgument, "query_text is required")
	}
	topK := int(req.GetTopK())
	if topK <= 0 {
		return nil, status.Error(codes.InvalidArgument, "top_k must be greater than zero")
	}

	response, err := s.usecase.Generate(ctx, model.GenerateRequest{
		RequestID:       requestID,
		DatasetID:       datasetID,
		ModelID:         modelID,
		QueryText:       queryText,
		TopK:            topK,
		MetadataFilters: req.GetMetadataFilters(),
	})
	if err != nil {
		return nil, inferenceStatusError(err)
	}
	return generateResponseToPB(response), nil
}

func generateResponseToPB(response *model.GenerateResponse) *inferencepb.GenerateResponse {
	log.Trace("generateResponseToPB")

	contexts := make([]*inferencepb.RetrievedContext, len(response.Contexts))
	for i, ctx := range response.Contexts {
		contexts[i] = &inferencepb.RetrievedContext{
			EmbeddingRecordId:   ctx.EmbeddingRecordID.String(),
			EmbeddingSnapshotId: ctx.EmbeddingSnapshotID.String(),
			ChunkIndex:          int32(ctx.ChunkIndex),
			SourceText:          ctx.SourceText,
			Distance:            ctx.Distance,
			Similarity:          ctx.Similarity,
		}
	}
	return &inferencepb.GenerateResponse{
		DatasetId:             response.DatasetID.String(),
		ModelId:               response.ModelID.String(),
		QueryText:             response.QueryText,
		Answer:                response.Answer,
		Contexts:              contexts,
		RequestId:             response.RequestID.String(),
		PromptStrategyVersion: response.PromptStrategyVersion,
		GenerationProvider:    response.GenerationProvider,
		GenerationModel:       response.GenerationModel,
	}
}

func inferenceStatusError(err error) error {
	log.Trace("inferenceStatusError")

	if err == nil {
		return nil
	}
	code := rpcLib.MapToGRPCStatus(
		err,
		rpcLib.GRPCCode(codes.NotFound, domain.ErrDatasetNotFound),
		rpcLib.GRPCCode(codes.NotFound, domain.ErrModelNotFound),
		rpcLib.GRPCCode(codes.FailedPrecondition, domain.ErrModelNotReady),
		rpcLib.GRPCCode(codes.FailedPrecondition, domain.ErrModelMismatch),
		rpcLib.GRPCCode(codes.InvalidArgument, domain.ErrValidationFailed),
		rpcLib.GRPCCode(codes.FailedPrecondition, domain.ErrDatasetNotReady),
		rpcLib.GRPCCode(codes.Unavailable, domain.ErrRetrievalFailed),
		rpcLib.GRPCCode(codes.Internal, domain.ErrGenerationFailed),
		rpcLib.GRPCCodeFunc(codes.Canceled, func(err error) bool {
			return errors.Is(err, context.Canceled)
		}),
	)
	if code == codes.Unknown {
		code = codes.Internal
	}
	return status.Error(code, err.Error())
}
