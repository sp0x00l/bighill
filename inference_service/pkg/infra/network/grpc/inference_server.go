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

func (s *InferenceServer) RecordFeedback(ctx context.Context, req *inferencepb.RecordFeedbackRequest) (*inferencepb.RecordFeedbackResponse, error) {
	log.Trace("InferenceServer RecordFeedback")

	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "feedback request is required")
	}
	feedbackID, err := uuid.Parse(req.GetFeedbackId())
	if err != nil || feedbackID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid feedback_id")
	}
	requestID, err := uuid.Parse(req.GetRequestId())
	if err != nil || requestID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid request_id")
	}
	userID, err := uuid.Parse(req.GetUserId())
	if err != nil || userID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid user_id")
	}
	rating := int(req.GetRating())
	if rating < -1 || rating > 1 {
		return nil, status.Error(codes.InvalidArgument, "rating must be between -1 and 1")
	}
	feedback, err := s.usecase.RecordFeedback(ctx, &model.InferenceFeedback{
		FeedbackID:      feedbackID,
		RequestID:       requestID,
		UserID:          userID,
		Accepted:        req.GetAccepted(),
		Rating:          rating,
		Comment:         strings.TrimSpace(req.GetComment()),
		PreferredAnswer: strings.TrimSpace(req.GetPreferredAnswer()),
	}, feedbackID)
	if err != nil {
		return nil, inferenceStatusError(err)
	}
	return &inferencepb.RecordFeedbackResponse{
		FeedbackId: feedback.FeedbackID.String(),
		RequestId:  feedback.RequestID.String(),
	}, nil
}

func (s *InferenceServer) ExportPreferenceDataset(ctx context.Context, req *inferencepb.ExportPreferenceDatasetRequest) (*inferencepb.ExportPreferenceDatasetResponse, error) {
	log.Trace("InferenceServer ExportPreferenceDataset")

	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "preference dataset export request is required")
	}
	requestID, err := uuid.Parse(req.GetRequestId())
	if err != nil || requestID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid request_id")
	}
	var datasetID uuid.UUID
	if strings.TrimSpace(req.GetDatasetId()) != "" {
		datasetID, err = uuid.Parse(req.GetDatasetId())
		if err != nil || datasetID == uuid.Nil {
			return nil, status.Error(codes.InvalidArgument, "invalid dataset_id")
		}
	}
	var modelID uuid.UUID
	if strings.TrimSpace(req.GetModelId()) != "" {
		modelID, err = uuid.Parse(req.GetModelId())
		if err != nil || modelID == uuid.Nil {
			return nil, status.Error(codes.InvalidArgument, "invalid model_id")
		}
	}
	outputURI := strings.TrimSpace(req.GetOutputUri())
	if outputURI == "" {
		return nil, status.Error(codes.InvalidArgument, "output_uri is required")
	}
	minExamples := int(req.GetMinExamples())
	if minExamples < 0 {
		return nil, status.Error(codes.InvalidArgument, "min_examples cannot be negative")
	}
	limit := int(req.GetLimit())
	if limit <= 0 {
		return nil, status.Error(codes.InvalidArgument, "limit must be greater than zero")
	}
	dataset, err := s.usecase.ExportPreferenceDataset(ctx, model.PreferenceDatasetExportRequest{
		RequestID:   requestID,
		DatasetID:   datasetID,
		ModelID:     modelID,
		OutputURI:   outputURI,
		MinExamples: minExamples,
		Limit:       limit,
	})
	if err != nil {
		return nil, inferenceStatusError(err)
	}
	return &inferencepb.ExportPreferenceDatasetResponse{
		RequestId:    dataset.RequestID.String(),
		DatasetId:    dataset.DatasetID.String(),
		ModelId:      dataset.ModelID.String(),
		OutputUri:    dataset.OutputURI,
		ExampleCount: int32(dataset.ExampleCount()),
		Exported:     dataset.Exported,
	}, nil
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
