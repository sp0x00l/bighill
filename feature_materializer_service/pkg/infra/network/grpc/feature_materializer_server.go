package grpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	usecase "feature_materializer_service/pkg/app"
	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"
	featurepb "lib/data_contracts_lib/feature_materializer"
	rpcLib "lib/shared_lib/rpc"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

type FeatureMaterializerServer struct {
	featurepb.UnimplementedFeatureMaterializerServiceServer
	searchUsecase usecase.EmbeddingSearchUsecase
	grpcServer    *grpc.Server
}

func NewFeatureMaterializerGrpcServer(searchUsecase usecase.EmbeddingSearchUsecase) *FeatureMaterializerServer {
	log.Trace("NewFeatureMaterializerGrpcServer")

	if searchUsecase == nil {
		log.Fatal("NewFeatureMaterializerGrpcServer: searchUsecase is required")
	}
	return &FeatureMaterializerServer{
		searchUsecase: searchUsecase,
	}
}

func (s *FeatureMaterializerServer) Connect(port int) error {
	log.Trace("FeatureMaterializerServer Connect")

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
	featurepb.RegisterFeatureMaterializerServiceServer(s.grpcServer, s)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.WithError(err).WithField("port", port).Error("FeatureMaterializerServer failed to listen")
		return fmt.Errorf("failed to open gRPC port %d: %w", port, err)
	}

	if err := s.grpcServer.Serve(lis); err != nil {
		log.WithError(err).Error("FeatureMaterializerServer failed to serve")
		return fmt.Errorf("failed to serve gRPC: %w", err)
	}
	return nil
}

func (s *FeatureMaterializerServer) Close() {
	log.Trace("FeatureMaterializerServer Close")

	if s.grpcServer == nil {
		return
	}
	s.grpcServer.Stop()
}

func (s *FeatureMaterializerServer) SearchEmbeddings(ctx context.Context, req *featurepb.SearchEmbeddingsRequest) (*featurepb.SearchEmbeddingsResponse, error) {
	log.Trace("FeatureMaterializerServer SearchEmbeddings")

	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "search embeddings request is required")
	}
	datasetID, err := uuid.Parse(req.GetDatasetId())
	if err != nil || datasetID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid dataset_id")
	}
	queryText := strings.TrimSpace(req.GetQueryText())
	if queryText == "" {
		return nil, status.Error(codes.InvalidArgument, "query_text is required")
	}
	topK := int(req.GetTopK())
	if topK <= 0 {
		return nil, status.Error(codes.InvalidArgument, "top_k must be greater than zero")
	}

	result, err := s.searchUsecase.SearchEmbeddings(ctx, datasetID, queryText, topK)
	if err != nil {
		return nil, embeddingSearchStatusError(err)
	}
	return embeddingSearchResultToPB(result), nil
}

func embeddingSearchResultToPB(result *model.EmbeddingSearchResult) *featurepb.SearchEmbeddingsResponse {
	log.Trace("embeddingSearchResultToPB")

	if result == nil || result.EmbeddingSnapshot == nil {
		return &featurepb.SearchEmbeddingsResponse{}
	}
	snapshot := result.EmbeddingSnapshot
	matches := make([]*featurepb.EmbeddingSearchMatch, len(result.Matches))
	for i, match := range result.Matches {
		matches[i] = &featurepb.EmbeddingSearchMatch{
			EmbeddingRecordId:   match.EmbeddingRecordID.String(),
			EmbeddingSnapshotId: match.EmbeddingSnapshotID.String(),
			DatasetId:           match.DatasetID.String(),
			ChunkIndex:          int32(match.ChunkIndex),
			SourceText:          match.SourceText,
			Distance:            match.Distance,
			Similarity:          match.Similarity,
		}
	}

	return &featurepb.SearchEmbeddingsResponse{
		DatasetId:           snapshot.DatasetID.String(),
		EmbeddingSnapshotId: snapshot.EmbeddingSnapshotID.String(),
		FeatureSnapshotId:   snapshot.FeatureSnapshotID.String(),
		VectorStore:         snapshot.VectorStore,
		CollectionName:      snapshot.CollectionName,
		EmbeddingDimensions: int32(snapshot.EmbeddingDimensions),
		StrategyVersion:     snapshot.StrategyVersion,
		ChunkerName:         snapshot.ChunkerName,
		ChunkerVersion:      snapshot.ChunkerVersion,
		ChunkSize:           int32(snapshot.ChunkSize),
		ChunkOverlap:        int32(snapshot.ChunkOverlap),
		EmbeddingProvider:   snapshot.EmbeddingProvider,
		EmbeddingModel:      snapshot.EmbeddingModel,
		Matches:             matches,
	}
}

func embeddingSearchStatusError(err error) error {
	log.Trace("embeddingSearchStatusError")

	if err == nil {
		return nil
	}
	code := rpcLib.MapToGRPCStatus(
		err,
		rpcLib.GRPCCode(codes.NotFound, domain.ErrEmbeddingSnapshotNotFound),
		rpcLib.GRPCCode(codes.InvalidArgument, domain.ErrValidationFailed),
		rpcLib.GRPCCode(codes.FailedPrecondition, domain.ErrEmbeddingSearch),
		rpcLib.GRPCCodeFunc(codes.Canceled, func(err error) bool {
			return errors.Is(err, context.Canceled)
		}),
	)
	if code == codes.Unknown {
		code = codes.Internal
	}
	return status.Error(code, err.Error())
}
