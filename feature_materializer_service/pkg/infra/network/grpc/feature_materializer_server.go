package grpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	usecase "feature_materializer_service/pkg/app"
	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"
	featurepb "lib/data_contracts_lib/feature_materializer"
	"lib/shared_lib/ctxutil"
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
	searchUsecase      usecase.EmbeddingSearchUsecase
	graphSearchUsecase usecase.GraphSearchUsecase
	mu                 sync.Mutex
	grpcServer         *grpc.Server
	ready              atomic.Bool
}

func NewFeatureMaterializerGrpcServer(searchUsecase usecase.EmbeddingSearchUsecase, graphSearchUsecase usecase.GraphSearchUsecase) *FeatureMaterializerServer {
	log.Trace("NewFeatureMaterializerGrpcServer")

	if searchUsecase == nil {
		log.Fatal("NewFeatureMaterializerGrpcServer: searchUsecase is required")
	}
	if graphSearchUsecase == nil {
		log.Fatal("NewFeatureMaterializerGrpcServer: graphSearchUsecase is required")
	}
	return &FeatureMaterializerServer{
		searchUsecase:      searchUsecase,
		graphSearchUsecase: graphSearchUsecase,
	}
}

func (s *FeatureMaterializerServer) Connect(port int) error {
	log.Trace("FeatureMaterializerServer Connect")

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.WithError(err).WithField("port", port).Error("FeatureMaterializerServer failed to listen")
		return fmt.Errorf("failed to open gRPC port %d: %w", port, err)
	}
	return s.Serve(lis)
}

func (s *FeatureMaterializerServer) Serve(lis net.Listener) error {
	log.Trace("FeatureMaterializerServer Serve")

	grpcServer := rpcLib.NewServer(
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
	featurepb.RegisterFeatureMaterializerServiceServer(grpcServer, s)

	s.mu.Lock()
	s.grpcServer = grpcServer
	s.mu.Unlock()

	s.ready.Store(true)
	defer s.ready.Store(false)
	if err := grpcServer.Serve(lis); err != nil {
		if errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}
		log.WithError(err).Error("FeatureMaterializerServer failed to serve")
		return fmt.Errorf("failed to serve gRPC: %w", err)
	}
	return nil
}

func (s *FeatureMaterializerServer) Close() {
	log.Trace("FeatureMaterializerServer Close")

	s.mu.Lock()
	grpcServer := s.grpcServer
	s.mu.Unlock()
	if grpcServer == nil {
		return
	}
	s.ready.Store(false)
	grpcServer.Stop()
}

func (s *FeatureMaterializerServer) Shutdown(ctx context.Context) error {
	log.Trace("FeatureMaterializerServer Shutdown")

	s.mu.Lock()
	grpcServer := s.grpcServer
	s.mu.Unlock()
	if grpcServer == nil {
		return nil
	}
	s.ready.Store(false)
	done := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		grpcServer.Stop()
		return ctx.Err()
	}
}

func (s *FeatureMaterializerServer) Ready() bool {
	log.Trace("FeatureMaterializerServer Ready")

	return s.ready.Load()
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
	userID, err := uuid.Parse(req.GetUserId())
	if err != nil || userID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid user_id")
	}
	orgID, err := uuid.Parse(req.GetOrgId())
	if err != nil || orgID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid org_id")
	}
	ctx = ctxutil.WithActorOrg(ctx, userID, orgID)
	queryText := strings.TrimSpace(req.GetQueryText())
	if queryText == "" {
		return nil, status.Error(codes.InvalidArgument, "query_text is required")
	}
	topK := int(req.GetTopK())
	if topK <= 0 {
		return nil, status.Error(codes.InvalidArgument, "top_k must be greater than zero")
	}

	result, err := s.searchUsecase.SearchEmbeddings(ctx, userID, datasetID, queryText, topK)
	if err != nil {
		return nil, embeddingSearchStatusError(err)
	}
	return embeddingSearchResultToPB(result), nil
}

func (s *FeatureMaterializerServer) SearchGraph(ctx context.Context, req *featurepb.SearchGraphRequest) (*featurepb.SearchGraphResponse, error) {
	log.Trace("FeatureMaterializerServer SearchGraph")

	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "search graph request is required")
	}
	datasetID, err := uuid.Parse(req.GetDatasetId())
	if err != nil || datasetID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid dataset_id")
	}
	userID, err := uuid.Parse(req.GetUserId())
	if err != nil || userID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid user_id")
	}
	orgID, err := uuid.Parse(req.GetOrgId())
	if err != nil || orgID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid org_id")
	}
	ctx = ctxutil.WithActorOrg(ctx, userID, orgID)
	queryText := strings.TrimSpace(req.GetQueryText())
	if queryText == "" {
		return nil, status.Error(codes.InvalidArgument, "query_text is required")
	}
	topK := int(req.GetTopK())
	if topK <= 0 {
		return nil, status.Error(codes.InvalidArgument, "top_k must be greater than zero")
	}
	maxHops := int(req.GetMaxHops())
	if maxHops <= 0 {
		return nil, status.Error(codes.InvalidArgument, "max_hops must be greater than zero")
	}
	mode := model.ParseGraphSearchMode(req.GetMode())
	if !mode.IsValid() {
		return nil, status.Error(codes.InvalidArgument, "mode must be local or global")
	}
	result, err := s.graphSearchUsecase.SearchGraphWithMode(ctx, userID, datasetID, queryText, topK, maxHops, mode)
	if err != nil {
		return nil, graphSearchStatusError(err)
	}
	return graphSearchResultToPB(result), nil
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
			OrgId:               match.OrgID.String(),
			ChunkIndex:          int32(match.ChunkIndex),
			SourceText:          match.SourceText,
			Distance:            match.Distance,
			Similarity:          match.Similarity,
		}
	}

	return &featurepb.SearchEmbeddingsResponse{
		DatasetId:           snapshot.DatasetID.String(),
		OrgId:               snapshot.OrgID.String(),
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

func graphSearchResultToPB(result *model.GraphSearchResult) *featurepb.SearchGraphResponse {
	log.Trace("graphSearchResultToPB")

	if result == nil || result.GraphSnapshot == nil {
		return &featurepb.SearchGraphResponse{}
	}
	snapshot := result.GraphSnapshot
	contexts := make([]*featurepb.GraphRetrievedContext, len(result.Contexts))
	for i, context := range result.Contexts {
		contexts[i] = &featurepb.GraphRetrievedContext{
			GraphNodeChunkId:    context.GraphNodeChunkID.String(),
			GraphNodeId:         context.GraphNodeID.String(),
			EmbeddingRecordId:   context.EmbeddingRecordID.String(),
			EmbeddingSnapshotId: context.EmbeddingSnapshotID.String(),
			DatasetId:           context.DatasetID.String(),
			OrgId:               context.OrgID.String(),
			ChunkIndex:          int32(context.ChunkIndex),
			SourceText:          context.SourceText,
			Score:               context.Score,
		}
	}
	entities := make([]*featurepb.GraphMatchedEntity, len(result.MatchedEntities))
	for i, entity := range result.MatchedEntities {
		entities[i] = &featurepb.GraphMatchedEntity{
			GraphNodeId: entity.GraphNodeID.String(),
			Name:        entity.Name,
			Type:        entity.Type,
			Description: entity.Description,
			Score:       entity.Score,
		}
	}
	paths := make([]*featurepb.GraphPath, len(result.Paths))
	for i, path := range result.Paths {
		nodeIDs := make([]string, len(path.GraphNodeIDs))
		for j, nodeID := range path.GraphNodeIDs {
			nodeIDs[j] = nodeID.String()
		}
		paths[i] = &featurepb.GraphPath{
			GraphNodeIds:  nodeIDs,
			RelationTypes: path.RelationTypes,
			Score:         path.Score,
		}
	}
	communityReports := make([]*featurepb.GraphCommunityReport, len(result.CommunityReports))
	for i, report := range result.CommunityReports {
		communityReports[i] = &featurepb.GraphCommunityReport{
			GraphCommunityReportId: report.GraphCommunityReportID.String(),
			GraphCommunityId:       report.GraphCommunityID.String(),
			GraphSnapshotId:        report.GraphSnapshotID.String(),
			DatasetId:              report.DatasetID.String(),
			OrgId:                  report.OrgID.String(),
			CommunityKey:           report.CommunityKey,
			Level:                  int32(report.Level),
			Title:                  report.Title,
			Summary:                report.Summary,
			ReportText:             report.ReportText,
			Rank:                   report.Rank,
			Score:                  report.Score,
		}
	}
	return &featurepb.SearchGraphResponse{
		DatasetId:           snapshot.DatasetID.String(),
		OrgId:               snapshot.OrgID.String(),
		GraphSnapshotId:     snapshot.GraphSnapshotID.String(),
		FeatureSnapshotId:   snapshot.FeatureSnapshotID.String(),
		EmbeddingSnapshotId: snapshot.EmbeddingSnapshotID.String(),
		ProvenanceHash:      snapshot.ProvenanceHash,
		Mode:                result.Mode.String(),
		Contexts:            contexts,
		MatchedEntities:     entities,
		Paths:               paths,
		CommunityReports:    communityReports,
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

func graphSearchStatusError(err error) error {
	log.Trace("graphSearchStatusError")

	if err == nil {
		return nil
	}
	code := rpcLib.MapToGRPCStatus(
		err,
		rpcLib.GRPCCode(codes.NotFound, domain.ErrGraphSnapshotNotFound),
		rpcLib.GRPCCode(codes.InvalidArgument, domain.ErrValidationFailed),
		rpcLib.GRPCCode(codes.FailedPrecondition, domain.ErrGraphSearch),
		rpcLib.GRPCCodeFunc(codes.Canceled, func(err error) bool {
			return errors.Is(err, context.Canceled)
		}),
		rpcLib.GRPCCodeFunc(codes.DeadlineExceeded, func(err error) bool {
			return errors.Is(err, context.DeadlineExceeded)
		}),
	)
	return status.Error(code, err.Error())
}
