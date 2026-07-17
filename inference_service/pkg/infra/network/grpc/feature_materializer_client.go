package grpc

import (
	"context"
	"fmt"
	"time"

	usecase "inference_service/pkg/app"
	"inference_service/pkg/domain/model"
	featurepb "lib/data_contracts_lib/feature_materializer"
	"lib/shared_lib/ctxutil"
	rpcLib "lib/shared_lib/rpc"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type FeatureMaterializerClientConfig struct {
	Address       string
	DialTimeoutMs int
	CallTimeoutMs int
	RetryCount    int
}

func ValidateFeatureMaterializerClientConfig(config FeatureMaterializerClientConfig) error {
	log.Trace("ValidateFeatureMaterializerClientConfig")

	if config.Address == "" {
		return fmt.Errorf("feature materializer grpc address is required")
	}
	if config.DialTimeoutMs <= 0 {
		return fmt.Errorf("feature materializer grpc dial timeout must be greater than zero")
	}
	if config.CallTimeoutMs <= 0 {
		return fmt.Errorf("feature materializer grpc call timeout must be greater than zero")
	}
	if config.RetryCount <= 0 {
		return fmt.Errorf("feature materializer grpc retry count must be greater than zero")
	}
	return nil
}

type featureMaterializerClient struct {
	conn   *grpc.ClientConn
	client featurepb.FeatureMaterializerServiceClient
}

func NewFeatureMaterializerClient(ctx context.Context, config FeatureMaterializerClientConfig, opts ...grpc.DialOption) (usecase.RetrievalClient, error) {
	log.Trace("NewFeatureMaterializerClient")

	conn, err := rpcLib.NewClient(ctx, rpcLib.Config{
		Address:          config.Address,
		Insecure:         true,
		DialTimeout:      time.Duration(config.DialTimeoutMs) * time.Millisecond,
		PerCallTimeout:   time.Duration(config.CallTimeoutMs) * time.Millisecond,
		MaxRetryAttempts: config.RetryCount,
	}, opts...)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("feature materializer grpc connection instantiation failed")
		return nil, err
	}

	return &featureMaterializerClient{
		conn:   conn,
		client: featurepb.NewFeatureMaterializerServiceClient(conn),
	}, nil
}

func (c *featureMaterializerClient) Close() error {
	log.Trace("featureMaterializerClient Close")

	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *featureMaterializerClient) SearchEmbeddings(ctx context.Context, userID uuid.UUID, datasetID uuid.UUID, queryText string, topK int, metadataFilters map[string]string) ([]model.RetrievedContext, error) {
	log.Trace("featureMaterializerClient SearchEmbeddings")

	orgID, ok := ctxutil.OrgID(ctx)
	if !ok {
		return nil, fmt.Errorf("org id is required")
	}
	resp, err := c.client.SearchEmbeddings(ctx, &featurepb.SearchEmbeddingsRequest{
		DatasetId:       datasetID.String(),
		UserId:          userID.String(),
		OrgId:           orgID.String(),
		QueryText:       queryText,
		TopK:            int32(topK),
		MetadataFilters: metadataFilters,
	})
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("feature materializer search embeddings failed")
		return nil, fmt.Errorf("search embeddings: %w", rpcLib.ExtractGRPCErrMsg(err))
	}
	contexts, err := embeddingSearchMatchesToContexts(resp.GetMatches())
	if err != nil {
		return nil, err
	}
	return contexts, nil
}

func (c *featureMaterializerClient) SearchGraph(ctx context.Context, userID uuid.UUID, datasetID uuid.UUID, queryText string, topK int, maxHops int) ([]model.RetrievedContext, error) {
	log.Trace("featureMaterializerClient SearchGraph")

	orgID, ok := ctxutil.OrgID(ctx)
	if !ok {
		return nil, fmt.Errorf("org id is required")
	}
	resp, err := c.client.SearchGraph(ctx, &featurepb.SearchGraphRequest{
		DatasetId: datasetID.String(),
		UserId:    userID.String(),
		OrgId:     orgID.String(),
		QueryText: queryText,
		TopK:      int32(topK),
		MaxHops:   int32(maxHops),
	})
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("feature materializer search graph failed")
		return nil, fmt.Errorf("search graph: %w", rpcLib.ExtractGRPCErrMsg(err))
	}
	contexts, err := graphSearchContextsToRetrievedContexts(resp.GetContexts())
	if err != nil {
		return nil, err
	}
	return contexts, nil
}

func embeddingSearchMatchesToContexts(matches []*featurepb.EmbeddingSearchMatch) ([]model.RetrievedContext, error) {
	log.Trace("embeddingSearchMatchesToContexts")

	contexts := make([]model.RetrievedContext, 0, len(matches))
	for _, match := range matches {
		if match == nil {
			continue
		}
		recordID, err := uuid.Parse(match.GetEmbeddingRecordId())
		if err != nil || recordID == uuid.Nil {
			return nil, fmt.Errorf("feature materializer returned invalid embedding_record_id")
		}
		snapshotID, err := uuid.Parse(match.GetEmbeddingSnapshotId())
		if err != nil || snapshotID == uuid.Nil {
			return nil, fmt.Errorf("feature materializer returned invalid embedding_snapshot_id")
		}
		datasetID := uuid.Nil
		if match.GetDatasetId() != "" {
			parsedDatasetID, err := uuid.Parse(match.GetDatasetId())
			if err != nil || parsedDatasetID == uuid.Nil {
				return nil, fmt.Errorf("feature materializer returned invalid dataset_id")
			}
			datasetID = parsedDatasetID
		}
		contexts = append(contexts, model.RetrievedContext{
			EmbeddingRecordID:   recordID,
			EmbeddingSnapshotID: snapshotID,
			DatasetID:           datasetID,
			ChunkIndex:          int(match.GetChunkIndex()),
			SourceText:          match.GetSourceText(),
			Distance:            match.GetDistance(),
			Similarity:          match.GetSimilarity(),
		})
	}
	return contexts, nil
}

func graphSearchContextsToRetrievedContexts(matches []*featurepb.GraphRetrievedContext) ([]model.RetrievedContext, error) {
	log.Trace("graphSearchContextsToRetrievedContexts")

	contexts := make([]model.RetrievedContext, 0, len(matches))
	for _, match := range matches {
		if match == nil {
			continue
		}
		recordID, err := uuid.Parse(match.GetEmbeddingRecordId())
		if err != nil || recordID == uuid.Nil {
			return nil, fmt.Errorf("feature materializer returned invalid embedding_record_id")
		}
		snapshotID, err := uuid.Parse(match.GetEmbeddingSnapshotId())
		if err != nil || snapshotID == uuid.Nil {
			return nil, fmt.Errorf("feature materializer returned invalid embedding_snapshot_id")
		}
		datasetID, err := uuid.Parse(match.GetDatasetId())
		if err != nil || datasetID == uuid.Nil {
			return nil, fmt.Errorf("feature materializer returned invalid dataset_id")
		}
		contexts = append(contexts, model.RetrievedContext{
			EmbeddingRecordID:   recordID,
			EmbeddingSnapshotID: snapshotID,
			DatasetID:           datasetID,
			ChunkIndex:          int(match.GetChunkIndex()),
			SourceText:          match.GetSourceText(),
			Similarity:          match.GetScore(),
		})
	}
	return contexts, nil
}
