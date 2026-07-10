package materialization

import (
	"context"
	"fmt"
	"strings"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
)

const (
	chunkerGoTokenWindow    = "go-token-window"
	chunkerGoStructureAware = "go-structure-aware-token-window"
)

type EmbeddingProvider interface {
	Embed(ctx context.Context, texts []string) ([][]float32, error)
	Dimensions() int
}

type EmbeddingWriter struct {
	store       ArtifactStore
	provider    EmbeddingProvider
	chunker     Chunker
	strategy    model.EmbeddingStrategy
	vectorStore string
	maxRows     int
}

func NewEmbeddingWriter(store ArtifactStore, provider EmbeddingProvider, chunker Chunker, strategy model.EmbeddingStrategy, vectorStore string, maxRows int) *EmbeddingWriter {
	log.Trace("NewEmbeddingWriter")

	strategy = model.NormalizeEmbeddingStrategy(strategy)
	if chunker == nil {
		switch strings.ToLower(strategy.ChunkerName) {
		case chunkerGoTokenWindow:
			chunker = NewTokenWindowChunker(strategy)
		case chunkerGoStructureAware:
			chunker = NewStructureAwareTokenWindowChunker(strategy)
		default:
			chunker = unsupportedChunker{name: strategy.ChunkerName}
		}
	}
	vectorStore = strings.TrimSpace(vectorStore)
	if vectorStore == "" {
		log.Fatalf("NewEmbeddingWriter: vector store is required")
	}
	if maxRows <= 0 {
		log.Fatalf("NewEmbeddingWriter: max rows must be greater than zero")
	}
	return &EmbeddingWriter{
		store:       store,
		provider:    provider,
		chunker:     chunker,
		strategy:    strategy,
		vectorStore: vectorStore,
		maxRows:     maxRows,
	}
}

func (w *EmbeddingWriter) SupportsEmbeddings(featureSnapshot *model.FeatureSnapshot) bool {
	log.Trace("EmbeddingWriter SupportsEmbeddings")

	return featureSnapshot != nil && featureSnapshot.ProcessingProfile.RequiresEmbeddings()
}

func (w *EmbeddingWriter) MaterializeEmbeddings(ctx context.Context, featureSnapshot *model.FeatureSnapshot, embeddingSnapshot *model.EmbeddingSnapshot) (*model.EmbeddingSnapshot, []model.EmbeddingRecord, error) {
	log.Trace("EmbeddingWriter MaterializeEmbeddings")

	if w.provider == nil || w.chunker == nil {
		return nil, nil, domain.ErrEmbeddingMaterialize.Extend("embedding provider and chunker are required")
	}
	strategy := model.NormalizeEmbeddingStrategy(w.strategy)
	if err := model.ValidateEmbeddingStrategy(strategy); err != nil {
		return nil, nil, domain.ErrEmbeddingMaterialize.Extend(err.Error())
	}

	data, err := w.store.Read(ctx, featureSnapshot.StorageLocation)
	if err != nil {
		return nil, nil, err
	}
	texts, err := ExtractTextRowsFromParquet(ctx, data, w.maxRows)
	if err != nil {
		return nil, nil, err
	}
	texts, err = cleanTextRows(ctx, NewBasicTextCleaner(), texts)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: clean feature rows: %w", domain.ErrEmbeddingMaterialize, err)
	}
	chunks, err := w.chunker.Chunk(ctx, texts)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: chunk feature rows: %w", domain.ErrEmbeddingMaterialize, err)
	}
	chunkTexts := make([]string, len(chunks))
	for i, chunk := range chunks {
		chunkTexts[i] = chunk.Text
	}

	vectors, err := w.provider.Embed(ctx, chunkTexts)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: generate embeddings: %w", domain.ErrEmbeddingMaterialize, err)
	}
	if len(vectors) != len(chunks) {
		return nil, nil, fmt.Errorf("%w: embedding count mismatch: expected %d got %d", domain.ErrEmbeddingMaterialize, len(chunks), len(vectors))
	}

	records := make([]model.EmbeddingRecord, len(chunks))
	for i, chunk := range chunks {
		records[i] = model.EmbeddingRecord{
			EmbeddingSnapshotID: embeddingSnapshot.EmbeddingSnapshotID,
			DatasetID:           featureSnapshot.DatasetID,
			UserID:              featureSnapshot.UserID,
			OrgID:               featureSnapshot.OrgID,
			ChunkIndex:          chunk.ChunkIndex,
			SourceText:          chunk.Text,
			Vector:              normalizeVector(vectors[i]),
		}
	}
	out := *embeddingSnapshot
	out.DatasetID = featureSnapshot.DatasetID
	out.UserID = featureSnapshot.UserID
	out.OrgID = featureSnapshot.OrgID
	out.VectorStore = w.vectorStore
	out.CollectionName = featureSnapshot.TableName
	out.EmbeddingDimensions = w.provider.Dimensions()
	out.StrategyVersion = strategy.StrategyVersion
	out.ExtractorName = strategy.ExtractorName
	out.ExtractorVersion = strategy.ExtractorVersion
	out.CleanerName = strategy.CleanerName
	out.CleanerVersion = strategy.CleanerVersion
	out.ChunkerName = strategy.ChunkerName
	out.ChunkerVersion = strategy.ChunkerVersion
	out.ChunkSize = strategy.ChunkSize
	out.ChunkOverlap = strategy.ChunkOverlap
	out.EmbeddingProvider = strategy.EmbeddingProvider
	out.EmbeddingModel = strategy.EmbeddingModel
	out.EmbeddingCount = int64(len(records))
	out.Status = model.SnapshotStatusReady
	return &out, records, nil
}
