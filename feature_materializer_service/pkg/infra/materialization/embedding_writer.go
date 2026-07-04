package materialization

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"strings"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
)

const defaultEmbeddingDimensions = 384

type EmbeddingRecordRepository interface {
	SaveEmbeddingRecords(ctx context.Context, records []model.EmbeddingRecord) error
}

type EmbeddingProvider interface {
	Embed(ctx context.Context, texts []string) ([][]float32, error)
	Dimensions() int
}

type DeterministicEmbeddingProvider struct {
	dimensions int
}

func NewDeterministicEmbeddingProvider(dimensions int) *DeterministicEmbeddingProvider {
	log.Trace("NewDeterministicEmbeddingProvider")

	if dimensions <= 0 {
		dimensions = defaultEmbeddingDimensions
	}
	return &DeterministicEmbeddingProvider{dimensions: dimensions}
}

func (p *DeterministicEmbeddingProvider) Dimensions() int {
	log.Trace("DeterministicEmbeddingProvider Dimensions")

	return p.dimensions
}

func (p *DeterministicEmbeddingProvider) Embed(_ context.Context, texts []string) ([][]float32, error) {
	log.Trace("DeterministicEmbeddingProvider Embed")

	vectors := make([][]float32, len(texts))
	for textIndex, text := range texts {
		vector := make([]float32, p.dimensions)
		for i := range vector {
			hash := sha256.Sum256([]byte(fmt.Sprintf("%d:%s", i, text)))
			raw := binary.BigEndian.Uint32(hash[:4])
			vector[i] = (float32(raw%20000) - 10000) / 10000
		}
		vectors[textIndex] = normalizeVector(vector)
	}
	return vectors, nil
}

type EmbeddingWriter struct {
	store       ArtifactStore
	repository  EmbeddingRecordRepository
	provider    EmbeddingProvider
	chunker     Chunker
	strategy    model.EmbeddingStrategy
	vectorStore string
	maxRows     int
}

func NewEmbeddingWriter(store ArtifactStore, repository EmbeddingRecordRepository, provider EmbeddingProvider, chunker Chunker, strategy model.EmbeddingStrategy, vectorStore string, maxRows int) *EmbeddingWriter {
	log.Trace("NewEmbeddingWriter")

	strategy = model.NormalizeEmbeddingStrategy(strategy)
	if chunker == nil {
		switch strings.ToLower(strategy.ChunkerName) {
		case "go-token-window":
			chunker = NewTokenWindowChunker(strategy)
		case "go-structure-aware-token-window":
			chunker = NewStructureAwareTokenWindowChunker(strategy)
		default:
			chunker = unsupportedChunker{name: strategy.ChunkerName}
		}
	}
	if vectorStore == "" {
		vectorStore = "pgvector"
	}
	if maxRows <= 0 {
		maxRows = 1000
	}
	return &EmbeddingWriter{
		store:       store,
		repository:  repository,
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

func (w *EmbeddingWriter) MaterializeEmbeddings(ctx context.Context, featureSnapshot *model.FeatureSnapshot, embeddingSnapshot *model.EmbeddingSnapshot) (*model.EmbeddingSnapshot, error) {
	log.Trace("EmbeddingWriter MaterializeEmbeddings")

	if w.repository == nil || w.provider == nil || w.chunker == nil {
		return nil, domain.ErrEmbeddingMaterialize.Extend("embedding repository, provider, and chunker are required")
	}
	strategy := model.NormalizeEmbeddingStrategy(w.strategy)

	data, err := w.store.Read(ctx, featureSnapshot.StorageLocation)
	if err != nil {
		return nil, err
	}
	texts, err := ExtractTextRowsFromParquet(ctx, data, w.maxRows)
	if err != nil {
		return nil, err
	}
	texts, err = cleanTextRows(ctx, NewBasicTextCleaner(), texts)
	if err != nil {
		return nil, fmt.Errorf("%w: clean feature rows: %w", domain.ErrEmbeddingMaterialize, err)
	}
	chunks, err := w.chunker.Chunk(ctx, texts)
	if err != nil {
		return nil, fmt.Errorf("%w: chunk feature rows: %w", domain.ErrEmbeddingMaterialize, err)
	}
	chunkTexts := make([]string, len(chunks))
	for i, chunk := range chunks {
		chunkTexts[i] = chunk.Text
	}

	vectors, err := w.provider.Embed(ctx, chunkTexts)
	if err != nil {
		return nil, fmt.Errorf("%w: generate embeddings: %w", domain.ErrEmbeddingMaterialize, err)
	}
	if len(vectors) != len(chunks) {
		return nil, fmt.Errorf("%w: embedding count mismatch: expected %d got %d", domain.ErrEmbeddingMaterialize, len(chunks), len(vectors))
	}

	records := make([]model.EmbeddingRecord, len(chunks))
	for i, chunk := range chunks {
		records[i] = model.EmbeddingRecord{
			EmbeddingSnapshotID: embeddingSnapshot.EmbeddingSnapshotID,
			DatasetID:           featureSnapshot.DatasetID,
			UserID:              featureSnapshot.UserID,
			ChunkIndex:          chunk.ChunkIndex,
			SourceText:          chunk.Text,
			Vector:              normalizeVector(vectors[i]),
		}
	}
	if err := w.repository.SaveEmbeddingRecords(ctx, records); err != nil {
		return nil, fmt.Errorf("%w: save embedding records: %w", domain.ErrEmbeddingMaterialize, err)
	}

	out := *embeddingSnapshot
	out.DatasetID = featureSnapshot.DatasetID
	out.UserID = featureSnapshot.UserID
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
	return &out, nil
}
