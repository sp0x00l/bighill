package materialization

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"

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
	vectorStore string
	maxRows     int
}

func NewEmbeddingWriter(store ArtifactStore, repository EmbeddingRecordRepository, provider EmbeddingProvider, vectorStore string, maxRows int) *EmbeddingWriter {
	log.Trace("NewEmbeddingWriter")

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
		vectorStore: vectorStore,
		maxRows:     maxRows,
	}
}

func (w *EmbeddingWriter) MaterializeEmbeddings(ctx context.Context, featureSnapshot *model.FeatureSnapshot, embeddingSnapshot *model.EmbeddingSnapshot) (*model.EmbeddingSnapshot, error) {
	log.Trace("EmbeddingWriter MaterializeEmbeddings")

	if w.repository == nil || w.provider == nil {
		return nil, domain.ErrEmbeddingMaterialize.Extend("embedding repository and provider are required")
	}

	data, err := w.store.Read(ctx, featureSnapshot.StorageLocation)
	if err != nil {
		return nil, err
	}
	texts, err := ExtractTextRowsFromParquet(ctx, data, w.maxRows)
	if err != nil {
		return nil, err
	}

	vectors, err := w.provider.Embed(ctx, texts)
	if err != nil {
		return nil, fmt.Errorf("%w: generate embeddings: %w", domain.ErrEmbeddingMaterialize, err)
	}

	records := make([]model.EmbeddingRecord, len(texts))
	for i, text := range texts {
		records[i] = model.EmbeddingRecord{
			EmbeddingSnapshotID: embeddingSnapshot.EmbeddingSnapshotID,
			DatasetID:           featureSnapshot.DatasetID,
			SourceText:          text,
			Vector:              vectors[i],
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
	out.EmbeddingCount = int64(len(records))
	out.Status = model.SnapshotStatusReady
	return &out, nil
}
