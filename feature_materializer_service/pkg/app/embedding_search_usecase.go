package app

import (
	"context"
	"fmt"
	"math"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"
	"lib/shared_lib/ctxutil"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

type EmbeddingSearchUsecase interface {
	SearchEmbeddings(ctx context.Context, userID uuid.UUID, datasetID uuid.UUID, queryText string, topK int) (*model.EmbeddingSearchResult, error)
}

type QueryEmbeddingProvider interface {
	Embed(ctx context.Context, texts []string) ([][]float32, error)
	Dimensions() int
}

type QueryEmbeddingProviderFactory func(strategy model.EmbeddingStrategy) (QueryEmbeddingProvider, error)

type embeddingSearchUsecase struct {
	repository      EmbeddingSearchRepository
	providerFactory QueryEmbeddingProviderFactory
}

func NewEmbeddingSearchUsecase(repository EmbeddingSearchRepository, providerFactory QueryEmbeddingProviderFactory) EmbeddingSearchUsecase {
	log.Trace("NewEmbeddingSearchUsecase")

	return &embeddingSearchUsecase{
		repository:      repository,
		providerFactory: providerFactory,
	}
}

func (u *embeddingSearchUsecase) SearchEmbeddings(ctx context.Context, userID uuid.UUID, datasetID uuid.UUID, queryText string, topK int) (result *model.EmbeddingSearchResult, err error) {
	log.Trace("EmbeddingSearchUsecase SearchEmbeddings")

	ctx, span := startFeatureMaterializerSpan(ctx, "feature_materializer_service/app", "embedding.search",
		attribute.String("user_id", userID.String()),
		attribute.String("dataset_id", datasetID.String()),
		attribute.Int("top_k", topK),
	)
	defer endFeatureMaterializerSpanOnReturn(ctx, span, &err)

	if _, ok := ctxutil.OrgID(ctx); !ok {
		return nil, domain.ErrValidationFailed.Extend("org_id is required")
	}
	ctx = ctxutil.WithTenantID(ctx, userID)
	activeSnapshot, err := u.repository.ReadActiveEmbeddingSnapshot(ctx, userID, datasetID)
	if err != nil {
		return nil, err
	}
	strategy := embeddingStrategyFromSnapshot(activeSnapshot)
	if err := model.ValidateEmbeddingStrategy(strategy); err != nil {
		return nil, fmt.Errorf("%w: active embedding snapshot strategy is invalid: %w", domain.ErrEmbeddingSearch, err)
	}
	provider, err := u.providerFactory(strategy)
	if err != nil {
		return nil, fmt.Errorf("%w: create query embedding provider: %w", domain.ErrEmbeddingSearch, err)
	}
	if provider.Dimensions() != activeSnapshot.EmbeddingDimensions {
		return nil, domain.ErrEmbeddingSearch.Extend("query embedding provider dimensions do not match active embedding snapshot")
	}

	vectors, err := provider.Embed(ctx, []string{queryText})
	if err != nil {
		return nil, fmt.Errorf("%w: embed query: %w", domain.ErrEmbeddingSearch, err)
	}
	if len(vectors) != 1 {
		return nil, domain.ErrEmbeddingSearch.Extend("query embedding provider returned unexpected vector count")
	}
	queryVector := normalizeVector(vectors[0])
	records, err := u.repository.SearchEmbeddingRecords(ctx, activeSnapshot, queryVector, topK)
	if err != nil {
		return nil, err
	}
	return &model.EmbeddingSearchResult{
		EmbeddingSnapshot: activeSnapshot,
		Matches:           records,
	}, nil
}

func embeddingStrategyFromSnapshot(snapshot *model.EmbeddingSnapshot) model.EmbeddingStrategy {
	log.Trace("embeddingStrategyFromSnapshot")

	if snapshot == nil {
		return model.EmbeddingStrategy{}
	}
	return model.NormalizeEmbeddingStrategy(model.EmbeddingStrategy{
		StrategyVersion:     snapshot.StrategyVersion,
		ExtractorName:       snapshot.ExtractorName,
		ExtractorVersion:    snapshot.ExtractorVersion,
		CleanerName:         snapshot.CleanerName,
		CleanerVersion:      snapshot.CleanerVersion,
		ChunkerName:         snapshot.ChunkerName,
		ChunkerVersion:      snapshot.ChunkerVersion,
		ChunkSize:           snapshot.ChunkSize,
		ChunkOverlap:        snapshot.ChunkOverlap,
		EmbeddingProvider:   snapshot.EmbeddingProvider,
		EmbeddingModel:      snapshot.EmbeddingModel,
		EmbeddingDimensions: snapshot.EmbeddingDimensions,
	})
}

func normalizeVector(vector []float32) []float32 {
	log.Trace("normalizeVector")

	var sum float64
	for _, value := range vector {
		v := float64(value)
		sum += v * v
	}
	if sum == 0 {
		return vector
	}
	norm := float32(math.Sqrt(sum))
	out := make([]float32, len(vector))
	for i, value := range vector {
		out[i] = value / norm
	}
	return out
}
