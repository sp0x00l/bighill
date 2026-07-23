package app

import (
	"context"
	"fmt"
	"strings"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"
	"lib/shared_lib/ctxutil"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

type GraphSearchUsecase interface {
	SearchGraph(ctx context.Context, userID uuid.UUID, datasetID uuid.UUID, queryText string, topK int, maxHops int) (*model.GraphSearchResult, error)
	SearchGraphWithMode(ctx context.Context, userID uuid.UUID, datasetID uuid.UUID, queryText string, topK int, maxHops int, mode model.GraphSearchMode) (*model.GraphSearchResult, error)
	SearchGraphWithModeAndPolicy(ctx context.Context, userID uuid.UUID, datasetID uuid.UUID, queryText string, topK int, maxHops int, mode model.GraphSearchMode, policy model.RetrievalPolicy) (*model.GraphSearchResult, error)
}

type graphSearchUsecase struct {
	repository      GraphSearchRepository
	providerFactory QueryEmbeddingProviderFactory
}

func NewGraphSearchUsecase(repository GraphSearchRepository, providerFactory QueryEmbeddingProviderFactory) GraphSearchUsecase {
	log.Trace("NewGraphSearchUsecase")

	return &graphSearchUsecase{
		repository:      repository,
		providerFactory: providerFactory,
	}
}

func (u *graphSearchUsecase) SearchGraph(ctx context.Context, userID uuid.UUID, datasetID uuid.UUID, queryText string, topK int, maxHops int) (result *model.GraphSearchResult, err error) {
	log.Trace("GraphSearchUsecase SearchGraph")

	return u.SearchGraphWithMode(ctx, userID, datasetID, queryText, topK, maxHops, model.GraphSearchModeLocal)
}

func (u *graphSearchUsecase) SearchGraphWithMode(ctx context.Context, userID uuid.UUID, datasetID uuid.UUID, queryText string, topK int, maxHops int, mode model.GraphSearchMode) (result *model.GraphSearchResult, err error) {
	log.Trace("GraphSearchUsecase SearchGraph")

	return u.SearchGraphWithModeAndPolicy(ctx, userID, datasetID, queryText, topK, maxHops, mode, model.RetrievalPolicy{})
}

func (u *graphSearchUsecase) SearchGraphWithModeAndPolicy(ctx context.Context, userID uuid.UUID, datasetID uuid.UUID, queryText string, topK int, maxHops int, mode model.GraphSearchMode, policy model.RetrievalPolicy) (result *model.GraphSearchResult, err error) {
	log.Trace("GraphSearchUsecase SearchGraphWithModeAndPolicy")

	mode = model.ParseGraphSearchMode(mode.String())
	if !mode.IsValid() {
		return nil, domain.ErrGraphSearch.Extend("graph search mode must be local or global")
	}
	policy = model.NormalizeRetrievalPolicy(policy, topK)
	if !policy.Mode.IsValid() {
		return nil, domain.ErrValidationFailed.Extend("retrieval mode must be ann_iterative or exact_authorized")
	}
	ctx, span := startFeatureMaterializerSpan(ctx, "feature_materializer_service/app", "graph.search",
		attribute.String("user_id", userID.String()),
		attribute.String("dataset_id", datasetID.String()),
		attribute.String("mode", mode.String()),
		attribute.String("retrieval_mode", policy.Mode.String()),
		attribute.Int("top_k", topK),
		attribute.Int("max_hops", maxHops),
	)
	defer endFeatureMaterializerSpanOnReturn(ctx, span, &err)

	ctx = ctxutil.WithTenantID(ctx, userID)
	queryText = strings.TrimSpace(queryText)
	if queryText == "" {
		return nil, domain.ErrGraphSearch.Extend("query_text is required")
	}
	activeSnapshot, err := u.repository.ReadActiveGraphSnapshot(ctx, userID, datasetID)
	if err != nil {
		return nil, err
	}
	embeddingSnapshot, err := u.repository.ReadEmbeddingSnapshot(ctx, activeSnapshot.EmbeddingSnapshotID)
	if err != nil {
		return nil, fmt.Errorf("%w: read graph embedding snapshot: %w", domain.ErrGraphSearch, err)
	}
	if u.providerFactory == nil {
		return nil, domain.ErrGraphSearch.Extend("query embedding provider factory is required")
	}
	strategy := embeddingStrategyFromSnapshot(embeddingSnapshot)
	provider, err := u.providerFactory(strategy)
	if err != nil {
		return nil, fmt.Errorf("%w: create query embedding provider: %w", domain.ErrGraphSearch, err)
	}
	if provider.Dimensions() != embeddingSnapshot.EmbeddingDimensions {
		return nil, domain.ErrGraphSearch.Extend("query embedding provider dimensions do not match graph embedding snapshot")
	}
	vectors, err := provider.Embed(ctx, []string{queryText})
	if err != nil {
		return nil, fmt.Errorf("%w: embed graph query: %w", domain.ErrGraphSearch, err)
	}
	if len(vectors) != 1 {
		return nil, domain.ErrGraphSearch.Extend("query embedding provider returned unexpected vector count")
	}
	seed := model.GraphSearchSeed{
		QueryText:           queryText,
		QueryVector:         normalizeVector(vectors[0]),
		EmbeddingDimensions: embeddingSnapshot.EmbeddingDimensions,
		Mode:                mode,
	}
	return u.repository.SearchGraphWithPolicy(ctx, activeSnapshot, seed, topK, maxHops, policy)
}
