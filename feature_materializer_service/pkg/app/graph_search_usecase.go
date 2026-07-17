package app

import (
	"context"

	"feature_materializer_service/pkg/domain/model"
	"lib/shared_lib/ctxutil"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

type GraphSearchUsecase interface {
	SearchGraph(ctx context.Context, userID uuid.UUID, datasetID uuid.UUID, queryText string, topK int, maxHops int) (*model.GraphSearchResult, error)
}

type graphSearchUsecase struct {
	repository GraphSearchRepository
}

func NewGraphSearchUsecase(repository GraphSearchRepository) GraphSearchUsecase {
	log.Trace("NewGraphSearchUsecase")

	return &graphSearchUsecase{repository: repository}
}

func (u *graphSearchUsecase) SearchGraph(ctx context.Context, userID uuid.UUID, datasetID uuid.UUID, queryText string, topK int, maxHops int) (result *model.GraphSearchResult, err error) {
	log.Trace("GraphSearchUsecase SearchGraph")

	ctx, span := startFeatureMaterializerSpan(ctx, "feature_materializer_service/app", "graph.search",
		attribute.String("user_id", userID.String()),
		attribute.String("dataset_id", datasetID.String()),
		attribute.Int("top_k", topK),
		attribute.Int("max_hops", maxHops),
	)
	defer endFeatureMaterializerSpanOnReturn(ctx, span, &err)

	ctx = ctxutil.WithTenantID(ctx, userID)
	activeSnapshot, err := u.repository.ReadActiveGraphSnapshot(ctx, userID, datasetID)
	if err != nil {
		return nil, err
	}
	return u.repository.SearchGraph(ctx, activeSnapshot, queryText, topK, maxHops)
}
