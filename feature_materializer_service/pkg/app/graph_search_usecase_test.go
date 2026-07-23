package app_test

import (
	"context"
	"errors"

	usecase "feature_materializer_service/pkg/app"
	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"
	"lib/shared_lib/ctxutil"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type graphSearchRepoStub struct {
	activeGraph     *model.GraphSnapshot
	boundEmbedding  *model.EmbeddingSnapshot
	readEmbeddingID uuid.UUID
	searchedGraph   *model.GraphSnapshot
	searchedSeed    model.GraphSearchSeed
	searchedTopK    int
	searchedMaxHops int
	result          *model.GraphSearchResult
}

func (s *graphSearchRepoStub) ReadActiveGraphSnapshot(_ context.Context, _ uuid.UUID, _ uuid.UUID) (*model.GraphSnapshot, error) {
	return s.activeGraph, nil
}

func (s *graphSearchRepoStub) ReadEmbeddingSnapshot(_ context.Context, embeddingSnapshotID uuid.UUID) (*model.EmbeddingSnapshot, error) {
	s.readEmbeddingID = embeddingSnapshotID
	return s.boundEmbedding, nil
}

func (s *graphSearchRepoStub) SearchGraph(_ context.Context, graphSnapshot *model.GraphSnapshot, seed model.GraphSearchSeed, topK int, maxHops int) (*model.GraphSearchResult, error) {
	s.searchedGraph = graphSnapshot
	s.searchedSeed = seed
	s.searchedTopK = topK
	s.searchedMaxHops = maxHops
	if s.result != nil {
		return s.result, nil
	}
	return &model.GraphSearchResult{GraphSnapshot: graphSnapshot}, nil
}

var _ = Describe("GraphSearchUsecase", func() {
	It("embeds graph queries with the graph-bound embedding snapshot strategy", func() {
		graphSnapshot := validGraphSnapshot()
		graphSnapshot.Status = model.SnapshotStatusReady
		embeddingSnapshot := validSearchEmbeddingSnapshot(graphSnapshot.DatasetID)
		embeddingSnapshot.EmbeddingSnapshotID = graphSnapshot.EmbeddingSnapshotID
		embeddingSnapshot.EmbeddingModel = "graph-bound-embedding-model"
		embeddingSnapshot.EmbeddingDimensions = 2
		repo := &graphSearchRepoStub{
			activeGraph:    graphSnapshot,
			boundEmbedding: embeddingSnapshot,
		}
		var receivedStrategy model.EmbeddingStrategy
		uc := usecase.NewGraphSearchUsecase(repo, func(strategy model.EmbeddingStrategy) (usecase.QueryEmbeddingProvider, error) {
			receivedStrategy = strategy
			return queryEmbeddingProviderStub{
				dimensions: embeddingSnapshot.EmbeddingDimensions,
				vectors:    [][]float32{{3, 4}},
			}, nil
		})

		ctx := ctxutil.WithActorOrg(context.Background(), graphSnapshot.UserID, graphSnapshot.OrgID)
		result, err := uc.SearchGraph(ctx, graphSnapshot.UserID, graphSnapshot.DatasetID, "semantic routing question", 7, 3)

		Expect(err).NotTo(HaveOccurred())
		Expect(result.GraphSnapshot).To(Equal(graphSnapshot))
		Expect(repo.readEmbeddingID).To(Equal(graphSnapshot.EmbeddingSnapshotID))
		Expect(receivedStrategy.EmbeddingModel).To(Equal("graph-bound-embedding-model"))
		Expect(receivedStrategy.EmbeddingProvider).To(Equal(embeddingSnapshot.EmbeddingProvider))
		Expect(repo.searchedGraph).To(Equal(graphSnapshot))
		Expect(repo.searchedTopK).To(Equal(7))
		Expect(repo.searchedMaxHops).To(Equal(3))
		Expect(repo.searchedSeed.QueryText).To(Equal("semantic routing question"))
		Expect(repo.searchedSeed.EmbeddingDimensions).To(Equal(2))
		Expect(repo.searchedSeed.Mode).To(Equal(model.GraphSearchModeLocal))
		Expect(repo.searchedSeed.QueryVector[0]).To(BeNumerically("~", 0.6, 0.0001))
		Expect(repo.searchedSeed.QueryVector[1]).To(BeNumerically("~", 0.8, 0.0001))
	})

	It("passes global graph search mode through the embedded seed", func() {
		graphSnapshot := validGraphSnapshot()
		graphSnapshot.Status = model.SnapshotStatusReady
		embeddingSnapshot := validSearchEmbeddingSnapshot(graphSnapshot.DatasetID)
		embeddingSnapshot.EmbeddingSnapshotID = graphSnapshot.EmbeddingSnapshotID
		embeddingSnapshot.EmbeddingDimensions = 2
		repo := &graphSearchRepoStub{
			activeGraph:    graphSnapshot,
			boundEmbedding: embeddingSnapshot,
		}
		uc := usecase.NewGraphSearchUsecase(repo, func(model.EmbeddingStrategy) (usecase.QueryEmbeddingProvider, error) {
			return queryEmbeddingProviderStub{
				dimensions: embeddingSnapshot.EmbeddingDimensions,
				vectors:    [][]float32{{1, 0}},
			}, nil
		})

		ctx := ctxutil.WithActorOrg(context.Background(), graphSnapshot.UserID, graphSnapshot.OrgID)
		result, err := uc.SearchGraphWithMode(ctx, graphSnapshot.UserID, graphSnapshot.DatasetID, "routing overview", 4, 2, model.GraphSearchModeGlobal)

		Expect(err).NotTo(HaveOccurred())
		Expect(result.GraphSnapshot).To(Equal(graphSnapshot))
		Expect(repo.searchedSeed.Mode).To(Equal(model.GraphSearchModeGlobal))
		Expect(repo.searchedTopK).To(Equal(4))
	})

	It("rejects unsupported graph search modes before reading snapshots", func() {
		repo := &graphSearchRepoStub{}
		uc := usecase.NewGraphSearchUsecase(repo, nil)

		result, err := uc.SearchGraphWithMode(context.Background(), uuid.New(), uuid.New(), "query", 5, 2, model.GraphSearchMode("wide"))

		Expect(result).To(BeNil())
		Expect(errors.Is(err, domain.ErrGraphSearch)).To(BeTrue())
		Expect(repo.readEmbeddingID).To(Equal(uuid.Nil))
	})

	It("rejects empty graph queries before reading snapshots", func() {
		repo := &graphSearchRepoStub{}
		uc := usecase.NewGraphSearchUsecase(repo, nil)

		result, err := uc.SearchGraph(context.Background(), uuid.New(), uuid.New(), "   ", 5, 2)

		Expect(result).To(BeNil())
		Expect(errors.Is(err, domain.ErrGraphSearch)).To(BeTrue())
		Expect(repo.readEmbeddingID).To(Equal(uuid.Nil))
		Expect(repo.searchedGraph).To(BeNil())
	})

	It("requires a query embedding provider factory", func() {
		graphSnapshot := validGraphSnapshot()
		graphSnapshot.Status = model.SnapshotStatusReady
		embeddingSnapshot := validSearchEmbeddingSnapshot(graphSnapshot.DatasetID)
		embeddingSnapshot.EmbeddingSnapshotID = graphSnapshot.EmbeddingSnapshotID
		repo := &graphSearchRepoStub{
			activeGraph:    graphSnapshot,
			boundEmbedding: embeddingSnapshot,
		}
		uc := usecase.NewGraphSearchUsecase(repo, nil)

		ctx := ctxutil.WithActorOrg(context.Background(), graphSnapshot.UserID, graphSnapshot.OrgID)
		result, err := uc.SearchGraph(ctx, graphSnapshot.UserID, graphSnapshot.DatasetID, "query", 5, 2)

		Expect(result).To(BeNil())
		Expect(errors.Is(err, domain.ErrGraphSearch)).To(BeTrue())
		Expect(repo.searchedGraph).To(BeNil())
	})

	It("rejects query providers that do not match the graph-bound snapshot dimensions", func() {
		graphSnapshot := validGraphSnapshot()
		graphSnapshot.Status = model.SnapshotStatusReady
		embeddingSnapshot := validSearchEmbeddingSnapshot(graphSnapshot.DatasetID)
		embeddingSnapshot.EmbeddingSnapshotID = graphSnapshot.EmbeddingSnapshotID
		embeddingSnapshot.EmbeddingDimensions = 2
		repo := &graphSearchRepoStub{
			activeGraph:    graphSnapshot,
			boundEmbedding: embeddingSnapshot,
		}
		uc := usecase.NewGraphSearchUsecase(repo, func(model.EmbeddingStrategy) (usecase.QueryEmbeddingProvider, error) {
			return queryEmbeddingProviderStub{dimensions: 3}, nil
		})

		ctx := ctxutil.WithActorOrg(context.Background(), graphSnapshot.UserID, graphSnapshot.OrgID)
		result, err := uc.SearchGraph(ctx, graphSnapshot.UserID, graphSnapshot.DatasetID, "query", 5, 2)

		Expect(result).To(BeNil())
		Expect(errors.Is(err, domain.ErrGraphSearch)).To(BeTrue())
		Expect(repo.searchedGraph).To(BeNil())
	})
})
