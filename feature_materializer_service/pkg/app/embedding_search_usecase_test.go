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

type embeddingSearchRepoStub struct {
	activeSnapshot *model.EmbeddingSnapshot
	activeErr      error
	userID         uuid.UUID
	datasetID      uuid.UUID
	queryVector    []float32
	topK           int
	records        []model.EmbeddingRecord
}

func (s *embeddingSearchRepoStub) ReadActiveEmbeddingSnapshot(_ context.Context, userID uuid.UUID, datasetID uuid.UUID) (*model.EmbeddingSnapshot, error) {
	s.userID = userID
	s.datasetID = datasetID
	if s.activeErr != nil {
		return nil, s.activeErr
	}
	return s.activeSnapshot, nil
}

func (s *embeddingSearchRepoStub) SearchEmbeddingRecords(_ context.Context, _ *model.EmbeddingSnapshot, queryVector []float32, topK int) ([]model.EmbeddingRecord, error) {
	s.queryVector = queryVector
	s.topK = topK
	return s.records, nil
}

type queryEmbeddingProviderStub struct {
	dimensions int
	vectors    [][]float32
	err        error
}

func (s queryEmbeddingProviderStub) Embed(context.Context, []string) ([][]float32, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.vectors, nil
}

func (s queryEmbeddingProviderStub) Dimensions() int {
	return s.dimensions
}

var _ = Describe("EmbeddingSearchUsecase", func() {
	It("embeds the query with the active snapshot strategy and searches that snapshot", func() {
		datasetID := uuid.New()
		activeSnapshot := validSearchEmbeddingSnapshot(datasetID)
		repo := &embeddingSearchRepoStub{
			activeSnapshot: activeSnapshot,
			records: []model.EmbeddingRecord{{
				EmbeddingRecordID:   uuid.New(),
				EmbeddingSnapshotID: activeSnapshot.EmbeddingSnapshotID,
				DatasetID:           datasetID,
				OrgID:               activeSnapshot.OrgID,
				ChunkIndex:          1,
				SourceText:          "matched chunk",
				Distance:            0.25,
				Similarity:          0.75,
			}},
		}
		var receivedStrategy model.EmbeddingStrategy
		providerFactory := func(strategy model.EmbeddingStrategy) (usecase.QueryEmbeddingProvider, error) {
			receivedStrategy = strategy
			return queryEmbeddingProviderStub{
				dimensions: activeSnapshot.EmbeddingDimensions,
				vectors:    [][]float32{{3, 4}},
			}, nil
		}
		uc := usecase.NewEmbeddingSearchUsecase(repo, providerFactory)

		ctx := ctxutil.WithActorOrg(context.Background(), activeSnapshot.UserID, activeSnapshot.OrgID)
		result, err := uc.SearchEmbeddings(ctx, activeSnapshot.UserID, datasetID, "what happened?", 7)

		Expect(err).NotTo(HaveOccurred())
		Expect(repo.userID).To(Equal(activeSnapshot.UserID))
		Expect(repo.datasetID).To(Equal(datasetID))
		Expect(result.EmbeddingSnapshot).To(Equal(activeSnapshot))
		Expect(result.Matches).To(HaveLen(1))
		Expect(receivedStrategy.EmbeddingProvider).To(Equal(activeSnapshot.EmbeddingProvider))
		Expect(receivedStrategy.EmbeddingModel).To(Equal(activeSnapshot.EmbeddingModel))
		Expect(receivedStrategy.ExtractorName).To(Equal(activeSnapshot.ExtractorName))
		Expect(receivedStrategy.CleanerName).To(Equal(activeSnapshot.CleanerName))
		Expect(repo.topK).To(Equal(7))
		Expect(repo.queryVector).To(HaveLen(2))
		Expect(repo.queryVector[0]).To(BeNumerically("~", 0.6, 0.0001))
		Expect(repo.queryVector[1]).To(BeNumerically("~", 0.8, 0.0001))
	})

	It("rejects query providers that do not match the active snapshot dimensions", func() {
		activeSnapshot := validSearchEmbeddingSnapshot(uuid.New())
		repo := &embeddingSearchRepoStub{activeSnapshot: activeSnapshot}
		uc := usecase.NewEmbeddingSearchUsecase(repo, func(model.EmbeddingStrategy) (usecase.QueryEmbeddingProvider, error) {
			return queryEmbeddingProviderStub{dimensions: activeSnapshot.EmbeddingDimensions + 1}, nil
		})

		ctx := ctxutil.WithActorOrg(context.Background(), activeSnapshot.UserID, activeSnapshot.OrgID)
		result, err := uc.SearchEmbeddings(ctx, activeSnapshot.UserID, activeSnapshot.DatasetID, "query", 5)

		Expect(result).To(BeNil())
		Expect(errors.Is(err, domain.ErrEmbeddingSearch)).To(BeTrue())
	})

	It("rejects active snapshots with unsupported embedding providers", func() {
		activeSnapshot := validSearchEmbeddingSnapshot(uuid.New())
		activeSnapshot.EmbeddingProvider = "unknown"
		repo := &embeddingSearchRepoStub{activeSnapshot: activeSnapshot}
		factoryCalled := false
		uc := usecase.NewEmbeddingSearchUsecase(repo, func(model.EmbeddingStrategy) (usecase.QueryEmbeddingProvider, error) {
			factoryCalled = true
			return nil, errors.New("embedding_provider \"unknown\" is not supported")
		})

		ctx := ctxutil.WithActorOrg(context.Background(), activeSnapshot.UserID, activeSnapshot.OrgID)
		result, err := uc.SearchEmbeddings(ctx, activeSnapshot.UserID, activeSnapshot.DatasetID, "query", 5)

		Expect(result).To(BeNil())
		Expect(errors.Is(err, domain.ErrEmbeddingSearch)).To(BeTrue())
		Expect(err.Error()).To(ContainSubstring("embedding_provider"))
		Expect(factoryCalled).To(BeTrue())
		Expect(repo.queryVector).To(BeNil())
	})

	It("rejects active snapshots with incomplete strategy metadata", func() {
		activeSnapshot := validSearchEmbeddingSnapshot(uuid.New())
		activeSnapshot.CleanerName = ""
		repo := &embeddingSearchRepoStub{activeSnapshot: activeSnapshot}
		factoryCalled := false
		uc := usecase.NewEmbeddingSearchUsecase(repo, func(model.EmbeddingStrategy) (usecase.QueryEmbeddingProvider, error) {
			factoryCalled = true
			return nil, errors.New("cleaner_name is required")
		})

		ctx := ctxutil.WithActorOrg(context.Background(), activeSnapshot.UserID, activeSnapshot.OrgID)
		result, err := uc.SearchEmbeddings(ctx, activeSnapshot.UserID, activeSnapshot.DatasetID, "query", 5)

		Expect(result).To(BeNil())
		Expect(errors.Is(err, domain.ErrEmbeddingSearch)).To(BeTrue())
		Expect(err.Error()).To(ContainSubstring("cleaner_name is required"))
		Expect(factoryCalled).To(BeTrue())
		Expect(repo.queryVector).To(BeNil())
	})
})

func validSearchEmbeddingSnapshot(datasetID uuid.UUID) *model.EmbeddingSnapshot {
	return &model.EmbeddingSnapshot{
		EmbeddingSnapshotID: uuid.New(),
		FeatureSnapshotID:   uuid.New(),
		DatasetID:           datasetID,
		UserID:              uuid.New(),
		OrgID:               uuid.New(),
		VectorStore:         "pgvector",
		CollectionName:      "movies",
		EmbeddingDimensions: 2,
		StrategyVersion:     "rag-v1",
		ExtractorName:       model.DefaultExtractorName,
		ExtractorVersion:    model.DefaultExtractorVersion,
		CleanerName:         model.DefaultCleanerName,
		CleanerVersion:      model.DefaultCleanerVersion,
		ChunkerName:         "go-token-window",
		ChunkerVersion:      "v1",
		ChunkSize:           384,
		ChunkOverlap:        64,
		EmbeddingProvider:   "ollama",
		EmbeddingModel:      model.DefaultEmbeddingModel,
		ActiveForRetrieval:  true,
		Status:              model.SnapshotStatusReady,
	}
}
