package app_test

import (
	"context"
	"errors"

	usecase "feature_materializer_service/pkg/app"
	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type embeddingSearchRepoStub struct {
	activeSnapshot *model.EmbeddingSnapshot
	activeErr      error
	queryVector    []float32
	topK           int
	records        []model.EmbeddingRecord
}

func (s *embeddingSearchRepoStub) ReadActiveEmbeddingSnapshot(context.Context, uuid.UUID) (*model.EmbeddingSnapshot, error) {
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

		result, err := uc.SearchEmbeddings(context.Background(), datasetID, "what happened?", 7)

		Expect(err).NotTo(HaveOccurred())
		Expect(result.EmbeddingSnapshot).To(Equal(activeSnapshot))
		Expect(result.Matches).To(HaveLen(1))
		Expect(receivedStrategy.EmbeddingProvider).To(Equal(activeSnapshot.EmbeddingProvider))
		Expect(receivedStrategy.EmbeddingModel).To(Equal(activeSnapshot.EmbeddingModel))
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

		result, err := uc.SearchEmbeddings(context.Background(), activeSnapshot.DatasetID, "query", 5)

		Expect(result).To(BeNil())
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
	})
})

func validSearchEmbeddingSnapshot(datasetID uuid.UUID) *model.EmbeddingSnapshot {
	return &model.EmbeddingSnapshot{
		EmbeddingSnapshotID: uuid.New(),
		FeatureSnapshotID:   uuid.New(),
		DatasetID:           datasetID,
		UserID:              uuid.New(),
		VectorStore:         "pgvector",
		CollectionName:      "movies",
		EmbeddingDimensions: 2,
		StrategyVersion:     "rag-v1",
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
