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

type embeddingSnapshotRepoStub struct {
	embeddingSnapshot *model.EmbeddingSnapshot
	saveErr           error
	readyID           uuid.UUID
	failedID          uuid.UUID
}

func (s *embeddingSnapshotRepoStub) SavePendingEmbeddingSnapshot(_ context.Context, featureSnapshotID, _ uuid.UUID, _ model.EmbeddingStrategy) (*model.EmbeddingSnapshot, error) {
	if s.saveErr != nil {
		return nil, s.saveErr
	}
	if s.embeddingSnapshot != nil {
		return s.embeddingSnapshot, nil
	}
	return validEmbeddingSnapshot(featureSnapshotID), nil
}

func (s *embeddingSnapshotRepoStub) MarkEmbeddingReady(_ context.Context, embeddingSnapshot *model.EmbeddingSnapshot) error {
	s.readyID = embeddingSnapshot.EmbeddingSnapshotID
	return nil
}

func (s *embeddingSnapshotRepoStub) MarkEmbeddingFailed(_ context.Context, embeddingSnapshotID uuid.UUID, _ string) error {
	s.failedID = embeddingSnapshotID
	return nil
}

func (s *embeddingSnapshotRepoStub) ReadEmbeddingByIdempotencyKey(context.Context, uuid.UUID) (*model.EmbeddingSnapshot, error) {
	return s.embeddingSnapshot, nil
}

type featureSnapshotReaderStub struct {
	featureSnapshot *model.FeatureSnapshot
	err             error
}

func (s featureSnapshotReaderStub) ReadFeatureSnapshot(context.Context, uuid.UUID) (*model.FeatureSnapshot, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.featureSnapshot, nil
}

type embeddingWriterStub struct {
	embeddingSnapshot *model.EmbeddingSnapshot
	err               error
}

func (s embeddingWriterStub) MaterializeEmbeddings(_ context.Context, _ *model.FeatureSnapshot, embeddingSnapshot *model.EmbeddingSnapshot) (*model.EmbeddingSnapshot, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.embeddingSnapshot != nil {
		return s.embeddingSnapshot, nil
	}
	embeddingSnapshot.VectorStore = "pgvector"
	embeddingSnapshot.CollectionName = "dataset_movies"
	return embeddingSnapshot, nil
}

var _ = Describe("EmbeddingMaterializationUsecase", func() {
	It("saves pending embeddings when no writer is configured", func() {
		repo := &embeddingSnapshotRepoStub{}
		uc := usecase.NewEmbeddingMaterializationUsecase(repo, nil, nil)

		embeddingSnapshot, err := uc.MaterializeEmbeddings(context.Background(), uuid.New(), uuid.New(), model.EmbeddingStrategy{})

		Expect(err).NotTo(HaveOccurred())
		Expect(embeddingSnapshot.Status).To(Equal(model.SnapshotStatusPending))
	})

	It("materializes and marks embeddings ready", func() {
		featureSnapshot := validFeatureSnapshot(uuid.New())
		embeddingSnapshot := validEmbeddingSnapshot(featureSnapshot.FeatureSnapshotID)
		repo := &embeddingSnapshotRepoStub{embeddingSnapshot: embeddingSnapshot}
		uc := usecase.NewEmbeddingMaterializationUsecase(repo, featureSnapshotReaderStub{featureSnapshot: featureSnapshot}, embeddingWriterStub{})

		result, err := uc.MaterializeEmbeddings(context.Background(), featureSnapshot.FeatureSnapshotID, uuid.New(), model.EmbeddingStrategy{})

		Expect(err).NotTo(HaveOccurred())
		Expect(repo.readyID).To(Equal(embeddingSnapshot.EmbeddingSnapshotID))
		Expect(result.VectorStore).To(Equal("pgvector"))
	})

	It("returns replay records from repository idempotency errors", func() {
		existing := validEmbeddingSnapshot(uuid.New())
		repo := &embeddingSnapshotRepoStub{saveErr: &domain.EmbeddingsAlreadyMaterializedError{Record: existing}}
		uc := usecase.NewEmbeddingMaterializationUsecase(repo, nil, nil)

		embeddingSnapshot, err := uc.MaterializeEmbeddings(context.Background(), uuid.New(), uuid.New(), model.EmbeddingStrategy{})

		Expect(err).To(HaveOccurred())
		Expect(embeddingSnapshot).To(Equal(existing))
	})

	It("marks failed when the writer fails", func() {
		expectedErr := errors.New("embedding writer failed")
		featureSnapshot := validFeatureSnapshot(uuid.New())
		repo := &embeddingSnapshotRepoStub{embeddingSnapshot: validEmbeddingSnapshot(featureSnapshot.FeatureSnapshotID)}
		uc := usecase.NewEmbeddingMaterializationUsecase(repo, featureSnapshotReaderStub{featureSnapshot: featureSnapshot}, embeddingWriterStub{err: expectedErr})

		result, err := uc.MaterializeEmbeddings(context.Background(), featureSnapshot.FeatureSnapshotID, uuid.New(), model.EmbeddingStrategy{})

		Expect(result).To(BeNil())
		Expect(errors.Is(err, expectedErr)).To(BeTrue())
		Expect(errors.Is(err, domain.ErrEmbeddingMaterialize)).To(BeTrue())
		Expect(repo.failedID).To(Equal(repo.embeddingSnapshot.EmbeddingSnapshotID))
	})
})

func validEmbeddingSnapshot(featureSnapshotID uuid.UUID) *model.EmbeddingSnapshot {
	return &model.EmbeddingSnapshot{
		EmbeddingSnapshotID: uuid.New(),
		FeatureSnapshotID:   featureSnapshotID,
		DatasetID:           uuid.New(),
		UserID:              uuid.New(),
		Status:              model.SnapshotStatusPending,
	}
}
