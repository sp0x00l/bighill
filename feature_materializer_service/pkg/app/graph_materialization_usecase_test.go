package app_test

import (
	"context"
	"errors"

	usecase "feature_materializer_service/pkg/app"
	"feature_materializer_service/pkg/domain/model"
	"lib/shared_lib/authz"
	"lib/shared_lib/userevents"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("GraphMaterializationUsecase", func() {
	It("publishes a graph-ready user event after marking a graph snapshot ready", func() {
		graphSnapshot := validGraphSnapshot()
		repo := &graphSnapshotRepoStub{
			graphSnapshot: graphSnapshot,
			chunks: []model.GraphChunk{{
				EmbeddingRecordID:   uuid.New(),
				EmbeddingSnapshotID: graphSnapshot.EmbeddingSnapshotID,
				DatasetID:           graphSnapshot.DatasetID,
				UserID:              graphSnapshot.UserID,
				OrgID:               graphSnapshot.OrgID,
				ChunkIndex:          0,
				SourceText:          "Ada founded BigHill.",
			}},
		}
		publisher := &userEventPublisherStub{}
		extractor := &graphExtractorStub{extraction: &model.GraphExtraction{
			Entities: []model.GraphExtractionEntity{{
				ID:          "e1",
				Name:        "Ada",
				Type:        "person",
				Description: "Founder",
				ChunkIndex:  0,
			}},
		}}
		uc := usecase.NewGraphMaterializationUsecase(repo, &snapshotUnitOfWorkStub{}, snapshotEventBuilderStub{}, extractor, usecase.WithGraphUserEventPublisher(publisher))

		snapshot, err := uc.MaterializeGraph(context.Background(), graphSnapshot.EmbeddingSnapshotID, uuid.New(), model.GraphExtractionStrategy{})

		Expect(err).NotTo(HaveOccurred())
		Expect(snapshot.GraphSnapshotID).To(Equal(graphSnapshot.GraphSnapshotID))
		Expect(publisher.events).To(HaveLen(1))
		event := publisher.events[0]
		Expect(event.EventType).To(Equal(userevents.EventTypeSnapshotGraphReady))
		Expect(event.Severity).To(Equal(userevents.SeveritySuccess))
		Expect(event.RequiredPermission).To(Equal(authz.PermissionDataRead))
		Expect(event.UserID).To(Equal(graphSnapshot.UserID.String()))
		Expect(event.OrgID).To(Equal(graphSnapshot.OrgID.String()))
		Expect(event.Resource.ID).To(Equal(graphSnapshot.GraphSnapshotID.String()))
		Expect(event.Status.State).To(Equal(model.SnapshotStatusReady.String()))
		Expect(event.Status.Phase).To(Equal(userevents.StatusPhaseMaterialization))
		Expect(event.Metadata).To(HaveKeyWithValue("dataset_id", graphSnapshot.DatasetID.String()))
	})

	It("publishes a graph-failed user event after marking extraction failure", func() {
		graphSnapshot := validGraphSnapshot()
		repo := &graphSnapshotRepoStub{graphSnapshot: graphSnapshot}
		publisher := &userEventPublisherStub{}
		extractorErr := errors.New("extractor unavailable")
		uc := usecase.NewGraphMaterializationUsecase(repo, &snapshotUnitOfWorkStub{}, snapshotEventBuilderStub{}, &graphExtractorStub{err: extractorErr}, usecase.WithGraphUserEventPublisher(publisher))

		snapshot, err := uc.MaterializeGraph(context.Background(), graphSnapshot.EmbeddingSnapshotID, uuid.New(), model.GraphExtractionStrategy{})

		Expect(snapshot).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring("extractor unavailable")))
		Expect(repo.failedID).To(Equal(graphSnapshot.GraphSnapshotID))
		Expect(repo.failure).To(Equal("extractor unavailable"))
		Expect(publisher.events).To(HaveLen(1))
		event := publisher.events[0]
		Expect(event.EventType).To(Equal(userevents.EventTypeSnapshotGraphFailed))
		Expect(event.Severity).To(Equal(userevents.SeverityError))
		Expect(event.RequiredPermission).To(Equal(authz.PermissionDataRead))
		Expect(event.Status.State).To(Equal(model.SnapshotStatusFailed.String()))
		Expect(event.Error).NotTo(BeNil())
	})
})

type graphSnapshotRepoStub struct {
	graphSnapshot *model.GraphSnapshot
	chunks        []model.GraphChunk
	failedID      uuid.UUID
	failure       string
}

func (s *graphSnapshotRepoStub) SavePendingGraphSnapshot(_ context.Context, _ pgx.Tx, embeddingSnapshotID, _ uuid.UUID, strategy model.GraphExtractionStrategy) (*model.GraphSnapshot, error) {
	s.graphSnapshot.EmbeddingSnapshotID = embeddingSnapshotID
	s.graphSnapshot.ExtractionModel = model.ApplyGraphExtractionStrategyDefaults(strategy).ExtractionModel
	s.graphSnapshot.ExtractionPromptVersion = model.ApplyGraphExtractionStrategyDefaults(strategy).ExtractionPromptVersion
	s.graphSnapshot.ExtractionSchemaVersion = model.ApplyGraphExtractionStrategyDefaults(strategy).ExtractionSchemaVersion
	return s.graphSnapshot, nil
}

func (s *graphSnapshotRepoStub) ReadEmbeddingChunks(context.Context, uuid.UUID) ([]model.GraphChunk, error) {
	return s.chunks, nil
}

func (s *graphSnapshotRepoStub) SaveGraphMaterialization(_ context.Context, _ pgx.Tx, materialization *model.GraphMaterialization) error {
	s.graphSnapshot.EntityCount = materialization.Snapshot.EntityCount
	s.graphSnapshot.EdgeCount = materialization.Snapshot.EdgeCount
	return nil
}

func (s *graphSnapshotRepoStub) MarkGraphReady(_ context.Context, _ pgx.Tx, graphSnapshot *model.GraphSnapshot) error {
	graphSnapshot.MaterializationEventSeq = 1
	graphSnapshot.ActiveForRetrieval = true
	return nil
}

func (s *graphSnapshotRepoStub) MarkGraphFailed(_ context.Context, _ pgx.Tx, graphSnapshotID uuid.UUID, reason string) error {
	s.failedID = graphSnapshotID
	s.failure = reason
	return nil
}

func (s *graphSnapshotRepoStub) ReadGraphByIdempotencyKey(context.Context, uuid.UUID) (*model.GraphSnapshot, error) {
	return s.graphSnapshot, nil
}

type graphExtractorStub struct {
	extraction *model.GraphExtraction
	err        error
}

func (s *graphExtractorStub) ExtractGraph(context.Context, []model.GraphChunk, model.GraphExtractionStrategy) (*model.GraphExtraction, error) {
	return s.extraction, s.err
}

type userEventPublisherStub struct {
	events []userevents.Event
	err    error
}

func (s *userEventPublisherStub) Publish(_ context.Context, event userevents.Event) error {
	s.events = append(s.events, event)
	return s.err
}

func validGraphSnapshot() *model.GraphSnapshot {
	return &model.GraphSnapshot{
		GraphSnapshotID:     uuid.New(),
		FeatureSnapshotID:   uuid.New(),
		EmbeddingSnapshotID: uuid.New(),
		DatasetID:           uuid.New(),
		UserID:              uuid.New(),
		OrgID:               uuid.New(),
		Status:              model.SnapshotStatusPending,
	}
}
