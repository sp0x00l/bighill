package app_test

import (
	"context"
	"errors"
	"fmt"

	usecase "feature_materializer_service/pkg/app"
	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"
	"lib/shared_lib/authz"
	"lib/shared_lib/userevents"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
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

		snapshot, err := uc.MaterializeGraph(context.Background(), graphSnapshot.EmbeddingSnapshotID, uuid.New(), validGraphExtractionStrategy())

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
		Expect(repo.savedMaterialization.CommunityReports).To(BeEmpty())
	})

	It("publishes a graph-failed user event after marking extraction failure", func() {
		graphSnapshot := validGraphSnapshot()
		repo := &graphSnapshotRepoStub{graphSnapshot: graphSnapshot}
		publisher := &userEventPublisherStub{}
		extractorErr := errors.New("extractor unavailable")
		uc := usecase.NewGraphMaterializationUsecase(repo, &snapshotUnitOfWorkStub{}, snapshotEventBuilderStub{}, &graphExtractorStub{err: extractorErr}, usecase.WithGraphUserEventPublisher(publisher))

		snapshot, err := uc.MaterializeGraph(context.Background(), graphSnapshot.EmbeddingSnapshotID, uuid.New(), validGraphExtractionStrategy())

		Expect(snapshot).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring("extractor unavailable")))
		Expect(repo.failedID).To(Equal(graphSnapshot.GraphSnapshotID))
		Expect(repo.failure).To(Equal("extractor unavailable"))
		Expect(repo.failedSnapshot.ProvenanceHash).NotTo(BeEmpty())
		Expect(repo.failedSnapshot.ChunkCount).To(Equal(int64(0)))
		Expect(publisher.events).To(HaveLen(1))
		event := publisher.events[0]
		Expect(event.EventType).To(Equal(userevents.EventTypeSnapshotGraphFailed))
		Expect(event.Severity).To(Equal(userevents.SeverityError))
		Expect(event.RequiredPermission).To(Equal(authz.PermissionDataRead))
		Expect(event.Status.State).To(Equal(model.SnapshotStatusFailed.String()))
		Expect(event.Error).NotTo(BeNil())
	})

	It("propagates a typed graph extraction error into the graph-failed user event", func() {
		graphSnapshot := validGraphSnapshot()
		repo := &graphSnapshotRepoStub{graphSnapshot: graphSnapshot}
		publisher := &userEventPublisherStub{}
		extractorErr := domain.ErrGraphExtractionInvalid.Extend("relations[0].source must reference an entity id")
		uc := usecase.NewGraphMaterializationUsecase(repo, &snapshotUnitOfWorkStub{}, snapshotEventBuilderStub{}, &graphExtractorStub{err: extractorErr}, usecase.WithGraphUserEventPublisher(publisher))

		snapshot, err := uc.MaterializeGraph(context.Background(), graphSnapshot.EmbeddingSnapshotID, uuid.New(), validGraphExtractionStrategy())

		Expect(snapshot).To(BeNil())
		Expect(errors.Is(err, domain.ErrGraphMaterialize)).To(BeTrue())
		Expect(errors.Is(err, domain.ErrGraphExtractionInvalid)).To(BeTrue())
		Expect(publisher.events).To(HaveLen(1))
		Expect(publisher.events[0].Error).NotTo(BeNil())
		Expect(publisher.events[0].Error.Code).To(Equal(domain.ErrGraphExtractionInvalid.Code))
		Expect(publisher.events[0].Error.TechnicalDetail).To(ContainSubstring("relations[0].source must reference an entity id"))
	})

	It("marks the graph snapshot failed when entity resolution fails", func() {
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
				SourceText:          "Aurora Relay connects Beacon Hub.",
			}},
		}
		resolverErr := domain.ErrGraphEntityResolution.Extend("provider unavailable")
		extractor := &graphExtractorStub{extraction: &model.GraphExtraction{
			Entities: []model.GraphExtractionEntity{{
				ID:         "e1",
				Name:       "Aurora Relay",
				Type:       "system",
				ChunkIndex: 0,
			}},
		}}
		uc := usecase.NewGraphMaterializationUsecase(repo, &snapshotUnitOfWorkStub{}, snapshotEventBuilderStub{}, extractor, usecase.WithGraphEntityResolver(&graphEntityResolverStub{err: resolverErr}))

		snapshot, err := uc.MaterializeGraph(context.Background(), graphSnapshot.EmbeddingSnapshotID, uuid.New(), validGraphExtractionStrategy())

		Expect(snapshot).To(BeNil())
		Expect(errors.Is(err, domain.ErrGraphMaterialize)).To(BeTrue())
		Expect(errors.Is(err, domain.ErrGraphEntityResolution)).To(BeTrue())
		Expect(repo.failedID).To(Equal(graphSnapshot.GraphSnapshotID))
		Expect(repo.failure).To(ContainSubstring("provider unavailable"))
	})

	It("propagates typed graph persistence failures under graph materialization", func() {
		graphSnapshot := validGraphSnapshot()
		persistErr := errors.New("insert failed")
		repo := &graphSnapshotRepoStub{
			graphSnapshot: graphSnapshot,
			saveGraphErr:  fmt.Errorf("%w: insert graph node embedding: %w", domain.ErrGraphPersistence, persistErr),
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
		extractor := &graphExtractorStub{extraction: &model.GraphExtraction{
			Entities: []model.GraphExtractionEntity{{
				ID:         "e1",
				Name:       "Ada",
				Type:       "person",
				ChunkIndex: 0,
			}},
		}}
		uc := usecase.NewGraphMaterializationUsecase(repo, &snapshotUnitOfWorkStub{}, snapshotEventBuilderStub{}, extractor)

		snapshot, err := uc.MaterializeGraph(context.Background(), graphSnapshot.EmbeddingSnapshotID, uuid.New(), validGraphExtractionStrategy())

		Expect(snapshot).To(BeNil())
		Expect(errors.Is(err, domain.ErrGraphMaterialize)).To(BeTrue())
		Expect(errors.Is(err, domain.ErrGraphPersistence)).To(BeTrue())
		Expect(errors.Is(err, persistErr)).To(BeTrue())
		Expect(err).To(MatchError(ContainSubstring("insert graph node embedding")))
	})

	It("builds connected-component community reports from the resolved graph", func() {
		graphSnapshot := validGraphSnapshot()
		embeddingSnapshot := validSearchEmbeddingSnapshot(graphSnapshot.DatasetID)
		embeddingSnapshot.EmbeddingSnapshotID = graphSnapshot.EmbeddingSnapshotID
		embeddingSnapshot.FeatureSnapshotID = graphSnapshot.FeatureSnapshotID
		embeddingSnapshot.UserID = graphSnapshot.UserID
		embeddingSnapshot.OrgID = graphSnapshot.OrgID
		embeddingSnapshot.EmbeddingDimensions = 2
		recordA := uuid.New()
		recordB := uuid.New()
		recordC := uuid.New()
		repo := &graphSnapshotRepoStub{
			graphSnapshot:     graphSnapshot,
			embeddingSnapshot: embeddingSnapshot,
			chunks: []model.GraphChunk{
				{EmbeddingRecordID: recordA, EmbeddingSnapshotID: graphSnapshot.EmbeddingSnapshotID, DatasetID: graphSnapshot.DatasetID, UserID: graphSnapshot.UserID, OrgID: graphSnapshot.OrgID, ChunkIndex: 0, SourceText: "Aurora Relay routes to Beacon Hub."},
				{EmbeddingRecordID: recordB, EmbeddingSnapshotID: graphSnapshot.EmbeddingSnapshotID, DatasetID: graphSnapshot.DatasetID, UserID: graphSnapshot.UserID, OrgID: graphSnapshot.OrgID, ChunkIndex: 1, SourceText: "Beacon Hub accepts routed traffic."},
				{EmbeddingRecordID: recordC, EmbeddingSnapshotID: graphSnapshot.EmbeddingSnapshotID, DatasetID: graphSnapshot.DatasetID, UserID: graphSnapshot.UserID, OrgID: graphSnapshot.OrgID, ChunkIndex: 2, SourceText: "Citadel Index stores audit state."},
			},
		}
		extractor := &graphExtractorStub{extraction: &model.GraphExtraction{
			Entities: []model.GraphExtractionEntity{
				{ID: "aurora", Name: "Aurora Relay", Type: "system", Description: "Primary route", ChunkIndex: 0},
				{ID: "beacon", Name: "Beacon Hub", Type: "system", Description: "Downstream endpoint", ChunkIndex: 1},
				{ID: "citadel", Name: "Citadel Index", Type: "system", Description: "Audit store", ChunkIndex: 2},
			},
			Relations: []model.GraphExtractionRelation{{
				Source:      "aurora",
				Target:      "beacon",
				Type:        "CONNECTS_TO",
				Description: "Routes traffic",
				Weight:      1,
			}},
		}}
		resolver := &graphEntityResolverStub{
			decorate: func(materialization *model.GraphMaterialization) *model.GraphMaterialization {
				for _, node := range materialization.Nodes {
					vector := []float32{0, 1}
					if node.EntityKey == "system:aurora relay" || node.EntityKey == "system:beacon hub" {
						vector = []float32{1, 0}
					}
					materialization.NodeEmbeddings = append(materialization.NodeEmbeddings, model.GraphNodeEmbedding{
						EntityKey:           node.EntityKey,
						EmbeddingSnapshotID: materialization.Snapshot.EmbeddingSnapshotID,
						DatasetID:           materialization.Snapshot.DatasetID,
						UserID:              materialization.Snapshot.UserID,
						OrgID:               materialization.Snapshot.OrgID,
						EmbeddingText:       node.Name,
						Vector:              vector,
					})
				}
				return materialization
			},
		}
		reportProvider := &recordingQueryEmbeddingProviderStub{
			dimensions: 2,
			vectors: [][]float32{
				{3, 4},
				{0, 5},
			},
		}
		reporter := usecase.NewEmbeddingGraphCommunityReporter(func(model.EmbeddingStrategy) (usecase.QueryEmbeddingProvider, error) {
			return reportProvider, nil
		})
		uc := usecase.NewGraphMaterializationUsecase(repo, &snapshotUnitOfWorkStub{}, snapshotEventBuilderStub{}, extractor, usecase.WithGraphEntityResolver(resolver), usecase.WithGraphCommunityReporter(reporter))

		snapshot, err := uc.MaterializeGraph(context.Background(), graphSnapshot.EmbeddingSnapshotID, uuid.New(), validGraphExtractionStrategy())

		Expect(err).NotTo(HaveOccurred())
		Expect(snapshot).NotTo(BeNil())
		Expect(repo.savedMaterialization.Communities).To(HaveLen(2))
		Expect(repo.savedMaterialization.CommunityMembers).To(HaveLen(3))
		Expect(repo.savedMaterialization.CommunityReports).To(HaveLen(2))
		auroraReport := graphCommunityReportByTitle(repo.savedMaterialization.CommunityReports, "Aurora Relay / Beacon Hub")
		Expect(auroraReport.CommunityKey).NotTo(BeEmpty())
		Expect(auroraReport.ReportVersion).To(Equal(model.GraphCommunityReportExtractiveV1))
		Expect(auroraReport.ReportText).To(ContainSubstring("Relationships:"))
		Expect(auroraReport.ReportText).To(ContainSubstring("CONNECTS_TO"))
		Expect(auroraReport.ReportText).To(ContainSubstring("Aurora Relay routes to Beacon Hub."))
		Expect(auroraReport.EmbeddingText).To(Equal(auroraReport.ReportText))
		Expect(auroraReport.Vector[0]).To(BeNumerically("~", 0.6, 0.0001))
		Expect(auroraReport.Vector[1]).To(BeNumerically("~", 0.8, 0.0001))
		citadelReport := graphCommunityReportByTitle(repo.savedMaterialization.CommunityReports, "Citadel Index")
		Expect(citadelReport.ReportText).To(ContainSubstring("isolated graph community"))
		Expect(citadelReport.EmbeddingText).To(Equal(citadelReport.ReportText))
		Expect(citadelReport.Vector).To(Equal([]float32{0, 1}))
		Expect(reportProvider.texts).To(Equal([]string{auroraReport.ReportText, citadelReport.ReportText}))
	})

	It("marks the graph snapshot failed when community report embedding fails", func() {
		graphSnapshot := validGraphSnapshot()
		reportErr := errors.New("community embedding unavailable")
		repo := &graphSnapshotRepoStub{
			graphSnapshot: graphSnapshot,
			chunks: []model.GraphChunk{{
				EmbeddingRecordID:   uuid.New(),
				EmbeddingSnapshotID: graphSnapshot.EmbeddingSnapshotID,
				DatasetID:           graphSnapshot.DatasetID,
				UserID:              graphSnapshot.UserID,
				OrgID:               graphSnapshot.OrgID,
				ChunkIndex:          0,
				SourceText:          "Aurora Relay connects Beacon Hub.",
			}},
		}
		extractor := &graphExtractorStub{extraction: &model.GraphExtraction{
			Entities: []model.GraphExtractionEntity{{
				ID:         "e1",
				Name:       "Aurora Relay",
				Type:       "system",
				ChunkIndex: 0,
			}},
		}}
		reporter := usecase.NewEmbeddingGraphCommunityReporter(func(model.EmbeddingStrategy) (usecase.QueryEmbeddingProvider, error) {
			return nil, reportErr
		})
		uc := usecase.NewGraphMaterializationUsecase(repo, &snapshotUnitOfWorkStub{}, snapshotEventBuilderStub{}, extractor, usecase.WithGraphCommunityReporter(reporter))

		snapshot, err := uc.MaterializeGraph(context.Background(), graphSnapshot.EmbeddingSnapshotID, uuid.New(), validGraphExtractionStrategy())

		Expect(snapshot).To(BeNil())
		Expect(errors.Is(err, domain.ErrGraphMaterialize)).To(BeTrue())
		Expect(errors.Is(err, domain.ErrGraphCommunityReport)).To(BeTrue())
		Expect(errors.Is(err, reportErr)).To(BeTrue())
		Expect(repo.failedID).To(Equal(graphSnapshot.GraphSnapshotID))
		Expect(repo.failure).To(ContainSubstring("community embedding unavailable"))
	})

	It("records graph failure using an uncancelled context after extraction cancellation", func() {
		graphSnapshot := validGraphSnapshot()
		ctx, cancel := context.WithCancel(context.Background())
		repo := &graphSnapshotRepoStub{
			graphSnapshot: graphSnapshot,
			chunks: []model.GraphChunk{{
				EmbeddingRecordID:   uuid.New(),
				EmbeddingSnapshotID: graphSnapshot.EmbeddingSnapshotID,
				DatasetID:           graphSnapshot.DatasetID,
				UserID:              graphSnapshot.UserID,
				OrgID:               graphSnapshot.OrgID,
				ChunkIndex:          0,
				SourceText:          "Aurora Relay connects Beacon Hub.",
			}},
			recordFailedContextErr: true,
		}
		extractor := &graphExtractorStub{err: context.Canceled, cancel: cancel}
		uc := usecase.NewGraphMaterializationUsecase(repo, &snapshotUnitOfWorkStub{}, snapshotEventBuilderStub{}, extractor)

		snapshot, err := uc.MaterializeGraph(ctx, graphSnapshot.EmbeddingSnapshotID, uuid.New(), validGraphExtractionStrategy())

		Expect(snapshot).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring("context canceled")))
		Expect(repo.failedID).To(Equal(graphSnapshot.GraphSnapshotID))
		Expect(repo.failedContextErr).NotTo(HaveOccurred())
	})
})

type graphSnapshotRepoStub struct {
	graphSnapshot          *model.GraphSnapshot
	embeddingSnapshot      *model.EmbeddingSnapshot
	chunks                 []model.GraphChunk
	saveGraphErr           error
	savedMaterialization   *model.GraphMaterialization
	failedID               uuid.UUID
	failedSnapshot         *model.GraphSnapshot
	failure                string
	recordFailedContextErr bool
	failedContextErr       error
}

func (s *graphSnapshotRepoStub) SavePendingGraphSnapshot(_ context.Context, _ pgx.Tx, embeddingSnapshotID, _ uuid.UUID, strategy model.GraphExtractionStrategy) (*model.GraphSnapshot, error) {
	log.Trace("graphSnapshotRepoStub SavePendingGraphSnapshot")

	s.graphSnapshot.EmbeddingSnapshotID = embeddingSnapshotID
	s.graphSnapshot.ExtractionModel = model.ApplyGraphExtractionStrategyDefaults(strategy).ExtractionModel
	s.graphSnapshot.ExtractionPromptVersion = model.ApplyGraphExtractionStrategyDefaults(strategy).ExtractionPromptVersion
	s.graphSnapshot.ExtractionSchemaVersion = model.ApplyGraphExtractionStrategyDefaults(strategy).ExtractionSchemaVersion
	return s.graphSnapshot, nil
}

func (s *graphSnapshotRepoStub) ReadEmbeddingChunks(context.Context, uuid.UUID) ([]model.GraphChunk, error) {
	log.Trace("graphSnapshotRepoStub ReadEmbeddingChunks")

	return s.chunks, nil
}

func (s *graphSnapshotRepoStub) ReadEmbeddingSnapshot(_ context.Context, embeddingSnapshotID uuid.UUID) (*model.EmbeddingSnapshot, error) {
	log.Trace("graphSnapshotRepoStub ReadEmbeddingSnapshot")

	if s.embeddingSnapshot != nil {
		return s.embeddingSnapshot, nil
	}
	snapshot := validSearchEmbeddingSnapshot(s.graphSnapshot.DatasetID)
	snapshot.EmbeddingSnapshotID = embeddingSnapshotID
	snapshot.FeatureSnapshotID = s.graphSnapshot.FeatureSnapshotID
	snapshot.UserID = s.graphSnapshot.UserID
	snapshot.OrgID = s.graphSnapshot.OrgID
	return snapshot, nil
}

func (s *graphSnapshotRepoStub) SaveGraphMaterialization(_ context.Context, _ pgx.Tx, materialization *model.GraphMaterialization) error {
	log.Trace("graphSnapshotRepoStub SaveGraphMaterialization")

	if s.saveGraphErr != nil {
		return s.saveGraphErr
	}
	s.savedMaterialization = materialization
	s.graphSnapshot.EntityCount = materialization.Snapshot.EntityCount
	s.graphSnapshot.EdgeCount = materialization.Snapshot.EdgeCount
	return nil
}

func (s *graphSnapshotRepoStub) MarkGraphReady(_ context.Context, _ pgx.Tx, graphSnapshot *model.GraphSnapshot) error {
	log.Trace("graphSnapshotRepoStub MarkGraphReady")

	graphSnapshot.MaterializationEventSeq = 1
	graphSnapshot.ActiveForRetrieval = true
	return nil
}

func (s *graphSnapshotRepoStub) MarkGraphFailed(ctx context.Context, _ pgx.Tx, graphSnapshot *model.GraphSnapshot, reason string) error {
	log.Trace("graphSnapshotRepoStub MarkGraphFailed")

	if s.recordFailedContextErr {
		s.failedContextErr = ctx.Err()
	}
	s.failedID = graphSnapshot.GraphSnapshotID
	s.failedSnapshot = graphSnapshot
	s.failure = reason
	return nil
}

func (s *graphSnapshotRepoStub) ReadGraphByIdempotencyKey(context.Context, uuid.UUID) (*model.GraphSnapshot, error) {
	log.Trace("graphSnapshotRepoStub ReadGraphByIdempotencyKey")

	return s.graphSnapshot, nil
}

type graphExtractorStub struct {
	extraction *model.GraphExtraction
	err        error
	cancel     context.CancelFunc
}

func (s *graphExtractorStub) ExtractGraph(context.Context, []model.GraphChunk, model.GraphExtractionStrategy) (*model.GraphExtraction, error) {
	log.Trace("graphExtractorStub ExtractGraph")

	if s.cancel != nil {
		s.cancel()
	}
	return s.extraction, s.err
}

type graphEntityResolverStub struct {
	out      *model.GraphMaterialization
	err      error
	decorate func(*model.GraphMaterialization) *model.GraphMaterialization
}

func (s *graphEntityResolverStub) ResolveGraphEntities(_ context.Context, materialization *model.GraphMaterialization, _ *model.EmbeddingSnapshot) (*model.GraphMaterialization, error) {
	log.Trace("graphEntityResolverStub ResolveGraphEntities")

	if s.err != nil {
		return nil, s.err
	}
	if s.out != nil {
		return s.out, nil
	}
	if s.decorate != nil {
		return s.decorate(materialization), nil
	}
	return materialization, nil
}

type recordingQueryEmbeddingProviderStub struct {
	dimensions int
	vectors    [][]float32
	texts      []string
	err        error
}

func (s *recordingQueryEmbeddingProviderStub) Embed(_ context.Context, texts []string) ([][]float32, error) {
	log.Trace("recordingQueryEmbeddingProviderStub Embed")

	s.texts = append([]string(nil), texts...)
	if s.err != nil {
		return nil, s.err
	}
	return s.vectors, nil
}

func (s *recordingQueryEmbeddingProviderStub) Dimensions() int {
	log.Trace("recordingQueryEmbeddingProviderStub Dimensions")

	return s.dimensions
}

func graphCommunityReportByTitle(reports []model.GraphCommunityReport, title string) model.GraphCommunityReport {
	for _, report := range reports {
		if report.Title == title {
			return report
		}
	}
	return model.GraphCommunityReport{}
}

type userEventPublisherStub struct {
	events []userevents.Event
	err    error
}

func (s *userEventPublisherStub) Publish(_ context.Context, event userevents.Event) error {
	log.Trace("userEventPublisherStub Publish")

	s.events = append(s.events, event)
	return s.err
}

func validGraphSnapshot() *model.GraphSnapshot {
	log.Trace("validGraphSnapshot")

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

func validGraphExtractionStrategy() model.GraphExtractionStrategy {
	log.Trace("validGraphExtractionStrategy")

	return model.GraphExtractionStrategy{
		ExtractionModel:         "graph-model",
		ExtractionPromptVersion: model.DefaultGraphExtractionPromptVersion,
		ExtractionSchemaVersion: model.DefaultGraphExtractionSchemaVersion,
	}
}
