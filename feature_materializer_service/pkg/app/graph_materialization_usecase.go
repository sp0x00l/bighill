package app

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"
	contractprompts "lib/data_contracts_lib/prompts"
	"lib/shared_lib/authz"
	serializers "lib/shared_lib/serializer"
	shareduow "lib/shared_lib/uow"
	"lib/shared_lib/userevents"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

const (
	graphExtractionPromptV1Version = "graph_extraction_prompt_v1"
	graphExtractionPromptNone      = "none"

	graphUserEventSourceService = "feature_materializer_service"
	graphMaterializeOperation   = "graph.materialize"
	graphSnapshotReadyTitle     = "Graph snapshot ready"
	graphSnapshotReadyMessage   = "The graph snapshot is ready for retrieval."
	graphSnapshotFailedTitle    = "Graph snapshot failed"
	graphEventActionViewDataset = "View dataset"

	graphEventMetadataDatasetID           = "dataset_id"
	graphEventMetadataFeatureSnapshotID   = "feature_snapshot_id"
	graphEventMetadataEmbeddingSnapshotID = "embedding_snapshot_id"
	graphEventMetadataProvenanceHash      = "provenance_hash"
	graphEventMetadataExtractionModel     = "extraction_model"
	graphEventMetadataEntityCount         = "entity_count"
	graphEventMetadataEdgeCount           = "edge_count"

	graphFailureRecordTimeout = 10 * time.Second
)

type GraphMaterializationUsecase interface {
	MaterializeGraph(ctx context.Context, embeddingSnapshotID uuid.UUID, idempotencyKey uuid.UUID, strategy model.GraphExtractionStrategy) (*model.GraphSnapshot, error)
}

type GraphMaterializationOption func(*graphMaterializationUsecase)

type graphMaterializationUsecase struct {
	repo               GraphSnapshotRepository
	unitOfWork         SnapshotUnitOfWorkAdapter
	eventBuilder       SnapshotEventBuilder
	extractor          GraphExtractor
	userEventPublisher UserEventPublisher
	encoder            *serializers.Encoder
}

func WithGraphUserEventPublisher(publisher UserEventPublisher) GraphMaterializationOption {
	log.Trace("WithGraphUserEventPublisher")

	return func(u *graphMaterializationUsecase) {
		u.userEventPublisher = publisher
	}
}

func NewGraphMaterializationUsecase(repo GraphSnapshotRepository, unitOfWork SnapshotUnitOfWorkAdapter, eventBuilder SnapshotEventBuilder, extractor GraphExtractor, opts ...GraphMaterializationOption) GraphMaterializationUsecase {
	log.Trace("NewGraphMaterializationUsecase")

	usecase := &graphMaterializationUsecase{
		repo:               repo,
		unitOfWork:         unitOfWork,
		eventBuilder:       eventBuilder,
		extractor:          extractor,
		userEventPublisher: userevents.NewNoopPublisher(),
		encoder:            serializers.NewJSONSerializer(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(usecase)
		}
	}
	return usecase
}

func (u *graphMaterializationUsecase) MaterializeGraph(ctx context.Context, embeddingSnapshotID uuid.UUID, idempotencyKey uuid.UUID, strategy model.GraphExtractionStrategy) (out *model.GraphSnapshot, err error) {
	log.Trace("GraphMaterializationUsecase MaterializeGraph")

	strategy = model.ApplyGraphExtractionStrategyDefaults(strategy)
	ctx, span := startFeatureMaterializerSpan(ctx, "feature_materializer_service/app", "graph.materialize",
		attribute.String("embedding_snapshot_id", embeddingSnapshotID.String()),
		attribute.String("idempotency_key", idempotencyKey.String()),
		attribute.String("extraction_model", strategy.ExtractionModel),
		attribute.String("extraction_schema_version", strategy.ExtractionSchemaVersion),
	)
	defer endFeatureMaterializerSpanOnReturn(ctx, span, &err)

	graphSnapshot, err := u.savePendingGraphSnapshot(ctx, embeddingSnapshotID, idempotencyKey, strategy)
	if err != nil {
		if existing, ok := domain.IsGraphAlreadyMaterialized(err); ok {
			return existing, err
		}
		return nil, err
	}
	chunks, err := u.repo.ReadEmbeddingChunks(ctx, embeddingSnapshotID)
	if err != nil {
		return nil, err
	}
	graphSnapshot.ChunkCount = int64(len(chunks))
	graphSnapshot.ProvenanceHash, err = u.graphProvenanceHash(graphSnapshot, strategy)
	if err != nil {
		return nil, err
	}
	extraction, err := u.extractor.ExtractGraph(ctx, chunks, strategy)
	if err != nil {
		outErr := fmt.Errorf("%w: %w", domain.ErrGraphMaterialize, err)
		failureCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), graphFailureRecordTimeout)
		defer cancel()
		if markErr := u.markGraphFailed(failureCtx, graphSnapshot, err); markErr != nil {
			return nil, errors.Join(outErr, fmt.Errorf("mark graph snapshot failed: %w", markErr))
		}
		return nil, outErr
	}
	materialization := graphMaterializationFromExtraction(graphSnapshot, chunks, extraction)
	if err := u.markGraphReady(ctx, materialization); err != nil {
		return nil, err
	}
	return materialization.Snapshot, nil
}

func (u *graphMaterializationUsecase) savePendingGraphSnapshot(ctx context.Context, embeddingSnapshotID, idempotencyKey uuid.UUID, strategy model.GraphExtractionStrategy) (*model.GraphSnapshot, error) {
	log.Trace("GraphMaterializationUsecase savePendingGraphSnapshot")

	var graphSnapshot *model.GraphSnapshot
	err := u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
		out, err := u.repo.SavePendingGraphSnapshot(ctx, tx, embeddingSnapshotID, idempotencyKey, strategy)
		if err != nil {
			return err
		}
		graphSnapshot = out
		return nil
	})
	return graphSnapshot, err
}

func (u *graphMaterializationUsecase) markGraphReady(ctx context.Context, materialization *model.GraphMaterialization) error {
	log.Trace("GraphMaterializationUsecase markGraphReady")

	err := u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, enqueue shareduow.EnqueueFunc) error {
		if err := u.repo.SaveGraphMaterialization(ctx, tx, materialization); err != nil {
			return err
		}
		if err := u.repo.MarkGraphReady(ctx, tx, materialization.Snapshot); err != nil {
			return err
		}
		msg, err := u.eventBuilder.GraphSnapshotReadyMessage(materialization.Snapshot)
		if err != nil {
			return err
		}
		if err := enqueue(msg); err != nil {
			return fmt.Errorf("enqueue graph snapshot ready: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	materialization.Snapshot.Status = model.SnapshotStatusReady
	u.publishGraphSnapshotReadyUserEvent(ctx, materialization.Snapshot)
	return nil
}

func (u *graphMaterializationUsecase) markGraphFailed(ctx context.Context, graphSnapshot *model.GraphSnapshot, cause error) error {
	log.Trace("GraphMaterializationUsecase markGraphFailed")

	reason := ""
	if cause != nil {
		reason = cause.Error()
	}
	err := u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
		return u.repo.MarkGraphFailed(ctx, tx, graphSnapshot, reason)
	})
	if err != nil {
		return err
	}
	graphSnapshot.Status = model.SnapshotStatusFailed
	graphSnapshot.FailureReason = strings.TrimSpace(reason)
	u.publishGraphSnapshotFailedUserEvent(ctx, graphSnapshot, cause)
	return nil
}

func (u *graphMaterializationUsecase) graphProvenanceHash(snapshot *model.GraphSnapshot, strategy model.GraphExtractionStrategy) (string, error) {
	log.Trace("GraphMaterializationUsecase graphProvenanceHash")

	promptHash, err := graphPromptContentHash(strategy.ExtractionPromptVersion)
	if err != nil {
		return "", err
	}
	payload := struct {
		FeatureSnapshotID       string `json:"feature_snapshot_id"`
		EmbeddingSnapshotID     string `json:"embedding_snapshot_id"`
		ExtractionModel         string `json:"extraction_model"`
		ExtractionPromptHash    string `json:"extraction_prompt_hash"`
		ExtractionPromptVersion string `json:"extraction_prompt_version"`
		ExtractionSchemaVersion string `json:"extraction_schema_version"`
	}{
		FeatureSnapshotID:       snapshot.FeatureSnapshotID.String(),
		EmbeddingSnapshotID:     snapshot.EmbeddingSnapshotID.String(),
		ExtractionModel:         strategy.ExtractionModel,
		ExtractionPromptHash:    promptHash,
		ExtractionPromptVersion: strategy.ExtractionPromptVersion,
		ExtractionSchemaVersion: strategy.ExtractionSchemaVersion,
	}
	canonical, err := u.encoder.Serialize(payload)
	if err != nil {
		return "", err
	}
	return userevents.SHA256String(string(canonical)), nil
}

func graphPromptContentHash(promptVersion string) (string, error) {
	log.Trace("graphPromptContentHash")

	switch strings.TrimSpace(promptVersion) {
	case graphExtractionPromptV1Version:
		return userevents.SHA256String(string(contractprompts.GraphExtractionPromptV1())), nil
	case graphExtractionPromptNone:
		return "", nil
	default:
		return "", domain.ErrValidationFailed.Extend("unsupported graph extraction prompt version")
	}
}

func (u *graphMaterializationUsecase) publishGraphSnapshotReadyUserEvent(ctx context.Context, graphSnapshot *model.GraphSnapshot) {
	log.Trace("GraphMaterializationUsecase publishGraphSnapshotReadyUserEvent")

	u.publishGraphSnapshotUserEvent(ctx, graphSnapshot, userevents.EventTypeSnapshotGraphReady, userevents.SeveritySuccess, graphSnapshotReadyTitle, graphSnapshotReadyMessage, nil)
}

func (u *graphMaterializationUsecase) publishGraphSnapshotFailedUserEvent(ctx context.Context, graphSnapshot *model.GraphSnapshot, cause error) {
	log.Trace("GraphMaterializationUsecase publishGraphSnapshotFailedUserEvent")

	classified := userevents.ClassifyError(userevents.ClassificationInput{
		Service:          graphUserEventSourceService,
		Operation:        graphMaterializeOperation,
		ResourceType:     userevents.ResourceTypeSnapshot,
		DomainErrorCode:  domain.ServiceErrorCode(cause),
		RawFailureReason: graphSnapshot.FailureReason,
	})
	u.publishGraphSnapshotUserEvent(ctx, graphSnapshot, userevents.EventTypeSnapshotGraphFailed, userevents.SeverityError, graphSnapshotFailedTitle, classified.Message, &classified)
}

func (u *graphMaterializationUsecase) publishGraphSnapshotUserEvent(ctx context.Context, graphSnapshot *model.GraphSnapshot, eventType string, severity string, title string, message string, detail *userevents.ErrorDetail) {
	log.Trace("GraphMaterializationUsecase publishGraphSnapshotUserEvent")

	event := userevents.Event{
		SourceService:      graphUserEventSourceService,
		EventType:          eventType,
		Severity:           severity,
		RequiredPermission: authz.PermissionDataRead,
		UserID:             optionalGraphEventUUID(graphSnapshot.UserID),
		OrgID:              optionalGraphEventUUID(graphSnapshot.OrgID),
		Resource:           userevents.NewResource(userevents.ResourceTypeSnapshot, graphSnapshot.GraphSnapshotID, "graph snapshot", "/datasets/"+graphSnapshot.DatasetID.String()),
		Status: userevents.Status{
			State:    graphSnapshot.Status.String(),
			Phase:    userevents.StatusPhaseMaterialization,
			Progress: graphSnapshotProgress(graphSnapshot),
		},
		Title:       title,
		Message:     message,
		ActionLabel: graphEventActionViewDataset,
		ActionHref:  "/datasets/" + graphSnapshot.DatasetID.String(),
		Error:       detail,
		Metadata: map[string]string{
			graphEventMetadataDatasetID:           graphSnapshot.DatasetID.String(),
			graphEventMetadataFeatureSnapshotID:   graphSnapshot.FeatureSnapshotID.String(),
			graphEventMetadataEmbeddingSnapshotID: graphSnapshot.EmbeddingSnapshotID.String(),
			graphEventMetadataProvenanceHash:      graphSnapshot.ProvenanceHash,
			graphEventMetadataExtractionModel:     graphSnapshot.ExtractionModel,
			graphEventMetadataEntityCount:         fmt.Sprint(graphSnapshot.EntityCount),
			graphEventMetadataEdgeCount:           fmt.Sprint(graphSnapshot.EdgeCount),
		},
	}
	if err := u.userEventPublisher.Publish(ctx, event); err != nil {
		userevents.LogPublishFailure(ctx, err, event)
	}
}

func graphSnapshotProgress(graphSnapshot *model.GraphSnapshot) int {
	log.Trace("graphSnapshotProgress")

	if graphSnapshot == nil || graphSnapshot.ChunkCount <= 0 {
		return 0
	}
	progress := int((graphSnapshot.ChunksProcessed * 100) / graphSnapshot.ChunkCount)
	if progress < 0 {
		return 0
	}
	if progress > 100 {
		return 100
	}
	return progress
}

func optionalGraphEventUUID(id uuid.UUID) string {
	log.Trace("optionalGraphEventUUID")

	if id == uuid.Nil {
		return ""
	}
	return id.String()
}

func graphMaterializationFromExtraction(snapshot *model.GraphSnapshot, chunks []model.GraphChunk, extraction *model.GraphExtraction) *model.GraphMaterialization {
	log.Trace("graphMaterializationFromExtraction")

	if extraction == nil {
		extraction = &model.GraphExtraction{}
	}
	nodeByKey := map[string]model.GraphNode{}
	entityIDToKey := map[string]string{}
	chunkByIndex := map[int]model.GraphChunk{}
	for _, chunk := range chunks {
		chunkByIndex[chunk.ChunkIndex] = chunk
	}
	nodeChunkSeen := map[string]struct{}{}
	nodeChunks := []model.GraphNodeChunk{}
	for _, entity := range extraction.Entities {
		key := graphEntityKey(entity.Name, entity.Type)
		if key == "" {
			continue
		}
		if trimmedID := strings.TrimSpace(entity.ID); trimmedID != "" {
			entityIDToKey[trimmedID] = key
		}
		node := nodeByKey[key]
		if node.EntityKey == "" {
			node = model.GraphNode{
				GraphSnapshotID: snapshot.GraphSnapshotID,
				DatasetID:       snapshot.DatasetID,
				UserID:          snapshot.UserID,
				OrgID:           snapshot.OrgID,
				EntityKey:       key,
				Name:            strings.TrimSpace(entity.Name),
				Type:            strings.TrimSpace(entity.Type),
				Description:     strings.TrimSpace(entity.Description),
			}
		} else if node.Description == "" && strings.TrimSpace(entity.Description) != "" {
			node.Description = strings.TrimSpace(entity.Description)
		}
		node.MentionCount++
		nodeByKey[key] = node
		if chunk, ok := chunkByIndex[entity.ChunkIndex]; ok {
			chunkKey := key + "\x00" + chunk.EmbeddingRecordID.String()
			if _, seen := nodeChunkSeen[chunkKey]; !seen {
				nodeChunks = append(nodeChunks, model.GraphNodeChunk{
					GraphSnapshotID:     snapshot.GraphSnapshotID,
					EntityKey:           key,
					EmbeddingRecordID:   chunk.EmbeddingRecordID,
					EmbeddingSnapshotID: chunk.EmbeddingSnapshotID,
					DatasetID:           snapshot.DatasetID,
					UserID:              snapshot.UserID,
					OrgID:               snapshot.OrgID,
					ChunkIndex:          chunk.ChunkIndex,
					SourceText:          chunk.SourceText,
				})
				nodeChunkSeen[chunkKey] = struct{}{}
			}
		}
	}
	nodes := make([]model.GraphNode, 0, len(nodeByKey))
	for _, node := range nodeByKey {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].EntityKey < nodes[j].EntityKey })

	edges := []model.GraphEdge{}
	for _, relation := range extraction.Relations {
		sourceKey := entityIDToKey[strings.TrimSpace(relation.Source)]
		if sourceKey == "" {
			sourceKey = graphEntityKey(relation.Source, "")
		}
		targetKey := entityIDToKey[strings.TrimSpace(relation.Target)]
		if targetKey == "" {
			targetKey = graphEntityKey(relation.Target, "")
		}
		sourceKey = resolveRelationEntityKey(sourceKey, nodeByKey)
		targetKey = resolveRelationEntityKey(targetKey, nodeByKey)
		if sourceKey == "" || targetKey == "" || sourceKey == targetKey {
			continue
		}
		weight := relation.Weight
		if weight <= 0 {
			weight = 1
		}
		edges = append(edges, model.GraphEdge{
			GraphSnapshotID: snapshot.GraphSnapshotID,
			DatasetID:       snapshot.DatasetID,
			UserID:          snapshot.UserID,
			OrgID:           snapshot.OrgID,
			SourceEntityKey: sourceKey,
			TargetEntityKey: targetKey,
			RelationType:    strings.TrimSpace(relation.Type),
			Description:     strings.TrimSpace(relation.Description),
			Weight:          weight,
		})
	}
	snapshot.ChunksProcessed = int64(len(chunks))
	snapshot.EntityCount = int64(len(nodes))
	snapshot.EdgeCount = int64(len(edges))
	snapshot.Status = model.SnapshotStatusReady
	return &model.GraphMaterialization{
		Snapshot:   snapshot,
		Nodes:      nodes,
		Edges:      edges,
		NodeChunks: nodeChunks,
	}
}

func graphEntityKey(name string, entityType string) string {
	name = strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(name)), " "))
	entityType = strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(entityType)), " "))
	if name == "" {
		return ""
	}
	if entityType == "" {
		return name
	}
	return entityType + ":" + name
}

func resolveRelationEntityKey(nameKey string, nodes map[string]model.GraphNode) string {
	if _, ok := nodes[nameKey]; ok {
		return nameKey
	}
	for key := range nodes {
		if strings.HasSuffix(key, ":"+nameKey) {
			return key
		}
	}
	return ""
}
