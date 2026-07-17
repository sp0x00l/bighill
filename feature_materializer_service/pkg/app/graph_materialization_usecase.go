package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"
	serializers "lib/shared_lib/serializer"
	shareduow "lib/shared_lib/uow"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

type GraphMaterializationUsecase interface {
	MaterializeGraph(ctx context.Context, embeddingSnapshotID uuid.UUID, idempotencyKey uuid.UUID, strategy model.GraphExtractionStrategy) (*model.GraphSnapshot, error)
}

type graphMaterializationUsecase struct {
	repo         GraphSnapshotRepository
	unitOfWork   SnapshotUnitOfWorkAdapter
	eventBuilder SnapshotEventBuilder
	extractor    GraphExtractor
	encoder      *serializers.Encoder
}

func NewGraphMaterializationUsecase(repo GraphSnapshotRepository, unitOfWork SnapshotUnitOfWorkAdapter, eventBuilder SnapshotEventBuilder, extractor GraphExtractor) GraphMaterializationUsecase {
	log.Trace("NewGraphMaterializationUsecase")

	return &graphMaterializationUsecase{
		repo:         repo,
		unitOfWork:   unitOfWork,
		eventBuilder: eventBuilder,
		extractor:    extractor,
		encoder:      serializers.NewJSONSerializer(),
	}
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
		if markErr := u.markGraphFailed(ctx, graphSnapshot.GraphSnapshotID, err.Error()); markErr != nil {
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

	return u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, enqueue shareduow.EnqueueFunc) error {
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
}

func (u *graphMaterializationUsecase) markGraphFailed(ctx context.Context, graphSnapshotID uuid.UUID, reason string) error {
	log.Trace("GraphMaterializationUsecase markGraphFailed")

	return u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
		return u.repo.MarkGraphFailed(ctx, tx, graphSnapshotID, reason)
	})
}

func (u *graphMaterializationUsecase) graphProvenanceHash(snapshot *model.GraphSnapshot, strategy model.GraphExtractionStrategy) (string, error) {
	log.Trace("GraphMaterializationUsecase graphProvenanceHash")

	payload := struct {
		FeatureSnapshotID       string `json:"feature_snapshot_id"`
		EmbeddingSnapshotID     string `json:"embedding_snapshot_id"`
		ExtractionModel         string `json:"extraction_model"`
		ExtractionPromptVersion string `json:"extraction_prompt_version"`
		ExtractionSchemaVersion string `json:"extraction_schema_version"`
	}{
		FeatureSnapshotID:       snapshot.FeatureSnapshotID.String(),
		EmbeddingSnapshotID:     snapshot.EmbeddingSnapshotID.String(),
		ExtractionModel:         strategy.ExtractionModel,
		ExtractionPromptVersion: strategy.ExtractionPromptVersion,
		ExtractionSchemaVersion: strategy.ExtractionSchemaVersion,
	}
	canonical, err := u.encoder.Serialize(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
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
