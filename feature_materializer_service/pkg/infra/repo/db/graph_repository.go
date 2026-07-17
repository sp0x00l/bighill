package db

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"
	coreDB "lib/shared_lib/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
)

func (r *SnapshotRepository) SavePendingGraphSnapshot(ctx context.Context, tx pgx.Tx, embeddingSnapshotID, idempotencyKey uuid.UUID, strategy model.GraphExtractionStrategy) (*model.GraphSnapshot, error) {
	log.Trace("SnapshotRepository SavePendingGraphSnapshot")

	strategy = model.ApplyGraphExtractionStrategyDefaults(strategy)
	embeddingSnapshot, err := r.readEmbeddingSnapshot(ctx, tx, embeddingSnapshotID)
	if err != nil {
		return nil, err
	}
	query := `INSERT INTO ` + r.Name + `.graph_snapshots (
		feature_snapshot_id, embedding_snapshot_id, dataset_id, user_id, org_id, idempotency_key,
		extraction_model, extraction_prompt_version, extraction_schema_version, status
	) VALUES (
		@feature_snapshot_id, @embedding_snapshot_id, @dataset_id, @user_id, @org_id, @idempotency_key,
		@extraction_model, @extraction_prompt_version, @extraction_schema_version, @status::snapshot_status_enum
	)
	ON CONFLICT (idempotency_key) DO NOTHING
	RETURNING ` + graphSnapshotColumns()

	graphSnapshot, err := scanGraphSnapshot(tx.QueryRow(ctx, query, pgx.NamedArgs{
		"feature_snapshot_id":       pgtype.UUID{Bytes: embeddingSnapshot.FeatureSnapshotID, Valid: true},
		"embedding_snapshot_id":     pgtype.UUID{Bytes: embeddingSnapshot.EmbeddingSnapshotID, Valid: true},
		"dataset_id":                pgtype.UUID{Bytes: embeddingSnapshot.DatasetID, Valid: true},
		"user_id":                   pgtype.UUID{Bytes: embeddingSnapshot.UserID, Valid: true},
		"org_id":                    pgtype.UUID{Bytes: embeddingSnapshot.OrgID, Valid: embeddingSnapshot.OrgID != uuid.Nil},
		"idempotency_key":           pgtype.UUID{Bytes: idempotencyKey, Valid: true},
		"extraction_model":          strategy.ExtractionModel,
		"extraction_prompt_version": strategy.ExtractionPromptVersion,
		"extraction_schema_version": strategy.ExtractionSchemaVersion,
		"status":                    model.SnapshotStatusPending.String(),
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return r.resolveGraphSnapshotIdempotencyConflict(ctx, tx, idempotencyKey)
		}
		if coreDB.IsForeignKeyViolation(err) {
			return nil, domain.ErrValidationFailed.Extend("tenant projection is not ready")
		}
		r.LogPoolStatsOnError(ctx, "insert graph snapshot failed", err)
		return nil, fmt.Errorf("insert graph snapshot: %w", err)
	}
	return graphSnapshot, nil
}

func (r *SnapshotRepository) ReadEmbeddingChunks(ctx context.Context, embeddingSnapshotID uuid.UUID) ([]model.GraphChunk, error) {
	log.Trace("SnapshotRepository ReadEmbeddingChunks")

	query := `SELECT embedding_record_id::text, embedding_snapshot_id::text, dataset_id::text, user_id::text, org_id::text,
			chunk_index, source_text
		FROM ` + r.Name + `.embedding_records
		WHERE embedding_snapshot_id = @embedding_snapshot_id
		ORDER BY chunk_index`
	rows, err := r.Pool.Query(ctx, query, pgx.NamedArgs{
		"embedding_snapshot_id": pgtype.UUID{Bytes: embeddingSnapshotID, Valid: true},
	})
	if err != nil {
		return nil, fmt.Errorf("read embedding chunks: %w", err)
	}
	defer rows.Close()

	chunks := []model.GraphChunk{}
	for rows.Next() {
		var recordID, snapshotID, datasetID, userID, orgID string
		chunk := model.GraphChunk{}
		if err := rows.Scan(&recordID, &snapshotID, &datasetID, &userID, &orgID, &chunk.ChunkIndex, &chunk.SourceText); err != nil {
			return nil, err
		}
		chunk.EmbeddingRecordID = uuid.MustParse(recordID)
		chunk.EmbeddingSnapshotID = uuid.MustParse(snapshotID)
		chunk.DatasetID = uuid.MustParse(datasetID)
		chunk.UserID = uuid.MustParse(userID)
		chunk.OrgID = uuid.MustParse(orgID)
		chunks = append(chunks, chunk)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read embedding chunks rows: %w", err)
	}
	return chunks, nil
}

func (r *SnapshotRepository) SaveGraphMaterialization(ctx context.Context, tx pgx.Tx, materialization *model.GraphMaterialization) error {
	log.Trace("SnapshotRepository SaveGraphMaterialization")

	if materialization == nil || materialization.Snapshot == nil {
		return domain.ErrValidationFailed.Extend("graph materialization is required")
	}
	nodeIDs := make(map[string]uuid.UUID, len(materialization.Nodes))
	for _, node := range materialization.Nodes {
		var idRaw string
		err := tx.QueryRow(ctx, `INSERT INTO `+r.Name+`.graph_nodes (
				graph_snapshot_id, dataset_id, user_id, org_id, entity_key, name, entity_type, description, mention_count
			) VALUES (
				@graph_snapshot_id, @dataset_id, @user_id, @org_id, @entity_key, @name, @entity_type, @description, @mention_count
			)
			ON CONFLICT (graph_snapshot_id, entity_key) DO UPDATE SET
				description = EXCLUDED.description,
				mention_count = EXCLUDED.mention_count
			RETURNING graph_node_id::text`, pgx.NamedArgs{
			"graph_snapshot_id": pgtype.UUID{Bytes: materialization.Snapshot.GraphSnapshotID, Valid: true},
			"dataset_id":        pgtype.UUID{Bytes: node.DatasetID, Valid: true},
			"user_id":           pgtype.UUID{Bytes: node.UserID, Valid: true},
			"org_id":            pgtype.UUID{Bytes: node.OrgID, Valid: node.OrgID != uuid.Nil},
			"entity_key":        node.EntityKey,
			"name":              node.Name,
			"entity_type":       node.Type,
			"description":       node.Description,
			"mention_count":     node.MentionCount,
		}).Scan(&idRaw)
		if err != nil {
			return fmt.Errorf("insert graph node: %w", err)
		}
		nodeIDs[node.EntityKey] = uuid.MustParse(idRaw)
	}
	for _, chunk := range materialization.NodeChunks {
		nodeID := nodeIDs[chunk.EntityKey]
		if nodeID == uuid.Nil {
			continue
		}
		if _, err := tx.Exec(ctx, `INSERT INTO `+r.Name+`.graph_node_chunks (
				graph_snapshot_id, graph_node_id, embedding_record_id, embedding_snapshot_id,
				dataset_id, user_id, org_id, chunk_index, source_text
			) VALUES (
				@graph_snapshot_id, @graph_node_id, @embedding_record_id, @embedding_snapshot_id,
				@dataset_id, @user_id, @org_id, @chunk_index, @source_text
			)
			ON CONFLICT (graph_node_id, embedding_record_id) DO NOTHING`, pgx.NamedArgs{
			"graph_snapshot_id":     pgtype.UUID{Bytes: materialization.Snapshot.GraphSnapshotID, Valid: true},
			"graph_node_id":         pgtype.UUID{Bytes: nodeID, Valid: true},
			"embedding_record_id":   pgtype.UUID{Bytes: chunk.EmbeddingRecordID, Valid: true},
			"embedding_snapshot_id": pgtype.UUID{Bytes: chunk.EmbeddingSnapshotID, Valid: true},
			"dataset_id":            pgtype.UUID{Bytes: chunk.DatasetID, Valid: true},
			"user_id":               pgtype.UUID{Bytes: chunk.UserID, Valid: true},
			"org_id":                pgtype.UUID{Bytes: chunk.OrgID, Valid: chunk.OrgID != uuid.Nil},
			"chunk_index":           chunk.ChunkIndex,
			"source_text":           chunk.SourceText,
		}); err != nil {
			return fmt.Errorf("insert graph node chunk: %w", err)
		}
	}
	for _, edge := range materialization.Edges {
		sourceID := nodeIDs[edge.SourceEntityKey]
		targetID := nodeIDs[edge.TargetEntityKey]
		if sourceID == uuid.Nil || targetID == uuid.Nil {
			continue
		}
		relationType := strings.TrimSpace(edge.RelationType)
		if relationType == "" {
			relationType = "RELATED_TO"
		}
		if _, err := tx.Exec(ctx, `INSERT INTO `+r.Name+`.graph_edges (
				graph_snapshot_id, dataset_id, user_id, org_id, source_node_id, target_node_id,
				relation_type, description, weight
			) VALUES (
				@graph_snapshot_id, @dataset_id, @user_id, @org_id, @source_node_id, @target_node_id,
				@relation_type, @description, @weight
			)`, pgx.NamedArgs{
			"graph_snapshot_id": pgtype.UUID{Bytes: materialization.Snapshot.GraphSnapshotID, Valid: true},
			"dataset_id":        pgtype.UUID{Bytes: edge.DatasetID, Valid: true},
			"user_id":           pgtype.UUID{Bytes: edge.UserID, Valid: true},
			"org_id":            pgtype.UUID{Bytes: edge.OrgID, Valid: edge.OrgID != uuid.Nil},
			"source_node_id":    pgtype.UUID{Bytes: sourceID, Valid: true},
			"target_node_id":    pgtype.UUID{Bytes: targetID, Valid: true},
			"relation_type":     relationType,
			"description":       edge.Description,
			"weight":            edge.Weight,
		}); err != nil {
			return fmt.Errorf("insert graph edge: %w", err)
		}
	}
	return nil
}

func (r *SnapshotRepository) MarkGraphReady(ctx context.Context, tx pgx.Tx, graphSnapshot *model.GraphSnapshot) error {
	log.Trace("SnapshotRepository MarkGraphReady")

	eventSeq, err := r.nextMaterializationEventSeq(ctx, tx, graphSnapshot.DatasetID, graphSnapshot.OrgID)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE `+r.Name+`.graph_snapshots
		SET active_for_retrieval = false
		WHERE dataset_id = @dataset_id
			AND org_id = @org_id
			AND active_for_retrieval = true
			AND graph_snapshot_id != @graph_snapshot_id`, pgx.NamedArgs{
		"dataset_id":        pgtype.UUID{Bytes: graphSnapshot.DatasetID, Valid: true},
		"org_id":            pgtype.UUID{Bytes: graphSnapshot.OrgID, Valid: graphSnapshot.OrgID != uuid.Nil},
		"graph_snapshot_id": pgtype.UUID{Bytes: graphSnapshot.GraphSnapshotID, Valid: true},
	}); err != nil {
		return fmt.Errorf("deactivate previous active graph snapshots: %w", err)
	}
	tag, err := tx.Exec(ctx, `UPDATE `+r.Name+`.graph_snapshots
		SET status = @status::snapshot_status_enum,
			materialization_event_seq = @materialization_event_seq,
			active_for_retrieval = true,
			provenance_hash = @provenance_hash,
			extraction_model = @extraction_model,
			extraction_prompt_version = @extraction_prompt_version,
			extraction_schema_version = @extraction_schema_version,
			chunk_count = @chunk_count,
			chunks_processed = @chunks_processed,
			entity_count = @entity_count,
			edge_count = @edge_count,
			failure_reason = NULL
		WHERE graph_snapshot_id = @graph_snapshot_id`, pgx.NamedArgs{
		"graph_snapshot_id":         pgtype.UUID{Bytes: graphSnapshot.GraphSnapshotID, Valid: true},
		"materialization_event_seq": eventSeq,
		"provenance_hash":           graphSnapshot.ProvenanceHash,
		"extraction_model":          graphSnapshot.ExtractionModel,
		"extraction_prompt_version": graphSnapshot.ExtractionPromptVersion,
		"extraction_schema_version": graphSnapshot.ExtractionSchemaVersion,
		"chunk_count":               graphSnapshot.ChunkCount,
		"chunks_processed":          graphSnapshot.ChunksProcessed,
		"entity_count":              graphSnapshot.EntityCount,
		"edge_count":                graphSnapshot.EdgeCount,
		"status":                    model.SnapshotStatusReady.String(),
	})
	if err != nil {
		return fmt.Errorf("mark graph snapshot ready: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: graph_snapshot_id=%s", domain.ErrGraphSnapshotNotFound, graphSnapshot.GraphSnapshotID)
	}
	graphSnapshot.MaterializationEventSeq = eventSeq
	graphSnapshot.ActiveForRetrieval = true
	return nil
}

func (r *SnapshotRepository) MarkGraphFailed(ctx context.Context, tx pgx.Tx, graphSnapshotID uuid.UUID, reason string) error {
	log.Trace("SnapshotRepository MarkGraphFailed")

	tag, err := tx.Exec(ctx, `UPDATE `+r.Name+`.graph_snapshots
		SET status = @status::snapshot_status_enum, active_for_retrieval = false, failure_reason = @failure_reason
		WHERE graph_snapshot_id = @graph_snapshot_id`, pgx.NamedArgs{
		"graph_snapshot_id": pgtype.UUID{Bytes: graphSnapshotID, Valid: true},
		"failure_reason":    reason,
		"status":            model.SnapshotStatusFailed.String(),
	})
	if err != nil {
		return fmt.Errorf("mark graph snapshot failed: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: graph_snapshot_id=%s", domain.ErrGraphSnapshotNotFound, graphSnapshotID)
	}
	return nil
}

func (r *SnapshotRepository) ReadGraphByIdempotencyKey(ctx context.Context, idempotencyKey uuid.UUID) (*model.GraphSnapshot, error) {
	log.Trace("SnapshotRepository ReadGraphByIdempotencyKey")

	return r.readGraphByIdempotencyKey(ctx, r.Pool, idempotencyKey)
}

func (r *SnapshotRepository) ReadActiveGraphSnapshot(ctx context.Context, userID uuid.UUID, datasetID uuid.UUID) (*model.GraphSnapshot, error) {
	log.Trace("SnapshotRepository ReadActiveGraphSnapshot")

	query := `SELECT ` + graphSnapshotColumns() + ` FROM ` + r.Name + `.graph_snapshots
		WHERE dataset_id = @dataset_id
			AND org_id = @org_id
			AND active_for_retrieval = true
			AND status = @status::snapshot_status_enum
		ORDER BY updated_at DESC
		LIMIT 1`
	graphSnapshot, err := scanGraphSnapshot(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"dataset_id": pgtype.UUID{Bytes: datasetID, Valid: true},
		"user_id":    pgtype.UUID{Bytes: userID, Valid: true},
		"org_id":     pgtype.UUID{Bytes: orgIDFromContext(ctx), Valid: orgIDFromContext(ctx) != uuid.Nil},
		"status":     model.SnapshotStatusReady.String(),
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: dataset_id=%s", domain.ErrGraphSnapshotNotFound, datasetID)
		}
		return nil, fmt.Errorf("read active graph snapshot: %w", err)
	}
	return graphSnapshot, nil
}

func (r *SnapshotRepository) SearchGraph(ctx context.Context, graphSnapshot *model.GraphSnapshot, queryText string, topK int, maxHops int) (*model.GraphSearchResult, error) {
	log.Trace("SnapshotRepository SearchGraph")

	if graphSnapshot == nil {
		return nil, domain.ErrGraphSnapshotNotFound.Extend("active graph snapshot is required")
	}
	if topK <= 0 {
		topK = 5
	}
	if maxHops <= 0 {
		maxHops = 2
	}
	needle := "%" + strings.ToLower(strings.TrimSpace(queryText)) + "%"
	query := `WITH RECURSIVE seed AS (
			SELECT graph_node_id, name, entity_type, description, 1.0::double precision AS score
			FROM ` + r.Name + `.graph_nodes
			WHERE graph_snapshot_id = @graph_snapshot_id
				AND org_id = @org_id
				AND (
					lower(name) LIKE @needle
					OR lower(entity_type) LIKE @needle
					OR lower(description) LIKE @needle
				)
			ORDER BY name
			LIMIT @limit
		),
		walk(node_id, path, relation_types, depth, score) AS (
			SELECT graph_node_id, ARRAY[graph_node_id], ARRAY[]::text[], 0, score FROM seed
			UNION ALL
			SELECT edge.target_node_id,
				walk.path || edge.target_node_id,
				walk.relation_types || edge.relation_type,
				walk.depth + 1,
				walk.score * GREATEST(edge.weight, 0.01)
			FROM walk
			JOIN ` + r.Name + `.graph_edges edge
				ON edge.graph_snapshot_id = @graph_snapshot_id
				AND edge.org_id = @org_id
				AND edge.source_node_id = walk.node_id
			WHERE walk.depth < @max_hops
				AND NOT edge.target_node_id = ANY(walk.path)
		),
		connected AS (
			SELECT node_id, MIN(depth) AS depth, MAX(score) AS score
			FROM walk
			GROUP BY node_id
		)
		SELECT chunk.graph_node_chunk_id::text, chunk.graph_node_id::text,
			chunk.embedding_record_id::text, chunk.embedding_snapshot_id::text,
			chunk.dataset_id::text, chunk.chunk_index, chunk.source_text,
			connected.score::double precision, chunk.org_id::text
		FROM connected
		JOIN ` + r.Name + `.graph_node_chunks chunk ON chunk.graph_node_id = connected.node_id
		ORDER BY connected.depth ASC, connected.score DESC, chunk.chunk_index ASC
		LIMIT @limit`
	rows, err := r.Pool.Query(ctx, query, pgx.NamedArgs{
		"graph_snapshot_id": pgtype.UUID{Bytes: graphSnapshot.GraphSnapshotID, Valid: true},
		"org_id":            pgtype.UUID{Bytes: graphSnapshot.OrgID, Valid: graphSnapshot.OrgID != uuid.Nil},
		"needle":            needle,
		"limit":             topK,
		"max_hops":          maxHops,
	})
	if err != nil {
		return nil, fmt.Errorf("search graph contexts: %w", err)
	}
	defer rows.Close()
	contexts := []model.GraphRetrievedContext{}
	for rows.Next() {
		context, err := scanGraphRetrievedContext(rows)
		if err != nil {
			return nil, err
		}
		contexts = append(contexts, context)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read graph context rows: %w", err)
	}
	entities, err := r.searchGraphMatchedEntities(ctx, graphSnapshot, needle, topK)
	if err != nil {
		return nil, err
	}
	paths, err := r.searchGraphPaths(ctx, graphSnapshot, needle, topK, maxHops)
	if err != nil {
		return nil, err
	}
	return &model.GraphSearchResult{
		GraphSnapshot:   graphSnapshot,
		Contexts:        contexts,
		MatchedEntities: entities,
		Paths:           paths,
	}, nil
}

func (r *SnapshotRepository) searchGraphMatchedEntities(ctx context.Context, graphSnapshot *model.GraphSnapshot, needle string, topK int) ([]model.GraphMatchedEntity, error) {
	rows, err := r.Pool.Query(ctx, `SELECT graph_node_id::text, name, entity_type, description, 1.0::double precision
		FROM `+r.Name+`.graph_nodes
		WHERE graph_snapshot_id = @graph_snapshot_id
			AND org_id = @org_id
			AND (
				lower(name) LIKE @needle
				OR lower(entity_type) LIKE @needle
				OR lower(description) LIKE @needle
			)
		ORDER BY name
		LIMIT @limit`, pgx.NamedArgs{
		"graph_snapshot_id": pgtype.UUID{Bytes: graphSnapshot.GraphSnapshotID, Valid: true},
		"org_id":            pgtype.UUID{Bytes: graphSnapshot.OrgID, Valid: graphSnapshot.OrgID != uuid.Nil},
		"needle":            needle,
		"limit":             topK,
	})
	if err != nil {
		return nil, fmt.Errorf("search graph matched entities: %w", err)
	}
	defer rows.Close()
	out := []model.GraphMatchedEntity{}
	for rows.Next() {
		var nodeID string
		entity := model.GraphMatchedEntity{}
		if err := rows.Scan(&nodeID, &entity.Name, &entity.Type, &entity.Description, &entity.Score); err != nil {
			return nil, err
		}
		entity.GraphNodeID = uuid.MustParse(nodeID)
		out = append(out, entity)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *SnapshotRepository) searchGraphPaths(ctx context.Context, graphSnapshot *model.GraphSnapshot, needle string, topK int, maxHops int) ([]model.GraphPath, error) {
	rows, err := r.Pool.Query(ctx, `WITH RECURSIVE seed AS (
			SELECT graph_node_id, name, 1.0::double precision AS score
			FROM `+r.Name+`.graph_nodes
			WHERE graph_snapshot_id = @graph_snapshot_id
				AND org_id = @org_id
				AND (
					lower(name) LIKE @needle
					OR lower(entity_type) LIKE @needle
					OR lower(description) LIKE @needle
				)
			ORDER BY name
			LIMIT @limit
		),
		walk(node_id, path, relation_types, depth, score) AS (
			SELECT graph_node_id, ARRAY[graph_node_id], ARRAY[]::text[], 0, score FROM seed
			UNION ALL
			SELECT edge.target_node_id,
				walk.path || edge.target_node_id,
				walk.relation_types || edge.relation_type,
				walk.depth + 1,
				walk.score * GREATEST(edge.weight, 0.01)
			FROM walk
			JOIN `+r.Name+`.graph_edges edge
				ON edge.graph_snapshot_id = @graph_snapshot_id
				AND edge.org_id = @org_id
				AND edge.source_node_id = walk.node_id
			WHERE walk.depth < @max_hops
				AND NOT edge.target_node_id = ANY(walk.path)
		)
		SELECT array_to_string(path, ',') AS graph_node_ids,
			array_to_string(relation_types, ',') AS relation_types,
			score::double precision
		FROM walk
		WHERE depth > 0
		ORDER BY depth ASC, score DESC
		LIMIT @limit`, pgx.NamedArgs{
		"graph_snapshot_id": pgtype.UUID{Bytes: graphSnapshot.GraphSnapshotID, Valid: true},
		"org_id":            pgtype.UUID{Bytes: graphSnapshot.OrgID, Valid: graphSnapshot.OrgID != uuid.Nil},
		"needle":            needle,
		"limit":             topK,
		"max_hops":          maxHops,
	})
	if err != nil {
		return nil, fmt.Errorf("search graph paths: %w", err)
	}
	defer rows.Close()

	out := []model.GraphPath{}
	for rows.Next() {
		var nodeIDsText, relationTypesText string
		path := model.GraphPath{}
		if err := rows.Scan(&nodeIDsText, &relationTypesText, &path.Score); err != nil {
			return nil, err
		}
		nodeIDs, err := parseUUIDCSV(nodeIDsText)
		if err != nil {
			return nil, err
		}
		path.GraphNodeIDs = nodeIDs
		path.RelationTypes = splitNonEmptyCSV(relationTypesText)
		out = append(out, path)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *SnapshotRepository) readEmbeddingSnapshot(ctx context.Context, queryer rowQuerier, embeddingSnapshotID uuid.UUID) (*model.EmbeddingSnapshot, error) {
	query := `SELECT ` + embeddingSnapshotColumns() + ` FROM ` + r.Name + `.embedding_snapshots WHERE embedding_snapshot_id = @embedding_snapshot_id`
	embeddingSnapshot, err := scanEmbeddingSnapshot(queryer.QueryRow(ctx, query, pgx.NamedArgs{
		"embedding_snapshot_id": pgtype.UUID{Bytes: embeddingSnapshotID, Valid: true},
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: embedding_snapshot_id=%s", domain.ErrEmbeddingSnapshotNotFound, embeddingSnapshotID)
		}
		return nil, fmt.Errorf("read embedding snapshot: %w", err)
	}
	return embeddingSnapshot, nil
}

func (r *SnapshotRepository) readGraphByIdempotencyKey(ctx context.Context, queryer rowQuerier, idempotencyKey uuid.UUID) (*model.GraphSnapshot, error) {
	query := `SELECT ` + graphSnapshotColumns() + ` FROM ` + r.Name + `.graph_snapshots WHERE idempotency_key = @idempotency_key`
	graphSnapshot, err := scanGraphSnapshot(queryer.QueryRow(ctx, query, pgx.NamedArgs{
		"idempotency_key": pgtype.UUID{Bytes: idempotencyKey, Valid: true},
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: idempotency_key=%s", domain.ErrGraphSnapshotNotFound, idempotencyKey)
		}
		return nil, fmt.Errorf("read graph by idempotency key: %w", err)
	}
	return graphSnapshot, nil
}

func (r *SnapshotRepository) resolveGraphSnapshotIdempotencyConflict(ctx context.Context, tx pgx.Tx, idempotencyKey uuid.UUID) (*model.GraphSnapshot, error) {
	existing, err := r.readGraphByIdempotencyKey(ctx, tx, idempotencyKey)
	if err != nil {
		return nil, fmt.Errorf("graph snapshot idempotency conflict lookup failed: idempotency_key=%s: %w", idempotencyKey, err)
	}
	switch existing.Status {
	case model.SnapshotStatusReady:
		return nil, &domain.GraphAlreadyMaterializedError{Record: existing}
	case model.SnapshotStatusFailed:
		return r.reopenGraphSnapshotForRetry(ctx, tx, existing.GraphSnapshotID)
	case model.SnapshotStatusPending:
		return nil, fmt.Errorf("%w: graph_snapshot_id=%s", domain.ErrGraphSnapshotInProgress, existing.GraphSnapshotID)
	default:
		return nil, fmt.Errorf("unsupported graph snapshot status %s", existing.Status.String())
	}
}

func (r *SnapshotRepository) reopenGraphSnapshotForRetry(ctx context.Context, tx pgx.Tx, graphSnapshotID uuid.UUID) (*model.GraphSnapshot, error) {
	query := `UPDATE ` + r.Name + `.graph_snapshots
		SET status = @status::snapshot_status_enum, failure_reason = NULL
		WHERE graph_snapshot_id = @graph_snapshot_id
		RETURNING ` + graphSnapshotColumns()
	graphSnapshot, err := scanGraphSnapshot(tx.QueryRow(ctx, query, pgx.NamedArgs{
		"graph_snapshot_id": pgtype.UUID{Bytes: graphSnapshotID, Valid: true},
		"status":            model.SnapshotStatusPending.String(),
	}))
	if err != nil {
		return nil, fmt.Errorf("reopen graph snapshot for retry: %w", err)
	}
	return graphSnapshot, nil
}

func graphSnapshotColumns() string {
	return `graph_snapshot_id::text, feature_snapshot_id::text, embedding_snapshot_id::text, dataset_id::text, user_id::text, org_id::text,
		materialization_event_seq, idempotency_key::text, provenance_hash, extraction_model, extraction_prompt_version,
		extraction_schema_version, chunk_count, chunks_processed, entity_count, edge_count, active_for_retrieval,
		status::text, COALESCE(failure_reason, '')`
}

func scanGraphSnapshot(row pgx.Row) (*model.GraphSnapshot, error) {
	var graphSnapshotID, featureSnapshotID, embeddingSnapshotID, datasetID, userID, orgID, idempotencyKey, statusRaw string
	graphSnapshot := &model.GraphSnapshot{}
	if err := row.Scan(
		&graphSnapshotID,
		&featureSnapshotID,
		&embeddingSnapshotID,
		&datasetID,
		&userID,
		&orgID,
		&graphSnapshot.MaterializationEventSeq,
		&idempotencyKey,
		&graphSnapshot.ProvenanceHash,
		&graphSnapshot.ExtractionModel,
		&graphSnapshot.ExtractionPromptVersion,
		&graphSnapshot.ExtractionSchemaVersion,
		&graphSnapshot.ChunkCount,
		&graphSnapshot.ChunksProcessed,
		&graphSnapshot.EntityCount,
		&graphSnapshot.EdgeCount,
		&graphSnapshot.ActiveForRetrieval,
		&statusRaw,
		&graphSnapshot.FailureReason,
	); err != nil {
		return nil, err
	}
	status, err := model.ToSnapshotStatus(statusRaw)
	if err != nil {
		return nil, err
	}
	graphSnapshot.GraphSnapshotID = uuid.MustParse(graphSnapshotID)
	graphSnapshot.FeatureSnapshotID = uuid.MustParse(featureSnapshotID)
	graphSnapshot.EmbeddingSnapshotID = uuid.MustParse(embeddingSnapshotID)
	graphSnapshot.DatasetID = uuid.MustParse(datasetID)
	graphSnapshot.UserID = uuid.MustParse(userID)
	graphSnapshot.OrgID = uuid.MustParse(orgID)
	graphSnapshot.IdempotencyKey = uuid.MustParse(idempotencyKey)
	graphSnapshot.Status = status
	return graphSnapshot, nil
}

func scanGraphRetrievedContext(row pgx.Row) (model.GraphRetrievedContext, error) {
	var nodeChunkID, nodeID, embeddingRecordID, embeddingSnapshotID, datasetID, orgID string
	context := model.GraphRetrievedContext{}
	if err := row.Scan(
		&nodeChunkID,
		&nodeID,
		&embeddingRecordID,
		&embeddingSnapshotID,
		&datasetID,
		&context.ChunkIndex,
		&context.SourceText,
		&context.Score,
		&orgID,
	); err != nil {
		return model.GraphRetrievedContext{}, err
	}
	context.GraphNodeChunkID = uuid.MustParse(nodeChunkID)
	context.GraphNodeID = uuid.MustParse(nodeID)
	context.EmbeddingRecordID = uuid.MustParse(embeddingRecordID)
	context.EmbeddingSnapshotID = uuid.MustParse(embeddingSnapshotID)
	context.DatasetID = uuid.MustParse(datasetID)
	context.OrgID = uuid.MustParse(orgID)
	return context, nil
}

func parseUUIDCSV(value string) ([]uuid.UUID, error) {
	log.Trace("parseUUIDCSV")

	parts := splitNonEmptyCSV(value)
	out := make([]uuid.UUID, 0, len(parts))
	for _, part := range parts {
		id, err := uuid.Parse(part)
		if err != nil {
			return nil, fmt.Errorf("parse graph path node id %q: %w", part, err)
		}
		out = append(out, id)
	}
	return out, nil
}

func splitNonEmptyCSV(value string) []string {
	log.Trace("splitNonEmptyCSV")

	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}
