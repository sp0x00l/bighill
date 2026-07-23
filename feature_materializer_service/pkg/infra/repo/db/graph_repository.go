package db

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"
	"lib/shared_lib/ctxutil"
	coreDB "lib/shared_lib/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
)

const (
	graphLexicalExactNameScore = 0.98
	graphLexicalExactTypeScore = 0.90
	graphLexicalPartialScore   = 0.85
	graphHybridMatchBoost      = 0.05
	graphHybridSemanticFloor   = 0.70
	graphMinimumSeedLimit      = 10
	graphSemanticChunkFanout   = 8
	graphMinimumLexicalToken   = 3
)

var graphLexicalStopwords = map[string]struct{}{
	"about":   {},
	"across":  {},
	"and":     {},
	"are":     {},
	"can":     {},
	"could":   {},
	"did":     {},
	"does":    {},
	"for":     {},
	"from":    {},
	"get":     {},
	"how":     {},
	"into":    {},
	"please":  {},
	"show":    {},
	"tell":    {},
	"the":     {},
	"through": {},
	"what":    {},
	"when":    {},
	"where":   {},
	"which":   {},
	"who":     {},
	"why":     {},
	"with":    {},
	"would":   {},
}

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
				graph_snapshot_id, dataset_id, user_id, org_id, entity_key, name, entity_type, description, mention_count, assertion_status
			) VALUES (
				@graph_snapshot_id, @dataset_id, @user_id, @org_id, @entity_key, @name, @entity_type, @description, @mention_count, @assertion_status::assertion_status_enum
			)
			ON CONFLICT (graph_snapshot_id, entity_key) DO UPDATE SET
				description = EXCLUDED.description,
				mention_count = EXCLUDED.mention_count,
				assertion_status = EXCLUDED.assertion_status
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
			"assertion_status":  assertionStatusValue(node.AssertionStatus),
		}).Scan(&idRaw)
		if err != nil {
			return graphPersistenceError("insert graph node", err)
		}
		nodeIDs[node.EntityKey] = uuid.MustParse(idRaw)
	}
	for _, alias := range materialization.NodeAliases {
		nodeID := nodeIDs[alias.CanonicalEntityKey]
		if nodeID == uuid.Nil {
			continue
		}
		if _, err := tx.Exec(ctx, `INSERT INTO `+r.Name+`.graph_node_aliases (
				graph_snapshot_id, graph_node_id, dataset_id, user_id, org_id, source_entity_key, alias, entity_type, assertion_status
			) VALUES (
				@graph_snapshot_id, @graph_node_id, @dataset_id, @user_id, @org_id, @source_entity_key, @alias, @entity_type, @assertion_status::assertion_status_enum
			)
			ON CONFLICT (graph_snapshot_id, graph_node_id, source_entity_key) DO UPDATE SET
				alias = EXCLUDED.alias,
				entity_type = EXCLUDED.entity_type,
				assertion_status = EXCLUDED.assertion_status`, pgx.NamedArgs{
			"graph_snapshot_id": pgtype.UUID{Bytes: materialization.Snapshot.GraphSnapshotID, Valid: true},
			"graph_node_id":     pgtype.UUID{Bytes: nodeID, Valid: true},
			"dataset_id":        pgtype.UUID{Bytes: alias.DatasetID, Valid: true},
			"user_id":           pgtype.UUID{Bytes: alias.UserID, Valid: true},
			"org_id":            pgtype.UUID{Bytes: alias.OrgID, Valid: alias.OrgID != uuid.Nil},
			"source_entity_key": alias.SourceEntityKey,
			"alias":             alias.Alias,
			"entity_type":       alias.Type,
			"assertion_status":  assertionStatusValue(alias.AssertionStatus),
		}); err != nil {
			return graphPersistenceError("insert graph node alias", err)
		}
	}
	for _, embedding := range materialization.NodeEmbeddings {
		nodeID := nodeIDs[embedding.EntityKey]
		if nodeID == uuid.Nil {
			continue
		}
		if _, err := tx.Exec(ctx, `INSERT INTO `+r.Name+`.graph_node_embeddings (
				graph_snapshot_id, graph_node_id, embedding_snapshot_id, dataset_id, user_id, org_id, embedding_text, embedding, assertion_status
			) VALUES (
				@graph_snapshot_id, @graph_node_id, @embedding_snapshot_id, @dataset_id, @user_id, @org_id, @embedding_text, @embedding, @assertion_status::assertion_status_enum
			)
			ON CONFLICT (graph_node_id, embedding_snapshot_id) DO UPDATE SET
				embedding_text = EXCLUDED.embedding_text,
				embedding = EXCLUDED.embedding,
				assertion_status = EXCLUDED.assertion_status`, pgx.NamedArgs{
			"graph_snapshot_id":     pgtype.UUID{Bytes: materialization.Snapshot.GraphSnapshotID, Valid: true},
			"graph_node_id":         pgtype.UUID{Bytes: nodeID, Valid: true},
			"embedding_snapshot_id": pgtype.UUID{Bytes: embedding.EmbeddingSnapshotID, Valid: true},
			"dataset_id":            pgtype.UUID{Bytes: embedding.DatasetID, Valid: true},
			"user_id":               pgtype.UUID{Bytes: embedding.UserID, Valid: true},
			"org_id":                pgtype.UUID{Bytes: embedding.OrgID, Valid: embedding.OrgID != uuid.Nil},
			"embedding_text":        embedding.EmbeddingText,
			"embedding":             vectorLiteral(embedding.Vector),
			"assertion_status":      assertionStatusValue(embedding.AssertionStatus),
		}); err != nil {
			return graphPersistenceError("insert graph node embedding", err)
		}
	}
	for _, chunk := range materialization.NodeChunks {
		nodeID := nodeIDs[chunk.EntityKey]
		if nodeID == uuid.Nil {
			continue
		}
		if _, err := tx.Exec(ctx, `INSERT INTO `+r.Name+`.graph_node_chunks (
				graph_snapshot_id, graph_node_id, embedding_record_id, embedding_snapshot_id,
				dataset_id, user_id, org_id, chunk_index, source_text, assertion_status
			) VALUES (
				@graph_snapshot_id, @graph_node_id, @embedding_record_id, @embedding_snapshot_id,
				@dataset_id, @user_id, @org_id, @chunk_index, @source_text, @assertion_status::assertion_status_enum
			)
			ON CONFLICT (graph_node_id, embedding_record_id) DO UPDATE SET
				chunk_index = EXCLUDED.chunk_index,
				source_text = EXCLUDED.source_text,
				assertion_status = EXCLUDED.assertion_status`, pgx.NamedArgs{
			"graph_snapshot_id":     pgtype.UUID{Bytes: materialization.Snapshot.GraphSnapshotID, Valid: true},
			"graph_node_id":         pgtype.UUID{Bytes: nodeID, Valid: true},
			"embedding_record_id":   pgtype.UUID{Bytes: chunk.EmbeddingRecordID, Valid: true},
			"embedding_snapshot_id": pgtype.UUID{Bytes: chunk.EmbeddingSnapshotID, Valid: true},
			"dataset_id":            pgtype.UUID{Bytes: chunk.DatasetID, Valid: true},
			"user_id":               pgtype.UUID{Bytes: chunk.UserID, Valid: true},
			"org_id":                pgtype.UUID{Bytes: chunk.OrgID, Valid: chunk.OrgID != uuid.Nil},
			"chunk_index":           chunk.ChunkIndex,
			"source_text":           chunk.SourceText,
			"assertion_status":      assertionStatusValue(chunk.AssertionStatus),
		}); err != nil {
			return graphPersistenceError("insert graph node chunk", err)
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
				relation_type, description, weight, assertion_status
			) VALUES (
				@graph_snapshot_id, @dataset_id, @user_id, @org_id, @source_node_id, @target_node_id,
				@relation_type, @description, @weight, @assertion_status::assertion_status_enum
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
			"assertion_status":  assertionStatusValue(edge.AssertionStatus),
		}); err != nil {
			return graphPersistenceError("insert graph edge", err)
		}
	}
	communityIDs := make(map[string]uuid.UUID, len(materialization.Communities))
	for _, community := range materialization.Communities {
		var idRaw string
		err := tx.QueryRow(ctx, `INSERT INTO `+r.Name+`.graph_communities (
				graph_snapshot_id, dataset_id, user_id, org_id, community_key, algorithm,
				community_level, title, summary, rank, entity_count, edge_count, assertion_status
			) VALUES (
				@graph_snapshot_id, @dataset_id, @user_id, @org_id, @community_key, @algorithm,
				@community_level, @title, @summary, @rank, @entity_count, @edge_count, @assertion_status::assertion_status_enum
			)
			ON CONFLICT (graph_snapshot_id, community_key) DO UPDATE SET
				algorithm = EXCLUDED.algorithm,
				community_level = EXCLUDED.community_level,
				title = EXCLUDED.title,
				summary = EXCLUDED.summary,
				rank = EXCLUDED.rank,
				entity_count = EXCLUDED.entity_count,
				edge_count = EXCLUDED.edge_count,
				assertion_status = EXCLUDED.assertion_status
			RETURNING graph_community_id::text`, pgx.NamedArgs{
			"graph_snapshot_id": pgtype.UUID{Bytes: materialization.Snapshot.GraphSnapshotID, Valid: true},
			"dataset_id":        pgtype.UUID{Bytes: community.DatasetID, Valid: true},
			"user_id":           pgtype.UUID{Bytes: community.UserID, Valid: true},
			"org_id":            pgtype.UUID{Bytes: community.OrgID, Valid: community.OrgID != uuid.Nil},
			"community_key":     community.CommunityKey,
			"algorithm":         community.Algorithm,
			"community_level":   community.Level,
			"title":             community.Title,
			"summary":           community.Summary,
			"rank":              community.Rank,
			"entity_count":      community.EntityCount,
			"edge_count":        community.EdgeCount,
			"assertion_status":  assertionStatusValue(community.AssertionStatus),
		}).Scan(&idRaw)
		if err != nil {
			return graphPersistenceError("insert graph community", err)
		}
		communityIDs[community.CommunityKey] = uuid.MustParse(idRaw)
	}
	for _, member := range materialization.CommunityMembers {
		communityID := communityIDs[member.CommunityKey]
		nodeID := nodeIDs[member.EntityKey]
		if communityID == uuid.Nil || nodeID == uuid.Nil {
			continue
		}
		if _, err := tx.Exec(ctx, `INSERT INTO `+r.Name+`.graph_community_members (
				graph_community_id, graph_snapshot_id, graph_node_id, dataset_id, user_id, org_id, community_key, entity_key, assertion_status
			) VALUES (
				@graph_community_id, @graph_snapshot_id, @graph_node_id, @dataset_id, @user_id, @org_id, @community_key, @entity_key, @assertion_status::assertion_status_enum
			)
			ON CONFLICT (graph_snapshot_id, graph_node_id) DO UPDATE SET
				graph_community_id = EXCLUDED.graph_community_id,
				community_key = EXCLUDED.community_key,
				entity_key = EXCLUDED.entity_key,
				assertion_status = EXCLUDED.assertion_status`, pgx.NamedArgs{
			"graph_community_id": pgtype.UUID{Bytes: communityID, Valid: true},
			"graph_snapshot_id":  pgtype.UUID{Bytes: materialization.Snapshot.GraphSnapshotID, Valid: true},
			"graph_node_id":      pgtype.UUID{Bytes: nodeID, Valid: true},
			"dataset_id":         pgtype.UUID{Bytes: member.DatasetID, Valid: true},
			"user_id":            pgtype.UUID{Bytes: member.UserID, Valid: true},
			"org_id":             pgtype.UUID{Bytes: member.OrgID, Valid: member.OrgID != uuid.Nil},
			"community_key":      member.CommunityKey,
			"entity_key":         member.EntityKey,
			"assertion_status":   assertionStatusValue(member.AssertionStatus),
		}); err != nil {
			return graphPersistenceError("insert graph community member", err)
		}
	}
	for _, report := range materialization.CommunityReports {
		communityID := communityIDs[report.CommunityKey]
		if communityID == uuid.Nil {
			continue
		}
		if _, err := tx.Exec(ctx, `INSERT INTO `+r.Name+`.graph_community_reports (
				graph_community_id, graph_snapshot_id, embedding_snapshot_id, dataset_id, user_id, org_id,
				community_key, community_level, title, summary, report_text, rank, report_version, embedding_text, embedding, assertion_status
			) VALUES (
				@graph_community_id, @graph_snapshot_id, @embedding_snapshot_id, @dataset_id, @user_id, @org_id,
				@community_key, @community_level, @title, @summary, @report_text, @rank, @report_version, @embedding_text, @embedding, @assertion_status::assertion_status_enum
			)
			ON CONFLICT (graph_community_id, report_version) DO UPDATE SET
				community_level = EXCLUDED.community_level,
				title = EXCLUDED.title,
				summary = EXCLUDED.summary,
				report_text = EXCLUDED.report_text,
				rank = EXCLUDED.rank,
				embedding_text = EXCLUDED.embedding_text,
				embedding = EXCLUDED.embedding,
				assertion_status = EXCLUDED.assertion_status`, pgx.NamedArgs{
			"graph_community_id":    pgtype.UUID{Bytes: communityID, Valid: true},
			"graph_snapshot_id":     pgtype.UUID{Bytes: materialization.Snapshot.GraphSnapshotID, Valid: true},
			"embedding_snapshot_id": pgtype.UUID{Bytes: report.EmbeddingSnapshotID, Valid: true},
			"dataset_id":            pgtype.UUID{Bytes: report.DatasetID, Valid: true},
			"user_id":               pgtype.UUID{Bytes: report.UserID, Valid: true},
			"org_id":                pgtype.UUID{Bytes: report.OrgID, Valid: report.OrgID != uuid.Nil},
			"community_key":         report.CommunityKey,
			"community_level":       report.Level,
			"title":                 report.Title,
			"summary":               report.Summary,
			"report_text":           report.ReportText,
			"rank":                  report.Rank,
			"report_version":        report.ReportVersion,
			"embedding_text":        report.EmbeddingText,
			"embedding":             nullableVectorLiteral(report.Vector),
			"assertion_status":      assertionStatusValue(report.AssertionStatus),
		}); err != nil {
			return graphPersistenceError("insert graph community report", err)
		}
	}
	return nil
}

func nullableVectorLiteral(vector []float32) any {
	log.Trace("nullableVectorLiteral")

	if len(vector) == 0 {
		return nil
	}
	return vectorLiteral(vector)
}

func graphPersistenceError(operation string, err error) error {
	log.Trace("graphPersistenceError")

	if coreDB.IsForeignKeyViolation(err) || coreDB.IsRowLevelSecurityViolation(err) {
		return fmt.Errorf("%w: %s: %w", domain.ErrValidationFailed.Extend("tenant projection is not ready"), operation, err)
	}
	return fmt.Errorf("%w: %s: %w", domain.ErrGraphPersistence, operation, err)
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

func (r *SnapshotRepository) MarkGraphFailed(ctx context.Context, tx pgx.Tx, graphSnapshot *model.GraphSnapshot, reason string) error {
	log.Trace("SnapshotRepository MarkGraphFailed")

	if graphSnapshot == nil {
		return domain.ErrValidationFailed.Extend("graph snapshot is required")
	}
	tag, err := tx.Exec(ctx, `UPDATE `+r.Name+`.graph_snapshots
		SET status = @status::snapshot_status_enum,
			active_for_retrieval = false,
			provenance_hash = @provenance_hash,
			extraction_model = @extraction_model,
			extraction_prompt_version = @extraction_prompt_version,
			extraction_schema_version = @extraction_schema_version,
			chunk_count = @chunk_count,
			chunks_processed = @chunks_processed,
			entity_count = @entity_count,
			edge_count = @edge_count,
			failure_reason = @failure_reason
		WHERE graph_snapshot_id = @graph_snapshot_id`, pgx.NamedArgs{
		"graph_snapshot_id":         pgtype.UUID{Bytes: graphSnapshot.GraphSnapshotID, Valid: true},
		"provenance_hash":           graphSnapshot.ProvenanceHash,
		"extraction_model":          graphSnapshot.ExtractionModel,
		"extraction_prompt_version": graphSnapshot.ExtractionPromptVersion,
		"extraction_schema_version": graphSnapshot.ExtractionSchemaVersion,
		"chunk_count":               graphSnapshot.ChunkCount,
		"chunks_processed":          graphSnapshot.ChunksProcessed,
		"entity_count":              graphSnapshot.EntityCount,
		"edge_count":                graphSnapshot.EdgeCount,
		"failure_reason":            reason,
		"status":                    model.SnapshotStatusFailed.String(),
	})
	if err != nil {
		return fmt.Errorf("mark graph snapshot failed: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: graph_snapshot_id=%s", domain.ErrGraphSnapshotNotFound, graphSnapshot.GraphSnapshotID)
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

func (r *SnapshotRepository) SearchGraph(ctx context.Context, graphSnapshot *model.GraphSnapshot, seed model.GraphSearchSeed, topK int, maxHops int) (*model.GraphSearchResult, error) {
	log.Trace("SnapshotRepository SearchGraph")

	return r.SearchGraphWithPolicy(ctx, graphSnapshot, seed, topK, maxHops, model.RetrievalPolicy{})
}

func (r *SnapshotRepository) SearchGraphWithPolicy(ctx context.Context, graphSnapshot *model.GraphSnapshot, seed model.GraphSearchSeed, topK int, maxHops int, policy model.RetrievalPolicy) (*model.GraphSearchResult, error) {
	log.Trace("SnapshotRepository SearchGraphWithPolicy")

	if graphSnapshot == nil {
		return nil, domain.ErrGraphSnapshotNotFound.Extend("active graph snapshot is required")
	}
	seed, lexicalTerms, lexicalPatterns, lexicalEnabled, err := normalizeGraphSearchSeed(seed)
	if err != nil {
		return nil, err
	}
	if topK <= 0 {
		topK = 5
	}
	if maxHops <= 0 {
		maxHops = 2
	}
	policy = normalizeRetrievalPolicy(policy, topK)
	if !policy.Mode.IsValid() {
		return nil, domain.ErrValidationFailed.Extend("retrieval mode must be ann_iterative or exact_authorized")
	}
	if seed.Mode == model.GraphSearchModeGlobal {
		return r.searchGraphGlobal(ctx, graphSnapshot, seed, lexicalTerms, lexicalPatterns, lexicalEnabled, topK, policy)
	}
	query := r.graphSeedCTE(seed.EmbeddingDimensions, true, policy) + `,
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
				AND ` + retrievalAuthorizationPredicate("edge.graph_edge_id", "edge.assertion_status") + `
		),
		connected AS (
			SELECT node_id, MIN(depth) AS depth, MAX(score) AS score
			FROM walk
			GROUP BY node_id
		)
		SELECT chunk.graph_node_chunk_id::text, chunk.graph_node_id::text,
			chunk.embedding_record_id::text, chunk.embedding_snapshot_id::text,
			chunk.dataset_id::text, chunk.chunk_index, chunk.source_text,
			connected.score::double precision, chunk.org_id::text, chunk.assertion_status::text
		FROM connected
		JOIN ` + r.Name + `.graph_node_chunks chunk ON chunk.graph_node_id = connected.node_id
			AND ` + retrievalAuthorizationAnyPredicate([]string{"chunk.graph_node_chunk_id", "chunk.embedding_record_id", "chunk.graph_node_id"}, "chunk.assertion_status") + `
		ORDER BY connected.depth ASC, connected.score DESC, chunk.chunk_index ASC
		LIMIT @limit`
	rows, err := r.Pool.Query(ctx, query, graphSearchNamedArgs(graphSnapshot, seed, lexicalTerms, lexicalPatterns, lexicalEnabled, topK, maxHops, policy))
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
	entities, err := r.searchGraphMatchedEntities(ctx, graphSnapshot, seed, lexicalTerms, lexicalPatterns, lexicalEnabled, topK, policy)
	if err != nil {
		return nil, err
	}
	paths, err := r.searchGraphPaths(ctx, graphSnapshot, seed, lexicalTerms, lexicalPatterns, lexicalEnabled, topK, maxHops, policy)
	if err != nil {
		return nil, err
	}
	return &model.GraphSearchResult{
		GraphSnapshot:   graphSnapshot,
		Mode:            model.GraphSearchModeLocal,
		Contexts:        contexts,
		MatchedEntities: entities,
		Paths:           paths,
		Disclosure:      retrievalDisclosure(policy, topK, len(contexts)),
	}, nil
}

func (r *SnapshotRepository) searchGraphGlobal(ctx context.Context, graphSnapshot *model.GraphSnapshot, seed model.GraphSearchSeed, lexicalTerms []string, lexicalPatterns []string, lexicalEnabled bool, topK int, policy model.RetrievalPolicy) (*model.GraphSearchResult, error) {
	log.Trace("SnapshotRepository searchGraphGlobal")

	rows, err := r.Pool.Query(ctx, r.graphCommunityReportSeedCTE(seed.EmbeddingDimensions, policy)+`
		SELECT report.graph_community_report_id::text,
			report.graph_community_id::text,
			report.graph_snapshot_id::text,
			report.dataset_id::text,
			report.org_id::text,
			report.community_key,
			report.community_level,
			report.title,
			report.summary,
			report.report_text,
			report.rank,
			community_seed.score::double precision,
			report.assertion_status::text
		FROM community_seed
		JOIN `+r.Name+`.graph_community_reports report
			ON report.graph_community_report_id = community_seed.graph_community_report_id
			AND report.graph_snapshot_id = @graph_snapshot_id
			AND report.embedding_snapshot_id = @embedding_snapshot_id
			AND report.dataset_id = @dataset_id
			AND report.org_id = @org_id
			AND `+retrievalAuthorizationPredicate("report.graph_community_report_id", "report.assertion_status")+`
		ORDER BY community_seed.score DESC, report.rank DESC, report.title
		LIMIT @limit`, graphSearchNamedArgs(graphSnapshot, seed, lexicalTerms, lexicalPatterns, lexicalEnabled, topK, 0, policy))
	if err != nil {
		return nil, fmt.Errorf("search graph community reports: %w", err)
	}
	defer rows.Close()

	reports := []model.GraphCommunityReportMatch{}
	for rows.Next() {
		report, err := scanGraphCommunityReportMatch(rows)
		if err != nil {
			return nil, err
		}
		reports = append(reports, report)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read graph community report rows: %w", err)
	}
	return &model.GraphSearchResult{
		GraphSnapshot:    graphSnapshot,
		Mode:             model.GraphSearchModeGlobal,
		CommunityReports: reports,
		Disclosure:       retrievalDisclosure(policy, topK, len(reports)),
	}, nil
}

func (r *SnapshotRepository) searchGraphMatchedEntities(ctx context.Context, graphSnapshot *model.GraphSnapshot, seed model.GraphSearchSeed, lexicalTerms []string, lexicalPatterns []string, lexicalEnabled bool, topK int, policy model.RetrievalPolicy) ([]model.GraphMatchedEntity, error) {
	rows, err := r.Pool.Query(ctx, r.graphSeedCTE(seed.EmbeddingDimensions, false, policy)+`
		SELECT graph_node_id::text, name, entity_type, description, score::double precision, assertion_status::text
		FROM seed
		ORDER BY score DESC, name
		LIMIT @limit`, graphSearchNamedArgs(graphSnapshot, seed, lexicalTerms, lexicalPatterns, lexicalEnabled, topK, 0, policy))
	if err != nil {
		return nil, fmt.Errorf("search graph matched entities: %w", err)
	}
	defer rows.Close()
	out := []model.GraphMatchedEntity{}
	for rows.Next() {
		var nodeID string
		var assertionStatus string
		entity := model.GraphMatchedEntity{}
		if err := rows.Scan(&nodeID, &entity.Name, &entity.Type, &entity.Description, &entity.Score, &assertionStatus); err != nil {
			return nil, err
		}
		entity.GraphNodeID = uuid.MustParse(nodeID)
		entity.AssertionStatus = model.ParseAssertionStatus(assertionStatus)
		out = append(out, entity)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *SnapshotRepository) searchGraphPaths(ctx context.Context, graphSnapshot *model.GraphSnapshot, seed model.GraphSearchSeed, lexicalTerms []string, lexicalPatterns []string, lexicalEnabled bool, topK int, maxHops int, policy model.RetrievalPolicy) ([]model.GraphPath, error) {
	rows, err := r.Pool.Query(ctx, r.graphSeedCTE(seed.EmbeddingDimensions, true, policy)+`,
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
				AND `+retrievalAuthorizationPredicate("edge.graph_edge_id", "edge.assertion_status")+`
		)
		SELECT array_to_string(path, ',') AS graph_node_ids,
			array_to_string(relation_types, ',') AS relation_types,
			score::double precision
		FROM walk
		WHERE depth > 0
		ORDER BY depth ASC, score DESC
		LIMIT @limit`, graphSearchNamedArgs(graphSnapshot, seed, lexicalTerms, lexicalPatterns, lexicalEnabled, topK, maxHops, policy))
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

func (r *SnapshotRepository) ReadEmbeddingSnapshot(ctx context.Context, embeddingSnapshotID uuid.UUID) (*model.EmbeddingSnapshot, error) {
	log.Trace("SnapshotRepository ReadEmbeddingSnapshot")

	return r.readEmbeddingSnapshot(ctx, r.Pool, embeddingSnapshotID)
}

func (r *SnapshotRepository) readEmbeddingSnapshot(ctx context.Context, queryer rowQuerier, embeddingSnapshotID uuid.UUID) (*model.EmbeddingSnapshot, error) {
	query := `SELECT ` + embeddingSnapshotColumns() + ` FROM ` + r.Name + `.embedding_snapshots
		WHERE embedding_snapshot_id = @embedding_snapshot_id
			AND (@system_context::boolean OR org_id = @org_id)`
	embeddingSnapshot, err := scanEmbeddingSnapshot(queryer.QueryRow(ctx, query, pgx.NamedArgs{
		"embedding_snapshot_id": pgtype.UUID{Bytes: embeddingSnapshotID, Valid: true},
		"org_id":                pgtype.UUID{Bytes: orgIDFromContext(ctx), Valid: orgIDFromContext(ctx) != uuid.Nil},
		"system_context":        ctxutil.IsSystemContext(ctx),
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: embedding_snapshot_id=%s", domain.ErrEmbeddingSnapshotNotFound, embeddingSnapshotID)
		}
		return nil, fmt.Errorf("read embedding snapshot: %w", err)
	}
	return embeddingSnapshot, nil
}

func normalizeGraphSearchSeed(seed model.GraphSearchSeed) (model.GraphSearchSeed, []string, []string, bool, error) {
	if seed.EmbeddingDimensions <= 0 {
		return seed, nil, nil, false, domain.ErrValidationFailed.Extend("embedding dimensions are required")
	}
	if len(seed.QueryVector) != seed.EmbeddingDimensions {
		return seed, nil, nil, false, domain.ErrValidationFailed.Extend("query vector dimensions do not match graph embedding snapshot")
	}
	seed.QueryText = strings.TrimSpace(seed.QueryText)
	seed.Mode = model.ParseGraphSearchMode(seed.Mode.String())
	if !seed.Mode.IsValid() {
		return seed, nil, nil, false, domain.ErrValidationFailed.Extend("graph search mode must be local or global")
	}
	lexicalTerms, lexicalPatterns := graphLexicalTermsAndPatterns(seed.QueryText)
	return seed, lexicalTerms, lexicalPatterns, len(lexicalTerms) > 0 && len(lexicalPatterns) > 0, nil
}

func graphLexicalTermsAndPatterns(queryText string) ([]string, []string) {
	tokens := strings.FieldsFunc(strings.ToLower(strings.TrimSpace(queryText)), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	normalizedPhrase := strings.Join(tokens, " ")
	if normalizedPhrase == "" {
		return nil, nil
	}
	terms := []string{normalizedPhrase}
	seen := map[string]struct{}{normalizedPhrase: {}}
	for _, token := range tokens {
		token = strings.TrimSpace(token)
		if len(token) < graphMinimumLexicalToken {
			continue
		}
		if _, stopword := graphLexicalStopwords[token]; stopword {
			continue
		}
		if _, ok := seen[token]; ok {
			continue
		}
		seen[token] = struct{}{}
		terms = append(terms, token)
	}
	patterns := make([]string, 0, len(terms))
	for _, term := range terms {
		patterns = append(patterns, "%"+term+"%")
	}
	return terms, patterns
}

func graphSearchNamedArgs(graphSnapshot *model.GraphSnapshot, seed model.GraphSearchSeed, lexicalTerms []string, lexicalPatterns []string, lexicalEnabled bool, topK int, maxHops int, policy model.RetrievalPolicy) pgx.NamedArgs {
	seedLimit := graphSeedLimit(topK)
	return mergeNamedArgs(pgx.NamedArgs{
		"graph_snapshot_id":     pgtype.UUID{Bytes: graphSnapshot.GraphSnapshotID, Valid: true},
		"embedding_snapshot_id": pgtype.UUID{Bytes: graphSnapshot.EmbeddingSnapshotID, Valid: true},
		"dataset_id":            pgtype.UUID{Bytes: graphSnapshot.DatasetID, Valid: true},
		"org_id":                pgtype.UUID{Bytes: graphSnapshot.OrgID, Valid: graphSnapshot.OrgID != uuid.Nil},
		"query_embedding":       vectorLiteral(seed.QueryVector),
		"lexical_enabled":       lexicalEnabled,
		"lexical_terms":         lexicalTerms,
		"lexical_patterns":      lexicalPatterns,
		"seed_limit":            seedLimit,
		"semantic_chunk_limit":  graphSemanticChunkLimit(seedLimit),
		"limit":                 topK,
		"max_hops":              maxHops,
	}, retrievalPolicyNamedArgs(policy, topK))
}

func graphSeedLimit(resultLimit int) int {
	if resultLimit < graphMinimumSeedLimit {
		return graphMinimumSeedLimit
	}
	return resultLimit
}

func graphSemanticChunkLimit(seedLimit int) int {
	if seedLimit <= 0 {
		return 0
	}
	return seedLimit * graphSemanticChunkFanout
}

func (r *SnapshotRepository) graphSeedCTE(dimensions int, recursive bool, policy model.RetrievalPolicy) string {
	with := "WITH"
	if recursive {
		with = "WITH RECURSIVE"
	}
	return fmt.Sprintf(`%s %s,
		%s,
		semantic_chunk_seed AS (
			SELECT gnc.graph_node_id,
				MAX(sc.semantic_score)::double precision AS semantic_score
			FROM semantic_chunks sc
			JOIN %s.graph_node_chunks gnc
				ON gnc.embedding_record_id = sc.embedding_record_id
				AND gnc.graph_snapshot_id = @graph_snapshot_id
				AND gnc.embedding_snapshot_id = @embedding_snapshot_id
				AND gnc.dataset_id = @dataset_id
				AND gnc.org_id = @org_id
				AND `+retrievalAuthorizationAnyPredicate([]string{"gnc.graph_node_chunk_id", "gnc.embedding_record_id", "gnc.graph_node_id"}, "gnc.assertion_status")+`
			GROUP BY gnc.graph_node_id
			ORDER BY semantic_score DESC
			LIMIT @seed_limit
		),
		semantic_seed AS (
			SELECT graph_node_id,
				MAX(semantic_score)::double precision AS semantic_score
			FROM (
				SELECT graph_node_id, semantic_score FROM semantic_nodes
				UNION ALL
				SELECT graph_node_id, semantic_score FROM semantic_chunk_seed
			) semantic_candidates
			GROUP BY graph_node_id
			ORDER BY semantic_score DESC
			LIMIT @seed_limit
		),
		lexical_seed AS (
			SELECT graph_node_id,
				MAX(lexical_score) AS lexical_score
			FROM (
				SELECT graph_node_id,
					CASE
						WHEN lower(name) = ANY(@lexical_terms::text[]) THEN %.2f::double precision
						WHEN lower(entity_type) = ANY(@lexical_terms::text[]) THEN %.2f::double precision
						ELSE %.2f::double precision
					END AS lexical_score
				FROM %s.graph_nodes
				WHERE graph_snapshot_id = @graph_snapshot_id
					AND dataset_id = @dataset_id
					AND org_id = @org_id
					AND `+retrievalAuthorizationPredicate("graph_node_id", "assertion_status")+`
					AND @lexical_enabled::boolean
					AND (
						lower(name) = ANY(@lexical_terms::text[])
						OR lower(entity_type) = ANY(@lexical_terms::text[])
						OR lower(name) LIKE ANY(@lexical_patterns::text[])
						OR lower(entity_type) LIKE ANY(@lexical_patterns::text[])
					)
				UNION ALL
				SELECT graph_node_id,
					CASE
						WHEN lower(alias) = ANY(@lexical_terms::text[]) THEN %.2f::double precision
						WHEN lower(entity_type) = ANY(@lexical_terms::text[]) THEN %.2f::double precision
						ELSE %.2f::double precision
					END AS lexical_score
				FROM %s.graph_node_aliases
				WHERE graph_snapshot_id = @graph_snapshot_id
					AND dataset_id = @dataset_id
					AND org_id = @org_id
					AND `+retrievalAuthorizationAnyPredicate([]string{"graph_node_alias_id", "graph_node_id"}, "assertion_status")+`
					AND @lexical_enabled::boolean
					AND (
						lower(alias) = ANY(@lexical_terms::text[])
						OR lower(entity_type) = ANY(@lexical_terms::text[])
						OR lower(alias) LIKE ANY(@lexical_patterns::text[])
						OR lower(entity_type) LIKE ANY(@lexical_patterns::text[])
					)
			) lexical_candidates
			GROUP BY graph_node_id
			ORDER BY lexical_score DESC, graph_node_id
			LIMIT @seed_limit
		),
		seed_scores AS (
			SELECT graph_node_id, MAX(semantic_score) AS semantic_score, MAX(lexical_score) AS lexical_score
			FROM (
				SELECT graph_node_id, semantic_score, NULL::double precision AS lexical_score FROM semantic_seed
				UNION ALL
				SELECT graph_node_id, NULL::double precision AS semantic_score, lexical_score FROM lexical_seed
			) candidates
			GROUP BY graph_node_id
		),
		seed AS (
			SELECT node.graph_node_id, node.name, node.entity_type, node.description, node.assertion_status,
				LEAST(1.0::double precision,
					GREATEST(COALESCE(seed_scores.semantic_score, 0), COALESCE(seed_scores.lexical_score, 0)) +
					CASE
						WHEN COALESCE(seed_scores.semantic_score, 0) >= %.2f::double precision
							AND seed_scores.lexical_score IS NOT NULL THEN %.2f::double precision
						ELSE 0::double precision
					END
				)::double precision AS score
			FROM seed_scores
			JOIN %s.graph_nodes node
				ON node.graph_node_id = seed_scores.graph_node_id
				AND node.graph_snapshot_id = @graph_snapshot_id
				AND node.dataset_id = @dataset_id
				AND node.org_id = @org_id
				AND `+retrievalAuthorizationPredicate("node.graph_node_id", "node.assertion_status")+`
			ORDER BY score DESC, node.name
			LIMIT @seed_limit
		)`,
		with,
		r.graphSemanticNodesCTE(dimensions, policy),
		r.graphSemanticChunksCTE(dimensions, policy),
		r.Name,
		graphLexicalExactNameScore,
		graphLexicalExactTypeScore,
		graphLexicalPartialScore,
		r.Name,
		graphLexicalExactNameScore,
		graphLexicalExactTypeScore,
		graphLexicalPartialScore,
		r.Name,
		graphHybridSemanticFloor,
		graphHybridMatchBoost,
		r.Name,
	)
}

func (r *SnapshotRepository) graphSemanticNodesCTE(dimensions int, policy model.RetrievalPolicy) string {
	log.Trace("SnapshotRepository graphSemanticNodesCTE")

	if policy.Mode == model.RetrievalModeExactAuthorized {
		return fmt.Sprintf(`semantic_node_authorized AS MATERIALIZED (
			SELECT gne.graph_node_id, gne.embedding
			FROM %s.graph_node_embeddings gne
			WHERE gne.graph_snapshot_id = @graph_snapshot_id
				AND gne.embedding_snapshot_id = @embedding_snapshot_id
				AND gne.dataset_id = @dataset_id
				AND gne.org_id = @org_id
				AND vector_dims(gne.embedding) = %d
				AND %s
		),
		semantic_nodes AS (
			SELECT graph_node_id,
				GREATEST(1 - (embedding::vector(%d) <=> @query_embedding::vector(%d)), 0)::double precision AS semantic_score
			FROM semantic_node_authorized
			ORDER BY embedding::vector(%d) <=> @query_embedding::vector(%d)
			LIMIT @seed_limit
		)`,
			r.Name,
			dimensions,
			retrievalAuthorizationPredicate("gne.graph_node_id", "gne.assertion_status"),
			dimensions,
			dimensions,
			dimensions,
			dimensions,
		)
	}
	return fmt.Sprintf(`semantic_node_candidates AS (
			SELECT gne.graph_node_id, gne.assertion_status,
				GREATEST(1 - (gne.embedding::vector(%d) <=> @query_embedding::vector(%d)), 0)::double precision AS semantic_score
			FROM %s.graph_node_embeddings gne
			WHERE gne.graph_snapshot_id = @graph_snapshot_id
				AND gne.embedding_snapshot_id = @embedding_snapshot_id
				AND gne.dataset_id = @dataset_id
				AND gne.org_id = @org_id
				AND vector_dims(gne.embedding) = %d
			ORDER BY gne.embedding::vector(%d) <=> @query_embedding::vector(%d)
			LIMIT @scan_budget
		),
		semantic_nodes AS (
			SELECT graph_node_id, semantic_score
			FROM semantic_node_candidates
			WHERE `+retrievalAuthorizationPredicate("graph_node_id", "assertion_status")+`
			ORDER BY semantic_score DESC
			LIMIT @seed_limit
		)`,
		dimensions,
		dimensions,
		r.Name,
		dimensions,
		dimensions,
		dimensions,
	)
}

func (r *SnapshotRepository) graphSemanticChunksCTE(dimensions int, policy model.RetrievalPolicy) string {
	log.Trace("SnapshotRepository graphSemanticChunksCTE")

	if policy.Mode == model.RetrievalModeExactAuthorized {
		return fmt.Sprintf(`semantic_chunk_authorized AS MATERIALIZED (
			SELECT er.embedding_record_id, er.embedding
			FROM %s.embedding_records er
			WHERE er.embedding_snapshot_id = @embedding_snapshot_id
				AND er.dataset_id = @dataset_id
				AND er.org_id = @org_id
				AND vector_dims(er.embedding) = %d
				AND %s
		),
		semantic_chunks AS (
			SELECT embedding_record_id,
				GREATEST(1 - (embedding::vector(%d) <=> @query_embedding::vector(%d)), 0)::double precision AS semantic_score
			FROM semantic_chunk_authorized
			ORDER BY embedding::vector(%d) <=> @query_embedding::vector(%d)
			LIMIT @semantic_chunk_limit
		)`,
			r.Name,
			dimensions,
			retrievalAuthorizationPredicate("er.embedding_record_id", "er.assertion_status"),
			dimensions,
			dimensions,
			dimensions,
			dimensions,
		)
	}
	return fmt.Sprintf(`semantic_chunk_candidates AS (
			SELECT er.embedding_record_id, er.assertion_status,
				GREATEST(1 - (er.embedding::vector(%d) <=> @query_embedding::vector(%d)), 0)::double precision AS semantic_score
			FROM %s.embedding_records er
			WHERE er.embedding_snapshot_id = @embedding_snapshot_id
				AND er.dataset_id = @dataset_id
				AND er.org_id = @org_id
				AND vector_dims(er.embedding) = %d
			ORDER BY er.embedding::vector(%d) <=> @query_embedding::vector(%d)
			LIMIT @semantic_chunk_limit
		),
		semantic_chunks AS (
			SELECT embedding_record_id, semantic_score
			FROM semantic_chunk_candidates
			WHERE `+retrievalAuthorizationPredicate("embedding_record_id", "assertion_status")+`
			ORDER BY semantic_score DESC
			LIMIT @semantic_chunk_limit
		)`,
		dimensions,
		dimensions,
		r.Name,
		dimensions,
		dimensions,
		dimensions,
	)
}

func (r *SnapshotRepository) graphCommunityReportSeedCTE(dimensions int, policy model.RetrievalPolicy) string {
	log.Trace("SnapshotRepository graphCommunityReportSeedCTE")

	return fmt.Sprintf(`WITH %s,
		lexical_reports AS (
			SELECT graph_community_report_id,
				MAX(lexical_score) AS lexical_score
			FROM (
				SELECT graph_community_report_id,
					CASE
						WHEN lower(title) = ANY(@lexical_terms::text[]) THEN %.2f::double precision
						ELSE %.2f::double precision
					END AS lexical_score
				FROM %s.graph_community_reports
				WHERE graph_snapshot_id = @graph_snapshot_id
					AND embedding_snapshot_id = @embedding_snapshot_id
					AND dataset_id = @dataset_id
					AND org_id = @org_id
					AND `+retrievalAuthorizationPredicate("graph_community_report_id", "assertion_status")+`
					AND @lexical_enabled::boolean
					AND (
						lower(title) = ANY(@lexical_terms::text[])
						OR lower(title) LIKE ANY(@lexical_patterns::text[])
						OR lower(summary) LIKE ANY(@lexical_patterns::text[])
						OR lower(report_text) LIKE ANY(@lexical_patterns::text[])
					)
			) lexical_candidates
			GROUP BY graph_community_report_id
			ORDER BY lexical_score DESC, graph_community_report_id
			LIMIT @seed_limit
		),
		community_report_scores AS (
			SELECT graph_community_report_id, MAX(semantic_score) AS semantic_score, MAX(lexical_score) AS lexical_score
			FROM (
				SELECT graph_community_report_id, semantic_score, NULL::double precision AS lexical_score FROM semantic_reports
				UNION ALL
				SELECT graph_community_report_id, NULL::double precision AS semantic_score, lexical_score FROM lexical_reports
			) candidates
			GROUP BY graph_community_report_id
		),
		community_seed AS (
			SELECT graph_community_report_id,
				LEAST(1.0::double precision,
					GREATEST(COALESCE(semantic_score, 0), COALESCE(lexical_score, 0)) +
					CASE
						WHEN COALESCE(semantic_score, 0) >= %.2f::double precision
							AND lexical_score IS NOT NULL THEN %.2f::double precision
						ELSE 0::double precision
					END
				)::double precision AS score
			FROM community_report_scores
			ORDER BY score DESC, graph_community_report_id
			LIMIT @seed_limit
		)`,
		r.graphSemanticReportsCTE(dimensions, policy),
		graphLexicalExactNameScore,
		graphLexicalPartialScore,
		r.Name,
		graphHybridSemanticFloor,
		graphHybridMatchBoost,
	)
}

func (r *SnapshotRepository) graphSemanticReportsCTE(dimensions int, policy model.RetrievalPolicy) string {
	log.Trace("SnapshotRepository graphSemanticReportsCTE")

	if policy.Mode == model.RetrievalModeExactAuthorized {
		return fmt.Sprintf(`semantic_report_authorized AS MATERIALIZED (
			SELECT report.graph_community_report_id, report.embedding
			FROM %s.graph_community_reports report
			WHERE report.graph_snapshot_id = @graph_snapshot_id
				AND report.embedding_snapshot_id = @embedding_snapshot_id
				AND report.dataset_id = @dataset_id
				AND report.org_id = @org_id
				AND vector_dims(report.embedding) = %d
				AND %s
		),
		semantic_reports AS (
			SELECT graph_community_report_id,
				GREATEST(1 - (embedding::vector(%d) <=> @query_embedding::vector(%d)), 0)::double precision AS semantic_score
			FROM semantic_report_authorized
			ORDER BY embedding::vector(%d) <=> @query_embedding::vector(%d)
			LIMIT @seed_limit
		)`,
			r.Name,
			dimensions,
			retrievalAuthorizationPredicate("report.graph_community_report_id", "report.assertion_status"),
			dimensions,
			dimensions,
			dimensions,
			dimensions,
		)
	}
	return fmt.Sprintf(`semantic_report_candidates AS (
			SELECT report.graph_community_report_id, report.assertion_status,
				GREATEST(1 - (report.embedding::vector(%d) <=> @query_embedding::vector(%d)), 0)::double precision AS semantic_score
			FROM %s.graph_community_reports report
			WHERE report.graph_snapshot_id = @graph_snapshot_id
				AND report.embedding_snapshot_id = @embedding_snapshot_id
				AND report.dataset_id = @dataset_id
				AND report.org_id = @org_id
				AND vector_dims(report.embedding) = %d
			ORDER BY report.embedding::vector(%d) <=> @query_embedding::vector(%d)
			LIMIT @scan_budget
		),
		semantic_reports AS (
			SELECT graph_community_report_id, semantic_score
			FROM semantic_report_candidates
			WHERE `+retrievalAuthorizationPredicate("graph_community_report_id", "assertion_status")+`
			ORDER BY semantic_score DESC
			LIMIT @seed_limit
		)`,
		dimensions,
		dimensions,
		r.Name,
		dimensions,
		dimensions,
		dimensions,
	)
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
	var assertionStatus string
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
		&assertionStatus,
	); err != nil {
		return model.GraphRetrievedContext{}, err
	}
	context.GraphNodeChunkID = uuid.MustParse(nodeChunkID)
	context.GraphNodeID = uuid.MustParse(nodeID)
	context.EmbeddingRecordID = uuid.MustParse(embeddingRecordID)
	context.EmbeddingSnapshotID = uuid.MustParse(embeddingSnapshotID)
	context.DatasetID = uuid.MustParse(datasetID)
	context.OrgID = uuid.MustParse(orgID)
	context.AssertionStatus = model.ParseAssertionStatus(assertionStatus)
	return context, nil
}

func scanGraphCommunityReportMatch(row pgx.Row) (model.GraphCommunityReportMatch, error) {
	log.Trace("scanGraphCommunityReportMatch")

	var reportID, communityID, snapshotID, datasetID, orgID string
	var assertionStatus string
	match := model.GraphCommunityReportMatch{}
	if err := row.Scan(
		&reportID,
		&communityID,
		&snapshotID,
		&datasetID,
		&orgID,
		&match.CommunityKey,
		&match.Level,
		&match.Title,
		&match.Summary,
		&match.ReportText,
		&match.Rank,
		&match.Score,
		&assertionStatus,
	); err != nil {
		return model.GraphCommunityReportMatch{}, err
	}
	match.GraphCommunityReportID = uuid.MustParse(reportID)
	match.GraphCommunityID = uuid.MustParse(communityID)
	match.GraphSnapshotID = uuid.MustParse(snapshotID)
	match.DatasetID = uuid.MustParse(datasetID)
	match.OrgID = uuid.MustParse(orgID)
	match.AssertionStatus = model.ParseAssertionStatus(assertionStatus)
	return match, nil
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
